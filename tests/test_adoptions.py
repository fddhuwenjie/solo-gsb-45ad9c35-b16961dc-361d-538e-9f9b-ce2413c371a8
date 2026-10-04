"""复核 changed 后采纳新判定为批次判定现行结果的测试（单事务 + 幂等）。

覆盖：changed 报告采纳成功（不可变替代结果写入 judgments、原结果与现行
结果同时可查、/judgments 历史保留、批次判定列表/详情给出现行结果）、同键
同目标重试 200、同键改目标 409、他键争抢同一原判定 409、unchanged/
source_unavailable/source_mismatch 报告不可采纳、采纳时来源已归档或被篡改
→ 409 且不留部分采纳、报告新候选/原候选与重跑或原判定记录不一致 → 409、
报告/判定 404、跨设备目标 409、隔离期间 409、采纳记录列表/详情查询。
"""
import json
import os
import sqlite3
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

DEVICE = "dev-01"
THRESHOLDS = {"tolerance": 0.5, "max_drift": 5e-4, "max_missed": 3, "max_false": 3}
# A：周期 10、双闪；C：周期 6、单闪（与 test_review_reports 一致的合成约定）
LIBRARY = {
    "aids": [
        {"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]},
        {"aid_id": "C", "period": 6.0, "flashes": [0.0]},
    ],
    "mode": "merge",
}
PSI = 1_700_000_000.0


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        c.db_path = str(tmp_path / "test.db")  # 便于直接篡改库
        yield c


def make_flashes(seq_per_batch, *, aid="A", psi=PSI, noise=0.01, seed=7):
    """按 aid 灯质生成闪光观测（无漂移、无漏误记），再切成若干连续批次。"""
    import random

    rng = random.Random(seed)
    period, flashes = {"A": (10.0, [0.0, 1.5]), "C": (6.0, [0.0])}[aid]
    timestamps = [
        psi + n * period + f + rng.uniform(-noise, noise)
        for n in range(sum(seq_per_batch))
        for f in flashes
    ]
    batches, idx, lid = [], 0, 0
    for n_cycles in seq_per_batch:
        obs = []
        for _ in range(n_cycles * len(flashes)):
            obs.append({"local_id": f"o{lid:04d}", "captured_at": timestamps[idx]})
            idx += 1
            lid += 1
        batches.append(obs)
    return batches


def upload_chain(client, device, batches):
    out, prev = [], ""
    for i, observations in enumerate(batches, start=1):
        r = client.post(
            f"/field-devices/{device}/batches",
            json={
                "batch_id": f"b-{i}",
                "sequence": i,
                "previous_digest": prev,
                "observations": observations,
                **THRESHOLDS,
            },
        )
        assert r.status_code == 201, r.text
        d = r.json()
        out.append((i, f"b-{i}", d["digest"], len(observations)))
        prev = d["digest"]
    return out


def prepare(client, device=DEVICE, seq_per_batch=(4, 3, 3), **kw):
    client.post("/library/import", json=LIBRARY)
    return upload_chain(client, device, make_flashes(seq_per_batch, **kw))


def create_judgment(client, device, key, frm, to):
    return client.post(
        f"/field-devices/{device}/batch-judgments",
        json={"idempotency_key": key, "from_sequence": frm, "to_sequence": to},
    )


def review(client, device, judgment_id, key):
    return client.post(
        f"/field-devices/{device}/batch-judgments/{judgment_id}/review-reports",
        json={"idempotency_key": key},
    )


def adopt(client, device, judgment_id, report_id, key):
    return client.post(
        f"/field-devices/{device}/batch-judgments/{judgment_id}/adoptions",
        json={"review_report_id": report_id, "idempotency_key": key},
    )


def execute_db(client, sql, params=()):
    conn = sqlite3.connect(client.db_path)
    try:
        conn.execute("PRAGMA busy_timeout = 5000")
        cur = conn.execute(sql, params)
        conn.commit()
        return cur
    finally:
        conn.close()


def tamper_saved_judgment(client, jid):
    """篡改底层判定响应（最佳航标 A → C、matched=1），使复核结论为 changed。"""
    conn = sqlite3.connect(client.db_path)
    try:
        row = conn.execute("SELECT response FROM judgments WHERE id = ?", (jid,)).fetchone()
        payload = json.loads(row[0])
        payload["best"]["aid_id"] = "C"
        payload["best"]["matched"] = 1
        conn.execute(
            "UPDATE judgments SET response = ? WHERE id = ?",
            (json.dumps(payload), jid),
        )
        conn.commit()
    finally:
        conn.close()


def judgment_count(client):
    return client.get("/judgments?limit=500").json()["count"]


def adoption_count(client):
    conn = sqlite3.connect(client.db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM batch_judgment_adoptions").fetchone()[0]
    finally:
        conn.close()


def changed_report(client, device=DEVICE, key_bj="k1", key_rv="rv-1"):
    """标准前置：判定 → 篡改原判定 → 复核得到 changed 报告。"""
    j = create_judgment(client, device, key_bj, 1, 3).json()
    tamper_saved_judgment(client, j["source_judgment_id"])
    r = review(client, device, j["batch_judgment_id"], key_rv)
    assert r.status_code == 201, r.text
    report = r.json()
    assert report["status"] == "changed"
    return j, report


def register_ack_archive(client, device, digests, through):
    client.post(f"/field-devices/{device}/consumers", json={"consumer_id": "arch"})
    for seq, digest in enumerate(digests, start=1):
        r = client.post(
            f"/field-devices/{device}/acknowledgments",
            json={"consumer_id": "arch", "sequence": seq, "batch_digest": digest},
        )
        assert r.status_code in (200, 201), r.text
    r = client.post(f"/field-devices/{device}/archive", json={"through_sequence": through})
    assert r.status_code == 201, r.text


# ---------------- 采纳成功 ----------------


def test_adopt_changed_report_succeeds(client):
    prepare(client)
    j, report = changed_report(client)
    before = judgment_count(client)

    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1")
    assert r.status_code == 201, r.text
    d = r.json()

    assert d["adoption_id"] == 1
    assert d["idempotency_key"] == "adopt-1"
    assert d["device_id"] == DEVICE
    assert d["batch_judgment_id"] == j["batch_judgment_id"]
    assert d["review_report_id"] == report["report_id"]
    assert d["library_version"] == j["library_version"] == 1
    assert d["original_judgment_id"] == j["source_judgment_id"]
    assert isinstance(d["current_judgment_id"], int)
    assert d["current_judgment_id"] != j["source_judgment_id"]
    assert d["adopted_at"]

    # 原/新候选与差异：原候选是被篡改的 C，新候选是从链数据重跑得到的 A
    assert d["original"]["judgment_id"] == j["source_judgment_id"]
    assert d["original"]["best_aid_id"] == "C"
    assert d["current"]["judgment_id"] == d["current_judgment_id"]
    assert d["current"]["best_aid_id"] == "A"
    assert d["current"]["best"]["matched"] == j["judgment"]["best"]["matched"]
    fields = {x["field"] for x in d["differences"]}
    assert {"best.aid_id", "best.matched"} <= fields

    # 现行结果是与 /judge 同构的完整判定响应（无 judgment_id 之外的字段缺失）
    cj = d["current_judgment"]
    assert cj["judgment_id"] == d["current_judgment_id"]
    assert cj["status"] == "matched"
    assert cj["library_source"] == "stored"
    assert cj["library_version"] == 1
    assert cj["best"]["aid_id"] == "A"
    assert cj["candidates"]
    assert cj["parameters"] == THRESHOLDS
    assert cj["observation_count"] == j["observation_count"]

    # 来源报告完整快照随采纳不可变保存
    assert d["source_report"]["report_id"] == report["report_id"]
    assert d["source_report"]["status"] == "changed"
    assert d["source_report"]["current"]["best_aid_id"] == "A"

    # 恰好新增一条 judgments 记录；采纳记录一条
    assert judgment_count(client) == before + 1
    assert adoption_count(client) == 1


def test_adoption_keeps_original_judgment_and_judgments_history(client):
    prepare(client)
    j, report = changed_report(client)
    d = adopt(
        client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1"
    ).json()

    # 原判定行仍可按原 id 查询，内容不被改写（仍是被篡改形态）
    orig = client.get(f"/judgments/{j['source_judgment_id']}")
    assert orig.status_code == 200
    assert orig.json()["response"]["best"]["aid_id"] == "C"

    # 新判定行可独立查询
    new = client.get(f"/judgments/{d['current_judgment_id']}")
    assert new.status_code == 200
    assert new.json()["response"]["best"]["aid_id"] == "A"
    assert new.json()["response"]["library_version"] == j["library_version"]

    # /judgments 历史按 id 倒序包含两条
    listing = client.get("/judgments?limit=50").json()["judgments"]
    ids = {row["id"] for row in listing}
    assert {j["source_judgment_id"], d["current_judgment_id"]} <= ids


def test_batch_judgment_detail_shows_original_and_current(client):
    prepare(client)
    j, report = changed_report(client)
    adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1")

    detail = client.get(f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}")
    assert detail.status_code == 200
    body = detail.json()

    # 原结果仍在 judgment 字段（不可变）
    assert body["source_judgment_id"] == j["source_judgment_id"]
    assert body["judgment"]["judgment_id"] == j["source_judgment_id"]
    assert body["judgment"]["best"]["aid_id"] == "C"
    # 现行结果同时给出
    assert body["current_judgment"]["best"]["aid_id"] == "A"
    adoption = body["adoption"]
    assert adoption["adoption_id"] == 1
    assert adoption["review_report_id"] == report["report_id"]
    assert adoption["original_judgment_id"] == j["source_judgment_id"]
    assert adoption["current_judgment_id"] == body["current_judgment"]["judgment_id"]
    assert adoption["current_judgment"]["best"]["aid_id"] == "A"


def test_batch_judgment_detail_without_adoption_has_null_current(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    body = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}"
    ).json()
    assert body["current_judgment"] is None
    assert body["adoption"] is None
    assert body["judgment"]["best"]["aid_id"] == "A"


def test_batch_judgment_list_includes_adoption_summary(client):
    prepare(client)
    j, report = changed_report(client)

    before = client.get(f"/field-devices/{DEVICE}/batch-judgments").json()
    (item,) = before["batch_judgments"]
    assert item["adoption_id"] is None
    assert item["current_judgment_id"] is None
    assert item["adopted_at"] is None

    adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1")
    after = client.get(f"/field-devices/{DEVICE}/batch-judgments").json()
    (item,) = after["batch_judgments"]
    assert item["adoption_id"] == 1
    assert item["current_judgment_id"] != j["source_judgment_id"]
    assert item["adopted_at"]


def test_adoption_uses_original_library_version_not_active_one(client):
    prepare(client)
    j, report = changed_report(client)
    # 采纳前激活一个与 A 不匹配的新版本：现行结果仍须按原 library_version=1 重跑
    client.post(
        "/library/import",
        json={"aids": [{"aid_id": "X", "period": 3.0, "flashes": [0.0]}], "mode": "replace"},
    )
    assert client.get("/library").json()["library_version"] == 2

    d = adopt(
        client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1"
    ).json()
    assert d["library_version"] == 1
    assert d["current"]["best_aid_id"] == "A"
    assert d["current_judgment"]["library_version"] == 1


# ---------------- 幂等与争抢 ----------------


def test_same_key_same_target_retry_returns_original_adoption(client):
    prepare(client)
    j, report = changed_report(client)
    r1 = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1")
    assert r1.status_code == 201
    first = r1.json()

    r2 = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1")
    assert r2.status_code == 200
    again = r2.json()
    assert again == first
    assert judgment_count(client) == first["current_judgment_id"]  # 未新增判定
    assert adoption_count(client) == 1


def test_same_key_different_report_target_conflicts(client):
    prepare(client)
    j, report1 = changed_report(client, key_rv="rv-1")
    # 同一被篡改判定再出一份 changed 报告
    report2 = review(client, DEVICE, j["batch_judgment_id"], "rv-2").json()
    assert report2["report_id"] != report1["report_id"]

    assert (
        adopt(client, DEVICE, j["batch_judgment_id"], report1["report_id"], "adopt-1").status_code
        == 201
    )
    # 同键改目标（另一报告）→ 409，键不被第二次请求复用
    r = adopt(client, DEVICE, j["batch_judgment_id"], report2["report_id"], "adopt-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["error"] == "adoption_conflict"
    assert d["reason"] == "idempotency_key_in_use"
    assert d["occupied_by_adoption_id"] == 1
    assert d["occupied_review_report_id"] == report1["report_id"]
    assert adoption_count(client) == 1


def test_same_key_different_batch_judgment_conflicts(client):
    prepare(client)
    j1 = create_judgment(client, DEVICE, "k1", 1, 2).json()
    j2 = create_judgment(client, DEVICE, "k2", 2, 3).json()
    tamper_saved_judgment(client, j1["source_judgment_id"])
    tamper_saved_judgment(client, j2["source_judgment_id"])
    rv1 = review(client, DEVICE, j1["batch_judgment_id"], "rv-1").json()
    rv2 = review(client, DEVICE, j2["batch_judgment_id"], "rv-2").json()

    assert adopt(client, DEVICE, j1["batch_judgment_id"], rv1["report_id"], "shared").status_code == 201
    r = adopt(client, DEVICE, j2["batch_judgment_id"], rv2["report_id"], "shared")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "idempotency_key_in_use"
    assert r.json()["detail"]["occupied_batch_judgment_id"] == j1["batch_judgment_id"]


def test_other_key_racing_same_original_judgment_conflicts(client):
    prepare(client)
    j, report1 = changed_report(client, key_rv="rv-1")
    report2 = review(client, DEVICE, j["batch_judgment_id"], "rv-2").json()

    r1 = adopt(client, DEVICE, j["batch_judgment_id"], report1["report_id"], "adopt-1")
    assert r1.status_code == 201
    r2 = adopt(client, DEVICE, j["batch_judgment_id"], report2["report_id"], "adopt-2")
    assert r2.status_code == 409
    d = r2.json()["detail"]
    assert d["reason"] == "already_adopted"
    assert d["occupied_by_adoption_id"] == 1
    assert d["occupied_by_review_report_id"] == report1["report_id"]
    # 第二份报告没有产生任何替代判定
    assert adoption_count(client) == 1


def test_concurrent_same_key_single_adoption(client):
    prepare(client)
    j, report = changed_report(client)
    barrier = threading.Barrier(4)
    results, lock = [], threading.Lock()

    def worker():
        barrier.wait()
        r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "race")
        with lock:
            results.append((r.status_code, r.json().get("adoption_id")))

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    statuses = {s for s, _ in results}
    assert statuses <= {200, 201}
    assert 201 in statuses
    assert {aid for _, aid in results} == {1}
    assert adoption_count(client) == 1
    assert judgment_count(client) == report["source_judgment_id"] + 1


# ---------------- 非 changed / 来源问题 / 结果不一致：409 且无部分采纳 ----------------


def test_adopt_unchanged_report_conflicts(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    rv = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert rv["status"] == "unchanged"

    before = judgment_count(client)
    r = adopt(client, DEVICE, j["batch_judgment_id"], rv["report_id"], "adopt-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "review_report_not_changed"
    assert d["report_status"] == "unchanged"
    assert judgment_count(client) == before
    assert adoption_count(client) == 0


def test_adopt_source_unavailable_report_conflicts(client):
    meta = prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    register_ack_archive(client, DEVICE, [m[2] for m in meta], through=2)
    rv = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert rv["status"] == "source_unavailable"

    before = judgment_count(client)
    r = adopt(client, DEVICE, j["batch_judgment_id"], rv["report_id"], "adopt-1")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "review_report_not_changed"
    assert judgment_count(client) == before
    assert adoption_count(client) == 0


def test_adopt_source_mismatch_report_conflicts(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    execute_db(
        client,
        "UPDATE device_batches SET digest = ? WHERE device_id = ? AND sequence = 2",
        ("0" * 64, DEVICE),
    )
    rv = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert rv["status"] == "source_mismatch"

    before = judgment_count(client)
    r = adopt(client, DEVICE, j["batch_judgment_id"], rv["report_id"], "adopt-1")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "review_report_not_changed"
    assert judgment_count(client) == before
    assert adoption_count(client) == 0


def test_adopt_conflicts_when_source_archived_after_report(client):
    """changed 报告之后批次被归档 → 采纳时重新核对为 source_unavailable，409 无残留。"""
    meta = prepare(client)
    j, report = changed_report(client)
    register_ack_archive(client, DEVICE, [m[2] for m in meta], through=3)

    before = judgment_count(client)
    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "source_unavailable"
    codes = {i["issue_code"] for i in d["issues"]}
    assert "batch_archived" in codes
    assert judgment_count(client) == before  # 没有写入替代判定
    assert adoption_count(client) == 0


def test_adopt_conflicts_when_source_tampered_after_report(client):
    """changed 报告之后批次摘要被篡改 → 采纳时重新核对为 source_mismatch，409 无残留。"""
    prepare(client)
    j, report = changed_report(client)
    execute_db(
        client,
        "UPDATE device_batches SET digest = ? WHERE device_id = ? AND sequence = 2",
        ("9" * 64, DEVICE),
    )

    before = judgment_count(client)
    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "source_mismatch"
    assert any(i["issue_code"] == "digest_mismatch" for i in d["issues"])
    assert judgment_count(client) == before
    assert adoption_count(client) == 0


def test_adopt_conflicts_when_thresholds_tampered_after_report(client):
    prepare(client)
    j, report = changed_report(client)
    new_thresholds = json.dumps({**THRESHOLDS, "tolerance": THRESHOLDS["tolerance"] + 0.1})
    execute_db(
        client,
        "UPDATE device_batches SET thresholds = ? WHERE device_id = ? AND sequence = 2",
        (new_thresholds, DEVICE),
    )

    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "source_mismatch"
    assert adoption_count(client) == 0


def test_adopt_conflicts_when_report_current_evidence_tampered(client):
    """报告保存的新候选被改写 → 重跑结果与报告不一致，409 review_result_mismatch。"""
    prepare(client)
    j, report = changed_report(client)
    conn = sqlite3.connect(client.db_path)
    try:
        row = conn.execute(
            "SELECT report FROM batch_judgment_review_reports WHERE report_id = ?",
            (report["report_id"],),
        ).fetchone()
        body = json.loads(row[0])
        body["current"]["best_aid_id"] = "C"
        conn.execute(
            "UPDATE batch_judgment_review_reports SET report = ? WHERE report_id = ?",
            (json.dumps(body), report["report_id"]),
        )
        conn.commit()
    finally:
        conn.close()

    before = judgment_count(client)
    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "review_result_mismatch"
    assert d["mismatched_side"] == "current"
    assert d["report_evidence"]["best_aid_id"] == "C"
    assert d["current_evidence"]["best_aid_id"] == "A"
    assert judgment_count(client) == before
    assert adoption_count(client) == 0


def test_adopt_conflicts_when_report_saved_evidence_stale(client):
    """报告之后原判定记录又被改动 → 报告原候选与当前原判定不一致，409。"""
    prepare(client)
    j, report = changed_report(client)
    conn = sqlite3.connect(client.db_path)
    try:
        row = conn.execute(
            "SELECT response FROM judgments WHERE id = ?",
            (j["source_judgment_id"],),
        ).fetchone()
        payload = json.loads(row[0])
        payload["best"]["aid_id"] = "X"  # 报告记录的是 C，现又改成 X
        conn.execute(
            "UPDATE judgments SET response = ? WHERE id = ?",
            (json.dumps(payload), j["source_judgment_id"]),
        )
        conn.commit()
    finally:
        conn.close()

    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "review_result_mismatch"
    assert d["mismatched_side"] == "saved"
    assert adoption_count(client) == 0


def test_adopt_conflicts_when_report_differences_tampered(client):
    prepare(client)
    j, report = changed_report(client)
    conn = sqlite3.connect(client.db_path)
    try:
        row = conn.execute(
            "SELECT report FROM batch_judgment_review_reports WHERE report_id = ?",
            (report["report_id"],),
        ).fetchone()
        body = json.loads(row[0])
        body["differences"].append({"field": "best.drift", "saved": 0.0, "current": 9.0})
        conn.execute(
            "UPDATE batch_judgment_review_reports SET report = ? WHERE report_id = ?",
            (json.dumps(body), report["report_id"],),
        )
        conn.commit()
    finally:
        conn.close()

    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "review_result_mismatch"
    assert d["mismatched_side"] == "differences"
    assert adoption_count(client) == 0


# ---------------- 404 / 422 / 跨目标 ----------------


def test_adopt_404_when_judgment_missing_or_other_device(client):
    prepare(client)
    j, report = changed_report(client)
    r = adopt(client, DEVICE, 999, report["report_id"], "adopt-1")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "batch_judgment_not_found"

    r = adopt(client, "dev-02", j["batch_judgment_id"], report["report_id"], "adopt-1")
    assert r.status_code == 404


def test_adopt_404_when_report_missing(client):
    prepare(client)
    j, _report = changed_report(client)
    r = adopt(client, DEVICE, j["batch_judgment_id"], 999, "adopt-1")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "review_report_not_found"
    assert adoption_count(client) == 0


def test_adopt_conflicts_when_report_belongs_to_other_judgment(client):
    prepare(client)
    j1 = create_judgment(client, DEVICE, "k1", 1, 2).json()
    j2 = create_judgment(client, DEVICE, "k2", 2, 3).json()
    tamper_saved_judgment(client, j1["source_judgment_id"])
    rv1 = review(client, DEVICE, j1["batch_judgment_id"], "rv-1").json()

    # 用判定 1 的报告去采纳判定 2 → 409 target_mismatch（不占键）
    r = adopt(client, DEVICE, j2["batch_judgment_id"], rv1["report_id"], "adopt-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "review_report_target_mismatch"
    assert d["report_batch_judgment_id"] == j1["batch_judgment_id"]

    # 该键未被消耗：随后可正常采纳判定 1
    ok = adopt(client, DEVICE, j1["batch_judgment_id"], rv1["report_id"], "adopt-1")
    assert ok.status_code == 201, ok.text


def test_adopt_conflicts_when_report_belongs_to_other_device(client):
    prepare(client)
    prepare(client, device="dev-02")
    j1 = create_judgment(client, DEVICE, "k1", 1, 3).json()
    j2 = create_judgment(client, "dev-02", "k2", 1, 3).json()
    tamper_saved_judgment(client, j1["source_judgment_id"])
    rv1 = review(client, DEVICE, j1["batch_judgment_id"], "rv-1").json()

    r = adopt(client, "dev-02", j2["batch_judgment_id"], rv1["report_id"], "adopt-1")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "review_report_target_mismatch"


def test_adopt_blank_key_and_bad_report_id_are_422(client):
    prepare(client)
    j, report = changed_report(client)
    r = client.post(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}/adoptions",
        json={"review_report_id": report["report_id"], "idempotency_key": "   "},
    )
    assert r.status_code == 422
    r = client.post(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}/adoptions",
        json={"review_report_id": 0, "idempotency_key": "adopt-1"},
    )
    assert r.status_code == 422


# ---------------- 隔离期间拒绝 ----------------


def test_adopt_blocked_during_isolation(client):
    prepare(client)
    j, report = changed_report(client)
    cursor = client.get(f"/field-devices/{DEVICE}/cursor").json()
    iso = client.post(
        f"/field-devices/{DEVICE}/isolation",
        json={
            "head_sequence": cursor["last_sequence"],
            "head_digest": cursor["head_digest"],
            "reason": "verify-chain 异常",
            "idempotency_key": "iso-1",
        },
    )
    assert iso.status_code == 201

    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1")
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "device_isolated"
    assert adoption_count(client) == 0
    # 采纳记录查询仍可用
    page = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}/adoptions"
    )
    assert page.status_code == 200
    assert page.json()["count"] == 0


# ---------------- 采纳记录查询 ----------------


def test_adoption_list_and_detail_queries(client):
    prepare(client)
    j, report = changed_report(client)
    d = adopt(
        client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1"
    ).json()

    page = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}/adoptions"
    )
    assert page.status_code == 200
    body = page.json()
    assert body["count"] == 1
    (item,) = body["adoptions"]
    assert item["adoption_id"] == 1
    assert item["review_report_id"] == report["report_id"]
    assert item["current_judgment_id"] == d["current_judgment_id"]
    assert item["original_judgment_id"] == j["source_judgment_id"]
    assert item["difference_count"] == len(d["differences"])
    assert item["adopted_at"]
    # 摘要不放完整判定/报告
    assert "current_judgment" not in item
    assert "source_report" not in item
    assert body["next_cursor"] is None

    detail = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}/adoptions/1"
    )
    assert detail.status_code == 200
    full = detail.json()
    assert full["current_judgment"]["best"]["aid_id"] == "A"
    assert full["source_report"]["report_id"] == report["report_id"]

    # 404：判定不存在 / 采纳属于另一判定 / 采纳属于另一设备
    assert client.get(
        f"/field-devices/{DEVICE}/batch-judgments/999/adoptions"
    ).status_code == 404
    other = create_judgment(client, DEVICE, "k9", 1, 1)
    assert other.status_code == 201
    assert client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{other.json()['batch_judgment_id']}/adoptions/1"
    ).status_code == 404
    assert client.get(
        f"/field-devices/dev-02/batch-judgments/{j['batch_judgment_id']}/adoptions/1"
    ).status_code == 404


def test_adopted_judgment_survives_later_archive_and_reviews(client):
    """采纳后批次再归档：原/现行结果与采纳来源仍可完整查询（不可变）。"""
    meta = prepare(client)
    j, report = changed_report(client)
    d = adopt(
        client, DEVICE, j["batch_judgment_id"], report["report_id"], "adopt-1"
    ).json()

    register_ack_archive(client, DEVICE, [m[2] for m in meta], through=3)

    # 原批次判定详情仍给原结果与现行结果
    detail = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}"
    ).json()
    assert detail["judgment"]["best"]["aid_id"] == "C"
    assert detail["current_judgment"]["best"]["aid_id"] == "A"
    assert detail["adoption"]["adoption_id"] == d["adoption_id"]

    # 新/旧底层判定仍可经 /judgments 回溯
    assert client.get(f"/judgments/{d['current_judgment_id']}").status_code == 200
    assert client.get(f"/judgments/{j['source_judgment_id']}").status_code == 200

    # 归档后复核来源不可得（source_unavailable），但不影响已采纳的现行结果
    rv = review(client, DEVICE, j["batch_judgment_id"], "rv-after")
    assert rv.status_code == 201
    assert rv.json()["status"] == "source_unavailable"
