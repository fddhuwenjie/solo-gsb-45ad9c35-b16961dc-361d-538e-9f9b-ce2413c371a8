"""复核新判定采纳测试（changed 报告 → 设备批次判定现行结果）。

覆盖：健康链路采纳成功（替代判定落 judgments、原判定不动、批次判定详情
同时给出原结果与现行结果）、非 changed 报告/他目标报告 409、目标或报告
404、幂等键重试 200 与改目标/他键争抢 409、报告后来源变化（归档/摘要
篡改/判定记录删除）→ source_changed 409、复核结果不一致 →
review_result_mismatch 409（均不留部分采纳、不占键）、替代判定沿用原
library_version、并发同键只落一次采纳，以及隔离期间仍可用。
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
        c.db_path = str(tmp_path / "test.db")  # 便于直接篡改库验证采纳重核
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
    """上传给定观测批次序列，返回 [(sequence, batch_id, digest, count), …]。"""
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
        json={"report_id": report_id, "idempotency_key": key},
    )


def tamper(client, sql, params=()):
    """直接改库模拟事故；采纳必须在同一事务内重新核对并整体拒绝。"""
    conn = sqlite3.connect(client.db_path)
    try:
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def tamper_judgment_best(client, judgment_id, aid_id="C", matched=1):
    """篡改底层判定记录的最佳候选（模拟事故），使复核结论为 changed。"""
    conn = sqlite3.connect(client.db_path)
    try:
        row = conn.execute(
            "SELECT response FROM judgments WHERE id = ?", (judgment_id,)
        ).fetchone()
        payload = json.loads(row[0])
        payload["best"]["aid_id"] = aid_id
        payload["best"]["matched"] = matched
        conn.execute(
            "UPDATE judgments SET response = ? WHERE id = ?",
            (json.dumps(payload), judgment_id),
        )
        conn.commit()
    finally:
        conn.close()


def judgment_rows(client):
    return client.get("/judgments?limit=50").json()["count"]


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


def changed_report(client, device=DEVICE, key="rv-1"):
    """准备链路 + 判定 + 篡改判定记录 + changed 复核报告，返回 (判定, 报告)。"""
    prepare(client, device)
    j = create_judgment(client, device, "k1", 1, 3).json()
    tamper_judgment_best(client, j["source_judgment_id"])
    r = review(client, device, j["batch_judgment_id"], key)
    assert r.status_code == 201, r.text
    assert r.json()["status"] == "changed"
    return j, r.json()


# ---------------- 采纳成功：替代判定落库，原判定与历史仍可查 ----------------


def test_adopt_changed_report_success(client):
    j, report = changed_report(client)
    before = judgment_rows(client)

    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1")
    assert r.status_code == 201, r.text
    d = r.json()

    assert d["adoption_id"] == 1
    assert d["idempotency_key"] == "ad-1"
    assert d["device_id"] == DEVICE
    assert d["batch_judgment_id"] == j["batch_judgment_id"]
    assert d["report_id"] == report["report_id"]
    assert d["source_judgment_id"] == j["source_judgment_id"]
    assert d["replacement_judgment_id"] != j["source_judgment_id"]
    assert d["library_version"] == j["library_version"]
    assert (d["from_sequence"], d["to_sequence"]) == (1, 3)
    assert d["differences"] == report["differences"]
    # 原/现行候选证据：original 为被篡改的保存值，current 为真实重跑结果
    assert d["original"]["judgment_id"] == j["source_judgment_id"]
    assert d["original"]["best_aid_id"] == "C"
    assert d["current"]["judgment_id"] == d["replacement_judgment_id"]
    assert d["current"]["best_aid_id"] == "A"
    assert d["current"]["best"]["matched"] == j["judgment"]["best"]["matched"]

    # 替代判定写入 judgments（+1 行），请求载荷与原判定一致
    assert judgment_rows(client) == before + 1
    repl = client.get(f"/judgments/{d['replacement_judgment_id']}").json()
    assert repl["status"] == "matched"
    assert repl["best_aid_id"] == "A"
    assert repl["library_version"] == j["library_version"]
    original = client.get(f"/judgments/{j['source_judgment_id']}").json()
    assert repl["request"] == original["request"]
    # 原判定记录保持原样（响应仍是事故后的保存值），/judgments 历史仍可查
    assert original["response"]["best"]["aid_id"] == "C"

    # 批次判定详情同时给出原结果与现行结果
    detail = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}"
    ).json()
    assert detail["source_judgment_id"] == j["source_judgment_id"]
    assert detail["current_judgment_id"] == d["replacement_judgment_id"]
    assert detail["judgment"]["best"]["aid_id"] == "C"  # 原结果（事故保存值）
    ad = detail["adoption"]
    assert ad["adoption_id"] == d["adoption_id"]
    assert ad["report_id"] == report["report_id"]
    assert ad["replacement_judgment_id"] == d["replacement_judgment_id"]
    assert ad["judgment"]["best"]["aid_id"] == "A"  # 现行结果
    assert ad["judgment"]["judgment_id"] == d["replacement_judgment_id"]


def test_batch_judgment_detail_before_adoption(client):
    """未采纳时详情无 adoption，现行判定 id 等于来源判定 id。"""
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()

    detail = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}"
    ).json()
    assert detail["adoption"] is None
    assert detail["current_judgment_id"] == j["source_judgment_id"]
    assert detail["judgment"]["best"]["aid_id"] == "A"


def test_adopt_uses_original_library_version(client):
    """采纳重跑与替代判定都用原 library_version，与当前激活版本无关。"""
    j, report = changed_report(client)
    assert j["library_version"] == 1
    client.post(
        "/library/import",
        json={"aids": [{"aid_id": "X", "period": 3.0, "flashes": [0.0]}], "mode": "replace"},
    )
    assert client.get("/library").json()["library_version"] == 2

    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1")
    assert r.status_code == 201, r.text
    d = r.json()
    assert d["library_version"] == 1
    repl = client.get(f"/judgments/{d['replacement_judgment_id']}").json()
    assert repl["library_version"] == 1
    assert repl["best_aid_id"] == "A"


# ---------------- 报告目标与结论校验 ----------------


def test_adopt_rejects_non_changed_reports(client):
    meta = prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    # unchanged 报告
    r1 = review(client, DEVICE, j["batch_judgment_id"], "rv-1")
    assert r1.json()["status"] == "unchanged"
    r = adopt(client, DEVICE, j["batch_judgment_id"], r1.json()["report_id"], "ad-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["error"] == "adoption_conflict"
    assert d["reason"] == "report_not_changed"
    assert d["report_status"] == "unchanged"

    # source_unavailable 报告（归档部分批次后复核）
    register_ack_archive(client, DEVICE, [m[2] for m in meta], through=2)
    r2 = review(client, DEVICE, j["batch_judgment_id"], "rv-2")
    assert r2.json()["status"] == "source_unavailable"
    r = adopt(client, DEVICE, j["batch_judgment_id"], r2.json()["report_id"], "ad-2")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "report_not_changed"
    assert r.json()["detail"]["report_status"] == "source_unavailable"

    # 均未留下采纳，也未新增判定
    detail = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}"
    ).json()
    assert detail["adoption"] is None


def test_adopt_rejects_report_of_other_judgment(client):
    prepare(client)
    j1 = create_judgment(client, DEVICE, "k1", 1, 2).json()
    j2 = create_judgment(client, DEVICE, "k2", 2, 3).json()
    tamper_judgment_best(client, j1["source_judgment_id"])
    report = review(client, DEVICE, j1["batch_judgment_id"], "rv-1").json()
    assert report["status"] == "changed"

    # 报告属于 j1，对 j2 采纳 → 409 report_target_mismatch
    r = adopt(client, DEVICE, j2["batch_judgment_id"], report["report_id"], "ad-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "report_target_mismatch"
    assert d["report_batch_judgment_id"] == j1["batch_judgment_id"]


def test_adopt_rejects_report_of_other_device(client):
    prepare(client)
    prepare(client, device="dev-02")
    j1 = create_judgment(client, DEVICE, "k1", 1, 3).json()
    j2 = create_judgment(client, "dev-02", "k2", 1, 3).json()
    tamper_judgment_best(client, j1["source_judgment_id"])
    report = review(client, DEVICE, j1["batch_judgment_id"], "rv-1").json()

    r = adopt(client, "dev-02", j2["batch_judgment_id"], report["report_id"], "ad-1")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "report_target_mismatch"
    assert r.json()["detail"]["report_device_id"] == DEVICE


def test_adopt_404_when_judgment_or_report_missing(client):
    j, report = changed_report(client)

    r = adopt(client, DEVICE, 999, report["report_id"], "ad-1")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "batch_judgment_not_found"

    r = adopt(client, "dev-02", j["batch_judgment_id"], report["report_id"], "ad-2")
    assert r.status_code == 404

    r = adopt(client, DEVICE, j["batch_judgment_id"], 999, "ad-3")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "review_report_not_found"

    # 404 不占键：同一键随后可正常采纳
    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1")
    assert r.status_code == 201


def test_adopt_blank_key_is_422(client):
    j, report = changed_report(client)
    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "   ")
    assert r.status_code == 422


# ---------------- 幂等与争抢 ----------------


def test_same_key_retry_returns_original_adoption(client):
    j, report = changed_report(client)
    r1 = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1")
    assert r1.status_code == 201
    first = r1.json()
    before = judgment_rows(client)

    r2 = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1")
    assert r2.status_code == 200
    assert r2.json() == first
    assert judgment_rows(client) == before  # 重试不再落判定

    # 采纳后来源被篡改，同键重试仍返回原采纳结果（不重新核对）
    tamper(client, "DELETE FROM device_batches WHERE device_id = ? AND sequence = 1",
           (DEVICE,))
    r3 = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1")
    assert r3.status_code == 200
    assert r3.json() == first


def test_same_key_different_target_conflicts(client):
    j, report = changed_report(client)
    # 同一判定再出一份 changed 报告
    report2 = review(client, DEVICE, j["batch_judgment_id"], "rv-2").json()
    assert report2["status"] == "changed"
    assert adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1").status_code == 201

    # 同键改报告 → 409 idempotency_key_in_use
    r = adopt(client, DEVICE, j["batch_judgment_id"], report2["report_id"], "ad-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "idempotency_key_in_use"
    assert d["occupied_by_adoption_id"] == 1
    assert d["occupied_report_id"] == report["report_id"]

    # 同键改判定目标 → 409 idempotency_key_in_use
    j2 = create_judgment(client, DEVICE, "k2", 2, 3).json()
    tamper_judgment_best(client, j2["source_judgment_id"])
    report3 = review(client, DEVICE, j2["batch_judgment_id"], "rv-3").json()
    r = adopt(client, DEVICE, j2["batch_judgment_id"], report3["report_id"], "ad-1")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "idempotency_key_in_use"


def test_other_key_competing_same_judgment_conflicts(client):
    j, report = changed_report(client)
    report2 = review(client, DEVICE, j["batch_judgment_id"], "rv-2").json()
    assert adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1").status_code == 201

    # 其他键争抢同一原判定 → 409 already_adopted
    r = adopt(client, DEVICE, j["batch_judgment_id"], report2["report_id"], "ad-2")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "already_adopted"
    assert d["occupied_by_adoption_id"] == 1
    assert d["occupied_idempotency_key"] == "ad-1"

    # 同一报告换键同样被拒
    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-3")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "already_adopted"


def test_concurrent_same_key_single_adoption(client):
    j, report = changed_report(client)
    barrier = threading.Barrier(4)
    results, lock = [], threading.Lock()

    def worker():
        barrier.wait()
        r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "race-key")
        with lock:
            results.append((r.status_code, r.json()["adoption_id"]))

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    statuses = {s for s, _ in results}
    assert statuses <= {200, 201}
    assert 201 in statuses
    assert {aid for _, aid in results} == {1}
    # 恰好落一次替代判定
    j2 = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}"
    ).json()
    assert j2["adoption"]["adoption_id"] == 1


# ---------------- 报告后来源变化 / 复核结果不一致 → 409 且不留部分采纳 ----------------


def test_adopt_source_changed_when_batches_archived_after_report(client):
    j, report = changed_report(client)
    meta = client.get(f"/field-devices/{DEVICE}/batches?limit=10").json()["batches"]
    register_ack_archive(client, DEVICE, [b["digest"] for b in meta], through=2)
    before = judgment_rows(client)

    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "source_changed"
    assert d["recheck_status"] == "source_unavailable"
    assert any(i["issue_code"] == "batch_archived" for i in d["issues"])

    # 不留部分采纳：无新判定、无采纳记录
    assert judgment_rows(client) == before
    detail = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}"
    ).json()
    assert detail["adoption"] is None


def test_adopt_source_changed_when_digest_tampered_after_report(client):
    j, report = changed_report(client)
    tamper(
        client,
        "UPDATE device_batches SET digest = ? WHERE device_id = ? AND sequence = 2",
        ("0" * 64, DEVICE),
    )

    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "source_changed"
    assert d["recheck_status"] == "source_mismatch"
    codes = {i["issue_code"] for i in d["issues"]}
    assert "digest_mismatch" in codes

    # 失败不占键：修复来源后同一键可成功采纳
    conn = sqlite3.connect(client.db_path)
    try:
        row = conn.execute(
            "SELECT digest FROM batch_judgment_batches "
            "WHERE batch_judgment_id = ? AND sequence = 2",
            (j["batch_judgment_id"],),
        ).fetchone()
        saved_digest = row[0]
    finally:
        conn.close()
    tamper(
        client,
        "UPDATE device_batches SET digest = ? WHERE device_id = ? AND sequence = 2",
        (saved_digest, DEVICE),
    )
    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1")
    assert r.status_code == 201, r.text


def test_adopt_source_changed_when_judgment_record_deleted(client):
    j, report = changed_report(client)
    tamper(client, "DELETE FROM judgments WHERE id = ?", (j["source_judgment_id"],))

    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "source_changed"
    assert d["recheck_status"] == "source_unavailable"
    assert any(i["issue_code"] == "judgment_record_missing" for i in d["issues"])


def test_adopt_review_result_mismatch_when_judgment_retampered(client):
    """报告后判定记录又被改写 → 重跑差异与报告不一致 → 409，不留部分采纳。"""
    j, report = changed_report(client)
    # 报告基于「best.aid_id=C, matched=1」；再改成 matched=2，差异随之改变
    tamper_judgment_best(client, j["source_judgment_id"], aid_id="C", matched=2)
    before = judgment_rows(client)

    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["reason"] == "review_result_mismatch"
    assert d["report_differences"] != d["recheck_differences"]

    assert judgment_rows(client) == before
    detail = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j['batch_judgment_id']}"
    ).json()
    assert detail["adoption"] is None

    # 以当前状态重新复核得到新 changed 报告后可正常采纳
    report2 = review(client, DEVICE, j["batch_judgment_id"], "rv-2").json()
    assert report2["status"] == "changed"
    r = adopt(client, DEVICE, j["batch_judgment_id"], report2["report_id"], "ad-2")
    assert r.status_code == 201, r.text
    assert r.json()["current"]["best_aid_id"] == "A"


# ---------------- 隔离期间仍可用（处置类操作） ----------------


def test_adopt_available_during_isolation(client):
    j, report = changed_report(client)
    cursor = client.get(f"/field-devices/{DEVICE}/cursor").json()
    r = client.post(
        f"/field-devices/{DEVICE}/isolation",
        json={
            "head_sequence": cursor["last_sequence"],
            "head_digest": cursor["head_digest"],
            "reason": "verify-chain 异常",
            "idempotency_key": "iso-1",
        },
    )
    assert r.status_code == 201, r.text

    r = adopt(client, DEVICE, j["batch_judgment_id"], report["report_id"], "ad-1")
    assert r.status_code == 201, r.text
    assert r.json()["current"]["best_aid_id"] == "A"
