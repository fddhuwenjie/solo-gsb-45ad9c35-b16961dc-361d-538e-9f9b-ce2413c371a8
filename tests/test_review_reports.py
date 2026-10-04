"""连续批次判定复核报告测试（只读快照复核 + 幂等报告）。

覆盖：健康链复核 unchanged（不写入 judgments）、保存判定被改写时 changed
（差异字段与原/新候选证据）、批次归档/删除 → source_unavailable、摘要/
阈值/前序链/合并观测被篡改 → source_mismatch、幂等键重试 200 与改设备/
判定目标 409、目标判定 404、复核期间隔离设备仍可用、并发同键只生成一份
报告，以及列表/详情查询。
"""
import json
import os
import sqlite3
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app.batches import canonical_payload_of, compute_digest
from app.main import create_app

DEVICE = "dev-01"
THRESHOLDS = {"tolerance": 0.5, "max_drift": 5e-4, "max_missed": 3, "max_false": 3}
# A：周期 10、双闪；C：周期 6、单闪（与 test_batch_judgments 一致的合成约定）
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
        c.db_path = str(tmp_path / "test.db")  # 便于直接篡改库验证复核结果
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


def tamper(client, sql, params=()):
    """直接改库模拟迁移/离线事故后的不一致；复核必须只报告、不修复。"""
    conn = sqlite3.connect(client.db_path)
    try:
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute(sql, params)
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


# ---------------- unchanged：来源完整且重跑一致 ----------------


def test_review_unchanged_on_healthy_chain(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    before = judgment_rows(client)

    r = review(client, DEVICE, j["batch_judgment_id"], "rv-1")
    assert r.status_code == 201, r.text
    d = r.json()

    assert d["report_id"] == 1
    assert d["idempotency_key"] == "rv-1"
    assert d["device_id"] == DEVICE
    assert d["batch_judgment_id"] == j["batch_judgment_id"]
    assert d["source_judgment_id"] == j["source_judgment_id"]
    assert (d["from_sequence"], d["to_sequence"]) == (1, 3)
    assert d["library_version"] == j["library_version"]
    assert d["status"] == "unchanged"
    assert d["checked_batches"] == 3
    assert d["archived_batches"] == 0
    assert d["issue_count"] == 0
    assert d["issues"] == []
    assert d["differences"] == []

    # 原/新候选证据齐全且一致
    assert d["saved"]["judgment_id"] == j["source_judgment_id"]
    assert d["saved"]["status"] == "matched"
    assert d["saved"]["best_aid_id"] == "A"
    assert d["saved"]["best"]["aid_id"] == "A"
    assert d["current"]["status"] == "matched"
    assert d["current"]["best_aid_id"] == "A"
    assert d["current"]["best"] == d["saved"]["best"]
    assert "judgment_id" not in d["current"]  # 重跑不落库，无判定 id

    # 复核不写入 judgments
    assert judgment_rows(client) == before


def test_review_unchanged_without_active_library(client):
    # 无激活特征库时判定为 no_candidate；复核重跑同样 no_candidate → unchanged
    upload_chain(client, DEVICE, make_flashes((2, 2)))
    j = create_judgment(client, DEVICE, "k1", 1, 2).json()
    assert j["library_version"] is None
    assert j["judgment"]["status"] == "no_candidate"

    r = review(client, DEVICE, j["batch_judgment_id"], "rv-1")
    assert r.status_code == 201, r.text
    d = r.json()
    assert d["status"] == "unchanged"
    assert d["library_version"] is None
    assert d["saved"]["status"] == "no_candidate"
    assert d["saved"]["best"] is None
    assert d["current"]["status"] == "no_candidate"
    assert d["current"]["best"] is None


def test_review_unchanged_after_new_library_version_activated(client):
    """复核用保存版本的特征库重跑：激活新版本不影响旧判定的复核结论。"""
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    assert j["library_version"] == 1
    # 激活一个会让同样观测判定为 no_candidate 的新版本
    client.post(
        "/library/import",
        json={"aids": [{"aid_id": "X", "period": 3.0, "flashes": [0.0]}], "mode": "replace"},
    )
    assert client.get("/library").json()["library_version"] == 2

    r = review(client, DEVICE, j["batch_judgment_id"], "rv-1")
    assert r.status_code == 201, r.text
    d = r.json()
    assert d["status"] == "unchanged"  # 仍按保存的 v1 重跑，而非当前激活的 v2
    assert d["library_version"] == 1
    assert d["current"]["best_aid_id"] == "A"


# ---------------- changed：保存的判定响应与重跑不一致 ----------------


def test_review_changed_when_saved_judgment_response_tampered(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    jid = j["source_judgment_id"]

    # 篡改底层判定记录的响应（模拟事故）：改写最佳航标与状态
    conn = sqlite3.connect(client.db_path)
    try:
        row = conn.execute("SELECT response FROM judgments WHERE id = ?", (jid,)).fetchone()
        payload = json.loads(row[0])
        payload["best"]["aid_id"] = "C"
        payload["best"]["matched"] = 1
        conn.execute("UPDATE judgments SET response = ? WHERE id = ?",
                     (json.dumps(payload), jid))
        conn.commit()
    finally:
        conn.close()

    before = judgment_rows(client)
    r = review(client, DEVICE, j["batch_judgment_id"], "rv-1")
    assert r.status_code == 201, r.text
    d = r.json()

    assert d["status"] == "changed"
    assert d["issues"] == []  # 来源核对全部通过，是判定记录本身与重跑不符
    fields = {x["field"] for x in d["differences"]}
    assert "best.aid_id" in fields
    assert "best.matched" in fields
    aid_diff = next(x for x in d["differences"] if x["field"] == "best.aid_id")
    assert aid_diff["saved"] == "C"      # 被篡改的保存值
    assert aid_diff["current"] == "A"    # 从原始链数据重跑得到的值
    # 原/新候选证据：saved 为篡改后内容，current 为真实重跑结果
    assert d["saved"]["best_aid_id"] == "C"
    assert d["current"]["best_aid_id"] == "A"
    assert d["current"]["best"]["matched"] == j["judgment"]["best"]["matched"]
    assert judgment_rows(client) == before  # 重跑不写入 judgments


def test_review_changed_when_saved_status_tampered(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    jid = j["source_judgment_id"]
    conn = sqlite3.connect(client.db_path)
    try:
        row = conn.execute("SELECT response FROM judgments WHERE id = ?", (jid,)).fetchone()
        payload = json.loads(row[0])
        payload["status"] = "no_candidate"
        payload["best"] = None
        payload["reason"] = "max_missed exceeded"
        conn.execute("UPDATE judgments SET response = ? WHERE id = ?",
                     (json.dumps(payload), jid))
        conn.commit()
    finally:
        conn.close()

    d = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert d["status"] == "changed"
    fields = {x["field"] for x in d["differences"]}
    assert {"status", "reason", "best.aid_id"} <= fields
    assert d["saved"]["status"] == "no_candidate"
    assert d["saved"]["best"] is None
    assert d["current"]["status"] == "matched"


# ---------------- source_unavailable：来源缺失 ----------------


def test_review_source_unavailable_when_batches_archived(client):
    meta = prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    register_ack_archive(client, DEVICE, [m[2] for m in meta], through=2)

    r = review(client, DEVICE, j["batch_judgment_id"], "rv-1")
    assert r.status_code == 201, r.text
    d = r.json()
    assert d["status"] == "source_unavailable"
    assert d["archived_batches"] == 2
    archived = [i for i in d["issues"] if i["issue_code"] == "batch_archived"]
    assert {i["sequence"] for i in archived} == {1, 2}
    # 墓碑摘要仍被核对（未篡改 → 无 digest_mismatch），但没有明细 → 不重跑
    assert not any(i["issue_code"] == "digest_mismatch" for i in d["issues"])
    assert d["current"] is None
    assert d["differences"] == []
    # 原候选证据仍随报告保存
    assert d["saved"]["best_aid_id"] == "A"


def test_review_source_unavailable_when_batch_deleted(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    tamper(client, "DELETE FROM device_batches WHERE device_id = ? AND sequence = 2",
           (DEVICE,))

    d = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert d["status"] == "source_unavailable"
    missing = [i for i in d["issues"] if i["issue_code"] == "batch_missing"]
    assert [i["sequence"] for i in missing] == [2]
    assert d["current"] is None


def test_review_source_unavailable_when_judgment_record_deleted(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    tamper(client, "DELETE FROM judgments WHERE id = ?", (j["source_judgment_id"],))

    d = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert d["status"] == "source_unavailable"
    assert any(i["issue_code"] == "judgment_record_missing" for i in d["issues"])
    assert d["saved"] is None
    assert d["current"] is None


def test_review_source_unavailable_when_chain_anchor_deleted(client):
    """区间首批的前序链锚（from_sequence-1）缺失 → 前序链无法完整核对。"""
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 2, 3).json()
    tamper(client, "DELETE FROM device_batches WHERE device_id = ? AND sequence = 1",
           (DEVICE,))

    d = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert d["status"] == "source_unavailable"
    anchor = [i for i in d["issues"] if i["issue_code"] == "previous_chain_anchor_missing"]
    assert [i["sequence"] for i in anchor] == [1]


# ---------------- source_mismatch：摘要/阈值/前序链不符 ----------------


def test_review_source_mismatch_when_digest_tampered(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    tamper(
        client,
        "UPDATE device_batches SET digest = ? WHERE device_id = ? AND sequence = 2",
        ("0" * 64, DEVICE),
    )

    d = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert d["status"] == "source_mismatch"
    codes = {i["issue_code"] for i in d["issues"]}
    # 保存摘要 ≠ 当前摘要；且当前摘要与明细重算也不符
    assert "digest_mismatch" in codes
    assert "digest_recompute_mismatch" in codes
    # 链式引用：seq3 的 previous_digest 仍指向原摘要 → 前序链断裂
    assert "previous_chain_broken" in codes
    assert d["current"] is None


def test_review_source_mismatch_when_thresholds_tampered(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    new_thresholds = json.dumps({**THRESHOLDS, "tolerance": THRESHOLDS["tolerance"] + 0.1})
    tamper(
        client,
        "UPDATE device_batches SET thresholds = ? WHERE device_id = ? AND sequence = 2",
        (new_thresholds, DEVICE),
    )

    d = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert d["status"] == "source_mismatch"
    th = [i for i in d["issues"] if i["issue_code"] == "thresholds_mismatch"]
    assert [i["sequence"] for i in th] == [2]
    assert th[0]["expected"] == THRESHOLDS
    assert d["current"] is None


def test_review_source_mismatch_when_previous_digest_tampered(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    tamper(
        client,
        "UPDATE device_batches SET previous_digest = ? WHERE device_id = ? AND sequence = 3",
        ("1" * 64, DEVICE),
    )

    d = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert d["status"] == "source_mismatch"
    codes = {i["issue_code"] for i in d["issues"]}
    assert "previous_digest_mismatch" in codes
    assert "previous_chain_broken" in codes


def test_review_source_mismatch_when_chain_anchor_digest_tampered(client):
    """区间外的前序批次摘要被改写 → 区间首批的链锚核对失败。"""
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 2, 3).json()
    tamper(
        client,
        "UPDATE device_batches SET digest = ? WHERE device_id = ? AND sequence = 1",
        ("2" * 64, DEVICE),
    )

    d = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert d["status"] == "source_mismatch"
    broken = [i for i in d["issues"] if i["issue_code"] == "previous_chain_broken"]
    assert [i["sequence"] for i in broken] == [2]
    assert broken[0]["actual"] != broken[0]["expected"]


def test_review_source_mismatch_when_observations_and_digest_forged(client):
    """明细与摘要被同步伪造（摘要核对全过）→ 合并观测与保存来源不符。"""
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()

    # 改写 seq2 的观测并按规范化规则重算摘要，并同步伪造链式引用与判定
    # 来源里保存的摘要，使逐批摘要/前序链核对全部通过
    forged_obs = [
        {"local_id": f"x{i:03d}", "captured_at": PSI + 9000.0 + i}
        for i in range(6)  # 与 seq2 原观测条数一致，避开条数核对
    ]
    digest = compute_digest(
        canonical_payload_of(DEVICE, "b-2", 2, forged_obs, THRESHOLDS)
    )
    tamper(
        client,
        "UPDATE device_batches SET observations = ?, digest = ? "
        "WHERE device_id = ? AND sequence = 2",
        (json.dumps(forged_obs), digest, DEVICE),
    )
    tamper(
        client,
        "UPDATE device_batches SET previous_digest = ? "
        "WHERE device_id = ? AND sequence = 3",
        (digest, DEVICE),
    )
    tamper(
        client,
        "UPDATE batch_judgment_batches SET digest = ? "
        "WHERE batch_judgment_id = ? AND sequence = 2",
        (digest, j["batch_judgment_id"]),
    )
    tamper(
        client,
        "UPDATE batch_judgment_batches SET previous_digest = ? "
        "WHERE batch_judgment_id = ? AND sequence = 3",
        (digest, j["batch_judgment_id"]),
    )

    d = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert d["status"] == "source_mismatch"
    obs_issue = [i for i in d["issues"] if i["issue_code"] == "observations_mismatch"]
    assert len(obs_issue) == 1
    assert obs_issue[0]["expected"] == {"observation_count": j["observation_count"]}
    assert d["current"] is None


def test_review_mismatch_takes_precedence_over_unavailable(client):
    """既有墓碑（来源缺失）又有摘要篡改（不符）时，结论为 source_mismatch。"""
    meta = prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    register_ack_archive(client, DEVICE, [m[2] for m in meta], through=1)
    tamper(
        client,
        "UPDATE device_batches SET digest = ? WHERE device_id = ? AND sequence = 3",
        ("3" * 64, DEVICE),
    )

    d = review(client, DEVICE, j["batch_judgment_id"], "rv-1").json()
    assert d["status"] == "source_mismatch"
    codes = {i["issue_code"] for i in d["issues"]}
    assert "batch_archived" in codes  # 缺失与不符都如实列出
    assert "digest_mismatch" in codes


# ---------------- 幂等与冲突 ----------------


def test_same_key_retry_returns_original_report(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    r1 = review(client, DEVICE, j["batch_judgment_id"], "rv-1")
    assert r1.status_code == 201
    first = r1.json()

    r2 = review(client, DEVICE, j["batch_judgment_id"], "rv-1")
    assert r2.status_code == 200
    again = r2.json()
    assert again["report_id"] == first["report_id"]
    assert again["created_at"] == first["created_at"]
    assert again == first

    # 首次复核后篡改库，重试仍返回原报告（不会重新复核）
    tamper(client, "DELETE FROM device_batches WHERE device_id = ? AND sequence = 1",
           (DEVICE,))
    r3 = review(client, DEVICE, j["batch_judgment_id"], "rv-1")
    assert r3.status_code == 200
    assert r3.json() == first


def test_same_key_different_judgment_conflicts(client):
    prepare(client)
    j1 = create_judgment(client, DEVICE, "k1", 1, 2).json()
    j2 = create_judgment(client, DEVICE, "k2", 2, 3).json()
    assert review(client, DEVICE, j1["batch_judgment_id"], "rv-1").status_code == 201

    r = review(client, DEVICE, j2["batch_judgment_id"], "rv-1")
    assert r.status_code == 409
    d = r.json()["detail"]
    assert d["error"] == "review_report_conflict"
    assert d["reason"] == "idempotency_key_in_use"
    assert d["occupied_by_report_id"] == 1
    assert d["occupied_device_id"] == DEVICE
    assert d["occupied_batch_judgment_id"] == j1["batch_judgment_id"]


def test_same_key_different_device_conflicts(client):
    prepare(client)
    prepare(client, device="dev-02")
    j1 = create_judgment(client, DEVICE, "k1", 1, 3).json()
    j2 = create_judgment(client, "dev-02", "k2", 1, 3).json()
    assert review(client, DEVICE, j1["batch_judgment_id"], "rv-1").status_code == 201

    r = review(client, "dev-02", j2["batch_judgment_id"], "rv-1")
    assert r.status_code == 409
    assert r.json()["detail"]["occupied_device_id"] == DEVICE


def test_concurrent_same_key_single_report(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    barrier = threading.Barrier(4)
    results, lock = [], threading.Lock()

    def worker():
        barrier.wait()
        r = review(client, DEVICE, j["batch_judgment_id"], "race-key")
        with lock:
            results.append((r.status_code, r.json()["report_id"]))

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    statuses = {s for s, _ in results}
    assert statuses <= {200, 201}
    assert 201 in statuses
    assert {rid for _, rid in results} == {1}


def test_review_report_keys_independent_of_batch_judgment_keys(client):
    """复核报告与批次判定使用各自独立的幂等键命名空间。"""
    prepare(client)
    j = create_judgment(client, DEVICE, "shared-key", 1, 3).json()
    r = review(client, DEVICE, j["batch_judgment_id"], "shared-key")
    assert r.status_code == 201


# ---------------- 404 与校验 ----------------


def test_review_404_when_judgment_missing_or_other_device(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()

    r = review(client, DEVICE, 999, "rv-1")
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "batch_judgment_not_found"

    r = review(client, "dev-02", j["batch_judgment_id"], "rv-2")
    assert r.status_code == 404


def test_review_key_not_consumed_by_failed_request(client):
    """404 的请求不占用幂等键，同一键随后可正常使用。"""
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    assert review(client, DEVICE, 999, "rv-1").status_code == 404
    r = review(client, DEVICE, j["batch_judgment_id"], "rv-1")
    assert r.status_code == 201


def test_review_blank_key_is_422(client):
    prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
    r = review(client, DEVICE, j["batch_judgment_id"], "   ")
    assert r.status_code == 422


# ---------------- 隔离期间仍可用 ----------------


def test_review_available_during_isolation(client):
    meta = prepare(client)
    j = create_judgment(client, DEVICE, "k1", 1, 3).json()
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

    # 隔离期间批次判定被挡住，但复核报告（只读核验类）仍可用
    blocked = create_judgment(client, DEVICE, "k2", 1, 2)
    assert blocked.status_code == 409
    r = review(client, DEVICE, j["batch_judgment_id"], "rv-1")
    assert r.status_code == 201, r.text
    assert r.json()["status"] == "unchanged"


# ---------------- 查询 ----------------


def test_review_list_and_detail_queries(client):
    prepare(client)
    j1 = create_judgment(client, DEVICE, "k1", 1, 2).json()
    j2 = create_judgment(client, DEVICE, "k2", 2, 3).json()
    review(client, DEVICE, j1["batch_judgment_id"], "rv-1")
    review(client, DEVICE, j1["batch_judgment_id"], "rv-2")
    review(client, DEVICE, j2["batch_judgment_id"], "rv-3")

    page = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j1['batch_judgment_id']}/review-reports"
    )
    assert page.status_code == 200
    items = page.json()["review_reports"]
    assert [i["report_id"] for i in items] == [1, 2]
    # 列表为摘要：不含问题清单、差异字段与候选证据
    assert "issues" not in items[0]
    assert "differences" not in items[0]
    assert "saved" not in items[0]
    assert items[0]["status"] == "unchanged"
    assert items[0]["source_judgment_id"] == j1["source_judgment_id"]
    assert items[0]["difference_count"] == 0

    # 游标分页
    p1 = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j1['batch_judgment_id']}/review-reports?limit=1"
    ).json()
    assert p1["count"] == 1 and p1["next_cursor"] == 1
    p2 = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j1['batch_judgment_id']}/review-reports"
        "?limit=1&after_id=1"
    ).json()
    assert [i["report_id"] for i in p2["review_reports"]] == [2]
    assert p2["next_cursor"] is None

    # 详情含完整报告
    detail = client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j1['batch_judgment_id']}/review-reports/1"
    )
    assert detail.status_code == 200
    assert detail.json()["saved"]["best_aid_id"] == "A"
    assert detail.json()["issues"] == []

    # 404：判定不存在 / 报告属于另一判定 / 报告属于另一设备
    assert client.get(
        f"/field-devices/{DEVICE}/batch-judgments/999/review-reports"
    ).status_code == 404
    assert client.get(
        f"/field-devices/{DEVICE}/batch-judgments/{j2['batch_judgment_id']}/review-reports/1"
    ).status_code == 404
    assert client.get(
        f"/field-devices/dev-02/batch-judgments/{j1['batch_judgment_id']}/review-reports/1"
    ).status_code == 404
