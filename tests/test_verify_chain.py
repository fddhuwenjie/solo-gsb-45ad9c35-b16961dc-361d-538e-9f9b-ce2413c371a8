"""设备链一致性核验测试（只读快照，不改数据）。

覆盖：健康链 ok（含归档墓碑、消费者确认、连续批次判定来源）、空设备/缺批次/
非法范围 4xx、范围内未归档批次重算摘要（digest_mismatch）、previous_digest
链、序号连续性（sequence_gap）、链游标 batch_count/last_sequence/head_digest、
archived_through 墓碑边界与墓碑骨架、消费者确认摘要与水位、判定来源摘要、
归档批次不重算只核对链式引用、核验不改任何数据，以及并发上传/确认/归档期间
结果来自同一快照。
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

THRESHOLDS = {"tolerance": 0.5, "max_drift": 5e-4, "max_missed": 3, "max_false": 3}
DEVICE = "dev-01"


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        c.db_path = str(tmp_path / "test.db")  # 便于直接篡改库验证核验结果
        yield c


def obs(n, base=1_700_000_000.0, step=0.5):
    return [
        {"local_id": f"o{i:03d}", "captured_at": base + i * step}
        for i in range(n)
    ]


def batch(batch_id, sequence, previous_digest="", observations=None, **kw):
    body = {
        "batch_id": batch_id,
        "sequence": sequence,
        "previous_digest": previous_digest,
        "observations": observations if observations is not None else obs(3),
        **THRESHOLDS,
    }
    body.update(kw)
    return body


def upload_chain(client, device, n):
    """上传 n 个链式批次，返回 (请求体列表, 摘要列表)（下标 0 = seq1）。"""
    bodies, digests = [], []
    prev = ""
    for seq in range(1, n + 1):
        b = batch(f"b-{seq}", seq, previous_digest=prev)
        r = client.post(f"/field-devices/{device}/batches", json=b)
        assert r.status_code == 201, r.text
        bodies.append(b)
        digests.append(r.json()["digest"])
        prev = digests[-1]
    return bodies, digests


def register(client, device, consumer_id):
    return client.post(
        f"/field-devices/{device}/consumers", json={"consumer_id": consumer_id}
    )


def ack(client, device, consumer_id, sequence, digest):
    return client.post(
        f"/field-devices/{device}/acknowledgments",
        json={"consumer_id": consumer_id, "sequence": sequence, "batch_digest": digest},
    )


def ack_all(client, device, consumer_id, digests):
    for seq, digest in enumerate(digests, start=1):
        r = ack(client, device, consumer_id, seq, digest)
        assert r.status_code in (200, 201), r.text


def archive(client, device, through):
    return client.post(
        f"/field-devices/{device}/archive", json={"through_sequence": through}
    )


def verify(client, device, frm, to):
    return client.post(
        f"/field-devices/{device}/verify-chain",
        json={"from_sequence": frm, "to_sequence": to},
    )


def tamper(client, sql, params=()):
    """直接改库模拟迁移/离线事故后的不一致；核验必须只报告、不修复。"""
    conn = sqlite3.connect(client.db_path)
    try:
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def issue_codes(resp):
    return [i["issue_code"] for i in resp["issues"]]


def issues_at(resp, sequence):
    return [i for i in resp["issues"] if i["sequence"] == sequence]


# ---------------- 健康链：ok 与快照 ----------------


def test_verify_healthy_chain_with_archive_consumers_and_judgment(client):
    client.post("/library/import", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
    })
    _, digests = upload_chain(client, DEVICE, 5)
    register(client, DEVICE, "archiver")
    register(client, DEVICE, "ml")
    ack_all(client, DEVICE, "archiver", digests)
    ack_all(client, DEVICE, "ml", digests[:2])
    r = archive(client, DEVICE, 2)
    assert r.status_code == 201
    # 连续批次判定来源（区间 3..4，未归档）
    r = client.post(
        f"/field-devices/{DEVICE}/batch-judgments",
        json={"idempotency_key": "k-3-4", "from_sequence": 3, "to_sequence": 4},
    )
    assert r.status_code == 201, r.text

    r = verify(client, DEVICE, 1, 5)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["issue_count"] == 0
    assert body["issues"] == []
    assert body["device_id"] == DEVICE
    assert body["from_sequence"] == 1 and body["to_sequence"] == 5
    # 范围 1..5：seq1/2 为墓碑（不重算），seq3..5 重算
    assert body["checked_batches"] == 5
    assert body["recomputed_batches"] == 3
    assert body["archived_batches"] == 2
    assert body["checked_acknowledgments"] == 7  # archiver 5 + ml 2
    assert body["checked_judgment_sources"] == 2  # 判定区间 3..4 的两批
    snap = body["snapshot"]
    assert snap["head_sequence"] == 5
    assert snap["head_digest"] == digests[4]
    assert snap["batch_count"] == 5
    assert snap["archived_through"] == 2
    assert snap["safe_sequence"] == 2  # min(archiver=5, ml=2)
    assert snap["consumer_count"] == 2
    assert snap["batch_judgment_count"] == 1


def test_verify_plain_chain_without_consumers(client):
    _, digests = upload_chain(client, DEVICE, 3)
    r = verify(client, DEVICE, 1, 3)
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["checked_acknowledgments"] == 0
    assert body["checked_judgment_sources"] == 0
    assert body["snapshot"]["safe_sequence"] == 0
    assert body["snapshot"]["consumer_count"] == 0
    assert body["snapshot"]["head_digest"] == digests[2]


def test_verify_subrange_snapshot_reports_chain_head(client):
    """核验子区间时，快照上限仍反映整条链的链头。"""
    _, digests = upload_chain(client, DEVICE, 5)
    r = verify(client, DEVICE, 2, 3)
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["checked_batches"] == 2
    assert body["recomputed_batches"] == 2
    assert body["snapshot"]["head_sequence"] == 5
    assert body["snapshot"]["head_digest"] == digests[4]


# ---------------- 4xx：空设备、缺批次、非法范围 ----------------


def test_verify_empty_device_404(client):
    r = verify(client, "dev-empty", 1, 1)
    assert r.status_code == 404
    detail = r.json()["detail"]
    assert detail["error"] == "device_not_found"
    assert detail["device_id"] == "dev-empty"


def test_verify_device_with_consumers_but_no_batches_404(client):
    register(client, "dev-c", "archiver")
    r = verify(client, "dev-c", 1, 2)
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "device_not_found"


def test_verify_range_beyond_head_404(client):
    upload_chain(client, DEVICE, 3)
    r = verify(client, DEVICE, 1, 9)
    assert r.status_code == 404
    detail = r.json()["detail"]
    assert detail["error"] == "batches_not_found"
    assert detail["head_sequence"] == 3
    assert detail["missing_count"] == 6  # seq4..9
    assert detail["missing_sequences"] == [4, 5, 6, 7, 8, 9]


def test_verify_range_with_gap_404(client):
    upload_chain(client, DEVICE, 4)
    tamper(client, "DELETE FROM device_batches WHERE device_id = ? AND sequence = 2", (DEVICE,))
    r = verify(client, DEVICE, 1, 4)
    assert r.status_code == 404
    detail = r.json()["detail"]
    assert detail["error"] == "batches_not_found"
    assert detail["missing_sequences"] == [2]
    assert detail["missing_count"] == 1


def test_verify_invalid_range_422(client):
    upload_chain(client, DEVICE, 2)
    assert verify(client, DEVICE, 3, 2).status_code == 422  # from > to
    assert verify(client, DEVICE, 0, 1).status_code == 422  # from < 1
    assert verify(client, DEVICE, 1, 0).status_code == 422  # to < 1
    r = client.post(f"/field-devices/{DEVICE}/verify-chain", json={})
    assert r.status_code == 422  # 缺字段


# ---------------- 摘要重算与链式引用 ----------------


def test_verify_digest_mismatch_on_tampered_observations(client):
    bodies, digests = upload_chain(client, DEVICE, 3)
    # 篡改 seq2 保存的观测（模拟迁移错位）：重算摘要 ≠ 保存摘要
    changed = [{"local_id": "x001", "captured_at": 1_700_000_100.0}]
    tamper(
        client,
        "UPDATE device_batches SET observations = ? WHERE device_id = ? AND sequence = 2",
        (json.dumps(changed), DEVICE),
    )
    r = verify(client, DEVICE, 1, 3)
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    hits = [i for i in body["issues"] if i["issue_code"] == "digest_mismatch"]
    assert len(hits) == 1
    hit = hits[0]
    assert hit["sequence"] == 2
    assert hit["actual"] == digests[1]  # 保存的摘要未变
    assert hit["expected"] != digests[1]  # 重算值随被篡改的观测改变
    assert len(hit["expected"]) == 64
    # 链式引用不受影响（保存的摘要字段本身未动）
    assert "previous_digest_mismatch" not in issue_codes(body)
    # 核验不改数据：被篡改的观测保持原样
    detail = client.get(f"/field-devices/{DEVICE}/batches/2").json()
    assert detail["observations"] == changed


def test_verify_digest_mismatch_on_tampered_stored_digest(client):
    _, digests = upload_chain(client, DEVICE, 3)
    forged = "ab" * 32
    tamper(
        client,
        "UPDATE device_batches SET digest = ? WHERE device_id = ? AND sequence = 2",
        (forged, DEVICE),
    )
    body = verify(client, DEVICE, 1, 3).json()
    assert body["ok"] is False
    codes = issue_codes(body)
    # 重算值 ≠ 被篡改的保存摘要
    hit = [i for i in body["issues"] if i["issue_code"] == "digest_mismatch"][0]
    assert hit["sequence"] == 2
    assert hit["expected"] == digests[1]  # 重算恢复出的真实摘要
    assert hit["actual"] == forged
    # seq3 保存的前序引用（真摘要）≠ seq2 当前保存的（被篡改）摘要 → 链断裂
    link = [i for i in body["issues"] if i["issue_code"] == "previous_digest_mismatch"]
    assert len(link) == 1
    assert link[0]["sequence"] == 3
    assert link[0]["expected"] == forged
    assert link[0]["actual"] == digests[1]
    assert "head_digest_mismatch" not in codes  # 链头未受影响


def test_verify_observation_count_mismatch(client):
    upload_chain(client, DEVICE, 2)
    tamper(
        client,
        "UPDATE device_batches SET observation_count = 99 "
        "WHERE device_id = ? AND sequence = 1",
        (DEVICE,),
    )
    body = verify(client, DEVICE, 1, 2).json()
    hits = [i for i in body["issues"] if i["issue_code"] == "observation_count_mismatch"]
    assert len(hits) == 1
    assert hits[0]["sequence"] == 1
    assert hits[0]["expected"] == 3  # 实际观测条数
    assert hits[0]["actual"] == 99
    # observation_count 不参与摘要，重算仍一致
    assert "digest_mismatch" not in issue_codes(body)


def test_verify_previous_digest_mismatch(client):
    _, digests = upload_chain(client, DEVICE, 3)
    tamper(
        client,
        "UPDATE device_batches SET previous_digest = ? "
        "WHERE device_id = ? AND sequence = 3",
        ("cd" * 32, DEVICE),
    )
    body = verify(client, DEVICE, 1, 3).json()
    hits = [i for i in body["issues"] if i["issue_code"] == "previous_digest_mismatch"]
    assert len(hits) == 1
    assert hits[0]["sequence"] == 3
    assert hits[0]["expected"] == digests[1]  # 上一批（seq2）保存的摘要
    assert hits[0]["actual"] == "cd" * 32
    assert body["ok"] is False


def test_verify_first_batch_previous_digest_must_be_empty(client):
    upload_chain(client, DEVICE, 2)
    tamper(
        client,
        "UPDATE device_batches SET previous_digest = ? "
        "WHERE device_id = ? AND sequence = 1",
        ("ef" * 32, DEVICE),
    )
    body = verify(client, DEVICE, 1, 2).json()
    hits = [i for i in body["issues"] if i["issue_code"] == "previous_digest_mismatch"]
    assert len(hits) == 1
    assert hits[0]["sequence"] == 1
    assert hits[0]["expected"] == ""  # 首批前序必须为空串
    assert hits[0]["actual"] == "ef" * 32


def test_verify_recompute_scoped_to_requested_range(client):
    """摘要重算只针对请求范围：范围外被篡改的明细不在本次报出。"""
    bodies, _ = upload_chain(client, DEVICE, 5)
    changed = [{"local_id": "x001", "captured_at": 1_700_000_100.0}]
    tamper(
        client,
        "UPDATE device_batches SET observations = ? WHERE device_id = ? AND sequence = 5",
        (json.dumps(changed), DEVICE),
    )
    body = verify(client, DEVICE, 1, 3).json()
    assert body["ok"] is True  # 范围 1..3 未被篡改；链式引用用的是保存摘要
    body = verify(client, DEVICE, 4, 5).json()
    assert body["ok"] is False
    hits = [i for i in body["issues"] if i["issue_code"] == "digest_mismatch"]
    assert [h["sequence"] for h in hits] == [5]


# ---------------- 序号连续性与链游标 ----------------


def test_verify_sequence_gap_and_cursor_after_batch_deleted(client):
    _, digests = upload_chain(client, DEVICE, 5)
    tamper(client, "DELETE FROM device_batches WHERE device_id = ? AND sequence = 3", (DEVICE,))
    # 范围 4..5 不缺批次 → 不 404；链上缺口与游标不一致作为问题报出
    body = verify(client, DEVICE, 4, 5).json()
    assert body["ok"] is False
    codes = issue_codes(body)
    gap = [i for i in body["issues"] if i["issue_code"] == "sequence_gap"]
    assert [g["sequence"] for g in gap] == [3]
    assert gap[0]["expected"] == "present" and gap[0]["actual"] == "missing"
    # seq4 保存的前序引用指向被删批次摘要，与 seq2 摘要不符 → 链断裂
    link = [i for i in body["issues"] if i["issue_code"] == "previous_digest_mismatch"]
    assert [l["sequence"] for l in link] == [4]
    assert link[0]["expected"] == digests[1]
    assert link[0]["actual"] == digests[2]
    # 游标仍记录 5 批，实际只剩 4 批
    cnt = [i for i in body["issues"] if i["issue_code"] == "batch_count_mismatch"]
    assert len(cnt) == 1 and cnt[0]["expected"] == 4 and cnt[0]["actual"] == 5
    assert "last_sequence_mismatch" not in codes  # 链头序号仍在
    assert "head_digest_mismatch" not in codes


def test_verify_chain_cursor_mismatches(client):
    _, digests = upload_chain(client, DEVICE, 3)
    tamper(
        client,
        "UPDATE device_chains SET batch_count = 7, last_sequence = 9, "
        "head_digest = ? WHERE device_id = ?",
        ("00" * 32, DEVICE),
    )
    body = verify(client, DEVICE, 1, 3).json()
    assert body["ok"] is False
    by_code = {}
    for i in body["issues"]:
        by_code.setdefault(i["issue_code"], []).append(i)
    assert by_code["batch_count_mismatch"][0]["expected"] == 3
    assert by_code["batch_count_mismatch"][0]["actual"] == 7
    assert by_code["last_sequence_mismatch"][0]["expected"] == 3
    assert by_code["last_sequence_mismatch"][0]["actual"] == 9
    # 游标指向的 seq9 不存在 → 链头批次缺失
    assert by_code["head_batch_missing"][0]["sequence"] == 9
    # 快照上限来自批次表（真实链上界），不采信被篡改的游标
    assert body["snapshot"]["head_sequence"] == 3
    assert body["snapshot"]["head_digest"] == digests[2]


def test_verify_head_digest_mismatch(client):
    _, digests = upload_chain(client, DEVICE, 2)
    tamper(
        client,
        "UPDATE device_chains SET head_digest = ? WHERE device_id = ?",
        ("11" * 32, DEVICE),
    )
    body = verify(client, DEVICE, 1, 2).json()
    hits = [i for i in body["issues"] if i["issue_code"] == "head_digest_mismatch"]
    assert len(hits) == 1
    assert hits[0]["sequence"] == 2
    assert hits[0]["expected"] == digests[1]
    assert hits[0]["actual"] == "11" * 32


def test_verify_chain_cursor_missing(client):
    upload_chain(client, DEVICE, 2)
    tamper(client, "DELETE FROM device_chains WHERE device_id = ?", (DEVICE,))
    body = verify(client, DEVICE, 1, 2).json()
    hits = [i for i in body["issues"] if i["issue_code"] == "chain_cursor_missing"]
    assert len(hits) == 1
    assert hits[0]["expected"] == "present" and hits[0]["actual"] == "missing"
    # 游标缺失时归档水位按 0 处理，批次明细核对不受影响
    assert "digest_mismatch" not in issue_codes(body)


# ---------------- archived_through 墓碑边界与骨架 ----------------


def test_verify_tombstone_boundary_violation(client):
    _, digests = upload_chain(client, DEVICE, 4)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    archive(client, DEVICE, 2)

    # seq1 被改回未归档（低于等于 archived_through=2，应为墓碑）
    tamper(
        client,
        "UPDATE device_batches SET archived_at = NULL, status = 'accepted' "
        "WHERE device_id = ? AND sequence = 1",
        (DEVICE,),
    )
    # seq4 被改成墓碑（大于 archived_through，不应为墓碑）；明细一并清空
    tamper(
        client,
        "UPDATE device_batches SET archived_at = 1700000000.0, status = 'archived', "
        "observations = NULL, thresholds = NULL "
        "WHERE device_id = ? AND sequence = 4",
        (DEVICE,),
    )
    body = verify(client, DEVICE, 1, 4).json()
    hits = [i for i in body["issues"] if i["issue_code"] == "tombstone_boundary_violation"]
    by_seq = {h["sequence"]: h for h in hits}
    assert set(by_seq) == {1, 4}
    assert by_seq[1]["expected"] == "archived" and by_seq[1]["actual"] == "accepted"
    assert by_seq[4]["expected"] == "accepted" and by_seq[4]["actual"] == "archived"
    # seq1 的 status 与 archived_at 被同时改回，骨架状态一致，无附加问题
    assert "tombstone_status_mismatch" not in issue_codes(body)


def test_verify_tombstone_skeleton_integrity(client):
    _, digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    archive(client, DEVICE, 2)

    # 墓碑状态列与 archived_at 不一致
    tamper(
        client,
        "UPDATE device_batches SET status = 'accepted' "
        "WHERE device_id = ? AND sequence = 1",
        (DEVICE,),
    )
    # 墓碑明细未被清空
    tamper(
        client,
        "UPDATE device_batches SET observations = '[]' "
        "WHERE device_id = ? AND sequence = 2",
        (DEVICE,),
    )
    body = verify(client, DEVICE, 1, 3).json()
    status_hits = [i for i in body["issues"] if i["issue_code"] == "tombstone_status_mismatch"]
    assert len(status_hits) == 1
    assert status_hits[0]["sequence"] == 1
    assert status_hits[0]["expected"] == "archived"
    assert status_hits[0]["actual"] == "accepted"
    detail_hits = [i for i in body["issues"] if i["issue_code"] == "tombstone_details_present"]
    assert len(detail_hits) == 1
    assert detail_hits[0]["sequence"] == 2
    assert detail_hits[0]["expected"] == "cleared"
    assert detail_hits[0]["actual"] == "present"


def test_verify_archived_through_beyond_head(client):
    upload_chain(client, DEVICE, 2)
    tamper(
        client,
        "UPDATE device_chains SET archived_through = 99 WHERE device_id = ?",
        (DEVICE,),
    )
    body = verify(client, DEVICE, 1, 2).json()
    hits = [i for i in body["issues"] if i["issue_code"] == "archived_through_beyond_head"]
    assert len(hits) == 1
    assert hits[0]["expected"] == 2 and hits[0]["actual"] == 99
    # 边界随之失真：链上批次都被认为应是墓碑
    boundary = [i for i in body["issues"] if i["issue_code"] == "tombstone_boundary_violation"]
    assert {b["sequence"] for b in boundary} == {1, 2}


def test_verify_archived_batches_not_recomputed(client):
    """归档批次不重算摘要：篡改墓碑保存的摘要只经链式引用在相邻批次报出。"""
    _, digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    archive(client, DEVICE, 2)

    forged = "22" * 32
    tamper(
        client,
        "UPDATE device_batches SET digest = ? WHERE device_id = ? AND sequence = 1",
        (forged, DEVICE),
    )
    body = verify(client, DEVICE, 1, 3).json()
    assert body["ok"] is False
    # seq1 是墓碑：不产生 digest_mismatch（不重算）
    assert not [
        i for i in body["issues"]
        if i["issue_code"] == "digest_mismatch" and i["sequence"] == 1
    ]
    # seq2 保存的前序引用（真摘要）≠ seq1 当前保存的（被篡改）摘要 → 链断裂
    link = [i for i in body["issues"] if i["issue_code"] == "previous_digest_mismatch"]
    assert len(link) == 1
    assert link[0]["sequence"] == 2
    assert link[0]["expected"] == forged
    assert link[0]["actual"] == digests[0]
    assert body["archived_batches"] == 2
    assert body["recomputed_batches"] == 1


# ---------------- 消费者确认摘要与水位 ----------------


def test_verify_ack_digest_mismatch(client):
    _, digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    tamper(
        client,
        "UPDATE device_batch_acks SET batch_digest = ? "
        "WHERE device_id = ? AND consumer_id = 'archiver' AND sequence = 2",
        ("33" * 32, DEVICE),
    )
    body = verify(client, DEVICE, 1, 3).json()
    hits = [i for i in body["issues"] if i["issue_code"] == "ack_digest_mismatch"]
    assert len(hits) == 1
    hit = hits[0]
    assert hit["sequence"] == 2
    assert hit["consumer_id"] == "archiver"
    assert hit["expected"] == digests[1]  # 批次当前摘要
    assert hit["actual"] == "33" * 32     # 确认记录里被篡改的摘要


def test_verify_ack_batch_missing(client):
    _, digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    tamper(client, "DELETE FROM device_batches WHERE device_id = ? AND sequence = 3", (DEVICE,))
    body = verify(client, DEVICE, 1, 2).json()
    hits = [i for i in body["issues"] if i["issue_code"] == "ack_batch_missing"]
    assert len(hits) == 1
    assert hits[0]["sequence"] == 3
    assert hits[0]["consumer_id"] == "archiver"
    # 水位仍指向 3，但链头只剩 2 → 水位越过链头
    assert "watermark_beyond_head" in issue_codes(body)


def test_verify_watermark_beyond_head(client):
    _, digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    tamper(
        client,
        "UPDATE device_consumers SET watermark = 5 "
        "WHERE device_id = ? AND consumer_id = 'archiver'",
        (DEVICE,),
    )
    body = verify(client, DEVICE, 1, 2).json()
    hits = [i for i in body["issues"] if i["issue_code"] == "watermark_beyond_head"]
    assert len(hits) == 1
    assert hits[0]["expected"] == 2 and hits[0]["actual"] == 5
    assert hits[0]["consumer_id"] == "archiver"


def test_verify_watermark_not_advanced(client):
    _, digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    # 水位被回拨到 1，但确认记录 1..3 都在 → 应按稀疏确认推进到 3
    tamper(
        client,
        "UPDATE device_consumers SET watermark = 1 "
        "WHERE device_id = ? AND consumer_id = 'archiver'",
        (DEVICE,),
    )
    body = verify(client, DEVICE, 1, 3).json()
    hits = [i for i in body["issues"] if i["issue_code"] == "watermark_not_advanced"]
    assert len(hits) == 1
    assert hits[0]["sequence"] == 2  # 首个未被水位覆盖的已确认序号
    assert hits[0]["expected"] == 3
    assert hits[0]["actual"] == 1


def test_verify_watermark_below_archived_through(client):
    _, digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    archive(client, DEVICE, 2)
    # 水位被改到归档水位之下（归档区间 1..2 的确认仍稀疏存在，推进重算会越过）
    tamper(
        client,
        "UPDATE device_consumers SET watermark = 1 "
        "WHERE device_id = ? AND consumer_id = 'archiver'",
        (DEVICE,),
    )
    body = verify(client, DEVICE, 1, 3).json()
    codes = issue_codes(body)
    below = [i for i in body["issues"] if i["issue_code"] == "watermark_below_archived_through"]
    assert len(below) == 1
    assert below[0]["expected"] == 2 and below[0]["actual"] == 1
    assert "watermark_not_advanced" in codes  # 确认 2、3 仍在，水位应已推进


def test_verify_watermark_gap_not_flagged(client):
    """乱序确认形成缺口时，水位停在首个缺口前是正确行为，不报问题。"""
    _, digests = upload_chain(client, DEVICE, 4)
    register(client, DEVICE, "archiver")
    # 跳过 seq2：水位只能到 1
    for seq in (1, 3, 4):
        ack(client, DEVICE, "archiver", seq, digests[seq - 1])
    st = client.get(f"/field-devices/{DEVICE}/archive-status").json()
    assert st["consumers"][0]["watermark"] == 1
    body = verify(client, DEVICE, 1, 4).json()
    assert body["ok"] is True
    assert "watermark_not_advanced" not in issue_codes(body)


# ---------------- 连续批次判定来源摘要 ----------------


def _create_judgment(client, device, key, frm, to):
    client.post("/library/import", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
    })
    return client.post(
        f"/field-devices/{device}/batch-judgments",
        json={"idempotency_key": key, "from_sequence": frm, "to_sequence": to},
    )


def test_verify_judgment_source_digest_mismatch(client):
    upload_chain(client, DEVICE, 3)
    r = _create_judgment(client, DEVICE, "k1", 1, 2)
    assert r.status_code == 201, r.text
    # 篡改判定来源里保存的 seq2 摘要
    tamper(
        client,
        "UPDATE batch_judgment_batches SET digest = ? "
        "WHERE sequence = 2",
        ("44" * 32,),
    )
    body = verify(client, DEVICE, 1, 3).json()
    hits = [
        i for i in body["issues"]
        if i["issue_code"] == "judgment_source_digest_mismatch"
    ]
    assert len(hits) == 1
    hit = hits[0]
    assert hit["sequence"] == 2
    assert hit["batch_judgment_id"] == 1
    assert hit["actual"] == "44" * 32
    # expected 为当前批次保存的摘要（链本身未被篡改）
    current = client.get(f"/field-devices/{DEVICE}/batches/2").json()["digest"]
    assert hit["expected"] == current
    # 链本身健康：无链式/重算问题
    assert "digest_mismatch" not in issue_codes(body)
    assert "previous_digest_mismatch" not in issue_codes(body)


def test_verify_judgment_source_detects_tampered_chain_batch(client):
    """链上批次摘要被篡改时，判定来源保存的摘要与当前摘要不一致。"""
    _, digests = upload_chain(client, DEVICE, 2)
    r = _create_judgment(client, DEVICE, "k1", 1, 2)
    assert r.status_code == 201, r.text
    tamper(
        client,
        "UPDATE device_batches SET digest = ? WHERE device_id = ? AND sequence = 1",
        ("55" * 32, DEVICE),
    )
    body = verify(client, DEVICE, 1, 2).json()
    codes = issue_codes(body)
    # 重算 ≠ 被篡改的保存摘要
    assert "digest_mismatch" in codes
    # 判定来源保存的（真）摘要 ≠ 当前（被篡改）摘要
    src = [
        i for i in body["issues"]
        if i["issue_code"] == "judgment_source_digest_mismatch"
    ]
    assert len(src) == 1
    assert src[0]["sequence"] == 1
    assert src[0]["expected"] == "55" * 32   # 当前批次摘要
    assert src[0]["actual"] == digests[0]    # 判定来源保存的摘要


def test_verify_judgment_source_batch_missing(client):
    _, digests = upload_chain(client, DEVICE, 4)
    r = _create_judgment(client, DEVICE, "k1", 1, 3)
    assert r.status_code == 201, r.text
    tamper(client, "DELETE FROM device_batches WHERE device_id = ? AND sequence = 3", (DEVICE,))
    body = verify(client, DEVICE, 4, 4).json()
    hits = [
        i for i in body["issues"]
        if i["issue_code"] == "judgment_source_batch_missing"
    ]
    assert len(hits) == 1
    assert hits[0]["sequence"] == 3
    assert hits[0]["batch_judgment_id"] == 1
    assert "sequence_gap" in issue_codes(body)


def test_verify_judgment_source_skeleton_fields(client):
    upload_chain(client, DEVICE, 2)
    r = _create_judgment(client, DEVICE, "k1", 1, 2)
    assert r.status_code == 201, r.text
    tamper(
        client,
        "UPDATE batch_judgment_batches SET batch_id = 'forged', "
        "previous_digest = ?, observation_count = 42 WHERE sequence = 1",
        ("66" * 32,),
    )
    body = verify(client, DEVICE, 1, 2).json()
    codes = issue_codes(body)
    assert "judgment_source_batch_id_mismatch" in codes
    assert "judgment_source_previous_digest_mismatch" in codes
    assert "judgment_source_observation_count_mismatch" in codes
    # 摘要字段未动 → 无摘要级问题
    assert "judgment_source_digest_mismatch" not in codes


# ---------------- 只读：发现问题不改数据 ----------------


def test_verify_does_not_modify_data(client):
    _, digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)
    forged = "77" * 32
    tamper(
        client,
        "UPDATE device_batches SET digest = ? WHERE device_id = ? AND sequence = 2",
        (forged, DEVICE),
    )
    tamper(
        client,
        "UPDATE device_consumers SET watermark = 0 "
        "WHERE device_id = ? AND consumer_id = 'archiver'",
        (DEVICE,),
    )
    before = client.get(f"/field-devices/{DEVICE}/batches/2").json()
    body = verify(client, DEVICE, 1, 3).json()
    assert body["ok"] is False
    assert body["issue_count"] >= 2
    # 再核验一次：问题清单一致（未被第一次核验修复）
    again = verify(client, DEVICE, 1, 3).json()
    assert again["issues"] == body["issues"]
    # 被篡改的数据保持原样：批次摘要仍是伪造值，水位仍是 0
    after = client.get(f"/field-devices/{DEVICE}/batches/2").json()
    assert after["digest"] == forged
    assert {k: v for k, v in after.items() if k != "last_replayed_at"} == {
        k: v for k, v in before.items() if k != "last_replayed_at"
    }
    st = client.get(f"/field-devices/{DEVICE}/archive-status").json()
    assert st["consumers"][0]["watermark"] == 0
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_sequence"] == 3 and cur["batch_count"] == 3


# ---------------- 并发：结果来自同一快照 ----------------


def test_verify_consistent_snapshot_during_concurrent_writes(client):
    """并发上传/确认/归档期间反复核验：每次结果都来自同一快照（恒 ok）。

    每个写操作都是单事务原子提交，核验的只读事务固定在某一快照上，
    因此任何一次核验都必须看到一条完全一致的链；快照上限（head_sequence）
    与快照链头摘要必须来自同一时刻。
    """
    n_initial, n_extra = 6, 6
    _, digests = upload_chain(client, DEVICE, n_initial)
    register(client, DEVICE, "archiver")
    ack_all(client, DEVICE, "archiver", digests)

    errors = []
    lock = threading.Lock()
    extra_digests = []

    def uploader():
        prev = digests[-1]
        for seq in range(n_initial + 1, n_initial + n_extra + 1):
            r = client.post(
                f"/field-devices/{DEVICE}/batches",
                json=batch(f"b-{seq}", seq, previous_digest=prev),
            )
            if r.status_code != 201:
                with lock:
                    errors.append(("upload", seq, r.status_code, r.text))
                return
            prev = r.json()["digest"]
            with lock:
                extra_digests.append(prev)

    def archiver():
        # 归档初始安全前缀（safe=6），与上传/核验并发
        r = archive(client, DEVICE, n_initial)
        if r.status_code not in (200, 201):
            with lock:
                errors.append(("archive", r.status_code, r.text))

    def confirmer():
        # 确认新上传的批次（乱序压力由上传线程串行保证存在性）
        for _ in range(50):
            cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
            head = cur["last_sequence"]
            if head > n_initial:
                d = client.get(f"/field-devices/{DEVICE}/batches/{head}").json()
                r = ack(client, DEVICE, "archiver", head, d["digest"])
                if r.status_code not in (200, 201):
                    with lock:
                        errors.append(("ack", head, r.status_code, r.text))
                    return

    threads = [
        threading.Thread(target=uploader),
        threading.Thread(target=archiver),
        threading.Thread(target=confirmer),
    ]
    for t in threads:
        t.start()

    known = {i + 1: d for i, d in enumerate(digests)}
    results = []
    while any(t.is_alive() for t in threads):
        r = verify(client, DEVICE, 1, n_initial)
        if r.status_code != 200:
            with lock:
                errors.append(("verify", r.status_code, r.text))
            break
        results.append(r.json())
    for t in threads:
        t.join()

    assert errors == []
    assert results  # 并发期间至少完成一次核验
    with lock:
        for i, d in enumerate(extra_digests, start=n_initial + 1):
            known[i] = d
    for body in results:
        # 每个快照都完全一致：链式追加/确认/归档均为单事务原子生效
        assert body["ok"] is True, body["issues"]
        snap = body["snapshot"]
        assert n_initial <= snap["head_sequence"] <= n_initial + n_extra
        # 快照上限与快照链头摘要来自同一时刻
        assert snap["head_digest"] == known[snap["head_sequence"]]
        assert snap["batch_count"] == snap["head_sequence"]
        assert snap["archived_through"] in (0, n_initial)
        # 核验范围 1..6 始终完整：归档后全部按墓碑核对
        assert body["checked_batches"] == n_initial
        assert body["recomputed_batches"] + body["archived_batches"] == n_initial

    # 收敛后再核验：全链 ok
    final = verify(client, DEVICE, 1, n_initial + n_extra).json()
    assert final["ok"] is True
    assert final["snapshot"]["head_sequence"] == n_initial + n_extra
