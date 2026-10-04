"""设备异常隔离与解除测试（应急闸门，不可变事件）。

覆盖：隔离幂等与链头核对、隔离期间六类处理接口 409 而查询/链核验/过期租约
回收可用、解除的三道闸门（链头未变、范围核验 ok、无未完成租约）、幂等键
占用与状态机（并发隔离/解除不越过状态）、状态与事件历史查询、空设备、
请求校验 4xx。
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
        c.db_path = str(tmp_path / "test.db")  # 便于直接篡改库验证解除闸门
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
    """上传 n 个链式批次，返回摘要列表（下标 0 = seq1）。"""
    digests = []
    prev = ""
    for seq in range(1, n + 1):
        r = client.post(
            f"/field-devices/{device}/batches",
            json=batch(f"b-{seq}", seq, previous_digest=prev),
        )
        assert r.status_code == 201, r.text
        digests.append(r.json()["digest"])
        prev = digests[-1]
    return digests


def register(client, device, consumer_id):
    return client.post(
        f"/field-devices/{device}/consumers", json={"consumer_id": consumer_id}
    )


def ack_all(client, device, consumer_id, digests):
    for seq, digest in enumerate(digests, start=1):
        r = client.post(
            f"/field-devices/{device}/acknowledgments",
            json={
                "consumer_id": consumer_id,
                "sequence": seq,
                "batch_digest": digest,
            },
        )
        assert r.status_code in (200, 201), r.text


def isolate(client, device, key, head_sequence, head_digest, reason="链核验发现摘要异常"):
    return client.post(
        f"/field-devices/{device}/isolation",
        json={
            "head_sequence": head_sequence,
            "head_digest": head_digest,
            "reason": reason,
            "idempotency_key": key,
        },
    )


def isolate_at_head(client, device, key, reason="链核验发现摘要异常"):
    """按当前游标隔离设备，返回响应。"""
    cur = client.get(f"/field-devices/{device}/cursor").json()
    return isolate(
        client, device, key, cur["last_sequence"], cur["head_digest"], reason
    )


def release(client, device, key, frm, to):
    return client.post(
        f"/field-devices/{device}/isolation/release",
        json={"from_sequence": frm, "to_sequence": to, "idempotency_key": key},
    )


def status(client, device):
    return client.get(f"/field-devices/{device}/isolation")


def tamper(client, sql, params=()):
    """直接改库模拟外部事故；隔离/解除必须只在事务内核对并拒绝。"""
    conn = sqlite3.connect(client.db_path)
    try:
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


# ---------------- 隔离：链头核对、幂等与状态 ----------------


def test_isolate_happy_path_and_status_query(client):
    digests = upload_chain(client, DEVICE, 3)
    r = isolate(client, DEVICE, "iso-1", 3, digests[2])
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == "isolated"
    assert body["device_id"] == DEVICE
    event = body["isolation"]
    assert event["event_id"] == 1
    assert event["action"] == "isolated"
    assert event["reason"] == "链核验发现摘要异常"
    assert event["idempotency_key"] == "iso-1"
    assert event["head_sequence"] == 3
    assert event["head_digest"] == digests[2]
    assert event["verify_from"] is None and event["verify_to"] is None
    assert event["verification"] is None
    assert event["created_at"] is not None

    st = status(client, DEVICE).json()
    assert st["isolated"] is True
    assert st["status"] == "isolated"
    assert st["current_isolation"]["event_id"] == event["event_id"]
    assert st["last_event"]["event_id"] == event["event_id"]
    assert st["event_count"] == 1


def test_isolate_idempotent_retry_same_key_same_params(client):
    digests = upload_chain(client, DEVICE, 2)
    first = isolate(client, DEVICE, "iso-1", 2, digests[1])
    assert first.status_code == 201
    retry = isolate(client, DEVICE, "iso-1", 2, digests[1])
    assert retry.status_code == 200
    assert retry.json() == first.json()
    assert status(client, DEVICE).json()["event_count"] == 1


def test_isolate_key_reuse_with_different_params_409(client):
    digests = upload_chain(client, DEVICE, 2)
    assert isolate(client, DEVICE, "iso-1", 2, digests[1]).status_code == 201
    r = isolate(client, DEVICE, "iso-1", 2, digests[1], reason="另一个原因")
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "isolation_conflict"
    assert detail["reason"] == "idempotency_key_in_use"
    assert detail["occupied_by_event"]["event_id"] == 1


def test_isolate_stale_chain_head_409(client):
    digests = upload_chain(client, DEVICE, 3)
    # 序号对但摘要不对
    r = isolate(client, DEVICE, "iso-1", 3, "00" * 32)
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "stale_chain_head"
    assert detail["expected_head_sequence"] == 3
    assert detail["expected_head_digest"] == digests[2]
    assert detail["provided_head_sequence"] == 3
    assert detail["provided_head_digest"] == "00" * 32
    # 序号落后（链头已推进）
    r = isolate(client, DEVICE, "iso-1", 2, digests[1])
    assert r.status_code == 409
    assert r.json()["detail"]["expected_head_sequence"] == 3
    # 设备未被隔离
    assert status(client, DEVICE).json()["isolated"] is False


def test_isolate_already_isolated_409(client):
    digests = upload_chain(client, DEVICE, 2)
    assert isolate(client, DEVICE, "iso-1", 2, digests[1]).status_code == 201
    r = isolate(client, DEVICE, "iso-2", 2, digests[1])
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "already_isolated"
    assert detail["current_isolation"]["idempotency_key"] == "iso-1"
    assert status(client, DEVICE).json()["event_count"] == 1


def test_isolate_empty_device_and_release_without_batches(client):
    r = isolate(client, "dev-empty", "iso-1", 0, "")
    assert r.status_code == 201, r.text
    assert r.json()["isolation"]["head_sequence"] == 0
    assert r.json()["isolation"]["head_digest"] == ""
    # 空设备隔离期间上传仍被挡住
    up = client.post(
        "/field-devices/dev-empty/batches", json=batch("b-1", 1)
    )
    assert up.status_code == 409
    assert up.json()["detail"]["error"] == "device_isolated"
    # 空链解除：跳过范围核验（verification 为 null）
    r = release(client, "dev-empty", "rel-1", 1, 1)
    assert r.status_code == 201, r.text
    assert r.json()["status"] == "released"
    assert r.json()["release"]["verification"] is None
    # 解除后上传恢复
    up = client.post("/field-devices/dev-empty/batches", json=batch("b-1", 1))
    assert up.status_code == 201


def test_isolate_request_validation_422(client):
    upload_chain(client, DEVICE, 1)
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    base = {
        "head_sequence": cur["last_sequence"],
        "head_digest": cur["head_digest"],
        "reason": "r",
        "idempotency_key": "k",
    }
    bad_payloads = [
        {**base, "head_digest": "zz" * 32},          # 非十六进制
        {**base, "head_digest": "ab" * 20},          # 长度不足 64
        {**base, "reason": "   "},                   # 空白原因
        {**base, "reason": ""},                      # 空原因
        {**base, "idempotency_key": "  "},           # 空白幂等键
        {**base, "head_sequence": -1},               # 负序号
        {**base, "head_sequence": 0},                # 0 序号却带摘要
        {**base, "head_sequence": 1, "head_digest": ""},  # 有序号却无摘要
    ]
    for payload in bad_payloads:
        r = client.post(f"/field-devices/{DEVICE}/isolation", json=payload)
        assert r.status_code == 422, payload
    # 大写摘要会被规范化为小写（与 previous_digest 规范化一致），合法但不匹配链头 → 409
    r = isolate(client, DEVICE, "iso-up", 1, "AB" * 32)
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "stale_chain_head"
    # 合法请求：大写形式的真链头摘要规范化后匹配 → 201
    r = isolate(client, DEVICE, "iso-1", 1, cur["head_digest"].upper())
    assert r.status_code == 201


# ---------------- 隔离期间：六类处理接口 409，查询/核验/回收可用 ----------------


@pytest.fixture()
def isolated_client(client):
    """已上传 4 批、登记消费者并隔离的设备；返回 (client, digests)。"""
    digests = upload_chain(client, DEVICE, 4)
    register(client, DEVICE, "archiver")
    r = isolate_at_head(client, DEVICE, "iso-1")
    assert r.status_code == 201, r.text
    return client, digests


def test_blocked_operations_during_isolation(isolated_client):
    client, digests = isolated_client

    # 上传批次（含相同载荷重放）被挡
    r = client.post(
        f"/field-devices/{DEVICE}/batches",
        json=batch("b-5", 5, previous_digest=digests[3]),
    )
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "device_isolated"
    assert detail["device_id"] == DEVICE
    assert detail["isolation_event_id"] == 1
    assert detail["isolation_reason"] == "链核验发现摘要异常"
    assert detail["head_sequence"] == 4
    assert detail["head_digest"] == digests[3]
    assert detail["isolated_at"] is not None
    replay = client.post(
        f"/field-devices/{DEVICE}/batches",
        json=batch("b-4", 4, previous_digest=digests[2]),
    )
    assert replay.status_code == 409
    assert replay.json()["detail"]["error"] == "device_isolated"

    # 消费者确认被挡
    r = client.post(
        f"/field-devices/{DEVICE}/acknowledgments",
        json={"consumer_id": "archiver", "sequence": 1, "batch_digest": digests[0]},
    )
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "device_isolated"

    # 归档被挡
    r = client.post(f"/field-devices/{DEVICE}/archive", json={"through_sequence": 1})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "device_isolated"

    # 租约领取被挡
    r = client.post(
        f"/field-devices/{DEVICE}/batch-leases",
        json={
            "consumer_id": "archiver",
            "max_batches": 2,
            "lease_duration_seconds": 60,
            "idempotency_key": "L1",
        },
    )
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "device_isolated"

    # 批次判定被挡
    client.post("/library/import", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
    })
    r = client.post(
        f"/field-devices/{DEVICE}/batch-judgments",
        json={"idempotency_key": "bj-1", "from_sequence": 1, "to_sequence": 2},
    )
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "device_isolated"


def test_lease_complete_blocked_during_isolation(client):
    digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    r = client.post(
        f"/field-devices/{DEVICE}/batch-leases",
        json={
            "consumer_id": "archiver",
            "max_batches": 2,
            "lease_duration_seconds": 600,
            "idempotency_key": "L1",
        },
    )
    assert r.status_code == 201
    lease_id = r.json()["lease"]["lease_id"]
    assert isolate_at_head(client, DEVICE, "iso-1").status_code == 201
    r = client.post(
        f"/field-devices/{DEVICE}/batch-leases/{lease_id}/complete",
        json={
            "consumer_id": "archiver",
            "items": [
                {"sequence": 1, "batch_digest": digests[0]},
                {"sequence": 2, "batch_digest": digests[1]},
            ],
        },
    )
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "device_isolated"


def test_queries_verify_and_lease_recover_available_during_isolation(isolated_client):
    client, digests = isolated_client
    # 全部查询接口在隔离期间仍可用
    assert client.get(f"/field-devices/{DEVICE}/cursor").status_code == 200
    assert client.get(f"/field-devices/{DEVICE}/batches").status_code == 200
    assert client.get(f"/field-devices/{DEVICE}/batches/1").status_code == 200
    assert client.get(f"/field-devices/{DEVICE}/archive-status").status_code == 200
    assert client.get(f"/field-devices/{DEVICE}/batch-leases").status_code == 200
    assert client.get(f"/field-devices/{DEVICE}/batch-judgments").status_code == 200
    assert status(client, DEVICE).status_code == 200
    # 链核验仍可用且结果一致
    r = client.post(
        f"/field-devices/{DEVICE}/verify-chain",
        json={"from_sequence": 1, "to_sequence": 4},
    )
    assert r.status_code == 200
    assert r.json()["ok"] is True
    # 消费者登记不受影响（不在阻断清单内）
    r = register(client, DEVICE, "ml")
    assert r.status_code == 201


def test_expired_lease_recover_available_during_isolation(client):
    digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    r = client.post(
        f"/field-devices/{DEVICE}/batch-leases",
        json={
            "consumer_id": "archiver",
            "max_batches": 2,
            "lease_duration_seconds": 600,
            "idempotency_key": "L1",
        },
    )
    lease_id = r.json()["lease"]["lease_id"]
    # 强制过期后隔离
    tamper(client, "UPDATE batch_leases SET expires_at = 1 WHERE id = ?", (lease_id,))
    assert isolate_at_head(client, DEVICE, "iso-1").status_code == 201
    # 隔离期间回收过期租约仍可用
    r = client.post(
        f"/field-devices/{DEVICE}/batch-leases/{lease_id}/recover",
        json={"consumer_id": "archiver"},
    )
    assert r.status_code == 201, r.text
    assert r.json()["status"] == "reclaimed"


def test_isolation_does_not_affect_other_devices(isolated_client):
    client, _ = isolated_client
    digests = upload_chain(client, "dev-02", 2)
    register(client, "dev-02", "archiver")
    r = client.post(
        "/field-devices/dev-02/acknowledgments",
        json={"consumer_id": "archiver", "sequence": 1, "batch_digest": digests[0]},
    )
    assert r.status_code == 201


# ---------------- 解除：三道闸门与幂等 ----------------


def test_release_happy_path_restores_flow(client):
    digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    assert isolate(client, DEVICE, "iso-1", 3, digests[2]).status_code == 201

    r = release(client, DEVICE, "rel-1", 1, 3)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == "released"
    event = body["release"]
    assert event["action"] == "released"
    assert event["idempotency_key"] == "rel-1"
    assert event["head_sequence"] == 3
    assert event["head_digest"] == digests[2]
    assert event["verify_from"] == 1 and event["verify_to"] == 3
    verification = event["verification"]
    assert verification["ok"] is True
    assert verification["checked_batches"] == 3
    assert verification["recomputed_batches"] == 3
    assert verification["snapshot"]["head_sequence"] == 3
    assert verification["snapshot"]["head_digest"] == digests[2]

    st = status(client, DEVICE).json()
    assert st["isolated"] is False
    assert st["status"] == "normal"
    assert st["current_isolation"] is None
    assert st["last_event"]["action"] == "released"
    assert st["event_count"] == 2

    # 原流程恢复：上传、确认、归档重新可用
    r = client.post(
        f"/field-devices/{DEVICE}/batches",
        json=batch("b-4", 4, previous_digest=digests[2]),
    )
    assert r.status_code == 201
    d4 = r.json()["digest"]
    r = client.post(
        f"/field-devices/{DEVICE}/acknowledgments",
        json={"consumer_id": "archiver", "sequence": 4, "batch_digest": d4},
    )
    assert r.status_code == 201


def test_release_idempotent_retry(client):
    digests = upload_chain(client, DEVICE, 2)
    assert isolate(client, DEVICE, "iso-1", 2, digests[1]).status_code == 201
    first = release(client, DEVICE, "rel-1", 1, 2)
    assert first.status_code == 201
    retry = release(client, DEVICE, "rel-1", 1, 2)
    assert retry.status_code == 200
    assert retry.json() == first.json()
    assert status(client, DEVICE).json()["event_count"] == 2


def test_release_not_isolated_409(client):
    upload_chain(client, DEVICE, 2)
    r = release(client, DEVICE, "rel-1", 1, 2)
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "isolation_conflict"
    assert detail["reason"] == "not_isolated"
    assert detail["last_event"] is None


def test_release_key_reuse_rules(client):
    digests = upload_chain(client, DEVICE, 3)
    assert isolate(client, DEVICE, "iso-1", 3, digests[2]).status_code == 201
    assert release(client, DEVICE, "rel-1", 1, 3).status_code == 201
    # 同键不同范围 → 键已占用
    r = release(client, DEVICE, "rel-1", 1, 2)
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "idempotency_key_in_use"
    # 再次隔离后，旧解除键即使参数相同也视为已占用（状态已推进）
    assert isolate(client, DEVICE, "iso-2", 3, digests[2]).status_code == 201
    r = release(client, DEVICE, "rel-1", 1, 3)
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "idempotency_key_in_use"
    # 旧隔离键同理
    r = isolate(client, DEVICE, "iso-1", 3, digests[2])
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "idempotency_key_in_use"
    # 新键正常解除
    assert release(client, DEVICE, "rel-2", 1, 3).status_code == 201


def test_release_chain_head_changed_409(client):
    digests = upload_chain(client, DEVICE, 3)
    assert isolate(client, DEVICE, "iso-1", 3, digests[2]).status_code == 201
    # 模拟外部事故：隔离期间链头被改动
    tamper(
        client,
        "UPDATE device_chains SET head_digest = ? WHERE device_id = ?",
        ("ff" * 32, DEVICE),
    )
    r = release(client, DEVICE, "rel-1", 1, 3)
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "chain_head_changed"
    assert detail["isolation_head_sequence"] == 3
    assert detail["isolation_head_digest"] == digests[2]
    assert detail["current_head_digest"] == "ff" * 32
    # 解除被拒绝：设备保持隔离
    assert status(client, DEVICE).json()["isolated"] is True
    assert status(client, DEVICE).json()["event_count"] == 1


def test_release_verification_failed_409(client):
    digests = upload_chain(client, DEVICE, 3)
    # 核验发现异常后隔离：篡改 seq2 保存的观测（链头字段不变，隔离可落）
    tamper(
        client,
        "UPDATE device_batches SET observations = ? WHERE device_id = ? AND sequence = 2",
        (json.dumps([{"local_id": "x001", "captured_at": 1_700_000_100.0}]), DEVICE),
    )
    assert isolate(client, DEVICE, "iso-1", 3, digests[2]).status_code == 201
    r = release(client, DEVICE, "rel-1", 1, 3)
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "verification_failed"
    assert detail["issue_count"] >= 1
    codes = [i["issue_code"] for i in detail["issues"]]
    assert "digest_mismatch" in codes
    # 设备保持隔离：上传仍被挡
    assert status(client, DEVICE).json()["isolated"] is True
    up = client.post(
        f"/field-devices/{DEVICE}/batches",
        json=batch("b-4", 4, previous_digest=digests[2]),
    )
    assert up.status_code == 409
    # 修复数据后可解除
    tamper(
        client,
        "UPDATE device_batches SET observations = ? WHERE device_id = ? AND sequence = 2",
        (json.dumps(obs(3)), DEVICE),
    )
    assert release(client, DEVICE, "rel-2", 1, 3).status_code == 201


def test_release_verification_range_invalid_409(client):
    digests = upload_chain(client, DEVICE, 2)
    assert isolate(client, DEVICE, "iso-1", 2, digests[1]).status_code == 201
    r = release(client, DEVICE, "rel-1", 1, 9)  # 右端越过链头
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "verification_range_invalid"
    assert detail["verification"]["error"] == "batches_not_found"
    assert status(client, DEVICE).json()["isolated"] is True


def test_release_blocked_by_outstanding_lease_then_reclaim_and_release(client):
    digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    r = client.post(
        f"/field-devices/{DEVICE}/batch-leases",
        json={
            "consumer_id": "archiver",
            "max_batches": 2,
            "lease_duration_seconds": 600,
            "idempotency_key": "L1",
        },
    )
    lease_id = r.json()["lease"]["lease_id"]
    assert isolate(client, DEVICE, "iso-1", 2, digests[1]).status_code == 201
    # 有未完成租约：拒绝解除
    r = release(client, DEVICE, "rel-1", 1, 2)
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "leases_outstanding"
    assert len(detail["outstanding_leases"]) == 1
    assert detail["outstanding_leases"][0]["lease_id"] == lease_id
    assert detail["outstanding_leases"][0]["expired"] is False
    # 强制过期 → 隔离期间回收 → 解除成功
    tamper(client, "UPDATE batch_leases SET expires_at = 1 WHERE id = ?", (lease_id,))
    r = client.post(
        f"/field-devices/{DEVICE}/batch-leases/{lease_id}/recover",
        json={"consumer_id": "archiver"},
    )
    assert r.status_code == 201
    r = release(client, DEVICE, "rel-2", 1, 2)
    assert r.status_code == 201, r.text


def test_release_request_validation_422(client):
    digests = upload_chain(client, DEVICE, 2)
    assert isolate(client, DEVICE, "iso-1", 2, digests[1]).status_code == 201
    assert release(client, DEVICE, "k", 3, 2).status_code == 422   # from > to
    assert release(client, DEVICE, "k", 0, 1).status_code == 422   # from < 1
    r = client.post(
        f"/field-devices/{DEVICE}/isolation/release",
        json={"from_sequence": 1, "to_sequence": 2, "idempotency_key": "  "},
    )
    assert r.status_code == 422
    r = client.post(f"/field-devices/{DEVICE}/isolation/release", json={})
    assert r.status_code == 422


# ---------------- 状态与事件历史查询 ----------------


def test_status_on_never_isolated_device(client):
    st = status(client, "dev-x").json()
    assert st["isolated"] is False
    assert st["status"] == "normal"
    assert st["current_isolation"] is None
    assert st["last_event"] is None
    assert st["event_count"] == 0
    events = client.get("/field-devices/dev-x/isolation/events").json()
    assert events["count"] == 0
    assert events["events"] == []
    assert events["next_cursor"] is None


def test_isolation_events_history_and_pagination(client):
    digests = upload_chain(client, DEVICE, 2)
    # 三个隔离-解除循环
    for i in range(1, 4):
        assert isolate(client, DEVICE, f"iso-{i}", 2, digests[1]).status_code == 201
        assert release(client, DEVICE, f"rel-{i}", 1, 2).status_code == 201
    st = status(client, DEVICE).json()
    assert st["isolated"] is False
    assert st["event_count"] == 6
    assert st["last_event"]["idempotency_key"] == "rel-3"

    page1 = client.get(
        f"/field-devices/{DEVICE}/isolation/events", params={"limit": 4}
    ).json()
    assert page1["count"] == 4
    assert [e["action"] for e in page1["events"]] == [
        "isolated", "released", "isolated", "released",
    ]
    assert page1["next_cursor"] == page1["events"][-1]["event_id"]
    page2 = client.get(
        f"/field-devices/{DEVICE}/isolation/events",
        params={"limit": 4, "after_id": page1["next_cursor"]},
    ).json()
    assert page2["count"] == 2
    assert page2["next_cursor"] is None
    ids = [e["event_id"] for e in page1["events"] + page2["events"]]
    assert ids == sorted(ids) and len(set(ids)) == 6
    # 解除事件带核验摘要，隔离事件带原因
    released = [e for e in page1["events"] if e["action"] == "released"][0]
    assert released["verification"]["ok"] is True
    assert released["reason"] is None
    isolated_ev = [e for e in page1["events"] if e["action"] == "isolated"][0]
    assert isolated_ev["reason"] == "链核验发现摘要异常"


# ---------------- 并发：隔离/解除不越过状态 ----------------


def test_concurrent_isolates_single_winner(client):
    digests = upload_chain(client, DEVICE, 3)
    results = []
    lock = threading.Lock()

    def do_isolate(key):
        r = isolate(client, DEVICE, key, 3, digests[2])
        with lock:
            results.append((key, r.status_code))

    threads = [threading.Thread(target=do_isolate, args=(f"iso-{i}",)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    codes = sorted(c for _, c in results)
    assert codes.count(201) == 1
    assert codes.count(409) == 5
    st = status(client, DEVICE).json()
    assert st["isolated"] is True
    assert st["event_count"] == 1


def test_concurrent_same_key_isolate_single_event(client):
    digests = upload_chain(client, DEVICE, 2)
    results = []
    lock = threading.Lock()

    def do_isolate():
        r = isolate(client, DEVICE, "iso-1", 2, digests[1])
        with lock:
            results.append(r.status_code)

    threads = [threading.Thread(target=do_isolate) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(results) == [200, 200, 200, 200, 201]
    assert status(client, DEVICE).json()["event_count"] == 1


def test_concurrent_releases_single_winner(client):
    digests = upload_chain(client, DEVICE, 2)
    assert isolate(client, DEVICE, "iso-1", 2, digests[1]).status_code == 201
    results = []
    lock = threading.Lock()

    def do_release(key):
        r = release(client, DEVICE, key, 1, 2)
        with lock:
            results.append(r.status_code)

    threads = [threading.Thread(target=do_release, args=(f"rel-{i}",)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(results).count(201) == 1
    assert sorted(results).count(409) == 4
    st = status(client, DEVICE).json()
    assert st["isolated"] is False
    assert st["event_count"] == 2  # 1 隔离 + 1 解除


def test_concurrent_isolate_and_release_never_skip_state(client):
    """隔离与解除并发：写锁串行化后状态机不被越过，最终状态必为二者之一且一致。"""
    digests = upload_chain(client, DEVICE, 2)
    outcomes = []
    lock = threading.Lock()

    def do_isolate():
        r = isolate(client, DEVICE, "iso-1", 2, digests[1])
        with lock:
            outcomes.append(("isolate", r.status_code))

    def do_release():
        r = release(client, DEVICE, "rel-1", 1, 2)
        with lock:
            outcomes.append(("release", r.status_code))

    t1 = threading.Thread(target=do_isolate)
    t2 = threading.Thread(target=do_release)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    st = status(client, DEVICE).json()
    by_action = dict(outcomes)
    if st["isolated"] is True:
        # 解除先到（not_isolated 409），隔离后落：最终隔离中
        assert by_action["release"] == 409
        assert by_action["isolate"] == 201
        assert st["event_count"] == 1
    else:
        # 隔离先落，解除随后成功：最终正常
        assert by_action["isolate"] == 201
        assert by_action["release"] == 201
        assert st["event_count"] == 2
