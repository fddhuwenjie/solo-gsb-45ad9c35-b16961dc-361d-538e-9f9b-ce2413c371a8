"""已登记消费者批次处理租约测试。

覆盖：领取（连续区间、≤20 上限、未归档、nothing_to_claim）、同消费者持约
期间同键重试返回原租约/他键 409、区间不会分给两个租约、完成时逐项核对摘要
（摘要变化/缺批次/顺序不符/过期/已回收整体拒绝且不推进）、完成写入确认推进
水位并释放租约、过期租约显式回收后才能重新领取、归档不清除租约占用明细
（含过期未回收）、租约查询（详情/分页/状态过滤）与并发领取。
"""
import os
import sqlite3
import sys
import threading
import time

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
        c.db_path = str(tmp_path / "test.db")
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
    digests = []
    prev = ""
    for seq in range(1, n + 1):
        r = client.post(
            f"/field-devices/{device}/batches",
            json=batch(f"b-{seq}", seq, previous_digest=prev),
        )
        assert r.status_code == 201
        digests.append(r.json()["digest"])
        prev = digests[-1]
    return digests


def register(client, device, consumer_id):
    return client.post(
        f"/field-devices/{device}/consumers", json={"consumer_id": consumer_id}
    )


def claim(client, device, consumer_id, *, key, max_batches=20, ttl=3600):
    return client.post(
        f"/field-devices/{device}/batch-leases",
        json={
            "consumer_id": consumer_id,
            "max_batches": max_batches,
            "lease_duration_seconds": ttl,
            "idempotency_key": key,
        },
    )


def complete(client, device, lease_id, consumer_id, items):
    return client.post(
        f"/field-devices/{device}/batch-leases/{lease_id}/complete",
        json={"consumer_id": consumer_id, "items": items},
    )


def recover(client, device, lease_id, consumer_id):
    return client.post(
        f"/field-devices/{device}/batch-leases/{lease_id}/recover",
        json={"consumer_id": consumer_id},
    )


def lease_items(lease_body):
    return [
        {"sequence": b["sequence"], "batch_digest": b["digest"]}
        for b in lease_body["batches"]
    ]


def force_expire(db_path, lease_id):
    """直接把租约到期时刻改到过去（测试确定性，不走时钟等待）。"""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE batch_leases SET expires_at = ? WHERE id = ?",
            (time.time() - 10, lease_id),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------- 领取 ----------------


def test_claim_requires_registered_consumer(client):
    upload_chain(client, DEVICE, 2)
    r = claim(client, DEVICE, "ghost", key="k1")
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "consumer_not_registered"
    assert detail["consumer_id"] == "ghost"
    # 未登记不产生租约
    lst = client.get(f"/field-devices/{DEVICE}/batch-leases").json()
    assert lst["count"] == 0


def test_claim_selects_contiguous_range_from_watermark_with_details(client):
    digests = upload_chain(client, DEVICE, 5)
    register(client, DEVICE, "archiver")

    r = claim(client, DEVICE, "archiver", key="lease-1", max_batches=3)
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "leased"
    lease = body["lease"]
    assert lease["status"] == "held"
    assert lease["consumer_id"] == "archiver"
    assert (lease["from_sequence"], lease["to_sequence"]) == (1, 3)
    assert lease["batch_count"] == 3
    assert lease["watermark_start"] == 0
    assert lease["lease_duration_seconds"] == 3600
    assert lease["expires_at"] > lease["granted_at"]
    assert lease["completed_at"] is None and lease["revoked_at"] is None

    seqs = [b["sequence"] for b in lease["batches"]]
    assert seqs == [1, 2, 3]
    for b, seq in zip(lease["batches"], (1, 2, 3)):
        assert b["digest"] == digests[seq - 1]
        assert b["lease_digest"] == digests[seq - 1]
        assert b["archived"] is False
        assert b["observation_count"] == len(b["observations"]) == 3
        assert b["thresholds"] == THRESHOLDS
        assert all(set(o) == {"local_id", "captured_at"} for o in b["observations"])

    # 领取不写确认、不推进水位
    st = client.get(f"/field-devices/{DEVICE}/archive-status").json()
    assert st["consumers"][0]["watermark"] == 0
    assert st["safe_sequence"] == 0
    assert [lr["lease_id"] for lr in st["held_leases"]] == [lease["lease_id"]]
    assert st["lease_bound"] == 0


def test_claim_continues_from_after_watermark(client):
    digests = upload_chain(client, DEVICE, 6)
    register(client, DEVICE, "archiver")

    r = claim(client, DEVICE, "archiver", key="a", max_batches=20)
    assert r.status_code == 201
    lid = r.json()["lease"]["lease_id"]
    complete(client, DEVICE, lid, "archiver", lease_items(r.json()["lease"]))
    assert client.get(
        f"/field-devices/{DEVICE}/archive-status"
    ).json()["consumers"][0]["watermark"] == 6

    # 水位已追平链头：暂无可领取批次
    r = claim(client, DEVICE, "archiver", key="b", max_batches=20)
    assert r.status_code == 200 and r.json()["status"] == "nothing_to_claim"

    # 设备再传 7、8 后重新领取，应恰好取到 7、8
    r7 = client.post(
        f"/field-devices/{DEVICE}/batches",
        json=batch("b-7", 7, previous_digest=digests[5],
                   observations=obs(3, base=1_700_000_100.0)),
    )
    client.post(
        f"/field-devices/{DEVICE}/batches",
        json=batch("b-8", 8, previous_digest=r7.json()["digest"],
                   observations=obs(3, base=1_700_000_110.0)),
    )
    r = claim(client, DEVICE, "archiver", key="c", max_batches=20)
    assert r.status_code == 201
    lease = r.json()["lease"]
    assert lease["watermark_start"] == 6
    assert [b["sequence"] for b in lease["batches"]] == [7, 8]


def test_claim_respects_max_batches_cap(client):
    upload_chain(client, DEVICE, 25)
    register(client, DEVICE, "archiver")
    r = claim(client, DEVICE, "archiver", key="a", max_batches=20)
    assert r.status_code == 201
    lease = r.json()["lease"]
    assert (lease["from_sequence"], lease["to_sequence"]) == (1, 20)
    assert len(lease["batches"]) == 20


@pytest.mark.parametrize(
    "payload",
    [
        {"consumer_id": "a", "max_batches": 0, "lease_duration_seconds": 10,
         "idempotency_key": "k"},
        {"consumer_id": "a", "max_batches": 21, "lease_duration_seconds": 10,
         "idempotency_key": "k"},
        {"consumer_id": "a", "max_batches": 5, "lease_duration_seconds": 0,
         "idempotency_key": "k"},
        {"consumer_id": "a", "max_batches": 5, "lease_duration_seconds": 3601,
         "idempotency_key": "k"},
        {"consumer_id": "a", "max_batches": 5, "lease_duration_seconds": 10,
         "idempotency_key": "   "},
    ],
)
def test_claim_request_validation(client, payload):
    r = client.post(f"/field-devices/{DEVICE}/batch-leases", json=payload)
    assert r.status_code == 422


def test_nothing_to_claim_creates_no_lease_and_is_repeatable(client):
    """水位已追平链头：领取不建租约、返回 200 nothing_to_claim，可反复请求。"""
    digests = upload_chain(client, DEVICE, 1)
    register(client, DEVICE, "archiver")
    lid = claim(client, DEVICE, "archiver", key="k").json()["lease"]["lease_id"]
    complete(
        client, DEVICE, lid, "archiver",
        [{"sequence": 1, "batch_digest": digests[0]}],
    )
    for key in ("retry-1", "retry-2"):
        r = claim(client, DEVICE, "archiver", key=key)
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "nothing_to_claim"
        assert body["lease"] is None
        assert body["watermark"] == 1 and body["head_sequence"] == 1
    # 没有多余租约（已完成的 1 条除外）
    lst = client.get(
        f"/field-devices/{DEVICE}/batch-leases", params={"status": "held"}
    ).json()
    assert lst["count"] == 0


# ---------------- 持约冲突与幂等 ----------------


def test_active_lease_same_key_returns_original_other_key_409(client):
    digests = upload_chain(client, DEVICE, 4)
    register(client, DEVICE, "archiver")
    r1 = claim(client, DEVICE, "archiver", key="k1", max_batches=2)
    assert r1.status_code == 201
    original = r1.json()

    # 同键重试：200 + 原租约（lease_id/区间/授予时刻不变）
    r2 = claim(client, DEVICE, "archiver", key="k1", max_batches=2)
    assert r2.status_code == 200
    again = r2.json()
    assert again["status"] == "held"
    assert again["lease"]["lease_id"] == original["lease"]["lease_id"]
    assert again["lease"]["granted_at"] == original["lease"]["granted_at"]
    assert again["lease"]["from_sequence"] == 1
    # 即使改了 max_batches/ttl 也返回原租约
    r2b = claim(client, DEVICE, "archiver", key="k1", max_batches=10, ttl=1)
    assert r2b.status_code == 200
    assert r2b.json()["lease"]["lease_id"] == original["lease"]["lease_id"]
    assert r2b.json()["lease"]["batch_count"] == 2

    # 他键：409 active_lease_exists，detail 附在持租约
    r3 = claim(client, DEVICE, "archiver", key="k2", max_batches=2)
    assert r3.status_code == 409
    detail = r3.json()["detail"]
    assert detail["reason"] == "active_lease_exists"
    assert detail["active_lease_id"] == original["lease"]["lease_id"]
    assert detail["expired"] is False
    assert detail["active_lease"]["from_sequence"] == 1

    # 区间没有分给第二个租约
    lst = client.get(f"/field-devices/{DEVICE}/batch-leases").json()
    assert lst["count"] == 1


def test_expired_lease_blocks_new_claim_until_reclaimed(client):
    upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    r1 = claim(client, DEVICE, "archiver", key="k1", max_batches=2, ttl=1)
    lid = r1.json()["lease"]["lease_id"]
    force_expire(client.db_path, lid)

    # 过期后他键 → 409 lease_expired
    r = claim(client, DEVICE, "archiver", key="k2")
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "lease_expired"
    assert detail["expired"] is True
    assert detail["active_lease_id"] == lid

    # 同键仍返回原租约（网络重试可拿回自己的租约）
    rsame = claim(client, DEVICE, "archiver", key="k1")
    assert rsame.status_code == 200
    assert rsame.json()["lease"]["lease_id"] == lid
    assert rsame.json()["lease"]["status"] == "expired"

    # 回收后可重新领取
    rr = recover(client, DEVICE, lid, "archiver")
    assert rr.status_code == 201 and rr.json()["status"] == "reclaimed"
    r2 = claim(client, DEVICE, "archiver", key="k2", max_batches=2)
    assert r2.status_code == 201
    assert [b["sequence"] for b in r2.json()["lease"]["batches"]] == [1, 2]


def test_recover_rules(client):
    upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    lid = claim(client, DEVICE, "archiver", key="k", ttl=1).json()["lease"]["lease_id"]

    # 未过期不能回收
    r = recover(client, DEVICE, lid, "archiver")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "lease_not_expired"

    # 别的消费者不能回收
    force_expire(client.db_path, lid)
    r = recover(client, DEVICE, lid, "someone-else")
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "lease_consumer_mismatch"

    # 正式回收 + 幂等重复回收
    assert recover(client, DEVICE, lid, "archiver").status_code == 201
    r = recover(client, DEVICE, lid, "archiver")
    assert r.status_code == 200 and r.json()["status"] == "already_reclaimed"

    # 完成一个已回收租约 → 409 lease_reclaimed
    r = complete(client, DEVICE, lid, "archiver",
                 [{"sequence": 1, "batch_digest": "00" * 32}])
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "lease_reclaimed"


# ---------------- 完成 ----------------


def test_complete_lease_checks_digests_advances_watermark_and_releases(client):
    digests = upload_chain(client, DEVICE, 4)
    register(client, DEVICE, "archiver")
    r = claim(client, DEVICE, "archiver", key="k", max_batches=3)
    lease = r.json()["lease"]
    lid = lease["lease_id"]

    # 缺批次：只交 2 项 → 409 items_mismatch，整体拒绝
    short = lease_items(lease)[:2]
    r = complete(client, DEVICE, lid, "archiver", short)
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "items_mismatch"
    assert detail["missing_sequences"] == [3]
    st = client.get(f"/field-devices/{DEVICE}/archive-status").json()
    assert st["consumers"][0]["watermark"] == 0
    assert len(st["held_leases"]) == 1

    # 多批次/乱序 → 409
    bad_order = list(reversed(lease_items(lease)))
    assert complete(client, DEVICE, lid, "archiver", bad_order).status_code == 409
    extra = lease_items(lease) + [{"sequence": 4, "batch_digest": digests[3]}]
    r = complete(client, DEVICE, lid, "archiver", extra)
    assert r.status_code == 409
    assert r.json()["detail"]["unexpected_sequences"] == [4]

    # 摘要变化 → 409 digest_changed，附租约摘要与提交摘要，整体不推进
    changed = lease_items(lease)
    changed[1]["batch_digest"] = "ab" * 32
    r = complete(client, DEVICE, lid, "archiver", changed)
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "digest_changed"
    assert detail["sequence"] == 2
    assert detail["provided_digest"] == "ab" * 32
    assert detail["lease_digest"] == digests[1]
    assert detail["current_digest"] == digests[1]
    st = client.get(f"/field-devices/{DEVICE}/archive-status").json()
    assert st["consumers"][0]["watermark"] == 0 and len(st["held_leases"]) == 1

    # 逐项正确 → 完成：写确认、推进水位到 3、释放租约
    r = complete(client, DEVICE, lid, "archiver", lease_items(lease))
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "completed"
    assert body["watermark"] == 3
    assert body["confirmed_count"] == 3
    assert body["lease"]["status"] == "completed"
    assert body["safe_sequence"] == 3
    st = client.get(f"/field-devices/{DEVICE}/archive-status").json()
    assert st["consumers"][0]["watermark"] == 3 and st["held_leases"] == []
    assert st["lease_bound"] is None

    # 重复完成 → 409 lease_already_completed
    r = complete(client, DEVICE, lid, "archiver", lease_items(lease))
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "lease_already_completed"

    # 区间已释放：可以立即领取下一段（4 起）
    r = claim(client, DEVICE, "archiver", key="k2")
    assert r.status_code == 201
    assert [b["sequence"] for b in r.json()["lease"]["batches"]] == [4]


def test_complete_expired_lease_rejected_wholesale(client):
    digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    r = claim(client, DEVICE, "archiver", key="k", ttl=1)
    lease = r.json()["lease"]
    force_expire(client.db_path, lease["lease_id"])

    r = complete(client, DEVICE, lease["lease_id"], "archiver", lease_items(lease))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "lease_expired"
    # 过期完成不写确认、不推进
    st = client.get(f"/field-devices/{DEVICE}/archive-status").json()
    assert st["consumers"][0]["watermark"] == 0
    acks = client.post(
        f"/field-devices/{DEVICE}/acknowledgments",
        json={"consumer_id": "archiver", "sequence": 1, "batch_digest": digests[0]},
    )
    assert acks.status_code == 201  # seq1 此前未被过期完成确认


def test_complete_with_manual_acks_still_advances(client):
    """租约期间部分批次已被手工确认：完成时幂等并入，水位仍越过整段。"""
    digests = upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    r = claim(client, DEVICE, "archiver", key="k", max_batches=3)
    lease = r.json()["lease"]
    # 乱序手工确认 2
    a = client.post(
        f"/field-devices/{DEVICE}/acknowledgments",
        json={"consumer_id": "archiver", "sequence": 2, "batch_digest": digests[1]},
    )
    assert a.status_code == 201 and a.json()["watermark"] == 0
    # 完成租约：1、2、3 全部到位，水位一次推进到 3
    r = complete(client, DEVICE, lease["lease_id"], "archiver", lease_items(lease))
    assert r.status_code == 200 and r.json()["watermark"] == 3


def test_complete_when_frozen_digest_no_longer_matches_current(client):
    """租约占用期间批次当前摘要被异常改动：即使提交的是租约冻结摘要也整体拒绝。"""
    digests = upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    r = claim(client, DEVICE, "archiver", key="k", max_batches=2)
    lease = r.json()["lease"]
    # 篡改批次 2 的当前摘要（模拟数据被旁路修改）
    conn = sqlite3.connect(client.db_path)
    conn.execute(
        "UPDATE device_batches SET digest = ? WHERE device_id = ? AND sequence = 2",
        ("ff" * 32, DEVICE),
    )
    conn.commit()
    conn.close()

    # 按租约冻结摘要完成：current_digest ≠ lease_digest → 409
    r = complete(client, DEVICE, lease["lease_id"], "archiver", lease_items(lease))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "digest_changed"
    assert detail["sequence"] == 2
    assert detail["lease_digest"] == digests[1]
    assert detail["current_digest"] == "ff" * 32
    assert client.get(
        f"/field-devices/{DEVICE}/archive-status"
    ).json()["consumers"][0]["watermark"] == 0


def test_complete_lease_ownership_and_existence(client):
    upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    lid = claim(client, DEVICE, "archiver", key="k").json()["lease"]["lease_id"]

    # 不存在 / 别的设备 → 404
    r = complete(client, DEVICE, 999, "archiver",
                 [{"sequence": 1, "batch_digest": "00" * 32}])
    assert r.status_code == 404
    assert r.json()["detail"]["error"] == "lease_not_found"
    r = complete(client, "dev-other", lid, "archiver",
                 [{"sequence": 1, "batch_digest": "00" * 32}])
    assert r.status_code == 404
    # 消费者不符 → 409 owner mismatch
    r = complete(client, DEVICE, lid, "thief",
                 [{"sequence": 1, "batch_digest": "00" * 32}])
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "lease_consumer_mismatch"


# ---------------- 归档不得清除租约占用明细 ----------------


def test_archive_blocked_by_held_lease_including_expired(client):
    digests = upload_chain(client, DEVICE, 5)
    register(client, DEVICE, "archiver")
    register(client, DEVICE, "ml")

    # archiver 手工确认到 4（safe_sequence 由未确认的 ml 挡在 0……）
    # 先让两个消费者都确认 1..4，再让 archiver 领取 5 的租约
    for cid in ("archiver", "ml"):
        for seq in range(1, 5):
            client.post(
                f"/field-devices/{DEVICE}/acknowledgments",
                json={"consumer_id": cid, "sequence": seq,
                      "batch_digest": digests[seq - 1]},
            )
    st = client.get(f"/field-devices/{DEVICE}/archive-status").json()
    assert st["safe_sequence"] == 4

    lease = claim(client, DEVICE, "archiver", key="k").json()["lease"]
    assert (lease["from_sequence"], lease["to_sequence"]) == (5, 5)

    # safe_sequence 仍允许归档 1..4；租约从 5 起，不挡 1..4
    r = client.post(f"/field-devices/{DEVICE}/archive",
                    json={"through_sequence": 4})
    assert r.status_code == 201
    assert r.json()["archived_through"] == 4

    # 两消费者确认 5 想把 safe 推到 5？archiver 的 5 尚在租约内（未完成），
    # ml 先确认 5 → safe=4。直接构造 archiver 手工确认 5：允许（租约不锁确认），
    # 此时 safe_sequence=5，但租约仍占用 5 → 归档 5 被租约挡住
    client.post(
        f"/field-devices/{DEVICE}/acknowledgments",
        json={"consumer_id": "ml", "sequence": 5, "batch_digest": digests[4]},
    )
    client.post(
        f"/field-devices/{DEVICE}/acknowledgments",
        json={"consumer_id": "archiver", "sequence": 5, "batch_digest": digests[4]},
    )
    st = client.get(f"/field-devices/{DEVICE}/archive-status").json()
    assert st["safe_sequence"] == 5
    assert st["lease_bound"] == 4
    r = client.post(f"/field-devices/{DEVICE}/archive",
                    json={"through_sequence": 5})
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "beyond_held_leases"
    assert [lr["lease_id"] for lr in detail["blocking_leases"]] == [lease["lease_id"]]
    assert detail["lease_bound"] == 4

    # 明细仍在（未被归档清除）
    detail5 = client.get(f"/field-devices/{DEVICE}/batches/5").json()
    assert detail5["archived"] is False and detail5["observations"] is not None

    # 租约过期但未回收：仍占用，归档继续被挡
    force_expire(client.db_path, lease["lease_id"])
    r = client.post(f"/field-devices/{DEVICE}/archive",
                    json={"through_sequence": 5})
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "beyond_held_leases"

    # 显式回收后（archiver 的确认已在），归档 5 成功并真正清除明细
    assert recover(client, DEVICE, lease["lease_id"], "archiver").status_code == 201
    r = client.post(f"/field-devices/{DEVICE}/archive",
                    json={"through_sequence": 5})
    assert r.status_code == 201 and r.json()["archived_through"] == 5
    detail5 = client.get(f"/field-devices/{DEVICE}/batches/5").json()
    assert detail5["archived"] is True and detail5["observations"] is None

    # 已回收/已完成租约不再出现在 held_leases
    st = client.get(f"/field-devices/{DEVICE}/archive-status").json()
    assert st["held_leases"] == [] and st["lease_bound"] is None


def test_overlapping_leases_for_different_consumers_allowed(client):
    upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "a")
    register(client, DEVICE, "b")
    ra = claim(client, DEVICE, "a", key="ka")
    rb = claim(client, DEVICE, "b", key="kb")
    assert ra.status_code == 201 and rb.status_code == 201
    assert ra.json()["lease"]["lease_id"] != rb.json()["lease"]["lease_id"]
    st = client.get(f"/field-devices/{DEVICE}/archive-status").json()
    assert {lr["consumer_id"] for lr in st["held_leases"]} == {"a", "b"}
    # a 完成不影响 b 的租约
    complete(client, DEVICE, ra.json()["lease"]["lease_id"], "a",
             lease_items(ra.json()["lease"]))
    st = client.get(f"/field-devices/{DEVICE}/archive-status").json()
    assert [lr["consumer_id"] for lr in st["held_leases"]] == ["b"]
    assert claim(client, DEVICE, "a", key="ka2").status_code == 200  # a 已追平链头


# ---------------- 查询 ----------------


def test_get_and_list_leases(client):
    upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    r = claim(client, DEVICE, "archiver", key="k1", max_batches=2)
    lid1 = r.json()["lease"]["lease_id"]

    got = client.get(f"/field-devices/{DEVICE}/batch-leases/{lid1}")
    assert got.status_code == 200
    assert got.json()["lease_id"] == lid1
    assert len(got.json()["batches"]) == 2

    assert client.get(f"/field-devices/{DEVICE}/batch-leases/999").status_code == 404
    assert client.get(f"/field-devices/dev-x/batch-leases/{lid1}").status_code == 404

    lst = client.get(f"/field-devices/{DEVICE}/batch-leases").json()
    assert lst["count"] == 1 and "batches" not in lst["leases"][0]
    assert lst["leases"][0]["lease_id"] == lid1
    assert lst["leases"][0]["status"] == "held"

    # 完成第一份租约，再领取/回收第二份，验证状态过滤
    complete(client, DEVICE, lid1, "archiver", lease_items(r.json()["lease"]))
    r2 = claim(client, DEVICE, "archiver", key="k2", ttl=1)
    lid2 = r2.json()["lease"]["lease_id"]
    force_expire(client.db_path, lid2)
    recover(client, DEVICE, lid2, "archiver")

    completed = client.get(
        f"/field-devices/{DEVICE}/batch-leases", params={"status": "completed"}
    ).json()
    assert [l["lease_id"] for l in completed["leases"]] == [lid1]
    revoked = client.get(
        f"/field-devices/{DEVICE}/batch-leases", params={"status": "revoked"}
    ).json()
    assert [l["lease_id"] for l in revoked["leases"]] == [lid2]
    held = client.get(
        f"/field-devices/{DEVICE}/batch-leases", params={"status": "held"}
    ).json()
    assert held["count"] == 0
    # consumer 过滤
    only = client.get(
        f"/field-devices/{DEVICE}/batch-leases",
        params={"consumer_id": "archiver"},
    ).json()
    assert only["count"] == 2
    assert client.get(
        f"/field-devices/{DEVICE}/batch-leases",
        params={"consumer_id": "nobody"},
    ).json()["count"] == 0


def test_expired_status_visible_in_queries(client):
    upload_chain(client, DEVICE, 2)
    register(client, DEVICE, "archiver")
    lid = claim(client, DEVICE, "archiver", key="k", ttl=1).json()["lease"]["lease_id"]
    force_expire(client.db_path, lid)
    got = client.get(f"/field-devices/{DEVICE}/batch-leases/{lid}").json()
    assert got["status"] == "expired"
    expired = client.get(
        f"/field-devices/{DEVICE}/batch-leases", params={"status": "expired"}
    ).json()
    assert [l["lease_id"] for l in expired["leases"]] == [lid]


# ---------------- 并发 ----------------


def test_concurrent_claims_one_lease_wins(client):
    upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    n = 10
    barrier = threading.Barrier(n)
    results = []
    lock = threading.Lock()

    def worker(i):
        barrier.wait()
        r = claim(client, DEVICE, "archiver", key=f"key-{i}")
        with lock:
            results.append(r.status_code)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(201) == 1
    assert results.count(409) == n - 1
    lst = client.get(f"/field-devices/{DEVICE}/batch-leases").json()
    assert lst["count"] == 1


def test_concurrent_same_key_claims_single_lease(client):
    upload_chain(client, DEVICE, 3)
    register(client, DEVICE, "archiver")
    n = 10
    barrier = threading.Barrier(n)
    results = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        r = claim(client, DEVICE, "archiver", key="same-key")
        with lock:
            results.append((r.status_code, r.json()["lease"]["lease_id"]))

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count((201, results[0][1])) >= 1
    assert len({lid for _, lid in results}) == 1
    assert sorted(code for code, _ in results) == [200] * (n - 1) + [201]
