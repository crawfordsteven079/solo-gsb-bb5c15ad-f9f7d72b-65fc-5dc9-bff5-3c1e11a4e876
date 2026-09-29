"""批量取件：整批原子消费、形状拒绝、失败原因、并发互斥、缓存不可用、审计入链。"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
import redis.exceptions
from httpx import AsyncClient
from sqlalchemy import select

from app.models import AuditLog, Delivery
from app.redis_client import DenyCache
from app.store import ACTIVE, CONSUMED
from app.utils import hash_token, sha256_hex

pytestmark = pytest.mark.asyncio


async def _create_text(client: AsyncClient, text: str = "秘密内容", ttl: int = 3600) -> dict:
    resp = await client.post("/api/v1/deliveries", json={"text": text, "ttl_seconds": ttl})
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _create_file(client: AsyncClient, payload: bytes = b"\x89PNG file", ttl: int = 3600) -> dict:
    resp = await client.post(
        "/api/v1/deliveries/file",
        data={"ttl_seconds": str(ttl)},
        files={"file": ("f.bin", payload, "application/octet-stream")},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _create_slot(client: AsyncClient, ttl: int = 3600) -> dict:
    resp = await client.post("/api/v1/blind-slots", json={"ttl_seconds": ttl})
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _create_submitted_slot(client: AsyncClient, text: str = "盲投文本") -> dict:
    slot = await _create_slot(client)
    submit = await client.post(slot["submit_url"], json={"text": text})
    assert submit.status_code == 200, submit.text
    return slot


async def _batch(client: AsyncClient, tokens: list[str]):
    return await client.post("/api/v1/batch-pickup", json={"tokens": tokens})


async def _status_of(db, delivery_id) -> str:
    async with db.session_factory() as session:
        row = await session.get(Delivery, delivery_id)
        return row.status


# ---------------- 整批成功 ----------------


async def test_batch_success_mixed_normal_and_blind(client: AsyncClient, db):
    d1 = await _create_text(client, text="第一份")
    slot = await _create_submitted_slot(client, text="盲投第二份")
    d3 = await _create_text(client, text="第三份")

    tokens = [d1["token"], slot["pickup_token"], d3["token"]]
    resp = await _batch(client, tokens)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["result"] == "success"
    items = body["items"]
    assert [i["position"] for i in items] == [1, 2, 3]
    # 按输入顺序返回投递 ID、文本与内容哈希
    assert [i["delivery_id"] for i in items] == [
        d1["delivery_id"], slot["slot_id"], d3["delivery_id"]
    ]
    assert [i["content"] for i in items] == ["第一份", "盲投第二份", "第三份"]
    for item, text in zip(items, ["第一份", "盲投第二份", "第三份"]):
        assert item["content_sha256"] == sha256_hex(text.encode("utf-8"))

    # 每份都已消费：单件取件得到 consumed
    for token in tokens:
        again = await client.post(f"/api/v1/pickup/{token}")
        blind_again = await client.post(f"/api/v1/blind-slots/pickup/{token}")
        assert again.status_code in (410, 404)
        assert blind_again.status_code in (410, 404)
    assert await _status_of(db, d1["delivery_id"]) == CONSUMED
    assert await _status_of(db, slot["slot_id"]) == CONSUMED
    assert await _status_of(db, d3["delivery_id"]) == CONSUMED


async def test_batch_of_one_and_twenty_boundary(client: AsyncClient):
    single = await _create_text(client, text="单件批次")
    resp = await _batch(client, [single["token"]])
    assert resp.status_code == 200
    assert resp.json()["items"][0]["content"] == "单件批次"

    many = [await _create_text(client, text=f"第{i}份") for i in range(20)]
    resp = await _batch(client, [d["token"] for d in many])
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["items"]) == 20
    assert [i["position"] for i in resp.json()["items"]] == list(range(1, 21))


# ---------------- 形状拒绝：空列表 / 超限 / 重复 ----------------


async def test_batch_rejects_empty_list(client: AsyncClient):
    resp = await _batch(client, [])
    assert resp.status_code == 422


async def test_batch_rejects_over_limit(client: AsyncClient):
    resp = await _batch(client, [f"tok-{i}" for i in range(21)])
    assert resp.status_code == 422


async def test_batch_rejects_duplicate_tokens(client: AsyncClient):
    d = await _create_text(client)
    resp = await _batch(client, [d["token"], "other", d["token"]])
    assert resp.status_code == 422
    # 整批拒绝：投递未被消费
    pickup = await client.post(f"/api/v1/pickup/{d['token']}")
    assert pickup.status_code == 200


async def test_batch_shape_rejections_leave_deliveries_untouched(client: AsyncClient, db):
    d = await _create_text(client)
    for bad in ([], [d["token"], d["token"]], [f"x{i}" for i in range(21)]):
        resp = await _batch(client, bad)
        assert resp.status_code == 422
    assert await _status_of(db, d["delivery_id"]) == ACTIVE


# ---------------- 失败原因：只报失败项，不消费其余项 ----------------


async def test_batch_unknown_token_fails_whole_batch(client: AsyncClient, db):
    d = await _create_text(client, text="连坐内容")
    resp = await _batch(client, [d["token"], "no-such-token"])
    assert resp.status_code == 409, resp.text
    body = resp.json()
    assert body["result"] == "failed"
    assert body["failures"] == [{"position": 2, "reason": "unknown"}]
    assert "items" not in body
    # 不消费其余项
    assert await _status_of(db, d["delivery_id"]) == ACTIVE
    pickup = await client.post(f"/api/v1/pickup/{d['token']}")
    assert pickup.status_code == 200
    assert pickup.json()["content"] == "连坐内容"


async def test_batch_file_delivery_is_type_mismatch(client: AsyncClient, db):
    f = await _create_file(client)
    t = await _create_text(client)
    resp = await _batch(client, [f["token"], t["token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 1, "reason": "type_mismatch"}]
    # 文本未被连坐消费；文件仍可走单件接口下载
    assert await _status_of(db, t["delivery_id"]) == ACTIVE
    assert await _status_of(db, f["delivery_id"]) == ACTIVE
    single = await client.post(f"/api/v1/pickup/{f['token']}")
    assert single.status_code == 200
    assert single.headers["x-drop-result"] == "success"


async def test_batch_submit_token_is_unknown(client: AsyncClient):
    slot = await _create_submitted_slot(client)
    resp = await _batch(client, [slot["submit_token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 1, "reason": "unknown"}]
    # 盲槽本身不受影响，收件人仍可正常取件
    pickup = await client.post(slot["pickup_url"])
    assert pickup.status_code == 200


async def test_batch_blind_slot_with_file_is_type_mismatch(client: AsyncClient):
    slot = await _create_slot(client)
    submit = await client.post(
        slot["submit_url"] + "/file",
        files={"file": ("盲.bin", b"\x89PNG blind", "application/octet-stream")},
    )
    assert submit.status_code == 200

    resp = await _batch(client, [slot["pickup_token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 1, "reason": "type_mismatch"}]
    # 文件未被消费，仍可经盲槽单件接口取走
    pickup = await client.post(slot["pickup_url"])
    assert pickup.status_code == 200
    assert pickup.content == b"\x89PNG blind"


async def test_batch_pending_slot_reports_pending(client: AsyncClient):
    slot = await _create_slot(client)
    resp = await _batch(client, [slot["pickup_token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 1, "reason": "pending"}]
    # 空槽不被消费：提交后仍可正常取件
    await client.post(slot["submit_url"], json={"text": "后来提交"})
    pickup = await client.post(slot["pickup_url"])
    assert pickup.status_code == 200
    assert pickup.json()["content"] == "后来提交"


async def test_batch_expired_reports_expired(client: AsyncClient, db):
    d = await _create_text(client, ttl=1)
    async with db.session_factory() as session:
        row = await session.get(Delivery, d["delivery_id"])
        row.expires_at = datetime.now(timezone.utc) - timedelta(seconds=5)
        await session.commit()

    resp = await _batch(client, [d["token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 1, "reason": "expired"}]
    # 惰性过期已落库
    assert await _status_of(db, d["delivery_id"]) == "expired"


async def test_batch_expiry_boundary_moment_is_expired(client: AsyncClient, db):
    """到期时刻等于裁决时刻视为过期：expires_at 不晚于 now 即不可取。"""
    d = await _create_text(client, ttl=60)
    async with db.session_factory() as session:
        row = await session.get(Delivery, d["delivery_id"])
        row.expires_at = datetime.now(timezone.utc)  # 裁决时刻必然不早于它
        await session.commit()

    resp = await _batch(client, [d["token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 1, "reason": "expired"}]


async def test_batch_consumed_and_revoked_reasons(client: AsyncClient):
    consumed = await _create_text(client)
    await client.post(f"/api/v1/pickup/{consumed['token']}")
    revoked = await _create_text(client)
    await client.post(f"/api/v1/revoke/{revoked['token']}")

    resp = await _batch(client, [consumed["token"], revoked["token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [
        {"position": 1, "reason": "consumed"},
        {"position": 2, "reason": "revoked"},
    ]


async def test_batch_mixed_failures_report_all_positions(client: AsyncClient, db):
    unknown = "no-such-token"
    expired = await _create_text(client, ttl=1)
    async with db.session_factory() as session:
        row = await session.get(Delivery, expired["delivery_id"])
        row.expires_at = datetime.now(timezone.utc) - timedelta(seconds=5)
        await session.commit()
    consumed = await _create_text(client)
    await client.post(f"/api/v1/pickup/{consumed['token']}")
    revoked = await _create_text(client)
    await client.post(f"/api/v1/revoke/{revoked['token']}")
    file_d = await _create_file(client)
    pending_slot = await _create_slot(client)
    good = await _create_text(client, text="无辜文本")

    tokens = [
        unknown,
        expired["token"],
        consumed["token"],
        revoked["token"],
        file_d["token"],
        pending_slot["pickup_token"],
        good["token"],
    ]
    resp = await _batch(client, tokens)
    assert resp.status_code == 409, resp.text
    body = resp.json()
    assert body["result"] == "failed"
    assert body["failures"] == [
        {"position": 1, "reason": "unknown"},
        {"position": 2, "reason": "expired"},
        {"position": 3, "reason": "consumed"},
        {"position": 4, "reason": "revoked"},
        {"position": 5, "reason": "type_mismatch"},
        {"position": 6, "reason": "pending"},
    ]
    # 失败响应不含任何内容；唯一可取的文本未被连坐消费
    assert "content" not in resp.text
    assert await _status_of(db, good["delivery_id"]) == ACTIVE


# ---------------- 并发：重叠批次 / 单件取件 / 撤回互斥 ----------------


async def test_concurrent_overlapping_batches_single_winner(client: AsyncClient, db):
    shared = await _create_text(client, text="共享内容")
    uniques = [await _create_text(client, text=f"独有-{i}") for i in range(10)]

    async def run_batch(tokens):
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post("/api/v1/batch-pickup", json={"tokens": tokens})

    results = await asyncio.gather(
        *[run_batch([shared["token"], u["token"]]) for u in uniques]
    )
    winners = [r for r in results if r.status_code == 200]
    losers = [r for r in results if r.status_code == 409]
    assert len(winners) == 1
    assert len(losers) == 9
    # 赢家的两件都按序返回
    witems = winners[0].json()["items"]
    assert [i["content"] for i in witems][0] == "共享内容"
    # 每个失败批次都只在共享令牌上报 consumed，且自己的独有令牌未被消费
    for r in losers:
        assert r.json()["failures"] == [{"position": 1, "reason": "consumed"}]

    winner_unique = witems[1]["delivery_id"]
    consumed_count = 0
    for u in uniques:
        status = await _status_of(db, u["delivery_id"])
        if u["delivery_id"] == winner_unique:
            assert status == CONSUMED
            consumed_count += 1
        else:
            assert status == ACTIVE  # 失败批次不留部分消费
    assert consumed_count == 1
    assert await _status_of(db, shared["delivery_id"]) == CONSUMED


async def test_concurrent_identical_batches_exactly_one_wins(client: AsyncClient):
    d1 = await _create_text(client, text="甲")
    d2 = await _create_text(client, text="乙")
    tokens = [d1["token"], d2["token"]]

    async def run_batch():
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post("/api/v1/batch-pickup", json={"tokens": tokens})

    results = await asyncio.gather(*[run_batch() for _ in range(20)])
    winners = [r for r in results if r.status_code == 200]
    losers = [r for r in results if r.status_code == 409]
    assert len(winners) == 1
    assert len(losers) == 19
    for r in losers:
        assert r.json()["failures"] == [
            {"position": 1, "reason": "consumed"},
            {"position": 2, "reason": "consumed"},
        ]


async def test_concurrent_batch_vs_single_pickup_is_mutex(client: AsyncClient, db):
    d1 = await _create_text(client, text="争抢点")
    d2 = await _create_text(client, text="批次另一件")

    async def single():
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post(f"/api/v1/pickup/{d1['token']}")

    async def batch():
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post(
                "/api/v1/batch-pickup", json={"tokens": [d1["token"], d2["token"]]}
            )

    single_resp, batch_resp = await asyncio.gather(single(), batch())
    if single_resp.status_code == 200:
        # 单件赢：整批失败且另一件未被连坐消费
        assert batch_resp.status_code == 409
        assert batch_resp.json()["failures"] == [{"position": 1, "reason": "consumed"}]
        assert await _status_of(db, d2["delivery_id"]) == ACTIVE
    else:
        # 批次赢：单件得到 consumed，两件都已被批次消费
        assert single_resp.status_code == 410
        assert batch_resp.status_code == 200
        assert await _status_of(db, d2["delivery_id"]) == CONSUMED
    assert await _status_of(db, d1["delivery_id"]) == CONSUMED


async def test_concurrent_batch_vs_revoke_is_mutex(client: AsyncClient, db):
    d1 = await _create_text(client, text="撤回竞争点")
    d2 = await _create_text(client, text="批次另一件")

    async def revoke():
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post(f"/api/v1/revoke/{d1['token']}")

    async def batch():
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post(
                "/api/v1/batch-pickup", json={"tokens": [d1["token"], d2["token"]]}
            )

    revoke_resp, batch_resp = await asyncio.gather(revoke(), batch())
    if revoke_resp.status_code == 200 and revoke_resp.json()["result"] == "revoked":
        # 撤回赢：整批失败报 revoked，另一件未被消费
        assert batch_resp.status_code == 409
        assert batch_resp.json()["failures"] == [{"position": 1, "reason": "revoked"}]
        assert await _status_of(db, d2["delivery_id"]) == ACTIVE
    else:
        # 批次赢：撤回得到 consumed
        assert batch_resp.status_code == 200
        assert revoke_resp.status_code == 410
        assert revoke_resp.json()["result"] == "consumed"
        assert await _status_of(db, d2["delivery_id"]) == CONSUMED


# ---------------- 缓存不可用仍遵守全有或全无 ----------------


class _BrokenRedis:
    """所有操作都抛错的 Redis 替身，模拟缓存宕机。"""

    async def get(self, *args, **kwargs):
        raise redis.exceptions.RedisError("redis down")

    async def eval(self, *args, **kwargs):
        raise redis.exceptions.RedisError("redis down")

    async def set(self, *args, **kwargs):
        raise redis.exceptions.RedisError("redis down")

    async def ping(self):
        raise redis.exceptions.RedisError("redis down")

    async def aclose(self):
        return None


async def test_batch_all_or_nothing_when_cache_unavailable(client: AsyncClient, db, cache):
    from app.main import app

    app.state.cache = DenyCache(_BrokenRedis())
    try:
        d1 = await _create_text(client, text="缓存宕机也要原子-1")
        d2 = await _create_text(client, text="缓存宕机也要原子-2")

        # 失败批次：不消费任何一项
        resp = await _batch(client, [d1["token"], "no-such-token", d2["token"]])
        assert resp.status_code == 409
        assert resp.json()["failures"] == [{"position": 2, "reason": "unknown"}]
        assert await _status_of(db, d1["delivery_id"]) == ACTIVE
        assert await _status_of(db, d2["delivery_id"]) == ACTIVE

        # 成功批次：一起消费
        ok = await _batch(client, [d1["token"], d2["token"]])
        assert ok.status_code == 200
        assert [i["content"] for i in ok.json()["items"]] == [
            "缓存宕机也要原子-1",
            "缓存宕机也要原子-2",
        ]
        assert await _status_of(db, d1["delivery_id"]) == CONSUMED
        assert await _status_of(db, d2["delivery_id"]) == CONSUMED
    finally:
        app.state.cache = cache


# ---------------- 审计入链 ----------------


async def test_batch_results_are_chained_without_plaintext(client: AsyncClient, db):
    d1 = await _create_text(client, text="审计批次内容-甲")
    slot = await _create_submitted_slot(client, text="审计批次内容-乙")
    ok_tokens = [d1["token"], slot["pickup_token"]]

    ok = await _batch(client, ok_tokens)
    assert ok.status_code == 200
    failed = await _batch(client, [d1["token"], "no-such-token"])
    assert failed.status_code == 409

    async with db.session_factory() as session:
        rows = (
            (await session.execute(
                select(AuditLog).where(AuditLog.action == "batch_pickup")
            ))
            .scalars()
            .all()
        )
    # 成功批次每项一条 success；失败批次每个失败项一条对应原因
    results = sorted(r.result for r in rows)
    assert results == ["consumed", "success", "success", "unknown"]
    for r in rows:
        assert r.detail["batch_id"]
        assert r.detail["batch_size"] == 2
        assert r.detail["position"] in (1, 2)
        # 不记录明文令牌或内容
        for token in ok_tokens:
            assert r.token_hash != token
            assert token not in str(r.detail)
        assert "审计批次内容" not in str(r.detail)
        if r.result == "success":
            assert r.delivery_id is not None
            assert r.token_hash in {hash_token(t) for t in ok_tokens}
    # 未知令牌的失败项只留哈希、无 delivery_id
    unknown_row = next(r for r in rows if r.result == "unknown")
    assert unknown_row.delivery_id is None
    assert unknown_row.token_hash == hash_token("no-such-token")

    # 全链校验通过
    verify = await client.get("/api/v1/audit/verify")
    assert verify.status_code == 200
    assert verify.json()["ok"] is True
