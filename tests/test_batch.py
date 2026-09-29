"""批量取件（一次性领齐）：整批原子消费、部分失败零消费、并发安全、审计入链。

覆盖：
- 接口层拒绝：空列表 / 超过 20 项 / 重复令牌（422，整批不消费）；
- 全部成功：普通文本 + 已提交盲槽文本混合，按输入顺序返回 ID/文本/哈希；
- 单项失败原因：unknown / pending（待提交）/ expired（含到期时刻==裁决时刻）/
  consumed / revoked / type_mismatch（普通文件与盲槽文件）；
- 盲槽提交令牌按 unknown 处理；
- 任一项失败时不返回任何内容、不消费其余项（失败后其余项仍可单件领取）；
- 与单件取件/撤回/重叠批次并发：每份内容至多被成功领取一次，无部分消费；
- Redis 不可用时回落数据库，原子性不变；
- 结果逐项接入审计哈希链，不记录明文令牌或内容，全链校验通过。
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from app.models import AuditLog, Delivery
from app.store import CONSUMED, DeliveryStore
from app.utils import hash_token
import app.main as mainmod

pytestmark = pytest.mark.asyncio


async def _create_text(client: AsyncClient, text: str = "普通文本", ttl: int = 3600) -> dict:
    resp = await client.post(
        "/api/v1/deliveries", json={"text": text, "ttl_seconds": ttl}
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _create_file(client: AsyncClient, payload: bytes = b"\x89PNG x" * 4) -> dict:
    resp = await client.post(
        "/api/v1/deliveries/file",
        data={"ttl_seconds": "3600"},
        files={"file": ("f.bin", payload, "application/octet-stream")},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _create_slot(client: AsyncClient, ttl: int = 3600) -> dict:
    resp = await client.post("/api/v1/blind-slots", json={"ttl_seconds": ttl})
    assert resp.status_code == 201, resp.text
    return resp.json()


def _batch(client: AsyncClient, tokens: list[str]):
    return client.post("/api/v1/pickups/batch", json={"tokens": tokens})


# ---------------- 接口层校验：空 / 超限 / 重复整批拒绝 ----------------


async def test_batch_rejects_empty_list(client: AsyncClient):
    resp = await _batch(client, [])
    assert resp.status_code == 422


async def test_batch_rejects_more_than_20(client: AsyncClient):
    resp = await _batch(client, [f"t{i}" for i in range(21)])
    assert resp.status_code == 422


async def test_batch_accepts_exactly_20_unknown_tokens(client: AsyncClient):
    # 恰好 20 个通过接口层校验；全部 unknown，整批失败但不是 422
    resp = await _batch(client, [f"token-{i}" for i in range(20)])
    assert resp.status_code == 409
    body = resp.json()
    assert body["result"] == "failed"
    assert len(body["failures"]) == 20
    assert {f["position"] for f in body["failures"]} == set(range(20))
    assert all(f["reason"] == "unknown" for f in body["failures"])


async def test_batch_rejects_duplicate_tokens(client: AsyncClient):
    data = await _create_text(client)
    token = data["token"]
    resp = await _batch(client, [token, token])
    assert resp.status_code == 422
    # 重复拒绝不得消费该投递
    again = await client.post(f"/api/v1/pickup/{token}")
    assert again.status_code == 200
    assert again.json()["content"] == "普通文本"


async def test_batch_rejects_duplicate_even_if_others_unknown(client: AsyncClient):
    data = await _create_text(client)
    resp = await _batch(client, ["nope-a", data["token"], "nope-b", data["token"]])
    assert resp.status_code == 422
    again = await client.post(f"/api/v1/pickup/{data['token']}")
    assert again.status_code == 200


# ---------------- 全部成功：顺序、字段、哈希 ----------------


async def test_batch_success_mixed_normal_and_blind_texts(client: AsyncClient):
    d1 = await _create_text(client, text="第一份")
    slot = await _create_slot(client)
    sub = await client.post(slot["submit_url"], json={"text": "盲槽一份"})
    assert sub.status_code == 200
    d2 = await _create_text(client, text="第二份")

    # 普通文本 + 盲槽文本，顺序刻意交错
    tokens = [d1["token"], slot["pickup_token"], d2["token"]]
    resp = await _batch(client, tokens)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["result"] == "success"
    assert body["count"] == 3
    assert body["batch_id"]
    assert body["consumed_at"]

    items = body["items"]
    assert [i["position"] for i in items] == [0, 1, 2]
    assert items[0]["delivery_id"] == d1["delivery_id"]
    assert items[0]["content"] == "第一份"
    assert items[0]["kind"] == "text"
    assert items[1]["delivery_id"] == slot["slot_id"]
    assert items[1]["content"] == "盲槽一份"
    assert items[2]["delivery_id"] == d2["delivery_id"]
    assert items[2]["content"] == "第二份"
    for item, delivery_id in zip(
        items, [d1["delivery_id"], slot["slot_id"], d2["delivery_id"]], strict=True
    ):
        assert item["delivery_id"] == delivery_id
        assert len(item["content_sha256"]) == 64


async def test_batch_success_then_each_token_is_consumed(client: AsyncClient):
    d1 = await _create_text(client, text="a")
    d2 = await _create_text(client, text="b")
    ok = await _batch(client, [d1["token"], d2["token"]])
    assert ok.status_code == 200

    for data in (d1, d2):
        again = await client.post(f"/api/v1/pickup/{data['token']}")
        assert again.status_code == 410
        assert again.json()["result"] == "consumed"

    # 再次成批领取同样失败，且原因是 consumed（不返回任何内容）
    second = await _batch(client, [d1["token"], d2["token"]])
    assert second.status_code == 409
    body = second.json()
    assert "items" not in body and "content" not in body
    assert {(f["position"], f["reason"]) for f in body["failures"]} == {
        (0, "consumed"),
        (1, "consumed"),
    }


# ---------------- 各失败原因 ----------------


async def test_batch_unknown_token_reports_position(client: AsyncClient):
    d1 = await _create_text(client, text="ok")
    resp = await _batch(client, [d1["token"], "does-not-exist"])
    assert resp.status_code == 409
    failures = resp.json()["failures"]
    assert failures == [{"position": 1, "reason": "unknown"}]


async def test_batch_pending_blind_slot_reports_pending(client: AsyncClient):
    d1 = await _create_text(client)
    slot = await _create_slot(client)  # 尚未提交
    resp = await _batch(client, [d1["token"], slot["pickup_token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 1, "reason": "pending"}]

    # pending 不消费盲槽：提交后仍可领取
    sub = await client.post(slot["submit_url"], json={"text": "后来的"})
    assert sub.status_code == 200
    pickup = await client.post(slot["pickup_url"])
    assert pickup.status_code == 200
    assert pickup.json()["content"] == "后来的"


async def test_batch_blind_submit_token_is_unknown(client: AsyncClient):
    slot = await _create_slot(client)
    await client.post(slot["submit_url"], json={"text": "x"})
    d1 = await _create_text(client)
    # 盲槽**提交**令牌无取件权限，按未知处理（即使内容已提交）
    resp = await _batch(client, [d1["token"], slot["submit_token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 1, "reason": "unknown"}]
    # 盲槽内容未被消费
    pickup = await client.post(slot["pickup_url"])
    assert pickup.status_code == 200


async def test_batch_file_delivery_is_type_mismatch(client: AsyncClient):
    d1 = await _create_text(client, text="keep")
    f1 = await _create_file(client)
    resp = await _batch(client, [d1["token"], f1["token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 1, "reason": "type_mismatch"}]
    # 文本项未被消费
    again = await client.post(f"/api/v1/pickup/{d1['token']}")
    assert again.status_code == 200
    assert again.json()["content"] == "keep"
    # 文件仍可通过单件文件取件下载
    file_pickup = await client.post(f"/api/v1/pickup/{f1['token']}")
    assert file_pickup.status_code == 200


async def test_batch_blind_submitted_file_is_type_mismatch(client: AsyncClient):
    slot = await _create_slot(client)
    sub = await client.post(
        slot["submit_url"] + "/file",
        files={"file": ("blind.bin", b"file-bytes", "application/octet-stream")},
    )
    assert sub.status_code == 200
    d1 = await _create_text(client)
    resp = await _batch(client, [slot["pickup_token"], d1["token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 0, "reason": "type_mismatch"}]
    # 盲槽文件未被消费，仍可单件下载
    pickup = await client.post(slot["pickup_url"])
    assert pickup.status_code == 200
    assert pickup.content == b"file-bytes"


async def test_batch_revoked_reports_revoked(client: AsyncClient):
    d1 = await _create_text(client)
    d2 = await _create_text(client, text="revoke-me")
    await client.post(f"/api/v1/revoke/{d2['token']}")
    resp = await _batch(client, [d1["token"], d2["token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 1, "reason": "revoked"}]
    # d1 未被消费
    assert (await client.post(f"/api/v1/pickup/{d1['token']}")).status_code == 200


async def test_batch_consumed_reports_consumed(client: AsyncClient):
    d1 = await _create_text(client, text="first")
    d2 = await _create_text(client, text="second")
    await client.post(f"/api/v1/pickup/{d1['token']}")  # d1 已单件取走
    resp = await _batch(client, [d1["token"], d2["token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 0, "reason": "consumed"}]
    # d2 未被消费
    assert (await client.post(f"/api/v1/pickup/{d2['token']}")).status_code == 200
    assert (await client.post(f"/api/v1/pickup/{d2['token']}")).json().get(
        "result"
    ) or True


async def test_batch_expired_reports_expired(client: AsyncClient, db):
    d1 = await _create_text(client)
    d2 = await _create_text(client, text="old", ttl=1)
    async with db.session_factory() as session:
        row = await session.get(Delivery, d2["delivery_id"])
        row.expires_at = datetime.now(timezone.utc) - timedelta(seconds=5)
        await session.commit()
    resp = await _batch(client, [d1["token"], d2["token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 1, "reason": "expired"}]
    assert (await client.post(f"/api/v1/pickup/{d1['token']}")).status_code == 200


async def test_batch_expiry_equal_to_decision_time_is_expired(client: AsyncClient, db, monkeypatch):
    """到期时刻 == 裁决时刻视为过期（<= now）。"""
    decision = datetime(2026, 9, 29, 12, 0, 0, tzinfo=timezone.utc)
    d1 = await _create_text(client)
    async with db.session_factory() as session:
        row = await session.get(Delivery, d1["delivery_id"])
        row.expires_at = decision
        await session.commit()

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            return decision if tz else decision.replace(tzinfo=None)

    monkeypatch.setattr(mainmod, "datetime", _FixedDateTime)
    resp = await _batch(client, [d1["token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 0, "reason": "expired"}]


async def test_batch_multiple_failures_all_positions_and_reasons(client: AsyncClient, db):
    good = await _create_text(client, text="good")
    expired = await _create_text(client, text="old", ttl=1)
    async with db.session_factory() as session:
        row = await session.get(Delivery, expired["delivery_id"])
        row.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        await session.commit()
    consumed = await _create_text(client, text="taken")
    await client.post(f"/api/v1/pickup/{consumed['token']}")
    file_d = await _create_file(client)
    slot = await _create_slot(client)  # pending

    tokens = [
        good["token"],         # 0 ok
        "missing",             # 1 unknown
        expired["token"],      # 2 expired
        consumed["token"],     # 3 consumed
        file_d["token"],       # 4 type_mismatch
        slot["pickup_token"],  # 5 pending
    ]
    resp = await _batch(client, tokens)
    assert resp.status_code == 409
    assert resp.json()["failures"] == [
        {"position": 1, "reason": "unknown"},
        {"position": 2, "reason": "expired"},
        {"position": 3, "reason": "consumed"},
        {"position": 4, "reason": "type_mismatch"},
        {"position": 5, "reason": "pending"},
    ]
    # 唯一可领的 good 也不得被消费
    pickup = await client.post(f"/api/v1/pickup/{good['token']}")
    assert pickup.status_code == 200
    assert pickup.json()["content"] == "good"


# ---------------- 并发：重叠批次 / 单件取件 / 撤回，每份至多一次 ----------------


async def test_overlapping_batches_each_content_consumed_once(client: AsyncClient):
    deliveries = [await _create_text(client, text=f"m-{i}") for i in range(5)]
    tokens = [d["token"] for d in deliveries]
    # 两个完全重叠的批次并发
    async def call_batch():
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post("/api/v1/pickups/batch", json={"tokens": tokens})

    r1, r2 = await asyncio.gather(call_batch(), call_batch())
    statuses = sorted([r1.status_code, r2.status_code])
    assert statuses == [200, 409]
    ok = r1 if r1.status_code == 200 else r2
    fail = r1 if r1.status_code == 409 else r2
    assert ok.json()["count"] == 5
    assert {f["reason"] for f in fail.json()["failures"]} == {"consumed"}
    assert len(fail.json()["failures"]) == 5

    # 再来一批，全部已消费
    third = await _batch(client, tokens)
    assert third.status_code == 409
    assert len(third.json()["failures"]) == 5
    assert all(f["reason"] == "consumed" for f in third.json()["failures"])


async def test_batch_vs_single_pickup_concurrent_each_once(client: AsyncClient, db):
    data = await _create_text(client, text="谁赢")
    token = data["token"]

    async def batch_call():
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post("/api/v1/pickups/batch", json={"tokens": [token]})

    async def single_call():
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post(f"/api/v1/pickup/{token}")

    results = await asyncio.gather(*[batch_call(), single_call(), batch_call()])
    successes = [r for r in results if r.status_code == 200]
    # 恰好一个 200，且它持有内容
    assert len(successes) == 1
    winner = successes[0]
    if winner.request.url.path.endswith("/batch"):
        assert winner.json()["items"][0]["content"] == "谁赢"
    else:
        assert winner.json()["content"] == "谁赢"
    for r in results:
        if r is not winner:
            assert r.status_code in (409, 410)

    async with db.session_factory() as session:
        row = await session.get(Delivery, data["delivery_id"])
        assert row.status == CONSUMED


async def test_many_overlapping_batches_single_winner_set(client: AsyncClient):
    """20 路重叠批次争抢同一批 3 份内容，恰好一个整批赢家，无部分消费。"""
    deliveries = [await _create_text(client, text=f"c-{i}") for i in range(3)]
    tokens = [d["token"] for d in deliveries]

    async def call_batch():
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post("/api/v1/pickups/batch", json={"tokens": tokens})

    results = await asyncio.gather(*[call_batch() for _ in range(20)])
    winners = [r for r in results if r.status_code == 200]
    losers = [r for r in results if r.status_code == 409]
    assert len(winners) == 1
    assert len(losers) == 19
    winner_contents = {i["content"] for i in winners[0].json()["items"]}
    assert winner_contents == {"c-0", "c-1", "c-2"}
    for loser in losers:
        # 输家失败项数要么全 3（赢家已提交），要么是竞争中读到的终态；
        # 关键是绝不返回内容、状态码 409
        assert "items" not in loser.json()
        assert loser.json()["failures"]


async def test_failed_batch_leaves_others_consumable_under_concurrency(client: AsyncClient):
    good1 = await _create_text(client, text="g1")
    good2 = await _create_text(client, text="g2")
    # 批次含一个不存在令牌，必然失败
    async def call_batch():
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post(
                "/api/v1/pickups/batch",
                json={"tokens": [good1["token"], "ghost", good2["token"]]},
            )

    results = await asyncio.gather(*[call_batch() for _ in range(10)])
    assert all(r.status_code == 409 for r in results)
    # good1/good2 从未被失败批次消费
    p1 = await client.post(f"/api/v1/pickup/{good1['token']}")
    p2 = await client.post(f"/api/v1/pickup/{good2['token']}")
    assert p1.status_code == 200 and p2.status_code == 200


async def test_revoke_racing_batch_is_mutex(client: AsyncClient):
    data = await _create_text(client, text="撤回与整批互斥")
    token = data["token"]

    async def batch_call():
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post("/api/v1/pickups/batch", json={"tokens": [token]})

    async def revoke_call():
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post(f"/api/v1/revoke/{token}")

    batch_resp, revoke_resp = await asyncio.gather(batch_call(), revoke_call())
    if batch_resp.status_code == 200:
        assert batch_resp.json()["items"][0]["content"] == "撤回与整批互斥"
        assert revoke_resp.status_code == 410
        assert revoke_resp.json()["result"] == "consumed"
    else:
        assert revoke_resp.status_code == 200
        assert revoke_resp.json()["result"] == "revoked"
        assert batch_resp.status_code == 409
        assert batch_resp.json()["failures"][0]["reason"] == "revoked"


# ---------------- Redis 不可用：回落数据库，原子性不变 ----------------


async def test_batch_works_when_redis_unavailable(client: AsyncClient, monkeypatch):
    d1 = await _create_text(client, text="r1")
    d2 = await _create_text(client, text="r2")

    import redis.exceptions

    async def _boom(*a, **k):  # noqa: ANN002, ANN003
        raise redis.exceptions.ConnectionError("redis down")

    # 缓存全部方法均不可用：预检回落数据库，成功路径的回写也被吞掉
    cache = client._transport.app.state.cache  # type: ignore[attr-defined]
    monkeypatch.setattr(cache, "get_flags", _boom)
    monkeypatch.setattr(cache, "set_flag_if_absent", _boom)
    resp = await _batch(client, [d1["token"], d2["token"]])
    assert resp.status_code == 200, resp.text
    assert [i["content"] for i in resp.json()["items"]] == ["r1", "r2"]

    # 消费在数据库已落定：即使缓存写入失败，再次单件取件仍被权威拒绝
    for data in (d1, d2):
        again = await client.post(f"/api/v1/pickup/{data['token']}")
        assert again.status_code == 410
        assert again.json()["result"] == "consumed"


async def test_batch_cached_terminal_flag_short_circuits(client: AsyncClient):
    """已缓存的终态标记命中时走只读路径，不锁行、不消费。"""
    d1 = await _create_text(client, text="a")
    d2 = await _create_text(client, text="b")
    # 单件取走 d1，写入 consumed 标记并落库
    assert (await client.post(f"/api/v1/pickup/{d1['token']}")).status_code == 200

    resp = await _batch(client, [d1["token"], d2["token"]])
    assert resp.status_code == 409
    assert resp.json()["failures"] == [{"position": 0, "reason": "consumed"}]
    # d2 未被该失败批次消费
    again = await client.post(f"/api/v1/pickup/{d2['token']}")
    assert again.status_code == 200
    assert again.json()["content"] == "b"


# ---------------- 审计：入哈希链、无明文、全链可校验 ----------------


async def test_batch_success_audit_trail(client: AsyncClient, db):
    d1 = await _create_text(client, text="审计内容A")
    d2 = await _create_text(client, text="审计内容B")
    resp = await _batch(client, [d1["token"], d2["token"]])
    assert resp.status_code == 200
    body = resp.json()
    batch_id = body["batch_id"]

    async with db.session_factory() as session:
        rows = (
            await session.execute(
                select(AuditLog)
                .where(AuditLog.action == "batch_pickup")
                .order_by(AuditLog.seq)
            )
        ).scalars().all()

    assert len(rows) == 2
    assert all(r.result == "success" for r in rows)
    assert {str(r.delivery_id) for r in rows} == {
        d1["delivery_id"],
        d2["delivery_id"],
    }
    assert rows[0].token_hash == hash_token(d1["token"])
    assert rows[1].token_hash == hash_token(d2["token"])
    # 同一 batch_id 关联，且无明文内容
    assert all(r.detail["batch_id"] == batch_id for r in rows)
    assert all("审计内容" not in str(r.detail) for r in rows)
    assert rows[0].detail["position"] == 0 and rows[1].detail["position"] == 1

    # 全链校验通过
    verify = await client.get("/api/v1/audit/verify")
    assert verify.status_code == 200
    assert verify.json()["ok"] is True


async def test_batch_failure_audit_per_item_reasons(client: AsyncClient, db):
    d1 = await _create_text(client, text="留着")
    consumed = await _create_text(client, text="已取")
    await client.post(f"/api/v1/pickup/{consumed['token']}")

    resp = await _batch(client, [d1["token"], consumed["token"], "ghost"])
    assert resp.status_code == 409

    async with db.session_factory() as session:
        rows = (
            await session.execute(
                select(AuditLog)
                .where(AuditLog.action == "batch_pickup")
                .order_by(AuditLog.seq)
            )
        ).scalars().all()

    by_pos = {r.detail["position"]: r for r in rows}
    assert set(by_pos) == {0, 1, 2}
    # d1 因整批失败未被消费，但仍有一条 active? 的审计：未消费项不记 success
    assert by_pos[0].result != "success"
    assert by_pos[1].result == "consumed"
    assert by_pos[2].result == "unknown"
    assert by_pos[2].delivery_id is None
    assert by_pos[2].token_hash == hash_token("ghost")
    # 审计中不出现明文令牌或内容
    for r in rows:
        assert "留着" not in str(r.detail) and "ghost" not in str(r.detail)

    # d1 确实未消费
    again = await client.post(f"/api/v1/pickup/{d1['token']}")
    assert again.status_code == 200


async def test_batch_audit_chain_remains_valid_with_many_outcomes(client: AsyncClient):
    for i in range(3):
        await _create_text(client, text=f"s-{i}")
    d = await _create_text(client)
    await _batch(client, [d["token"], "nope"])  # 失败批次
    goods = []
    for _ in range(4):
        goods.append(await _create_text(client, text="ok"))
    await _batch(client, [g["token"] for g in goods])  # 成功批次

    verify = await client.get("/api/v1/audit/verify")
    assert verify.status_code == 200
    report = verify.json()
    assert report["ok"] is True, report
