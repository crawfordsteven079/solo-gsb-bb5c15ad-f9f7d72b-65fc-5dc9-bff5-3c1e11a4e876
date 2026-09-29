"""盲投递请求（空槽）：创建 / pending / 一次性提交 / 并发裁决 / 撤回 / 过期 / 令牌隔离 / 审计。"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from app.models import AuditLog, Delivery
from app.store import ACTIVE, CONSUMED, EXPIRED, PENDING, REVOKED, DeliveryStore
from app.utils import hash_token

pytestmark = pytest.mark.asyncio


async def _create_slot(client: AsyncClient, ttl: int = 3600) -> dict:
    resp = await client.post("/api/v1/blind-slots", json={"ttl_seconds": ttl})
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _gather_posts(client: AsyncClient, paths: list[str]) -> list:
    async def post(path):
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post(path)

    return await asyncio.gather(*[post(p) for p in paths])


# ---------------- 创建空槽 ----------------


async def test_create_slot_returns_two_distinct_tokens(client: AsyncClient):
    data = await _create_slot(client)
    assert data["slot_id"]
    assert len(data["submit_token"]) >= 40
    assert len(data["pickup_token"]) >= 40
    assert data["submit_token"] != data["pickup_token"]
    assert data["submit_url"].endswith(f"/api/v1/blind-slots/submit/{data['submit_token']}")
    assert data["pickup_url"].endswith(f"/api/v1/blind-slots/pickup/{data['pickup_token']}")
    assert data["revoke_url"].endswith(f"/api/v1/blind-slots/revoke/{data['pickup_token']}")
    assert data["expires_at"]


async def test_create_slot_validates_ttl(client: AsyncClient):
    resp = await client.post("/api/v1/blind-slots", json={"ttl_seconds": 99999999})
    assert resp.status_code == 422


async def test_slot_row_starts_pending_and_empty(client: AsyncClient, db):
    data = await _create_slot(client)
    async with db.session_factory() as session:
        row = await session.get(Delivery, data["slot_id"])
        assert row.status == PENDING
        assert row.kind == "pending"
        assert row.content is None
        assert row.content_size is None
        assert row.content_sha256 is None
        assert row.submitted_at is None
        # 两类令牌分别哈希存储，且互不相同
        assert row.token_hash == hash_token(data["pickup_token"])
        assert row.submit_token_hash == hash_token(data["submit_token"])
        assert row.token_hash != row.submit_token_hash


# ---------------- 提交前：取件返回 pending ----------------


async def test_pickup_pending_before_submission(client: AsyncClient):
    data = await _create_slot(client)
    resp = await client.post(data["pickup_url"])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["result"] == "pending"
    assert body["slot_id"] == data["slot_id"]
    assert body["expires_at"]

    # pending 不改变状态，可反复查询
    again = await client.post(data["pickup_url"])
    assert again.status_code == 200
    assert again.json()["result"] == "pending"


# ---------------- 一次性提交 ----------------


async def test_submit_text_then_pickup_roundtrip(client: AsyncClient):
    data = await _create_slot(client)

    submit = await client.post(data["submit_url"], json={"text": "盲投内容"})
    assert submit.status_code == 200, submit.text
    sbody = submit.json()
    assert sbody["result"] == "success"
    assert sbody["slot_id"] == data["slot_id"]
    assert sbody["kind"] == "text"
    assert sbody["content_sha256"]
    assert sbody["submitted_at"]
    # 提交响应不得回传取件令牌或取件地址（提交者无取件权限）
    assert "pickup_url" not in sbody
    assert "pickup_token" not in sbody

    pickup = await client.post(data["pickup_url"])
    assert pickup.status_code == 200
    pbody = pickup.json()
    assert pbody["result"] == "success"
    assert pbody["content"] == "盲投内容"

    # 取件仍是原子单次
    second = await client.post(data["pickup_url"])
    assert second.status_code == 410
    assert second.json()["result"] == "consumed"


async def test_submit_file_then_pickup_binary(client: AsyncClient):
    data = await _create_slot(client)
    payload = b"\x89PNG blind-binary" * 8
    submit = await client.post(
        data["submit_url"] + "/file",
        files={"file": ("盲.png", payload, "image/png")},
    )
    assert submit.status_code == 200, submit.text
    assert submit.json()["kind"] == "file"

    pickup = await client.post(data["pickup_url"])
    assert pickup.status_code == 200
    assert pickup.content == payload
    assert pickup.headers["x-drop-result"] == "success"
    assert "attachment" in pickup.headers["content-disposition"]


async def test_submit_twice_second_is_submitted(client: AsyncClient, cache):
    data = await _create_slot(client)
    first = await client.post(data["submit_url"], json={"text": "first"})
    assert first.status_code == 200

    second = await client.post(data["submit_url"], json={"text": "second"})
    assert second.status_code == 409
    assert second.json()["result"] == "submitted"

    # 槽中内容只可能是赢家那份
    pickup = await client.post(data["pickup_url"])
    assert pickup.json()["content"] == "first"

    # 提交令牌终态已缓存在提交令牌独立命名空间
    flag = await cache.get_submit_flag(hash_token(data["submit_token"]))
    assert flag == "submitted"
    # 取件令牌命名空间下不得出现该标记（权限不互换）
    assert await cache.get_flag(hash_token(data["submit_token"])) is None


async def test_submit_rejects_oversize(client: AsyncClient):
    data = await _create_slot(client)
    resp = await client.post(data["submit_url"], json={"text": "x" * 2048})
    assert resp.status_code == 413  # 上限 1024（测试配置）

    # 超限提交不得占用空槽：随后一份合法提交仍可成功
    retry = await client.post(data["submit_url"], json={"text": "ok"})
    assert retry.status_code == 200


# ---------------- 并发提交：唯一赢家 ----------------


async def test_concurrent_submit_single_winner(client: AsyncClient, db):
    data = await _create_slot(client)

    async def submit(i):
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            return await ac.post(data["submit_url"], json={"text": f"candidate-{i}"})

    results = await asyncio.gather(*[submit(i) for i in range(50)])
    ok = [r for r in results if r.status_code == 200]
    conflicts = [r for r in results if r.status_code == 409]
    assert len(ok) == 1
    assert len(conflicts) == 49
    assert all(r.json()["result"] == "submitted" for r in conflicts)

    async with db.session_factory() as session:
        row = await session.get(Delivery, data["slot_id"])
        assert row.status == ACTIVE
        assert row.content is not None
        assert row.submitted_at is not None
        assert row.content.decode().startswith("candidate-")
        expected_content = row.content.decode()

    pickup = await client.post(data["pickup_url"])
    assert pickup.status_code == 200
    assert pickup.json()["content"] == expected_content


# ---------------- 撤回 ----------------


async def test_revoke_pending_slot_blocks_submit_and_pickup(client: AsyncClient):
    data = await _create_slot(client)
    revoked = await client.post(data["revoke_url"])
    assert revoked.status_code == 200
    assert revoked.json()["result"] == "revoked"

    submit = await client.post(data["submit_url"], json={"text": "late"})
    assert submit.status_code == 409
    assert submit.json()["result"] == "revoked"

    pickup = await client.post(data["pickup_url"])
    assert pickup.status_code == 409
    assert pickup.json()["result"] == "revoked"


async def test_revoke_after_submission_blocks_pickup(client: AsyncClient):
    data = await _create_slot(client)
    await client.post(data["submit_url"], json={"text": "secret"})

    revoked = await client.post(data["revoke_url"])
    assert revoked.status_code == 200
    assert revoked.json()["result"] == "revoked"

    pickup = await client.post(data["pickup_url"])
    assert pickup.status_code == 409
    assert pickup.json()["result"] == "revoked"


async def test_revoke_is_idempotent(client: AsyncClient):
    data = await _create_slot(client)
    first = await client.post(data["revoke_url"])
    assert first.json()["result"] == "revoked"
    second = await client.post(data["revoke_url"])
    assert second.status_code == 200
    assert second.json()["result"] == "already_revoked"


async def test_revoke_after_pickup_is_410_consumed(client: AsyncClient):
    data = await _create_slot(client)
    await client.post(data["submit_url"], json={"text": "x"})
    await client.post(data["pickup_url"])

    resp = await client.post(data["revoke_url"])
    assert resp.status_code == 410
    assert resp.json()["result"] == "consumed"


async def test_concurrent_submit_and_revoke_are_mutex(client: AsyncClient, db):
    """空槽上提交与撤回并发，两种合法结局由数据库裁决，不允许出现不一致：

    - 撤回先赢（pending→revoked）：提交必须得到 revoked，内容绝不入库；
    - 提交先赢（pending→active）：撤回发生在已提交槽上仍可成功
      （取件前允许收件人撤回），但之后取件必须被拒。
    """
    data = await _create_slot(client)

    async def post(path, body=None):
        async with AsyncClient(transport=client._transport, base_url="http://test") as ac:
            if body is not None:
                return await ac.post(path, json=body)
            return await ac.post(path)

    submit_resp, revoke_resp = await asyncio.gather(
        post(data["submit_url"], {"text": "race"}),
        post(data["revoke_url"]),
    )

    if submit_resp.status_code == 200:
        # 提交先赢（pending→active）。并发撤回的裁决：
        #  - 多数情况下在 active 槽上撤回成功；
        #  - 极端交错下（撤回条件更新在提交提交前后跨快照）可能落到已终态，
        #    此时它必须给出明确的终态原因，而不会是 5xx。
        assert revoke_resp.status_code in (200, 409, 410)
        pickup = await client.post(data["pickup_url"])
        assert pickup.status_code in (200, 409, 410)
        # 内容与“已取走/已撤回”互斥：拿到内容时撤回必然已失败
        if pickup.status_code == 200:
            assert pickup.json()["content"] == "race"
            assert revoke_resp.status_code == 410
            assert revoke_resp.json()["result"] == "consumed"
        else:
            assert pickup.json()["result"] in ("revoked", "consumed", "expired")
    else:
        assert submit_resp.status_code == 409
        assert submit_resp.json()["result"] == "revoked"
        assert revoke_resp.status_code == 200
        assert revoke_resp.json()["result"] == "revoked"

    # 任何结局下槽都不可能残留 pending
    async with db.session_factory() as session:
        row = await session.get(Delivery, data["slot_id"])
        assert row.status in (REVOKED, ACTIVE, CONSUMED)


async def test_concurrent_pickup_after_submit_single_winner(client: AsyncClient):
    data = await _create_slot(client)
    await client.post(data["submit_url"], json={"text": "once"})

    results = await _gather_posts(client, [data["pickup_url"]] * 50)
    ok = [r for r in results if r.status_code == 200]
    gone = [r for r in results if r.status_code == 410]
    assert len(ok) == 1
    assert ok[0].json()["content"] == "once"
    assert len(gone) == 49
    assert all(r.json()["result"] == "consumed" for r in gone)


# ---------------- 过期 ----------------


async def test_submit_to_expired_pending_slot_is_expired(client: AsyncClient, db):
    data = await _create_slot(client, ttl=1)

    async with db.session_factory() as session:
        row = await session.get(Delivery, data["slot_id"])
        row.expires_at = datetime.now(timezone.utc) - timedelta(seconds=5)
        await session.commit()

    resp = await client.post(data["submit_url"], json={"text": "late"})
    assert resp.status_code == 410
    assert resp.json()["result"] == "expired"


async def test_pickup_expired_pending_slot_is_expired(client: AsyncClient, db):
    data = await _create_slot(client, ttl=1)

    async with db.session_factory() as session:
        row = await session.get(Delivery, data["slot_id"])
        row.expires_at = datetime.now(timezone.utc) - timedelta(seconds=5)
        await session.commit()

    resp = await client.post(data["pickup_url"])
    assert resp.status_code == 410
    assert resp.json()["result"] == "expired"


async def test_submitted_slot_can_expire_before_pickup(client: AsyncClient, db):
    data = await _create_slot(client)
    await client.post(data["submit_url"], json={"text": "will expire"})

    async with db.session_factory() as session:
        row = await session.get(Delivery, data["slot_id"])
        row.expires_at = datetime.now(timezone.utc) - timedelta(seconds=5)
        await session.commit()

    pickup = await client.post(data["pickup_url"])
    assert pickup.status_code == 410
    assert pickup.json()["result"] == "expired"

    revoke = await client.post(data["revoke_url"])
    assert revoke.status_code == 410
    assert revoke.json()["result"] == "expired"


async def test_sweep_marks_pending_slot_expired(db):
    async with db.session_factory() as session:
        slot = await DeliveryStore(session).create_slot(
            submit_token_hash="a" * 64,
            pickup_token_hash="b" * 64,
            expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        )
        await session.commit()
        count = await DeliveryStore(session).sweep_expired()
        assert count == 1
        row = await session.get(Delivery, slot.id)
        assert row.status == EXPIRED


# ---------------- 令牌权限不可互换 ----------------


async def test_pickup_token_cannot_submit(client: AsyncClient):
    data = await _create_slot(client)
    resp = await client.post(
        f"/api/v1/blind-slots/submit/{data['pickup_token']}",
        json={"text": "forged"},
    )
    assert resp.status_code == 404
    assert resp.json()["result"] == "unknown"


async def test_submit_token_cannot_pickup_or_revoke(client: AsyncClient):
    data = await _create_slot(client)
    await client.post(data["submit_url"], json={"text": "x"})

    pickup = await client.post(f"/api/v1/blind-slots/pickup/{data['submit_token']}")
    assert pickup.status_code == 404
    assert pickup.json()["result"] == "unknown"

    revoke = await client.post(f"/api/v1/blind-slots/revoke/{data['submit_token']}")
    assert revoke.status_code == 404
    assert revoke.json()["result"] == "unknown"


async def test_blind_tokens_rejected_by_legacy_endpoints(client: AsyncClient):
    data = await _create_slot(client)
    await client.post(data["submit_url"], json={"text": "x"})

    # 盲槽取件令牌不能走普通取件/撤回
    legacy_pickup = await client.post(f"/api/v1/pickup/{data['pickup_token']}")
    assert legacy_pickup.status_code == 404
    assert legacy_pickup.json()["result"] == "unknown"

    legacy_revoke = await client.post(f"/api/v1/revoke/{data['pickup_token']}")
    assert legacy_revoke.status_code == 404
    assert legacy_revoke.json()["result"] == "unknown"

    # 盲槽提交令牌在任何旧接口上都无意义
    assert (await client.post(f"/api/v1/pickup/{data['submit_token']}")).status_code == 404
    assert (await client.post(f"/api/v1/revoke/{data['submit_token']}")).status_code == 404

    # 终态之后再交叉：取件令牌已带 consumed 缓存标记，旧接口仍必须 404，
    # 不得把盲槽的终态泄露给无权限的接口
    ok = await client.post(data["pickup_url"])
    assert ok.status_code == 200
    legacy_revoke_after = await client.post(
        f"/api/v1/revoke/{data['pickup_token']}"
    )
    assert legacy_revoke_after.status_code == 404
    assert legacy_revoke_after.json()["result"] == "unknown"
    legacy_pickup_after = await client.post(
        f"/api/v1/pickup/{data['pickup_token']}"
    )
    assert legacy_pickup_after.status_code == 404
    assert legacy_pickup_after.json()["result"] == "unknown"


async def test_legacy_token_rejected_by_blind_endpoints(client):
    # 普通投递的取件令牌不能用于盲槽接口
    resp = await client.post(
        "/api/v1/deliveries", json={"text": "normal", "ttl_seconds": 3600}
    )
    normal_token = resp.json()["token"]

    blind_pickup = await client.post(f"/api/v1/blind-slots/pickup/{normal_token}")
    assert blind_pickup.status_code == 404
    blind_revoke = await client.post(f"/api/v1/blind-slots/revoke/{normal_token}")
    assert blind_revoke.status_code == 404
    # 普通令牌也不可能具备提交权限
    blind_submit = await client.post(
        f"/api/v1/blind-slots/submit/{normal_token}", json={"text": "x"}
    )
    assert blind_submit.status_code == 404

    # 普通投递取件后（取件令牌已带 consumed 缓存标记），盲槽接口仍须 404：
    # 终态标记跨接口不得泄露权限或状态
    assert (await client.post(f"/api/v1/pickup/{normal_token}")).status_code == 200
    again = await client.post(f"/api/v1/blind-slots/pickup/{normal_token}")
    assert again.status_code == 404
    assert again.json()["result"] == "unknown"
    again_revoke = await client.post(f"/api/v1/blind-slots/revoke/{normal_token}")
    assert again_revoke.status_code == 404
    assert again_revoke.json()["result"] == "unknown"


async def test_unknown_tokens_are_404(client: AsyncClient):
    # 提交端点需要合法 JSON 体；带着 body 仍查无此令牌才是 unknown
    resp = await client.post("/api/v1/blind-slots/submit/nope", json={"text": "x"})
    assert resp.status_code == 404
    assert resp.json()["result"] == "unknown"
    assert (await client.post("/api/v1/blind-slots/pickup/nope")).status_code == 404
    assert (await client.post("/api/v1/blind-slots/revoke/nope")).status_code == 404


# ---------------- 审计 ----------------


async def test_blind_flow_audit_trail(client: AsyncClient, db):
    data = await _create_slot(client)
    await client.post(data["pickup_url"])  # pending
    await client.post(data["submit_url"], json={"text": "审计盲投"})  # success
    await client.post(data["submit_url"], json={"text": "again"})  # submitted
    await client.post(data["pickup_url"])  # success
    await client.post(data["pickup_url"])  # consumed

    async with db.session_factory() as session:
        rows = (
            await session.execute(
                select(AuditLog)
                .where(AuditLog.delivery_id == data["slot_id"])
                .order_by(AuditLog.seq)
            )
        ).scalars().all()

    pairs = [(r.action, r.result) for r in rows]
    assert ("blind_create", "success") in pairs
    assert ("blind_pickup", "pending") in pairs
    assert ("blind_submit", "success") in pairs
    assert ("blind_submit", "submitted") in pairs
    assert ("blind_pickup", "success") in pairs
    assert ("blind_pickup", "consumed") in pairs

    # 创建审计记录的是取件令牌哈希，提交记录的是提交令牌哈希
    create_row = next(r for r in rows if r.action == "blind_create")
    assert create_row.token_hash == hash_token(data["pickup_token"])
    assert create_row.detail["submit_token_hash"] == hash_token(data["submit_token"])
    submit_rows = [r for r in rows if r.action == "blind_submit"]
    assert all(r.token_hash == hash_token(data["submit_token"]) for r in submit_rows)
    # 审计中绝不出现明文内容
    assert all(not r.detail or "审计盲投" not in str(r.detail) for r in rows)


async def test_revoked_slot_audit_and_flags(client: AsyncClient, db, cache):
    data = await _create_slot(client)
    await client.post(data["revoke_url"])
    # 撤回后尝试提交
    await client.post(data["submit_url"], json={"text": "x"})

    async with db.session_factory() as session:
        rows = (
            await session.execute(
                select(AuditLog).where(AuditLog.delivery_id == data["slot_id"])
            )
        ).scalars().all()
    pairs = {(r.action, r.result) for r in rows}
    assert ("blind_revoke", "revoked") in pairs
    assert ("blind_submit", "revoked") in pairs

    # 撤回终态同时缓存到两类令牌（各自独立命名空间）
    assert await cache.get_flag(hash_token(data["pickup_token"])) == "revoked"
    assert await cache.get_submit_flag(hash_token(data["submit_token"])) == "revoked"


async def test_blind_actions_are_on_audit_chain(client: AsyncClient):
    """盲槽四类动作都追加到同一条哈希链，全链校验必须通过。"""
    data = await _create_slot(client)
    await client.post(data["pickup_url"])  # pending
    await client.post(data["submit_url"], json={"text": "入链"})
    await client.post(data["pickup_url"])  # success
    await client.post(data["revoke_url"])  # consumed

    verify = await client.get("/api/v1/audit/verify")
    assert verify.status_code == 200
    report = verify.json()
    assert report["ok"] is True, report
    assert report["count"] >= 4
