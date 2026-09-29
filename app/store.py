"""投递记录的数据访问层。

并发性正确性的核心：所有状态迁移都用 *单条带 WHERE 条件的 UPDATE ... RETURNING*
完成。PostgreSQL 会用行锁串行化对同一行的并发更新：
- 第一个满足条件的事务把 status 从 active 改走并返回行（赢家）；
- 其它事务的 WHERE 不再匹配，返回空（输家），随后读取当前终态给出明确结果。

审计哈希链的追加使用同样的裁决模式：对单行 ``audit_chain_state`` 做
``UPDATE ... WHERE last_seq=:expected`` 的条件更新，竞争失败的事务重试，
因此链序号的全序由数据库（而非应用层锁）决定。
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import datetime

import sqlalchemy.exc
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .audit_chain import (
    GENESIS_HASH,
    ChainEntry,
    chain_hash,
    event_digest_for,
)
from .models import AuditChainState, AuditLog, Delivery
from .utils import ensure_utc, utcnow

# 可能的结果状态
CONSUMED = "consumed"
REVOKED = "revoked"
EXPIRED = "expired"
ACTIVE = "active"
PENDING = "pending"      # 盲投递空槽：收件人已开槽，尚无提交者写入
SUBMITTED = "submitted"  # 提交竞争失败：槽已被另一个提交者占用
UNKNOWN = "unknown"
TYPE_MISMATCH = "type_mismatch"  # 批量取件仅收文本：文件投递（普通/盲槽）一律类型不符

# 盲槽内容提交成功时的 kind，空槽阶段固定为 pending
_PENDING_KIND = PENDING

_DELIVERY_COLS = (
    Delivery.id,
    Delivery.status,
    Delivery.content,
    Delivery.kind,
    Delivery.filename,
    Delivery.content_type,
    Delivery.content_size,
    Delivery.content_sha256,
    Delivery.expires_at,
    Delivery.consumed_at,
    Delivery.revoked_at,
)

# 导出/校验链记录时的列（绝不包含 deliveries.content 等明文投递内容）
_CHAIN_COLS = (
    AuditLog.seq,
    AuditLog.created_at,
    AuditLog.action,
    AuditLog.result,
    AuditLog.delivery_id,
    AuditLog.token_hash,
    AuditLog.client_ip,
    AuditLog.user_agent,
    AuditLog.detail,
    AuditLog.event_digest,
    AuditLog.prev_hash,
    AuditLog.entry_hash,
)

# 链追加的竞争重试上限（正常竞争下通常 1~2 次即完成）
_MAX_CHAIN_ATTEMPTS = 200

# 批量取件在死锁/快照过期下的整事务重试上限
_MAX_BATCH_ATTEMPTS = 200


@dataclass(frozen=True)
class BatchClaimItem:
    """批量取件中单个输入项的裁决视图。

    position 为输入列表下标（从 0 开始）；state 为该项原因：
    active（可消费文本）/ unknown / pending / expired / consumed /
    revoked / type_mismatch。row 仅在命中时存在，仅供同事务内组装结果，
    失败响应绝不从中取内容。
    """

    position: int
    state: str
    kind: str | None
    row_id: uuid.UUID | None = None
    row: Delivery | None = None


@dataclass(frozen=True)
class BatchClaimedContent:
    """整批成功时，单项内容的不可变快照（裁决事务内提取，commit 后不再回库）。"""

    position: int
    delivery_id: uuid.UUID
    content: bytes
    content_sha256: str


@dataclass(frozen=True)
class BatchClaimOutcome:
    """整批裁决结果：won=True 时 contents 按输入顺序携带文本快照。"""

    won: bool
    items: list[BatchClaimItem]
    contents: list[BatchClaimedContent] | None = None
    consumed_at: datetime | None = None


def _is_pg_deadlock(exc: sqlalchemy.exc.OperationalError) -> bool:
    """识别 PostgreSQL deadlock_detected（SQLSTATE 40P01）。"""
    orig = getattr(exc, "orig", None)
    return getattr(orig, "sqlstate", None) == "40P01"



class DeliveryStore:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(
        self,
        *,
        token_hash: str,
        kind: str,
        content: bytes,
        expires_at: datetime,
        filename: str | None = None,
        content_type: str | None = None,
        content_sha256: str,
    ) -> Delivery:
        delivery = Delivery(
            token_hash=token_hash,
            kind=kind,
            content=content,
            filename=filename,
            content_type=content_type,
            content_size=len(content),
            content_sha256=content_sha256,
            expires_at=expires_at,
        )
        self.session.add(delivery)
        await self.session.flush()
        return delivery

    async def create_slot(
        self,
        *,
        submit_token_hash: str,
        pickup_token_hash: str,
        expires_at: datetime,
    ) -> Delivery:
        """创建盲投递空槽：内容为空、status=pending，承载两类互不相同的令牌。

        提交令牌只能用于 ``submit_content``（按 ``submit_token_hash`` 定位）；
        取件令牌只能用于 ``claim_blind`` / ``revoke_blind``（按 ``token_hash``
        定位且限定盲槽）。两条唯一索引保证令牌互不冲突、权限不可互换。
        """
        slot = Delivery(
            token_hash=pickup_token_hash,
            submit_token_hash=submit_token_hash,
            kind=_PENDING_KIND,
            content=None,
            content_size=None,
            content_sha256=None,
            status=PENDING,
            expires_at=expires_at,
        )
        self.session.add(slot)
        await self.session.flush()
        return slot

    async def submit_content(
        self,
        submit_token_hash: str,
        *,
        kind: str,
        content: bytes,
        filename: str | None,
        content_type: str,
        content_sha256: str,
    ) -> tuple[bool, str, Delivery | None]:
        """向盲槽一次性写入内容。并发提交由单条条件 UPDATE 裁决。

        返回 (won, state, row)：
        - won=True  本调用是唯一提交成功者，槽由 pending 原子转为 active，
                    row 带槽 id 与过期时间（不含重复读取内容列）；
        - won=False state 为明确失败原因：
                    submitted（已被另一个提交者占用）/ revoked（已撤回）/
                    expired（已过期）/ unknown（提交令牌不存在，
                    或该令牌不具备提交权限——如拿取件令牌来提交）。

        裁决语句与取件同构：PostgreSQL 用行锁串行化，仅第一个满足
        ``submit_token_hash 匹配 AND status='pending' AND 未过期`` 的事务命中，
        并在同一条 UPDATE 中把内容、元数据与 pending→active 一并写入，
        因此绝不会出现“状态已激活但内容为空”的半成品槽。
        """
        now = utcnow()
        result = await self.session.execute(
            update(Delivery)
            .where(
                Delivery.submit_token_hash == submit_token_hash,
                Delivery.status == PENDING,
                Delivery.expires_at > now,
            )
            .values(
                status=ACTIVE,
                kind=kind,
                content=content,
                filename=filename,
                content_type=content_type,
                content_size=len(content),
                content_sha256=content_sha256,
                submitted_at=now,
            )
            .returning(
                Delivery.id,
                Delivery.status,
                Delivery.expires_at,
                Delivery.submitted_at,
            )
        )
        row = result.first()
        if row is not None:
            await self.session.commit()
            # RETURNING 行已带 id / status / expires_at / submitted_at，
            # commit 后不再回读（避免 ORM 过期属性在并发下惰性触发 IO）
            return True, ACTIVE, row

        # 未命中：结束本事务后，在独立事务里幂等地推进过期，再读终态
        await self.session.commit()
        await self._lazily_expire(submit_token_hash, now, by_submit_token=True, blind=True)

        current = (
            await self.session.execute(
                select(Delivery).where(Delivery.submit_token_hash == submit_token_hash)
            )
        ).scalar_one_or_none()
        if current is None:
            return False, UNKNOWN, None
        # active/consumed 都意味着“唯一一份内容已由他人成功写入”：
        # 正常裁决读到 active；极端竞态（取件紧接提交后）可能读到 consumed。
        if current.status == ACTIVE or current.status == CONSUMED:
            return False, SUBMITTED, current
        return False, current.status, current

    async def claim_blind(
        self, pickup_token_hash: str
    ) -> tuple[bool, str, Delivery | None]:
        """凭盲槽取件令牌原子取件。

        与普通取件 ``claim`` 的两点差别：
        - 只能命中盲槽（``submit_token_hash IS NOT NULL``），普通投递的
          取件令牌在此一律 unknown，权限不互换；
        - 盲槽尚未提交时不消费、不报错，返回 (False, 'pending', row)：
          内容到位后沿用同一条 active→consumed 条件更新原子单次取件。
        """
        now = utcnow()
        result = await self.session.execute(
            update(Delivery)
            .where(
                Delivery.token_hash == pickup_token_hash,
                Delivery.submit_token_hash.is_not(None),
                Delivery.status == ACTIVE,
                Delivery.expires_at > now,
            )
            .values(status=CONSUMED, consumed_at=now)
            .returning(*_DELIVERY_COLS)
        )
        row = result.first()
        if row is not None:
            await self.session.commit()
            return True, CONSUMED, row

        await self.session.commit()
        await self._lazily_expire(pickup_token_hash, now, by_submit_token=False, blind=True)

        current = (
            await self.session.execute(
                select(Delivery).where(
                    Delivery.token_hash == pickup_token_hash,
                    Delivery.submit_token_hash.is_not(None),
                )
            )
        ).scalar_one_or_none()
        if current is None:
            return False, UNKNOWN, None
        return False, current.status, current

    async def revoke_blind(
        self, pickup_token_hash: str
    ) -> tuple[bool, str, Delivery | None]:
        """收件人凭盲槽私有取件令牌撤回空槽或已提交的投递。

        pending（尚未提交）与 active（已提交等待取件）均可撤回；
        已 consumed / expired 返回相应终态；令牌不存在或属于普通投递
        则 unknown。返回结构与 ``revoke`` 相同。
        """
        now = utcnow()
        result = await self.session.execute(
            update(Delivery)
            .where(
                Delivery.token_hash == pickup_token_hash,
                Delivery.submit_token_hash.is_not(None),
                Delivery.status.in_((PENDING, ACTIVE)),
                Delivery.expires_at > now,
            )
            .values(status=REVOKED, revoked_at=now)
            .returning(
                Delivery.id,
                Delivery.status,
                Delivery.expires_at,
                Delivery.revoked_at,
                Delivery.submit_token_hash,
            )
        )
        row = result.first()
        if row is not None:
            await self.session.commit()
            # RETURNING 已带 id/revoked_at/submit_token_hash，无需 commit 后回读
            return True, REVOKED, row

        await self.session.commit()
        await self._lazily_expire(pickup_token_hash, now, by_submit_token=False, blind=True)

        current = (
            await self.session.execute(
                select(Delivery).where(
                    Delivery.token_hash == pickup_token_hash,
                    Delivery.submit_token_hash.is_not(None),
                )
            )
        ).scalar_one_or_none()
        if current is None:
            return False, UNKNOWN, None
        return False, current.status, current

    async def _lazily_expire(
        self,
        token_hash: str,
        now: datetime,
        *,
        by_submit_token: bool = False,
        blind: bool = False,
    ) -> bool:
        """把已到期且仍处于可过期状态的行提升为 expired。

        - by_submit_token=True 时按提交令牌列定位（提交路径），
          否则按取件令牌列定位（取件/撤回路径）；
        - blind=True 仅作用于盲槽，False 仅作用于普通投递：
          两类接口的失败路径互不触及对方的行（权限不互换）。

        必须在 *独立事务* 中执行：调用方的读事务可能持有旧快照
        （SQLite/REPEATABLE READ），在其中回写过期状态会覆盖
        并发赢家刚刚提交的终态。
        """
        col = Delivery.submit_token_hash if by_submit_token else Delivery.token_hash
        if blind:
            eligible = Delivery.status.in_((ACTIVE, PENDING))
            scope = Delivery.submit_token_hash.is_not(None)
        else:
            eligible = Delivery.status == ACTIVE
            scope = Delivery.submit_token_hash.is_(None)
        result = await self.session.execute(
            update(Delivery)
            .where(col == token_hash, scope, eligible, Delivery.expires_at <= now)
            .values(status=EXPIRED, expired_at=now)
        )
        await self.session.commit()
        return (result.rowcount or 0) > 0

    async def claim(self, token_hash: str) -> tuple[bool, str, Delivery | None]:
        """原子消费。

        返回 (won, state, row)：
        - won=True  本次调用是唯一赢家，state=consumed，row 带明文内容
                    （内容仅此一次从事务中取出）。
        - won=False 本次未抢到，state 为明确的失败原因：
                    consumed（已被他人取走）/ revoked / expired / unknown，
                    row 不含内容（由上层按需读取，但不返回给调用方）。

        正确性说明：
        - 条件 UPDATE ... WHERE status='active' AND 未过期 是唯一裁决点。
          PostgreSQL（READ COMMITTED）用行锁串行化并发更新，仅一个事务命中。
        - 未命中即提交，绝不在同一事务里回写过期，避免旧快照覆盖赢家终态。
        - 仅作用于普通投递（submit_token_hash IS NULL）：盲槽取件令牌在本
          接口不具备任何权限，一律走 unknown，两类令牌不互换。
        """
        now = utcnow()
        result = await self.session.execute(
            update(Delivery)
            .where(
                Delivery.token_hash == token_hash,
                Delivery.submit_token_hash.is_(None),
                Delivery.status == ACTIVE,
                Delivery.expires_at > now,
            )
            .values(status=CONSUMED, consumed_at=now)
            .returning(*_DELIVERY_COLS)
        )
        row = result.first()
        if row is not None:
            await self.session.commit()
            return True, CONSUMED, row

        # 未命中：结束本事务后，在独立事务里幂等地推进过期，再读终态
        await self.session.commit()
        await self._lazily_expire(token_hash, now)

        current = (
            await self.session.execute(
                select(Delivery).where(
                    Delivery.token_hash == token_hash,
                    Delivery.submit_token_hash.is_(None),
                )
            )
        ).scalar_one_or_none()
        if current is None:
            return False, UNKNOWN, None
        return False, current.status, current

    async def revoke(self, token_hash: str) -> tuple[bool, str, Delivery | None]:
        """原子撤回。返回 (won, state, row)。

        won=True 表示本次成功撤回；won=False 时 state 为
        consumed / expired / unknown（已被取走、已过期、令牌不存在）。
        仅作用于普通投递；盲槽的撤回走 revoke_blind（凭盲槽取件令牌）。
        """
        now = utcnow()
        result = await self.session.execute(
            update(Delivery)
            .where(
                Delivery.token_hash == token_hash,
                Delivery.submit_token_hash.is_(None),
                Delivery.status == ACTIVE,
                Delivery.expires_at > now,
            )
            .values(status=REVOKED, revoked_at=now)
            .returning(Delivery.id, Delivery.status, Delivery.expires_at, Delivery.revoked_at)
        )
        row = result.first()
        if row is not None:
            await self.session.commit()
            current = (
                await self.session.execute(
                    select(Delivery).where(
                        Delivery.token_hash == token_hash,
                        Delivery.submit_token_hash.is_(None),
                    )
                )
            ).scalar_one()
            return True, REVOKED, current

        await self.session.commit()
        await self._lazily_expire(token_hash, now)

        current = (
            await self.session.execute(
                select(Delivery).where(
                    Delivery.token_hash == token_hash,
                    Delivery.submit_token_hash.is_(None),
                )
            )
        ).scalar_one_or_none()
        if current is None:
            return False, UNKNOWN, None
        return False, current.status, current

    async def sweep_expired(self) -> int:
        """后台批量标记过期（惰性过期之外的兜底），返回影响行数。

        普通投递只在 active 上等待过期；盲槽在 pending（无人提交）或
        active（已提交待取件）两种状态下都可能到期。
        """
        result = await self.session.execute(
            update(Delivery)
            .where(
                Delivery.status.in_((ACTIVE, PENDING)),
                Delivery.expires_at <= utcnow(),
            )
            .values(status=EXPIRED, expired_at=utcnow())
        )
        await self.session.commit()
        return result.rowcount or 0

    # ---------------- 批量取件（一次性领齐） ----------------

    async def claim_batch(
        self, token_hashes: list[str]
    ) -> "BatchClaimOutcome":
        """原子批量消费：要么全部成功消费，要么一项也不消费。

        入参为按输入顺序排列、已去重的取件令牌哈希（普通投递与盲槽取件
        令牌均可；盲槽**提交**令牌查无此列，按 unknown 处理）。

        裁决在单个事务内完成：

        1. 用一条 ``SELECT ... WHERE token_hash IN (...) ORDER BY id FOR UPDATE``
           锁定全部命中行。PostgreSQL 以行锁把并发单件取件/撤回/重叠批次
           串行化；固定按 id 排序加锁，批次之间也不会互相死锁。
        2. 在锁保护下逐项分类。任一项不可领即整体放弃：先回滚（不留下
           任何部分消费），再在独立事务里惰性提升过期，返回各项原因。
        3. 全部可领（普通/盲槽的文本，active 且未到期）时，以一条带
           id 白名单的条件 UPDATE 完成整批 ``active→consumed``：在
           READ COMMITTED 下，对其中任一行的并发修改都会阻塞并在锁释放
           后按最新行版本重新判定 WHERE，竞争失败即整事务回滚重试，
           因此绝不可能出现“消费了一半”的提交。

        返回 ``BatchClaimOutcome``：won=True 时 rows 与 consumed_at
        有效；won=False 时 items 给出每项（含全部输入项）的分类。
        """
        is_sqlite = self.session.bind.dialect.name == "sqlite"
        last_error: Exception | None = None

        for _attempt in range(1, _MAX_BATCH_ATTEMPTS + 1):
            try:
                rows = await self._lock_batch_rows(token_hashes)
                items = self._classify_batch(token_hashes, rows)
                claimable = [
                    it for it in items if it.state == ACTIVE and it.kind == "text"
                ]
                if len(claimable) != len(token_hashes):
                    # 至少一项不满足：整体放弃，事务回滚，不产生任何消费
                    await self.session.rollback()
                    await self._lazily_expire_batch(
                        [it.row_id for it in items if it.row_id is not None]
                    )
                    return BatchClaimOutcome(won=False, items=items)

                now = utcnow()
                ids = [it.row_id for it in claimable]
                # 在状态迁移前提取内容快照：bulk UPDATE 不刷新会话中的
                # ORM 实例，commit 后更不能回读（避免并发下惰性 IO）。
                contents = [
                    BatchClaimedContent(
                        position=it.position,
                        delivery_id=it.row_id,
                        content=it.row.content,
                        content_sha256=it.row.content_sha256,
                    )
                    for it in claimable
                ]
                result = await self.session.execute(
                    update(Delivery)
                    .where(
                        Delivery.id.in_(ids),
                        Delivery.status == ACTIVE,
                        Delivery.expires_at > now,
                    )
                    .values(status=CONSUMED, consumed_at=now)
                    .execution_options(synchronize_session=False)
                )
                if result.rowcount != len(ids):
                    # 锁等待期间有并发赢家（单件取件/撤回/另一批次）改写了
                    # 其中某行：整事务回滚重试，绝不留下部分消费。
                    await self.session.rollback()
                    await asyncio.sleep(0)
                    continue
                await self.session.commit()
                # contents 与输入顺序一致（claimable 按 items 顺序过滤）
                return BatchClaimOutcome(
                    won=True, items=items, contents=contents, consumed_at=now
                )
            except sqlalchemy.exc.OperationalError as exc:
                last_error = exc
                # PG：锁等待中被死锁检测器中止（40P01）；SQLite/WAL：
                # 写事务快照过期（SQLITE_BUSY_SNAPSHOT）。均整事务回滚重试。
                await self.session.rollback()
                await asyncio.sleep(0)
                if not is_sqlite and not _is_pg_deadlock(exc):
                    raise
                continue

        raise RuntimeError(
            f"批量取件在 {_MAX_BATCH_ATTEMPTS} 次尝试后仍未完成: {last_error!r}"
        )

    async def inspect_batch(
        self, token_hashes: list[str]
    ) -> list["BatchClaimItem"]:
        """只读分类，不消费、不改状态（供快速拒绝路径给出各项原因）。

        分类结果与 ``claim_batch`` 失败路径完全一致；读取后在独立事务中
        惰性提升到期行，保持与单件取件相同的过期语义。
        """
        rows = await self._load_batch_rows(token_hashes)
        items = self._classify_batch(token_hashes, rows)
        await self.session.commit()
        await self._lazily_expire_batch(
            [it.row_id for it in items if it.row_id is not None]
        )
        return items

    async def _lock_batch_rows(self, token_hashes: list[str]) -> list[Delivery]:
        """锁定并返回全部命中的取件行（普通投递 + 盲槽），按 id 固定排序。

        盲槽提交令牌（存于 submit_token_hash 列）不会命中，留待分类为
        unknown。固定排序保证重叠批次按同一顺序申请行锁，杜绝批次间死锁。
        """
        stmt = (
            select(Delivery)
            .where(Delivery.token_hash.in_(token_hashes))
            .order_by(Delivery.id.asc())
        )
        if self.session.bind.dialect.name == "postgresql":
            stmt = stmt.with_for_update()
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def _load_batch_rows(self, token_hashes: list[str]) -> list[Delivery]:
        """``_lock_batch_rows`` 的非加锁版本（只读快速拒绝路径）。"""
        result = await self.session.execute(
            select(Delivery).where(Delivery.token_hash.in_(token_hashes))
        )
        return list(result.scalars().all())

    @staticmethod
    def _classify_batch(
        token_hashes: list[str], rows: list[Delivery]
    ) -> list["BatchClaimItem"]:
        """按输入顺序给出每项的裁决原因；锁内调用，行状态即裁决依据。

        终态（consumed/revoked/expired）优先，随后对待裁决行判断到期；
        active 且未到期的行中，文件投递（普通或盲槽）报 type_mismatch，
        盲槽空槽报 pending（待提交），其余文本为可消费（state=active）。
        到期时刻等于裁决时刻视为过期（expires_at <= now）。
        """
        now = utcnow()
        by_hash = {row.token_hash: row for row in rows}
        items: list[BatchClaimItem] = []
        for position, token_h in enumerate(token_hashes):
            row = by_hash.get(token_h)
            if row is None:
                items.append(
                    BatchClaimItem(position=position, state=UNKNOWN, kind=None)
                )
                continue
            if row.status in (CONSUMED, REVOKED, EXPIRED):
                items.append(
                    BatchClaimItem(
                        position=position,
                        state=row.status,
                        kind=row.kind,
                        row_id=row.id,
                        row=row,
                    )
                )
                continue
            if ensure_utc(row.expires_at) <= now:
                items.append(
                    BatchClaimItem(
                        position=position,
                        state=EXPIRED,
                        kind=row.kind,
                        row_id=row.id,
                        row=row,
                    )
                )
                continue
            if row.kind == "file":
                # 批量取件只交付文本：文件投递（普通/盲槽）类型不符
                items.append(
                    BatchClaimItem(
                        position=position,
                        state=TYPE_MISMATCH,
                        kind="file",
                        row_id=row.id,
                        row=row,
                    )
                )
                continue
            if row.status == PENDING or row.kind == PENDING:
                items.append(
                    BatchClaimItem(
                        position=position,
                        state=PENDING,
                        kind=PENDING,
                        row_id=row.id,
                        row=row,
                    )
                )
                continue
            # active 且未到期的文本（普通投递或已提交盲槽）：可消费
            items.append(
                BatchClaimItem(
                    position=position,
                    state=ACTIVE,
                    kind=row.kind,
                    row_id=row.id,
                    row=row,
                )
            )
        return items

    async def _lazily_expire_batch(self, row_ids: list) -> None:
        """把本批命中行中已到期且仍可过期的行幂等地提升为 expired。

        必须在调用方读/裁决事务结束后的*独立事务*中执行（与单件路径的
        ``_lazily_expire`` 同理），避免旧快照覆盖并发赢家刚提交的终态；
        同时覆盖普通投递与盲槽（本接口对两类取件令牌都有权限）。
        """
        if not row_ids:
            return
        now = utcnow()
        await self.session.execute(
            update(Delivery)
            .where(
                Delivery.id.in_(row_ids),
                Delivery.status.in_((ACTIVE, PENDING)),
                Delivery.expires_at <= now,
            )
            .values(status=EXPIRED, expired_at=now)
            .execution_options(synchronize_session=False)
        )
        await self.session.commit()

    # ---------------- 审计哈希链 ----------------

    async def write_audit(
        self,
        *,
        action: str,
        result: str,
        delivery_id: uuid.UUID | None = None,
        token_hash: str | None = None,
        detail: dict | None = None,
        client_ip: str | None = None,
        user_agent: str | None = None,
    ) -> AuditLog:
        """追加一条入链审计记录，并在同一事务中推进链游标。

        未知令牌等失败请求也走这里，因此同样入链。

        步骤（顺序是正确性的关键）：

        1. 读取链游标 (last_seq, last_hash)；
        2. 用读到的游标计算新序号、事件摘要与链哈希；
        3. **先**执行条件 UPDATE 推进游标（WHERE last_seq=:read），
           这是唯一裁决点——抢占成功后才 INSERT 审计行，因此竞态时
           不会先撞 seq 唯一索引；
        4. 命中：INSERT 审计行并提交（此时仍持有链游标行锁，序号无争议）；
           未命中：重试。

        并发裁决：

        - PostgreSQL（READ COMMITTED）：条件 UPDATE 在链游标单行上排队，
          持锁者提交后 WHERE 按最新行版本重新判定，只有一个事务命中；
          未命中的事务没有产生任何写入，直接重读游标重试即可，
          同事务中的其它写入（如新建投递的 INSERT）不受影响。
        - SQLite（测试，WAL）：读快照不随提交前进，且读事务首次写入时
          若他人已提交会得到 SQLITE_BUSY_SNAPSHOT。两种失配都以
          整事务回滚 + 重读重试解决；此路径只可能发生在纯审计追加场景
          （create 路径以写 INSERT 开始，拿到写锁后快照必然最新；
          claim/revoke 的状态迁移已提前提交）。
        """
        created_at = utcnow()
        ua = (user_agent or "")[:512] or None
        ip = (client_ip or None)[:64] if client_ip else None
        th = (token_hash or None)[:64] if token_hash else None

        is_sqlite = self.session.bind.dialect.name == "sqlite"

        for _attempt in range(1, _MAX_CHAIN_ATTEMPTS + 1):
            last_seq, last_hash = await self._load_chain_state()
            seq = last_seq + 1
            digest = event_digest_for(
                seq=seq,
                created_at=created_at,
                action=action,
                result=result,
                delivery_id=delivery_id,
                token_hash=th,
                client_ip=ip,
                user_agent=ua,
                detail=detail,
            )
            entry_hash_value = chain_hash(last_hash, digest)

            try:
                # 唯一裁决点：仅当链游标仍停在本事务读到的位置时才推进
                cursor_result = await self.session.execute(
                    update(AuditChainState)
                    .where(
                        AuditChainState.id == 1,
                        AuditChainState.last_seq == last_seq,
                    )
                    .values(
                        last_seq=seq,
                        last_hash=entry_hash_value,
                        updated_at=created_at,
                    )
                )
            except sqlalchemy.exc.OperationalError:
                # SQLite/WAL：读快照过期导致的 SQLITE_BUSY_SNAPSHOT。
                # 整事务回滚以获取新快照（此路径无同事务兄弟写入，见上）。
                if not is_sqlite:
                    raise
                await self.session.rollback()
                await asyncio.sleep(0)
                continue

            if cursor_result.rowcount == 1:
                # 已独占链游标行锁：审计行的 seq 无争议，随外层事务原子提交
                entry = AuditLog(
                    delivery_id=delivery_id,
                    token_hash=th,
                    action=action,
                    result=result,
                    detail=detail,
                    client_ip=ip,
                    user_agent=ua,
                    created_at=created_at,
                    seq=seq,
                    event_digest=digest,
                    prev_hash=last_hash,
                    entry_hash=entry_hash_value,
                )
                self.session.add(entry)
                await self.session.flush()
                await self.session.commit()
                return entry

            # 游标已被他人推进：
            # - PG：本 UPDATE 未产生任何写入，重读（READ COMMITTED 可见新提交）
            #   即可重试，同事务兄弟写入原样保留；
            # - SQLite：快照不会前进，需整事务回滚后重读。
            if is_sqlite:
                await self.session.rollback()
            await asyncio.sleep(0)

        raise RuntimeError(
            f"审计链追加在 {_MAX_CHAIN_ATTEMPTS} 次尝试后仍未成功"
        )

    async def _load_chain_state(self) -> tuple[int, str]:
        """读取链游标；极端情况下（旧库未种子化）就地自愈。"""
        state_row = (
            await self.session.execute(
                select(AuditChainState.last_seq, AuditChainState.last_hash).where(
                    AuditChainState.id == 1
                )
            )
        ).first()
        if state_row is None:
            self.session.add(
                AuditChainState(id=1, last_seq=0, last_hash=GENESIS_HASH)
            )
            await self.session.flush()
            return 0, GENESIS_HASH
        return state_row.last_seq, state_row.last_hash

    async def iter_audit_range(
        self,
        *,
        start: datetime | None,
        end: datetime | None,
    ) -> AsyncIterator[ChainEntry]:
        """按链序号顺序流式迭代时间范围内的审计记录（只读）。

        时间范围约定：created_at >= start 且 created_at < end（左闭右开）。
        只选择链/元数据列，不触碰任何投递明文内容。
        """
        stmt = select(*_CHAIN_COLS)
        if start is not None:
            stmt = stmt.where(AuditLog.created_at >= start)
        if end is not None:
            stmt = stmt.where(AuditLog.created_at < end)
        stmt = stmt.order_by(AuditLog.seq.asc()).execution_options(stream_results=True)

        result = await self.session.stream(stmt)
        try:
            async for row in result:
                yield ChainEntry.from_row(row)
        finally:
            await result.close()

    async def audit_range_meta(
        self,
        *,
        start: datetime | None,
        end: datetime | None,
    ) -> tuple[int, ChainEntry | None, ChainEntry | None]:
        """返回时间范围内的 (记录数, 首条, 末条)，与流式查询共享同一快照。"""
        filters = []
        if start is not None:
            filters.append(AuditLog.created_at >= start)
        if end is not None:
            filters.append(AuditLog.created_at < end)

        count = await self.session.scalar(
            select(func.count())
            .select_from(AuditLog)
            .where(*filters)
        )
        if not count:
            return 0, None, None

        first = (
            await self.session.execute(
                self._range_stmt(filters, ascending=True)
            )
        ).first()
        last = (
            await self.session.execute(
                self._range_stmt(filters, ascending=False)
            )
        ).first()
        return int(count or 0), ChainEntry.from_row(first), ChainEntry.from_row(last)

    @staticmethod
    def _range_stmt(filters: Iterable, *, ascending: bool):
        order = AuditLog.seq.asc() if ascending else AuditLog.seq.desc()
        return select(*_CHAIN_COLS).where(*filters).order_by(order).limit(1)

    async def entry_hash_before(self, first_seq: int) -> str | None:
        """供片段校验锚定：返回序号 ``first_seq - 1`` 记录的链哈希。

        库中不存在该记录（例如范围起点之前无记录）时返回 None。
        """
        if first_seq <= 1:
            return GENESIS_HASH
        row = await self.session.scalar(
            select(AuditLog.entry_hash)
            .where(AuditLog.seq == first_seq - 1)
        )
        return row
