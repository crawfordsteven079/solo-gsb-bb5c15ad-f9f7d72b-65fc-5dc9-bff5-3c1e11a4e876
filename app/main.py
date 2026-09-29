"""FastAPI 应用：创建 / 取件 / 撤回 / 盲投递空槽 / 健康检查。

原子消费链路：
    请求 → Redis 终态标记快速拒绝（best-effort，按令牌类型分命名空间）
         → PostgreSQL 单条条件 UPDATE ... RETURNING（正确性的唯一裁决）
         → 成功后回写 Redis 终态标记

盲投递链路：
    收件人开槽(pending) → 提交者按提交令牌条件 UPDATE 竞写(唯一赢家, pending→active)
                       → 收件人取件令牌：提交前 pending，提交后同一原子消费
                       → 撤回/过期同样以条件 UPDATE 裁决，失败方得明确终态
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timedelta

import redis.exceptions
from fastapi import (
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse, Response, StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .audit_chain import (
    GENESIS_HASH,
    ChainEntry,
    ChainFormatError,
    dumps_ndjson,
    verify_chain,
)
from .config import Settings, get_settings
from .db import Database
from .models import Delivery
from .redis_client import DenyCache, create_redis
from .schemas import (
    AuditVerifyResponse,
    BatchPickupFailureItem,
    BatchPickupFailureResponse,
    BatchPickupItem,
    BatchPickupRequest,
    BatchPickupResponse,
    BlindCreateRequest,
    BlindCreateResponse,
    BlindPendingResponse,
    BlindSubmitResponse,
    BlindSubmitTextRequest,
    CreateResponse,
    CreateTextRequest,
    DenyResponse,
    HealthResponse,
    RevokeResponse,
    TextPickupResponse,
)
from .store import (
    CONSUMED,
    EXPIRED,
    PENDING,
    REVOKED,
    SUBMITTED,
    UNKNOWN,
    DeliveryStore,
)
from .utils import (
    client_ip,
    content_disposition,
    ensure_utc,
    hash_token,
    new_token,
    sha256_hex,
    utcnow,
)

logger = logging.getLogger("dropbox")

# 终态 -> HTTP 状态码（取件与撤回共用）
_DENY_STATUS = {
    CONSUMED: status.HTTP_410_GONE,
    EXPIRED: status.HTTP_410_GONE,
    REVOKED: status.HTTP_409_CONFLICT,
    SUBMITTED: status.HTTP_409_CONFLICT,
    PENDING: status.HTTP_200_OK,
    UNKNOWN: status.HTTP_404_NOT_FOUND,
}
_DENY_DETAIL = {
    CONSUMED: "该令牌已被成功取件，内容仅可读取一次。",
    EXPIRED: "投递已过期，内容不可再读取。",
    REVOKED: "投递已被发送者撤回。",
    SUBMITTED: "空槽已被另一个提交者成功写入，提交令牌只能使用一次。",
    PENDING: "空槽尚未收到提交内容。",
    UNKNOWN: "取件令牌不存在。",
}
# 提交接口的拒绝文案（令牌语义不同，单独说明，避免泄露其它接口的权限语义）
_SUBMIT_DENY_DETAIL = {
    SUBMITTED: "该空槽已有提交者成功写入，提交令牌只能成功使用一次。",
    REVOKED: "空槽已被收件人撤回，无法再提交。",
    EXPIRED: "空槽已过期，无法再提交。",
    UNKNOWN: "提交令牌不存在。",
}


# ---------------- 依赖注入 ----------------


async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    db: Database = request.app.state.db
    async with db.session_factory() as session:
        yield session


def get_store(session: AsyncSession = Depends(get_db)) -> DeliveryStore:
    return DeliveryStore(session)


async def get_cache(request: Request) -> DenyCache:
    return request.app.state.cache


# ---------------- 应用生命周期 ----------------


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = get_settings()
    app.state.db = Database()
    app.state.cache = create_redis()
    await app.state.db.create_schema()

    stop = asyncio.Event()
    sweeper_task = asyncio.create_task(_sweep_loop(app.state.db, cfg, stop))
    try:
        yield
    finally:
        stop.set()
        sweeper_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sweeper_task
        await app.state.db.close()
        await app.state.cache.close()


async def _sweep_loop(db: Database, cfg: Settings, stop: asyncio.Event) -> None:
    """惰性过期之外的兜底：定时把到期 active 行批量标记为 expired。"""
    while not stop.is_set():
        try:
            async with db.session_factory() as session:
                count = await DeliveryStore(session).sweep_expired()
                if count:
                    logger.info("sweep marked %d deliveries expired", count)
        except Exception:  # noqa: BLE001 - 清扫失败不应杀死进程
            logger.exception("sweep cycle failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg.sweep_interval_seconds)
        except asyncio.TimeoutError:
            pass


app = FastAPI(
    title="一次性内容投递箱 API",
    version="1.0.0",
    description="创建带过期时间的文本/小文件投递，取件令牌只能成功读取一次。",
    lifespan=lifespan,
)


# ---------------- 辅助 ----------------


def _deny(result: str, http_status: int | None = None, *, details: dict | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=http_status or _DENY_STATUS[result],
        content=DenyResponse(
            result=result, detail=(details or _DENY_DETAIL)[result]
        ).model_dump(mode="json"),
    )


async def _audit(
    store: DeliveryStore,
    *,
    action: str,
    result: str,
    request: Request,
    delivery_id=None,
    token_hash: str | None = None,
    detail: dict | None = None,
) -> None:
    await store.write_audit(
        action=action,
        result=result,
        delivery_id=delivery_id,
        token_hash=token_hash,
        detail=detail,
        client_ip=client_ip(request),
        user_agent=request.headers.get("user-agent"),
    )


async def _fast_flag(cache: DenyCache, token_hash: str) -> str | None:
    """读 Redis 终态标记；Redis 故障时静默回落数据库。"""
    try:
        return await cache.get_flag(token_hash)
    except (redis.exceptions.RedisError, OSError):
        logger.warning("redis unavailable, falling back to database")
        return None


async def _fast_submit_flag(cache: DenyCache, token_hash: str) -> str | None:
    """读提交令牌命名空间下的终态标记；Redis 故障时静默回落数据库。"""
    try:
        return await cache.get_submit_flag(token_hash)
    except (redis.exceptions.RedisError, OSError):
        logger.warning("redis unavailable, falling back to database")
        return None


async def _remember_terminal(cache: DenyCache, token_hash: str, state: str, cfg: Settings) -> None:
    try:
        await cache.set_flag_if_absent(token_hash, state, cfg.redis_flag_ttl_seconds)
    except (redis.exceptions.RedisError, OSError):
        logger.warning("redis unavailable, terminal flag not cached")


async def _remember_submit_terminal(
    cache: DenyCache, token_hash: str, state: str, cfg: Settings
) -> None:
    try:
        await cache.set_submit_flag_if_absent(token_hash, state, cfg.redis_flag_ttl_seconds)
    except (redis.exceptions.RedisError, OSError):
        logger.warning("redis unavailable, terminal flag not cached")


def _validate_ttl(ttl_seconds: int, cfg: Settings) -> None:
    if not (cfg.ttl_min_seconds <= ttl_seconds <= cfg.ttl_max_seconds):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"ttl_seconds 必须在 {cfg.ttl_min_seconds} 与 "
                f"{cfg.ttl_max_seconds} 之间"
            ),
        )


def _check_size(size: int, cfg: Settings) -> None:
    if size > cfg.max_content_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"内容超过大小上限 {cfg.max_content_bytes} 字节",
        )


async def _delivery_id_by_hash(session: AsyncSession, token_hash: str):
    """快速拒绝路径下，用取件令牌哈希反查 delivery_id（普通投递，仅主键）。"""
    return await session.scalar(
        select(Delivery.id).where(
            Delivery.token_hash == token_hash,
            Delivery.submit_token_hash.is_(None),
        )
    )


async def _blind_id_by_pickup_hash(session: AsyncSession, token_hash: str):
    """快速拒绝路径下，用取件令牌哈希反查盲槽 id（仅盲槽，仅主键）。"""
    return await session.scalar(
        select(Delivery.id).where(
            Delivery.token_hash == token_hash,
            Delivery.submit_token_hash.is_not(None),
        )
    )


async def _blind_id_by_submit_hash(session: AsyncSession, token_hash: str):
    """快速拒绝路径下，用提交令牌哈希反查盲槽 id（仅盲槽，仅主键）。"""
    return await session.scalar(
        select(Delivery.id).where(Delivery.submit_token_hash == token_hash)
    )


# ---------------- 创建 ----------------


@app.post(
    "/api/v1/deliveries",
    response_model=CreateResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["deliveries"],
    summary="创建文本投递",
)
async def create_text(
    payload: CreateTextRequest,
    request: Request,
    store: DeliveryStore = Depends(get_store),
    cache: DenyCache = Depends(get_cache),
    cfg: Settings = Depends(get_settings),
) -> CreateResponse:
    _validate_ttl(payload.ttl_seconds, cfg)
    content = payload.text.encode("utf-8")
    _check_size(len(content), cfg)

    token, token_h = new_token()
    expires_at = utcnow() + timedelta(seconds=payload.ttl_seconds)
    digest = sha256_hex(content)

    delivery = await store.create(
        token_hash=token_h,
        kind="text",
        content=content,
        expires_at=expires_at,
        content_type="text/plain; charset=utf-8",
        content_sha256=digest,
    )
    await _audit(
        store,
        action="create",
        result="success",
        request=request,
        delivery_id=delivery.id,
        token_hash=token_h,
        detail={"kind": "text", "ttl_seconds": payload.ttl_seconds, "size": len(content)},
    )
    await store.session.commit()

    return CreateResponse(
        delivery_id=delivery.id,
        token=token,
        expires_at=expires_at,
        url=f"{str(request.base_url).rstrip('/')}/api/v1/pickup/{token}",
    )


@app.post(
    "/api/v1/deliveries/file",
    response_model=CreateResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["deliveries"],
    summary="创建小文件投递（multipart/form-data）",
)
async def create_file(
    request: Request,
    file: UploadFile = File(..., description="要投递的小文件"),
    ttl_seconds: int = Form(24 * 3600),
    store: DeliveryStore = Depends(get_store),
    cache: DenyCache = Depends(get_cache),
    cfg: Settings = Depends(get_settings),
) -> CreateResponse:
    _validate_ttl(ttl_seconds, cfg)

    # 流式读取，最多多读 1 字节以判定超限，避免把超大文件整块载入内存
    max_bytes = cfg.max_content_bytes
    chunks: list[bytes] = []
    total = 0
    oversize = False
    while chunk := await file.read(64 * 1024):
        total += len(chunk)
        if total > max_bytes:
            oversize = True
            break
        chunks.append(chunk)
    if oversize:
        _check_size(max_bytes + 1, cfg)  # 统一抛出 413

    content = b"".join(chunks)
    token, token_h = new_token()
    expires_at = utcnow() + timedelta(seconds=ttl_seconds)
    digest = sha256_hex(content)
    filename = file.filename or "file.bin"
    content_type = file.content_type or "application/octet-stream"

    delivery = await store.create(
        token_hash=token_h,
        kind="file",
        content=content,
        expires_at=expires_at,
        filename=filename[:255],
        content_type=content_type[:255],
        content_sha256=digest,
    )
    await _audit(
        store,
        action="create",
        result="success",
        request=request,
        delivery_id=delivery.id,
        token_hash=token_h,
        detail={"kind": "file", "ttl_seconds": ttl_seconds, "size": total,
                "filename": filename[:255]},
    )
    await store.session.commit()

    return CreateResponse(
        delivery_id=delivery.id,
        token=token,
        expires_at=expires_at,
        url=f"{str(request.base_url).rstrip('/')}/api/v1/pickup/{token}",
    )


# ---------------- 盲投递请求：空槽 ----------------


@app.post(
    "/api/v1/blind-slots",
    response_model=BlindCreateResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["blind-slots"],
    summary="收件人创建盲投递空槽（返回一次性提交令牌与私有取件令牌）",
)
async def create_blind_slot(
    payload: BlindCreateRequest,
    request: Request,
    store: DeliveryStore = Depends(get_store),
    cfg: Settings = Depends(get_settings),
) -> BlindCreateResponse:
    """收件人先开一个空槽，对外分发**提交令牌**，自己保留**取件令牌**。

    - 提交令牌：任何持有者至多成功写入一份文本/小文件，之后立即失效；
    - 取件令牌：提交前查询返回 pending，提交后沿用原子单次取件，
      并可用于撤回；
    - 两类令牌分别独立生成、分别哈希存储、按不同列唯一索引，
      任何接口都不会接受对方的令牌（交叉使用一律 unknown）。
    """
    _validate_ttl(payload.ttl_seconds, cfg)

    submit_token, submit_h = new_token()
    pickup_token, pickup_h = new_token()
    expires_at = utcnow() + timedelta(seconds=payload.ttl_seconds)

    slot = await store.create_slot(
        submit_token_hash=submit_h,
        pickup_token_hash=pickup_h,
        expires_at=expires_at,
    )
    await _audit(
        store,
        action="blind_create",
        result="success",
        request=request,
        delivery_id=slot.id,
        token_hash=pickup_h,
        detail={
            "ttl_seconds": payload.ttl_seconds,
            "submit_token_hash": submit_h,
        },
    )
    await store.session.commit()

    base = str(request.base_url).rstrip("/")
    return BlindCreateResponse(
        slot_id=slot.id,
        submit_token=submit_token,
        pickup_token=pickup_token,
        expires_at=expires_at,
        submit_url=f"{base}/api/v1/blind-slots/submit/{submit_token}",
        pickup_url=f"{base}/api/v1/blind-slots/pickup/{pickup_token}",
        revoke_url=f"{base}/api/v1/blind-slots/revoke/{pickup_token}",
    )


async def _submit_to_slot(
    *,
    submit_token: str,
    kind: str,
    content: bytes,
    filename: str | None,
    content_type: str,
    request: Request,
    store: DeliveryStore,
    cache: DenyCache,
    cfg: Settings,
    size: int,
):
    """盲槽提交的共用裁决逻辑（文本/文件）。"""
    submit_h = hash_token(submit_token)
    digest = sha256_hex(content)

    # 1) Redis 终态快速拒绝（submitted/revoked/expired；故障回落数据库）。
    #    使用提交令牌独立命名空间，绝不与取件令牌标记互相命中；命中后仍以
    #    数据库行核验作用域，非盲槽提交令牌一律回落裁决得到 unknown。
    flag = await _fast_submit_flag(cache, submit_h)
    if flag is not None:
        scoped_id = await _blind_id_by_submit_hash(store.session, submit_h)
        if scoped_id is not None:
            await _audit(
                store,
                action="blind_submit",
                result=flag,
                request=request,
                delivery_id=scoped_id,
                token_hash=submit_h,
            )
            await store.session.commit()
            return _deny(flag, details=_SUBMIT_DENY_DETAIL)
        # 标记存在但无对应盲槽：忽略标记，回落数据库得到 unknown

    # 2) 数据库条件更新裁决：并发提交只有一个 pending→active 赢家
    won, state, row = await store.submit_content(
        submit_h,
        kind=kind,
        content=content,
        filename=filename,
        content_type=content_type,
        content_sha256=digest,
    )

    if not won:
        delivery_id = getattr(row, "id", None)
        await _audit(
            store,
            action="blind_submit",
            result=state,
            request=request,
            delivery_id=delivery_id,
            token_hash=submit_h,
            detail={"kind": kind, "size": size},
        )
        await store.session.commit()
        if state != UNKNOWN:
            await _remember_submit_terminal(cache, submit_h, state, cfg)
        return _deny(state, details=_SUBMIT_DENY_DETAIL)

    # 3) 唯一赢家：内容已在裁决 UPDATE 中一次性落库
    await _audit(
        store,
        action="blind_submit",
        result="success",
        request=request,
        delivery_id=row.id,
        token_hash=submit_h,
        detail={"kind": kind, "size": size, "filename": filename},
    )
    await store.session.commit()
    await _remember_submit_terminal(cache, submit_h, SUBMITTED, cfg)

    # 注意：响应不回传任何取件令牌/取件地址——提交者无权取件，
    # 收件人的私有取件令牌只在创建空槽时出现一次。
    return BlindSubmitResponse(
        slot_id=row.id,
        kind=kind,
        content_sha256=digest,
        submitted_at=ensure_utc(row.submitted_at) or utcnow(),
    )


@app.post(
    "/api/v1/blind-slots/submit/{submit_token}",
    tags=["blind-slots"],
    summary="凭一次性提交令牌写入文本（并发下仅一人成功）",
    responses={
        200: {"description": "提交成功"},
        409: {"model": DenyResponse, "description": "槽已被提交（submitted）或已撤回（revoked）"},
        410: {"model": DenyResponse, "description": "已过期（expired）"},
        404: {"model": DenyResponse, "description": "提交令牌不存在（unknown）"},
    },
)
async def submit_blind_text(
    submit_token: str,
    payload: BlindSubmitTextRequest,
    request: Request,
    store: DeliveryStore = Depends(get_store),
    cache: DenyCache = Depends(get_cache),
    cfg: Settings = Depends(get_settings),
):
    content = payload.text.encode("utf-8")
    _check_size(len(content), cfg)
    return await _submit_to_slot(
        submit_token=submit_token,
        kind="text",
        content=content,
        filename=None,
        content_type="text/plain; charset=utf-8",
        request=request,
        store=store,
        cache=cache,
        cfg=cfg,
        size=len(content),
    )


@app.post(
    "/api/v1/blind-slots/submit/{submit_token}/file",
    tags=["blind-slots"],
    summary="凭一次性提交令牌写入小文件（multipart/form-data）",
    responses={
        200: {"description": "提交成功"},
        409: {"model": DenyResponse, "description": "槽已被提交（submitted）或已撤回（revoked）"},
        410: {"model": DenyResponse, "description": "已过期（expired）"},
        404: {"model": DenyResponse, "description": "提交令牌不存在（unknown）"},
        413: {"description": "内容超过大小上限"},
    },
)
async def submit_blind_file(
    submit_token: str,
    request: Request,
    file: UploadFile = File(..., description="要写入空槽的小文件"),
    store: DeliveryStore = Depends(get_store),
    cache: DenyCache = Depends(get_cache),
    cfg: Settings = Depends(get_settings),
):
    # 流式读取，最多多读 1 字节以判定超限，避免把超大文件整块载入内存
    max_bytes = cfg.max_content_bytes
    chunks: list[bytes] = []
    total = 0
    oversize = False
    while chunk := await file.read(64 * 1024):
        total += len(chunk)
        if total > max_bytes:
            oversize = True
            break
        chunks.append(chunk)
    if oversize:
        _check_size(max_bytes + 1, cfg)  # 统一抛出 413

    content = b"".join(chunks)
    filename = file.filename or "file.bin"
    content_type = file.content_type or "application/octet-stream"
    return await _submit_to_slot(
        submit_token=submit_token,
        kind="file",
        content=content,
        filename=filename[:255],
        content_type=content_type[:255],
        request=request,
        store=store,
        cache=cache,
        cfg=cfg,
        size=total,
    )


@app.post(
    "/api/v1/blind-slots/pickup/{token}",
    tags=["blind-slots"],
    summary="收件人凭私有取件令牌取件（提交前 pending，提交后原子单次）",
    responses={
        200: {"description": "取件成功，或内容尚未提交（result=pending）"},
        409: {"model": DenyResponse, "description": "已撤回"},
        410: {"model": DenyResponse, "description": "已消费或已过期"},
        404: {"model": DenyResponse, "description": "取件令牌不存在"},
    },
)
async def blind_pickup(
    token: str,
    request: Request,
    store: DeliveryStore = Depends(get_store),
    cache: DenyCache = Depends(get_cache),
    cfg: Settings = Depends(get_settings),
):
    token_h = hash_token(token)

    # 1) Redis 快速拒绝（consumed/revoked/expired）；pending 不是终态，从不缓存。
    #    命中后仍核验该令牌属于盲槽，普通投递令牌的标记不在此泄露。
    flag = await _fast_flag(cache, token_h)
    if flag is not None:
        scoped_id = await _blind_id_by_pickup_hash(store.session, token_h)
        if scoped_id is not None:
            await _audit(
                store,
                action="blind_pickup",
                result=flag,
                request=request,
                delivery_id=scoped_id,
                token_hash=token_h,
            )
            await store.session.commit()
            return _deny(flag)
        # 标记存在但本接口范围无此行：忽略标记，回落数据库得到 unknown

    # 2) 数据库原子消费：pending 原样返回，active 仅一个赢家取走内容
    won, state, row = await store.claim_blind(token_h)

    if won:
        await _audit(
            store,
            action="blind_pickup",
            result="success",
            request=request,
            delivery_id=row.id,
            token_hash=token_h,
            detail={"kind": row.kind, "size": row.content_size},
        )
        await store.session.commit()
        await _remember_terminal(cache, token_h, CONSUMED, cfg)

        consumed_at = ensure_utc(row.consumed_at) or utcnow()
        if row.kind == "file":
            headers = {
                "X-Drop-Result": "success",
                "X-Content-SHA256": row.content_sha256,
                "Content-Disposition": content_disposition(row.filename or "file.bin"),
            }
            return Response(
                content=row.content,
                media_type=row.content_type or "application/octet-stream",
                headers=headers,
            )

        return TextPickupResponse(
            delivery_id=row.id,
            content=row.content.decode("utf-8"),
            content_sha256=row.content_sha256,
            consumed_at=consumed_at,
        )

    delivery_id = getattr(row, "id", None)
    await _audit(
        store,
        action="blind_pickup",
        result=state,
        request=request,
        delivery_id=delivery_id,
        token_hash=token_h,
    )
    await store.session.commit()

    # 内容尚未提交：明确的 pending（HTTP 200，不消费令牌、不改变状态）
    if state == PENDING:
        return BlindPendingResponse(
            slot_id=row.id,
            expires_at=ensure_utc(row.expires_at),
        )

    if state != UNKNOWN:
        await _remember_terminal(cache, token_h, state, cfg)
    return _deny(state)


@app.post(
    "/api/v1/blind-slots/revoke/{token}",
    response_model=RevokeResponse,
    tags=["blind-slots"],
    summary="收件人凭私有取件令牌撤回空槽/已提交投递",
    responses={
        200: {"description": "撤回成功，或此前已撤回（幂等）"},
        409: {"model": DenyResponse, "description": "已被取件"},
        410: {"model": DenyResponse, "description": "已过期"},
        404: {"model": DenyResponse, "description": "取件令牌不存在"},
    },
)
async def blind_revoke(
    token: str,
    request: Request,
    store: DeliveryStore = Depends(get_store),
    cache: DenyCache = Depends(get_cache),
    cfg: Settings = Depends(get_settings),
):
    """撤回盲槽：空槽（pending）阶段撤回可阻止一切提交；已提交（active）
    阶段撤回则内容不再可被取走。提交令牌没有撤回权限。
    """
    token_h = hash_token(token)

    # 命中快标记后同样核验盲槽范围，普通投递令牌的标记不在此泄露
    flag = await _fast_flag(cache, token_h)
    if flag is not None:
        scoped_id = await _blind_id_by_pickup_hash(store.session, token_h)
        if scoped_id is not None:
            await _audit(
                store,
                action="blind_revoke",
                result=flag,
                request=request,
                delivery_id=scoped_id,
                token_hash=token_h,
            )
            await store.session.commit()
            if flag == REVOKED:
                return RevokeResponse(result="already_revoked", delivery_id=scoped_id)
            return _deny(flag)
        # 标记存在但本接口范围无此行：忽略标记，回落数据库得到 unknown

    won, state, row = await store.revoke_blind(token_h)
    delivery_id = getattr(row, "id", None)

    if won:
        await _audit(
            store,
            action="blind_revoke",
            result=REVOKED,
            request=request,
            delivery_id=delivery_id,
            token_hash=token_h,
        )
        await store.session.commit()
        # 取件令牌与提交令牌分别缓存终态（各自独立命名空间）：
        # 两条路径此后都被快速拒绝，且互不泄露到对方接口
        await _remember_terminal(cache, token_h, REVOKED, cfg)
        if row.submit_token_hash:
            await _remember_submit_terminal(cache, row.submit_token_hash, REVOKED, cfg)
        return RevokeResponse(
            result=REVOKED,
            delivery_id=delivery_id,
            revoked_at=ensure_utc(row.revoked_at),
        )

    await _audit(
        store,
        action="blind_revoke",
        result=state,
        request=request,
        delivery_id=delivery_id,
        token_hash=token_h,
    )
    await store.session.commit()
    if state != UNKNOWN:
        await _remember_terminal(cache, token_h, state, cfg)

    if state == CONSUMED or state == EXPIRED:
        return _deny(state, http_status=status.HTTP_410_GONE)
    return _deny(UNKNOWN)


# ---------------- 取件（原子消费） ----------------


@app.post(
    "/api/v1/pickup/{token}",
    tags=["deliveries"],
    summary="凭令牌取件（成功只能一次）",
    responses={
        200: {"description": "取件成功（文本返回 JSON，文件返回二进制流）"},
        409: {"model": DenyResponse, "description": "已撤回"},
        410: {"model": DenyResponse, "description": "已消费或已过期"},
        404: {"model": DenyResponse, "description": "令牌不存在"},
    },
)
async def pickup(
    token: str,
    request: Request,
    store: DeliveryStore = Depends(get_store),
    cache: DenyCache = Depends(get_cache),
    cfg: Settings = Depends(get_settings),
):
    token_h = hash_token(token)

    # 1) Redis 快速拒绝（已知终态）。命中后仍核验该令牌属于普通投递：
    #    盲槽取件令牌与本接口权限不互换，其终态标记不得在此泄露，落库裁决
    #    后必然得到 unknown。
    flag = await _fast_flag(cache, token_h)
    if flag is not None:
        scoped_id = await _delivery_id_by_hash(store.session, token_h)
        if scoped_id is not None:
            await _audit(
                store,
                action="pickup",
                result=flag,
                request=request,
                delivery_id=scoped_id,
                token_hash=token_h,
            )
            await store.session.commit()
            return _deny(flag)
        # 标记存在但本接口范围无此行：忽略标记，回落数据库得到 unknown

    # 2) 数据库原子消费：并发下只有一个事务能赢得这一行
    won, state, row = await store.claim(token_h)

    if not won:
        delivery_id = getattr(row, "id", None)
        await _audit(
            store,
            action="pickup",
            result=state,
            request=request,
            delivery_id=delivery_id,
            token_hash=token_h,
        )
        await store.session.commit()
        if state != UNKNOWN:
            await _remember_terminal(cache, token_h, state, cfg)
        return _deny(state)

    # 3) 赢家：内容只在此刻从事务中取出一次
    await _audit(
        store,
        action="pickup",
        result="success",
        request=request,
        delivery_id=row.id,
        token_hash=token_h,
        detail={"kind": row.kind, "size": row.content_size},
    )
    await store.session.commit()
    await _remember_terminal(cache, token_h, CONSUMED, cfg)

    consumed_at = ensure_utc(row.consumed_at) or utcnow()
    if row.kind == "file":
        headers = {
            "X-Drop-Result": "success",
            "X-Content-SHA256": row.content_sha256,
            "Content-Disposition": content_disposition(row.filename or "file.bin"),
        }
        return Response(
            content=row.content,
            media_type=row.content_type or "application/octet-stream",
            headers=headers,
        )

    return TextPickupResponse(
        delivery_id=row.id,
        content=row.content.decode("utf-8"),
        content_sha256=row.content_sha256,
        consumed_at=consumed_at,
    )


# ---------------- 批量取件（整批原子消费） ----------------


@app.post(
    "/api/v1/batch-pickup",
    response_model=BatchPickupResponse,
    tags=["deliveries"],
    summary="批量取件：1～20 个取件令牌一次性领齐（全有或全无）",
    responses={
        200: {"description": "整批成功：按输入顺序返回全部投递 ID、文本与内容哈希"},
        409: {
            "model": BatchPickupFailureResponse,
            "description": "任一项不满足：整批未消费，仅返回各失败项的输入位置与原因",
        },
        422: {"description": "空列表、超过 20 个或存在重复令牌：整批拒绝"},
    },
)
async def batch_pickup(
    payload: BatchPickupRequest,
    request: Request,
    store: DeliveryStore = Depends(get_store),
    cache: DenyCache = Depends(get_cache),
    cfg: Settings = Depends(get_settings),
):
    """收件人一次性领齐多份文本投递。

    - 接受按顺序排列的 1～20 个普通投递或盲槽**取件**令牌
      （盲槽提交令牌按未知处理；文件投递报类型不符）；
    - 全部令牌都对应“已提交、未到期且未消费/撤回的文本”时，才一起消费，
      并按输入顺序返回投递 ID、文本与内容哈希；
    - 任一项不满足（未知/待提交/过期/已消费/已撤回/类型不符）时整批失败：
      不返回任何内容，也不消费其余项，仅返回各失败项的位置与原因；
    - 重叠批次、单件取件、撤回并发时，每份内容最多被成功领取一次，
      失败批次不留部分消费；该保证只依赖数据库裁决，缓存不可用同样成立。
    """
    token_hashes = [hash_token(t) for t in payload.tokens]
    batch_id = uuid.uuid4()
    batch_size = len(token_hashes)

    won, outcomes = await store.claim_batch(token_hashes)

    if won:
        for outcome in outcomes:
            await _audit(
                store,
                action="batch_pickup",
                result="success",
                request=request,
                delivery_id=outcome.delivery_id,
                token_hash=outcome.token_hash,
                detail={
                    "batch_id": str(batch_id),
                    "position": outcome.position,
                    "batch_size": batch_size,
                    "kind": "text",
                    "size": outcome.content_size,
                },
            )
        for outcome in outcomes:
            await _remember_terminal(cache, outcome.token_hash, CONSUMED, cfg)
        return BatchPickupResponse(
            items=[
                BatchPickupItem(
                    position=outcome.position,
                    delivery_id=outcome.delivery_id,
                    content=outcome.content.decode("utf-8"),
                    content_sha256=outcome.content_sha256,
                )
                for outcome in outcomes
            ]
        )

    for outcome in outcomes:
        await _audit(
            store,
            action="batch_pickup",
            result=outcome.reason,
            request=request,
            delivery_id=outcome.delivery_id,
            token_hash=outcome.token_hash,
            detail={
                "batch_id": str(batch_id),
                "position": outcome.position,
                "batch_size": batch_size,
            },
        )
        if outcome.reason in (CONSUMED, REVOKED, EXPIRED):
            await _remember_terminal(cache, outcome.token_hash, outcome.reason, cfg)

    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content=BatchPickupFailureResponse(
            failures=[
                BatchPickupFailureItem(position=o.position, reason=o.reason)
                for o in outcomes
            ]
        ).model_dump(mode="json"),
    )


# ---------------- 撤回 ----------------


@app.post(
    "/api/v1/revoke/{token}",
    response_model=RevokeResponse,
    tags=["deliveries"],
    summary="发送者撤回投递（凭创建时返回的令牌）",
    responses={
        200: {"description": "撤回成功，或此前已撤回（幂等）"},
        409: {"model": DenyResponse, "description": "已被取件，无法撤回"},
        410: {"model": DenyResponse, "description": "已过期"},
        404: {"model": DenyResponse, "description": "令牌不存在"},
    },
)
async def revoke(
    token: str,
    request: Request,
    store: DeliveryStore = Depends(get_store),
    cache: DenyCache = Depends(get_cache),
    cfg: Settings = Depends(get_settings),
):
    token_h = hash_token(token)

    # Redis 快速拒绝同样先核验普通投递范围；盲槽令牌的标记不泄露到本接口
    flag = await _fast_flag(cache, token_h)
    if flag is not None:
        scoped_id = await _delivery_id_by_hash(store.session, token_h)
        if scoped_id is not None:
            await _audit(
                store,
                action="revoke",
                result=flag,
                request=request,
                delivery_id=scoped_id,
                token_hash=token_h,
            )
            await store.session.commit()
            if flag == REVOKED:
                return RevokeResponse(result="already_revoked", delivery_id=scoped_id)
            # consumed / expired 统一返回带 result 的 JSON 错误体
            return _deny(flag)
        # 标记存在但本接口范围无此行：忽略标记，回落数据库得到 unknown

    won, state, row = await store.revoke(token_h)
    delivery_id = getattr(row, "id", None)

    if won:
        await _audit(
            store,
            action="revoke",
            result=REVOKED,
            request=request,
            delivery_id=delivery_id,
            token_hash=token_h,
        )
        await store.session.commit()
        await _remember_terminal(cache, token_h, REVOKED, cfg)
        return RevokeResponse(
            result=REVOKED,
            delivery_id=delivery_id,
            revoked_at=ensure_utc(row.revoked_at),
        )

    # 未撤回成功：记录与终态一致的审计结果，给出明确响应
    await _audit(
        store,
        action="revoke",
        result=state,
        request=request,
        delivery_id=delivery_id,
        token_hash=token_h,
    )
    await store.session.commit()
    if state != UNKNOWN:
        await _remember_terminal(cache, token_h, state, cfg)

    if state == CONSUMED or state == EXPIRED:
        return _deny(state, http_status=status.HTTP_410_GONE)
    return _deny(UNKNOWN)


# ---------------- 审计：哈希链导出 / 校验 ----------------


def _validate_range(start, end) -> None:
    if start is not None and end is not None and start >= end:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="start 必须早于 end（时间范围为左闭右开）",
        )


@app.get(
    "/api/v1/audit/export",
    tags=["audit"],
    summary="按时间范围导出审计链记录（NDJSON，只读）",
    responses={
        200: {
            "description": (
                "每行一条链记录（application/x-ndjson）。响应头返回起止链哈希与"
                "记录数：X-Chain-Start-Hash / X-Chain-End-Hash / "
                "X-Chain-Record-Count / X-Chain-Genesis-Hash。"
            )
        },
        422: {"description": "时间范围非法"},
    },
)
async def export_audit(
    request: Request,
    start: datetime | None = Query(
        default=None, description="起始时间（含），ISO-8601，例如 2026-09-21T00:00:00Z"
    ),
    end: datetime | None = Query(
        default=None, description="结束时间（不含），ISO-8601"
    ),
):
    """流式导出时间范围内的审计链记录。

    - 只读：PostgreSQL 上以 READ ONLY + REPEATABLE READ 事务读取，
      计数/边界/流内容来自同一快照，且驱动层拒绝任何写操作；
    - 仅含审计字段与链哈希，**绝不包含投递明文内容**。
    """
    _validate_range(start, end)
    db: Database = request.app.state.db

    # 独立 session：其生命周期跨越流式响应（在生成器结束时关闭）
    session_cm = db.session_factory()
    session = await session_cm.__aenter__()
    try:
        conn = await session.connection()
        if conn.dialect.name == "postgresql":
            # 整个导出共享同一快照，且驱动层强制只读
            await conn.exec_driver_sql(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )

        audit_store = DeliveryStore(session)
        count, first, last = await audit_store.audit_range_meta(start=start, end=end)

        async def _stream():
            try:
                async for entry in audit_store.iter_audit_range(start=start, end=end):
                    yield dumps_ndjson(entry)
            finally:
                await session_cm.__aexit__(None, None, None)

        headers = {
            "Content-Type": "application/x-ndjson; charset=utf-8",
            "Content-Disposition": 'attachment; filename="audit-export.ndjson"',
            "X-Chain-Genesis-Hash": GENESIS_HASH,
            "X-Chain-Start-Hash": first.entry_hash if first else "",
            "X-Chain-End-Hash": last.entry_hash if last else "",
            "X-Chain-Record-Count": str(count),
        }
        return StreamingResponse(_stream(), status_code=200, headers=headers)
    except BaseException:
        await session_cm.__aexit__(None, None, None)
        raise


@app.get(
    "/api/v1/audit/verify",
    response_model=AuditVerifyResponse,
    tags=["audit"],
    summary="校验库内审计链完整性（缺行/乱序/重复/改写/断裂）",
)
async def verify_audit_db(
    start: datetime | None = Query(default=None, description="起始时间（含），ISO-8601"),
    end: datetime | None = Query(default=None, description="结束时间（不含），ISO-8601"),
    session: AsyncSession = Depends(get_db),
) -> AuditVerifyResponse:
    """全链（不带时间参数）或时间范围片段校验，返回第一处错误。

    全链校验从创世哈希锚定；片段校验自动以库中该片段首条的前一条记录
    作为链锚。只读，不产生新审计记录。
    """
    _validate_range(start, end)
    store = DeliveryStore(session)
    entries = [e async for e in store.iter_audit_range(start=start, end=end)]

    if not entries:
        return AuditVerifyResponse(ok=True)

    if start is None and end is None:
        expected_seq, anchor = 1, GENESIS_HASH
    else:
        expected_seq = entries[0].seq
        anchor = await store.entry_hash_before(expected_seq)

    report = verify_chain(entries, expected_seq=expected_seq, anchor_hash=anchor)
    return AuditVerifyResponse(**report.__dict__)


@app.post(
    "/api/v1/audit/verify",
    response_model=AuditVerifyResponse,
    tags=["audit"],
    summary="校验一份导出的 NDJSON 审计链（上传内容本身不入库）",
    responses={400: {"description": "NDJSON 格式非法"}, 413: {"description": "上传过大"}},
)
async def verify_audit_upload(
    request: Request,
    anchor_hash: str | None = Query(
        default=None,
        description="首条记录前一条的链哈希；首条为序号 1 时自动使用创世哈希",
    ),
) -> AuditVerifyResponse:
    """上传导出的 NDJSON 内容，按其 *行顺序* 校验，可检出乱序。

    与 GET 版的区别：GET 总是按库内 seq 排序，无法发现顺序颠倒；
    本接口按提交的行顺序逐行校验，因此能检出导出/传输过程中的乱序。
    """
    import json

    raw = await request.body()
    if len(raw) > 10 * 1024 * 1024:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="校验上传内容超过 10 MiB 上限",
        )

    entries = []
    try:
        text_body = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="请求体不是合法 UTF-8 文本",
        ) from exc

    for position, line in enumerate(text_body.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"第 {position} 行不是合法 JSON：{exc.msg}",
            ) from exc
        try:
            entries.append(ChainEntry.from_dict(obj, position=position))
        except ChainFormatError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
            ) from exc

    if not entries:
        return AuditVerifyResponse(ok=True)

    report = verify_chain(entries, anchor_hash=anchor_hash)
    return AuditVerifyResponse(**report.__dict__)


# ---------------- 健康检查 ----------------


@app.get("/health", response_model=HealthResponse, tags=["meta"], summary="健康检查")
async def health(cache: DenyCache = Depends(get_cache)) -> HealthResponse:
    db_state = redis_state = "ok"
    try:
        await app.state.db.ping()
    except Exception:  # noqa: BLE001
        db_state = "error"
    try:
        await cache.ping()
    except Exception:  # noqa: BLE001
        redis_state = "error"
    overall = "ok" if db_state == "ok" and redis_state == "ok" else "degraded"
    code = 200 if overall == "ok" else 503
    return JSONResponse(
        status_code=code,
        content=HealthResponse(
            status=overall, database=db_state, redis=redis_state
        ).model_dump(),
    )


@app.get("/", tags=["meta"], summary="服务信息")
async def root() -> dict:
    return {
        "service": "one-time-dropbox",
        "docs": "/docs",
        "endpoints": [
            "POST /api/v1/deliveries",
            "POST /api/v1/deliveries/file",
            "POST /api/v1/pickup/{token}",
            "POST /api/v1/batch-pickup",
            "POST /api/v1/revoke/{token}",
            "POST /api/v1/blind-slots",
            "POST /api/v1/blind-slots/submit/{submit_token}",
            "POST /api/v1/blind-slots/submit/{submit_token}/file",
            "POST /api/v1/blind-slots/pickup/{token}",
            "POST /api/v1/blind-slots/revoke/{token}",
            "GET  /api/v1/audit/export",
            "GET  /api/v1/audit/verify",
            "POST /api/v1/audit/verify",
            "GET  /health",
        ],
    }
