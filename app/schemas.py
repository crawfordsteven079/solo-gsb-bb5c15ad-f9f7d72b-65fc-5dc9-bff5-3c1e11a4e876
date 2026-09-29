"""Pydantic 请求/响应模型。"""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class CreateTextRequest(BaseModel):
    text: str = Field(min_length=1, description="要投递的文本内容")
    ttl_seconds: int = Field(
        default=24 * 3600,
        ge=1,
        description="有效期（秒），服务端会再次按配置区间校验",
    )


class CreateResponse(BaseModel):
    delivery_id: uuid.UUID
    token: str = Field(description="取件令牌，仅在本次响应中返回，之后无法再查到")
    expires_at: datetime
    url: str = Field(description="取件地址（POST）")


# ---------------- 盲投递请求（空槽） ----------------


class BlindCreateRequest(BaseModel):
    ttl_seconds: int = Field(
        default=24 * 3600,
        ge=1,
        description="空槽有效期（秒），逾期未提交/未取件即过期，服务端按配置区间复验",
    )


class BlindCreateResponse(BaseModel):
    slot_id: uuid.UUID = Field(description="空槽（投递）ID")
    submit_token: str = Field(
        description="一次性提交令牌：只能成功写入一份文本或小文件，仅此一次返回"
    )
    pickup_token: str = Field(
        description="私有取件令牌：供收件人查询 pending 与原子单次取件，仅此一次返回"
    )
    expires_at: datetime
    submit_url: str = Field(description="提交地址（POST，需提交令牌）")
    pickup_url: str = Field(description="取件地址（POST，需取件令牌）")
    revoke_url: str = Field(description="撤回地址（POST，需取件令牌）")


class BlindSubmitTextRequest(BaseModel):
    text: str = Field(min_length=1, description="要写入空槽的文本内容")


class BlindSubmitResponse(BaseModel):
    result: str = "success"  # success
    slot_id: uuid.UUID
    kind: str = "text"
    content_sha256: str
    submitted_at: datetime
    # 刻意不含取件地址：提交者不持有取件权限，私有取件令牌只在开槽响应中出现


class BlindPendingResponse(BaseModel):
    """取件令牌在内容提交前查询到的明确状态。"""

    result: str = "pending"
    slot_id: uuid.UUID
    expires_at: datetime


class TextPickupResponse(BaseModel):
    result: str = "success"  # success
    delivery_id: uuid.UUID
    kind: str = "text"
    content: str
    content_sha256: str
    consumed_at: datetime


# ---------------- 批量取件（一次性领齐） ----------------


class BatchPickupRequest(BaseModel):
    """按顺序排列的 1～20 个取件令牌（普通投递或盲槽取件令牌）。

    空列表、超过 20 项或存在重复令牌时，整批在接口层即被拒绝（422），
    不会进入裁决、不会消费任何内容。
    """

    tokens: list[str] = Field(
        description="按顺序排列的取件令牌（普通文本投递或已提交盲槽文本）",
        min_length=1,
        max_length=20,
    )

    @model_validator(mode="after")
    def _reject_empty_or_duplicate_tokens(self) -> "BatchPickupRequest":
        for token in self.tokens:
            if not isinstance(token, str) or not token.strip():
                raise ValueError("tokens 中的每个令牌都必须是非空字符串")
        if len(set(self.tokens)) != len(self.tokens):
            raise ValueError("tokens 中存在重复令牌，整批请求被拒绝")
        return self


class BatchPickupItem(BaseModel):
    """批量取件成功时的单项结果，顺序与输入一致。"""

    position: int = Field(description="该令牌在输入列表中的位置（从 0 开始）")
    delivery_id: uuid.UUID
    kind: str = "text"
    content: str = Field(description="文本内容（批量取件不交付文件）")
    content_sha256: str


class BatchPickupSuccessResponse(BaseModel):
    result: str = "success"
    batch_id: uuid.UUID = Field(description="本批裁决的关联 ID（仅用于审计关联）")
    count: int = Field(description="成功领取的份数（等于输入令牌数）")
    items: list[BatchPickupItem]
    consumed_at: datetime


class BatchFailureItem(BaseModel):
    """批量取件失败时，单个失败项的输入位置与原因。

    reason 取值：unknown（令牌不存在/盲槽提交令牌）/ pending（待提交）/
    expired（过期，到期时刻等于裁决时刻也算）/ consumed（已消费）/
    revoked（已撤回）/ type_mismatch（文件投递，类型不符）。
    """

    position: int = Field(description="失败项在输入列表中的位置（从 0 开始）")
    reason: str


class BatchPickupFailureResponse(BaseModel):
    """任一项不满足时的整批失败响应：只回各失败项位置与原因，不回任何内容。"""

    result: str = "failed"
    batch_id: uuid.UUID
    failures: list[BatchFailureItem]
    detail: str = "批量取件存在不可领取项，整批未消费任何内容。"


class DenyResponse(BaseModel):
    """取件/撤回/提交未成功时的统一明确结果。"""

    result: str  # consumed | revoked | expired | unknown | submitted | pending
    detail: str


class RevokeResponse(BaseModel):
    result: str  # revoked | already_revoked | consumed | expired | unknown
    delivery_id: uuid.UUID | None = None
    revoked_at: datetime | None = None


class HealthResponse(BaseModel):
    status: str
    database: str
    redis: str


class AuditVerifyResponse(BaseModel):
    """哈希链完整性校验报告；ok=False 时 error 指出第一处错误类型。"""

    ok: bool
    count: int = Field(default=0, description="实际校验的记录数")
    first_seq: int | None = Field(default=None, description="首条记录序号")
    last_seq: int | None = Field(default=None, description="末条记录序号")
    start_hash: str | None = Field(default=None, description="首条记录的链哈希")
    end_hash: str | None = Field(default=None, description="末条记录的链哈希")
    # missing | out_of_order | duplicate | field_tampered | chain_break
    error: str | None = Field(default=None, description="第一处错误的类型")
    seq: int | None = Field(default=None, description="第一处错误所在记录序号")
    position: int | None = Field(default=None, description="第一处错误所在行（从 1 开始）")
    detail: str | None = Field(default=None, description="中文错误说明")
