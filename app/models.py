"""请求/响应数据模型（Pydantic v2）。"""
from __future__ import annotations

import re
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class AidIn(BaseModel):
    """航标灯质特征：编号、周期（秒）、周期内闪光时刻（秒，[0, period)）。"""

    aid_id: str = Field(min_length=1, max_length=64, description="航标编号")
    period: float = Field(gt=0, description="灯质周期，秒")
    flashes: list[float] = Field(min_length=1, description="周期内闪光时刻，秒")

    @field_validator("flashes")
    @classmethod
    def _flashes_finite(cls, v: list[float]) -> list[float]:
        for f in v:
            if f != f or f in (float("inf"), float("-inf")):
                raise ValueError("flash offset must be a finite number")
        return v

    @model_validator(mode="after")
    def _offsets_in_range(self) -> "AidIn":
        for f in self.flashes:
            if not 0.0 <= f < self.period:
                raise ValueError(
                    f"flash offset {f} out of range [0, period={self.period})"
                )
        self.flashes = sorted(set(self.flashes))
        return self


class LibraryImportRequest(BaseModel):
    """导入 JSON 特征库。mode=merge 按编号 upsert；mode=replace 清空后导入。

    兼容旧接口：服务端基于当前激活版本原子创建并直接激活新版本。
    """

    aids: list[AidIn] = Field(min_length=1)
    mode: Literal["merge", "replace"] = "merge"


class VersionCreateRequest(BaseModel):
    """创建不可变版本草稿：提交完整特征库与基线版本号。

    草稿保存时即按 aid_id 计算相对 base_version 的增/删/改；激活时还会
    在单个事务内再次确认 base_version 仍是当前激活版本，否则返回 409。
    """

    aids: list[AidIn] = Field(min_length=1)
    base_version: int = Field(
        ge=1, description="基线版本号（当前激活版本或历史版本，不能是草稿）"
    )

    @field_validator("aids")
    @classmethod
    def _unique_aid_ids(cls, v: list[AidIn]) -> list[AidIn]:
        ids = [a.aid_id for a in v]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate aid_id in version aids")
        return v


class JudgeRequest(BaseModel):
    """判定请求：观测时间戳 + 判定参数，可内联特征库覆盖已存库。"""

    observations: list[float] = Field(default_factory=list, description="闪光观测时间戳（Unix 秒）")
    tolerance: float = Field(gt=0, description="时间容差，秒")
    max_drift: float = Field(ge=0, lt=1, description="最大线性钟漂（无量纲，s/s）")
    max_missed: int = Field(ge=0, description="漏记上限")
    max_false: int = Field(ge=0, description="误记上限")
    library: Optional[list[AidIn]] = Field(
        default=None,
        description="内联特征库；缺省时在请求开始固定当前激活版本，整个判定只读该快照",
    )

    @field_validator("observations")
    @classmethod
    def _obs_finite(cls, v: list[float]) -> list[float]:
        for t in v:
            if t != t or t in (float("inf"), float("-inf")):
                raise ValueError("observation timestamps must be finite numbers")
        return v


class PatrolWaypointIn(BaseModel):
    """计划航线航点：航点编号 + 该处航标编号（须存在于激活特征库版本）。"""

    waypoint_id: str = Field(min_length=1, max_length=64, description="航点编号（请求内唯一）")
    aid_id: str = Field(min_length=1, max_length=64, description="航标编号")


class PatrolWindowIn(BaseModel):
    """一个观测窗口：按采集顺序排列的闪光观测时间戳。"""

    observations: list[float] = Field(default_factory=list, description="闪光观测时间戳（Unix 秒）")

    @field_validator("observations")
    @classmethod
    def _obs_finite(cls, v: list[float]) -> list[float]:
        for t in v:
            if t != t or t in (float("inf"), float("-inf")):
                raise ValueError("observation timestamps must be finite numbers")
        return v


class PatrolAnalyzeRequest(BaseModel):
    """巡检整体核对请求：计划航线 + 观测窗口序列 + 与 /judge 相同的判定参数。"""

    waypoints: list[PatrolWaypointIn] = Field(
        max_length=50, description="计划航线航点（按航线顺序，waypoint_id 唯一）"
    )
    windows: list[PatrolWindowIn] = Field(
        max_length=20, description="观测窗口（按采集顺序）"
    )
    tolerance: float = Field(gt=0, description="时间容差，秒")
    max_drift: float = Field(ge=0, lt=1, description="最大线性钟漂（无量纲，s/s）")
    max_missed: int = Field(ge=0, description="漏记上限")
    max_false: int = Field(ge=0, description="误记上限")

    @field_validator("waypoints")
    @classmethod
    def _unique_waypoint_ids(cls, v: list[PatrolWaypointIn]) -> list[PatrolWaypointIn]:
        ids = [w.waypoint_id for w in v]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate waypoint_id in waypoints")
        return v


class BatchObservationIn(BaseModel):
    """一条现场观测：设备本地编号 + 采集时刻。"""

    local_id: str = Field(min_length=1, max_length=128, description="设备本地观测编号")
    captured_at: float = Field(description="采集时刻（Unix 秒）")

    @field_validator("captured_at")
    @classmethod
    def _captured_at_finite(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("captured_at must be a finite number")
        return v


class BatchUploadRequest(BaseModel):
    """现场设备批次上传：链式追加一批观测，保存时间戳与判定阈值，不判定。

    ``sequence`` 从 1 递增；``previous_digest`` 引用上一批摘要（首批为空）；
    观测按 ``(captured_at, local_id)`` 严格递增排列。服务端对排除摘要字段的
    规范化载荷计算 SHA-256 作为批次摘要。
    """

    batch_id: str = Field(
        min_length=1, max_length=128, description="客户端生成的批次唯一编号（设备内唯一）"
    )
    sequence: int = Field(ge=1, description="批次序号，从 1 递增")
    previous_digest: Optional[str] = Field(
        default="", description="上一批摘要（64 位十六进制）；首批为空"
    )
    observations: list[BatchObservationIn] = Field(
        min_length=1,
        max_length=100,
        description="观测（1~100 条，按 (captured_at, local_id) 严格递增）",
    )
    tolerance: float = Field(gt=0, description="时间容差，秒（同 /judge）")
    max_drift: float = Field(ge=0, lt=1, description="最大线性钟漂（无量纲，s/s，同 /judge）")
    max_missed: int = Field(ge=0, description="漏记上限（同 /judge）")
    max_false: int = Field(ge=0, description="误记上限（同 /judge）")

    @field_validator("previous_digest")
    @classmethod
    def _normalize_previous_digest(cls, v: Optional[str]) -> str:
        s = (v or "").strip().lower()
        if s and not re.fullmatch(r"[0-9a-f]{64}", s):
            raise ValueError(
                "previous_digest must be empty or a 64-char lowercase hex sha256"
            )
        return s

    @field_validator("observations")
    @classmethod
    def _observations_sorted(cls, v: list[BatchObservationIn]) -> list[BatchObservationIn]:
        for a, b in zip(v, v[1:]):
            if (a.captured_at, a.local_id) >= (b.captured_at, b.local_id):
                raise ValueError(
                    "observations must be strictly increasing by (captured_at, local_id)"
                )
        return v


class ConsumerRegisterRequest(BaseModel):
    """登记设备的必需下游消费者。

    登记后即成为设备的必需消费者，不可删除；安全归档水位
    （``safe_sequence``）恒取全部已登记消费者水位的最小值。
    """

    consumer_id: str = Field(
        min_length=1, max_length=128, description="必需消费者编号（设备内唯一）"
    )


class ReplayReportRequest(BaseModel):
    """为草稿版本创建回放报告：携带客户端幂等键。

    报告开始时冻结当前最大 ``judgment_id`` 作为水位，只重放该水位内使用
    服务端特征库的判定记录（内联库记录按原因跳过）。相同键重试返回原报告；
    键已被其他版本占用时返回 409。
    """

    idempotency_key: str = Field(
        min_length=1,
        max_length=128,
        description="客户端生成的幂等键（全局唯一；同键重试返回原报告）",
    )

    @field_validator("idempotency_key")
    @classmethod
    def _key_non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("idempotency_key must not be blank")
        return v


class BatchAckRequest(BaseModel):
    """下游消费者对单个批次的处理确认。

    携带 ``consumer_id``、批次 ``sequence`` 与该批次 ``batch_digest``：
    允许乱序、并发与重复提交。仅当批次存在且摘要相符时接受；重复确认幂等，
    摘要冲突返回 409。
    """

    consumer_id: str = Field(min_length=1, max_length=128, description="已登记的必需消费者编号")
    sequence: int = Field(ge=1, description="被确认批次的序号")
    batch_digest: str = Field(description="被确认批次的 SHA-256 摘要（64 位十六进制）")

    @field_validator("batch_digest")
    @classmethod
    def _normalize_batch_digest(cls, v: str) -> str:
        s = (v or "").strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", s):
            raise ValueError("batch_digest must be a 64-char lowercase hex sha256")
        return s


class BatchJudgmentRequest(BaseModel):
    """对设备连续批次区间创建一次归档前巡检判定（携带客户端幂等键）。

    服务在请求开始固定当前激活特征库版本，确认 ``from_sequence..to_sequence``
    序号连续、批次明细仍在（无墓碑、无缺批次）且各批保存的四项阈值完全一致，
    再按 ``(captured_at, sequence, local_id)`` 合并观测，复用现有 /judge 规则
    判定并保存不可变来源。相同幂等键重试返回原判定（HTTP 200）；相同键改范围
    或换设备返回 409。
    """

    idempotency_key: str = Field(
        min_length=1,
        max_length=128,
        description="客户端生成的幂等键（全局唯一；同键重试返回原判定）",
    )
    from_sequence: int = Field(ge=1, description="判定区间起始批次序号（含）")
    to_sequence: int = Field(ge=1, description="判定区间结束批次序号（含）")

    @field_validator("idempotency_key")
    @classmethod
    def _key_non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("idempotency_key must not be blank")
        return v

    @model_validator(mode="after")
    def _ordered_range(self) -> "BatchJudgmentRequest":
        if self.from_sequence > self.to_sequence:
            raise ValueError(
                f"from_sequence {self.from_sequence} must be <= to_sequence {self.to_sequence}"
            )
        return self


class BatchJudgmentReviewRequest(BaseModel):
    """复核一次连续批次判定：携带客户端幂等键。

    服务在单个只读 SQLite 事务内固定 ``batch_judgment_id`` 的不可变记录、
    各批摘要与保存的 ``library_version``，逐批核对当前批次链（已归档墓碑
    只核对摘要骨架、没有明细）；全部明细可用且摘要、前序链、阈值一致时，
    用保存版本的特征库重跑判定并比较新旧裁决（不写入 ``judgments``）。
    相同幂等键重试返回原报告（HTTP 200）；相同键改用其他设备或判定目标
    返回 409。
    """

    idempotency_key: str = Field(
        min_length=1,
        max_length=128,
        description="客户端生成的幂等键（全局唯一；同键重试返回原报告）",
    )

    @field_validator("idempotency_key")
    @classmethod
    def _key_non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("idempotency_key must not be blank")
        return v


class LeaseClaimRequest(BaseModel):
    """消费者领取批次处理租约。

    服务在单个 SQLite 写事务内从该消费者连续水位的下一批起选择未归档的连续
    区间（至多 20 批），保存租约及每批摘要并返回观测明细。同一消费者存在未
    过期租约时：相同幂等键重试返回原租约（HTTP 200），其他幂等键返回 409。
    """

    consumer_id: str = Field(min_length=1, max_length=128, description="已登记的必需消费者编号")
    max_batches: int = Field(
        ge=1, le=20, description="本次最多领取的批次数（连续区间，1~20）"
    )
    lease_duration_seconds: int = Field(
        ge=1, le=3600, description="租约有效期（秒，1~3600）"
    )
    idempotency_key: str = Field(
        min_length=1,
        max_length=128,
        description="客户端生成的幂等键；同键重试返回原租约，其他键在持约期间返回 409",
    )

    @field_validator("idempotency_key")
    @classmethod
    def _key_non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("idempotency_key must not be blank")
        return v


class LeaseCompleteItem(BaseModel):
    """完成租约时逐项核对的一批：序号 + 处理时持有的批次摘要。"""

    sequence: int = Field(ge=1, description="租约内批次序号")
    batch_digest: str = Field(description="处理时持有的该批次 SHA-256 摘要（64 位十六进制）")

    @field_validator("batch_digest")
    @classmethod
    def _normalize_batch_digest(cls, v: str) -> str:
        s = (v or "").strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", s):
            raise ValueError("batch_digest must be a 64-char lowercase hex sha256")
        return s


class LeaseCompleteRequest(BaseModel):
    """完成批次处理租约：逐项核对摘要，全部通过后才写入确认并推进水位。

    租约必须属于该消费者且仍在有效期内；提交项必须与租约批次逐项相符（序号
    集合、顺序与摘要完全一致）。摘要变化、缺批次或租约过期（或已回收）时整体
    拒绝，不写入任何确认、不推进水位。
    """

    consumer_id: str = Field(min_length=1, max_length=128, description="租约所属消费者编号")
    items: list[LeaseCompleteItem] = Field(
        min_length=1, max_length=20, description="租约内每批的处理摘要（逐项核对）"
    )

    @field_validator("items")
    @classmethod
    def _unique_sequences(cls, v: list["LeaseCompleteItem"]) -> list["LeaseCompleteItem"]:
        seqs = [i.sequence for i in v]
        if len(seqs) != len(set(seqs)):
            raise ValueError("duplicate sequence in lease completion items")
        return v


class LeaseRecoverRequest(BaseModel):
    """显式回收已过期的批次处理租约；回收后区间可被该消费者重新领取。"""

    consumer_id: str = Field(min_length=1, max_length=128, description="租约所属消费者编号")


class ArchiveRequest(BaseModel):
    """归档设备批次链的连续前缀。

    ``through_sequence`` 为期望归档到的右端序号（含），目标必须落在同一事务
    内核定的 ``safe_sequence`` 之内（全部必需消费者都已确认的连续前缀）；
    越界返回 409（附当前水位与阻塞消费者）。重复或更小目标幂等，
    ``archived_through`` 只进不退。
    """

    through_sequence: int = Field(
        ge=1, description="归档目标序号（含）：归档连续前缀 1..through_sequence"
    )


class IsolationRequest(BaseModel):
    """隔离设备：提交当前链头序号、摘要、隔离原因与客户端幂等键。

    服务在同一 SQLite 写事务内核对链头（``head_sequence`` / ``head_digest``
    必须等于设备当前链头；无批次设备为 ``0`` / 空串）并记录不可变隔离事件。
    相同幂等键且相同参数的重试返回原事件（HTTP 200）；键已被占用或设备已在
    隔离中返回 409。
    """

    head_sequence: int = Field(
        ge=0, description="当前链头序号（无批次设备为 0）"
    )
    head_digest: str = Field(
        default="",
        description="当前链头摘要（64 位十六进制小写；无批次设备为空串）",
    )
    reason: str = Field(
        min_length=1,
        max_length=500,
        description="隔离原因（运维备注，随事件不可变保存）",
    )
    idempotency_key: str = Field(
        min_length=1,
        max_length=128,
        description="客户端生成的幂等键（设备内按动作唯一；同键同参数重试返回原事件）",
    )

    @field_validator("head_digest")
    @classmethod
    def _normalize_head_digest(cls, v: str) -> str:
        s = (v or "").strip().lower()
        if s and not re.fullmatch(r"[0-9a-f]{64}", s):
            raise ValueError(
                "head_digest must be empty or a 64-char lowercase hex sha256"
            )
        return s

    @field_validator("reason")
    @classmethod
    def _reason_non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("reason must not be blank")
        return v

    @field_validator("idempotency_key")
    @classmethod
    def _key_non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("idempotency_key must not be blank")
        return v

    @model_validator(mode="after")
    def _head_consistent(self) -> "IsolationRequest":
        if self.head_sequence == 0 and self.head_digest != "":
            raise ValueError(
                "head_digest must be empty when head_sequence is 0 (empty chain)"
            )
        if self.head_sequence >= 1 and self.head_digest == "":
            raise ValueError(
                "head_digest must be the 64-char chain head digest "
                "when head_sequence >= 1"
            )
        return self


class IsolationReleaseRequest(BaseModel):
    """解除设备隔离：携带核验范围与客户端幂等键。

    服务在同一写事务内重新核对链头与隔离时记录的一致、对
    ``from_sequence..to_sequence`` 执行链一致性核验且结果为 ok、并确认设备
    上无未完成租约，全部通过才记录不可变解除事件并恢复原流程；任一不满足
    整体拒绝（409），设备保持隔离。
    """

    from_sequence: int = Field(ge=1, description="解除前核验区间起始批次序号（含）")
    to_sequence: int = Field(ge=1, description="解除前核验区间结束批次序号（含）")
    idempotency_key: str = Field(
        min_length=1,
        max_length=128,
        description="客户端生成的幂等键（设备内按动作唯一；同键同参数重试返回原事件）",
    )

    @field_validator("idempotency_key")
    @classmethod
    def _key_non_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("idempotency_key must not be blank")
        return v

    @model_validator(mode="after")
    def _ordered_range(self) -> "IsolationReleaseRequest":
        if self.from_sequence > self.to_sequence:
            raise ValueError(
                f"from_sequence {self.from_sequence} must be <= to_sequence {self.to_sequence}"
            )
        return self


class ChainVerifyRequest(BaseModel):
    """设备批次链一致性核验：提交序号范围，服务在只读事务快照内核验。

    范围内每个序号都必须已存在（缺批次返回 404）；未归档批次重算规范
    SHA-256 并与保存摘要比对，归档批次只核对保存的摘要与墓碑边界。核验
    为只读，不修改任何数据。
    """

    from_sequence: int = Field(ge=1, description="核验区间起始批次序号（含）")
    to_sequence: int = Field(ge=1, description="核验区间结束批次序号（含）")

    @model_validator(mode="after")
    def _ordered_range(self) -> "ChainVerifyRequest":
        if self.from_sequence > self.to_sequence:
            raise ValueError(
                f"from_sequence {self.from_sequence} must be <= to_sequence {self.to_sequence}"
            )
        return self
