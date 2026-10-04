"""SQLite 持久层：不可变特征库版本、判定记录、现场设备批次链与草稿回放报告。

特征库以版本为单位不可变存储：

- ``library_versions``：版本元数据，状态机 draft → active → historical；
- ``library_version_aids``：每个版本的完整航标快照（版本不可变，可复算差异）；
- ``library_state``：单行表，指向当前激活版本。

草稿回放报告（上线前评估草稿会如何改写既有判定）：

- ``replay_reports``：报告元数据——目标草稿版本、全局唯一的幂等键、开始时
  冻结的判定水位（当前最大 ``judgment_id``）与各类计数；
- ``replay_report_items``：逐条明细——服务端库记录保存新旧最佳候选证据与
  四分类（``unchanged`` / ``match_gained`` / ``match_lost`` /
  ``winner_changed``），内联库记录以 ``item_status='skipped'`` 加跳过原因
  保存。幂等键全局唯一（唯一约束兜底并发）；回放不写入 ``judgments``。

现场设备批次链：

- ``device_batches``：每个设备按 sequence 递增的批次记录（含观测时间戳、
  /judge 四项阈值、批次摘要与首次受理响应），(device_id, sequence) 与
  (device_id, batch_id) 均唯一。``status`` 固定为首次受理事实 ``accepted``，
  另以 ``replay_count`` / ``last_replayed_at`` 持久化幂等重放；
- ``device_chains``：每个设备的链头游标（last_sequence、head_digest）与
  最近一次事件（受理或重放）的状态/序号/时刻。

多下游消费者确认与安全归档水位：

- ``device_consumers``：每台设备的**必需消费者**登记（注册后不可删除），
  并缓存该消费者的连续确认水位 ``watermark``（已连续确认到的最大序号，
  初始 0；归档之后才登记的消费者从设备 ``archived_through`` 起步）；
  (device_id, consumer_id) 唯一；
- ``device_batch_acks``：每个消费者对批次的**稀疏确认**（只保存已确认的
  序号与当时摘要），(device_id, consumer_id, sequence) 唯一。

确认在单个事务内「校验消费者与批次 → 插入稀疏确认 → 把该消费者水位推进
至首个缺口」；写锁 + BEGIN IMMEDIATE 串行化并发确认，水位在事务内基于
最新状态整体重算，不会丢失推进。设备的 ``safe_sequence`` 取所有必需
消费者水位的最小值（无消费者或任一消费者水位为 0 时为 0）。

批次归档（清除已被全部必需消费者处理的连续前缀的明细载荷）：

- ``device_chains.archived_through``：已归档连续前缀的右端序号（单调不
  退，初始 0），``archived_at`` 为最近一次归档时刻；
- ``device_batches.archived_at`` 非空即**墓碑**：``status='archived'``，
  ``observations`` / ``thresholds`` 明细被清空为 NULL，仅保留
  ``batch_id``、序号、``previous_digest``、``digest``、条数、首次受理
  时刻/响应与重放统计。归档目标必须落在同一写事务内核验的
  ``safe_sequence`` 之内，越界抛 :class:`ArchiveConflict`（409，附当前
  水位与阻塞消费者）；重复或更小目标幂等。已归档批次仍可按摘要接受同
  载荷重放并累计次数，载荷变化仍返回 409；链头摘要与后续上传不变。

批次提交在单个事务内完成「幂等/链头校验 → 追加记录 → 更新链头」；重放也在
同一幂等事务内原子累加 ``replay_count`` 并刷新批次与链游标的最近事件——
不新增批次、不推进链头、不改写首次受理响应。写锁 + BEGIN IMMEDIATE +
唯一约束保证并发提交串行化，不会形成分叉、不会丢失重放计数。

连续批次归档前巡检判定：

- ``batch_judgments``：一次区间判定的不可变来源——全局唯一幂等键、设备、
  起止序号、固定的 ``library_version``、合并观测与四项阈值、底层
  ``judgments.id``；
- ``batch_judgment_batches``：区间内各批的摘要（序号、batch_id、前序/本批
  摘要、条数）。创建在单个 BEGIN IMMEDIATE 事务内完成幂等检查、区间校验
  （缺批次/墓碑/阈值不一致整体拒绝）、固定激活版本、判定与落库；同键重试
  返回原判定，改范围/换设备 409；与上传/归档共用写锁串行点，已保存结果
  不会引用半个范围，且批次随后归档不影响已保存来源的复核。

连续批次判定复核报告（运维确认已保存判定仍能从原始链数据重现）：

- ``batch_judgment_review_reports``：一次复核的不可变报告——全局唯一幂等
  键、设备、目标判定、结论（``unchanged`` / ``changed`` /
  ``source_unavailable`` / ``source_mismatch``）与完整报告 JSON（问题
  清单、差异字段、原/新候选证据）。复核在**单个只读事务**
  （``PRAGMA query_only`` 兜底）内固定快照：读取判定的不可变来源，逐批
  核对当前批次链（归档墓碑只核对摘要骨架），全部明细可用且摘要、前序链、
  阈值一致时才用保存版本的特征库重跑判定；重跑是纯计算，**不写入
  ``judgments``**，报告只写本表。同键重试返回原报告，改设备/判定目标 409。

设备链一致性核验（离线补传、归档或迁移后的运维核对）：

- ``verify_chain`` 在**单个只读事务**（``PRAGMA query_only`` 兜底）内固定
  快照：结构核对覆盖整条链（序号连续性、``previous_digest`` 链、链游标
  ``batch_count`` / ``last_sequence`` / ``head_digest``、``archived_through``
  墓碑边界与墓碑骨架、消费者确认摘要与水位、连续批次判定来源摘要），规范
  SHA-256 重算只针对请求范围内的未归档批次（归档批次只核对保存的摘要与
  边界）；返回快照上限、``ok`` 与问题清单（``issue_code`` / ``sequence`` /
  ``expected`` / ``actual``），发现问题不改任何数据。

已登记消费者的批次处理租约（避免多个处理进程重复读取同一设备链）：

- ``batch_leases``：消费者一次领取的连续区间租约（设备、消费者、幂等键、
  起止序号、领取时水位 ``watermark_start``、有效期与到期时刻、状态
  ``held``/``completed``/``revoked``）。部分唯一索引保证同一消费者在同一
  设备上至多持有一个 ``held`` 租约、持约期间幂等键在 (设备, 消费者) 内唯一；
- ``batch_lease_items``：租约内每批的序号与领取时保存的摘要。领取在单个
  BEGIN IMMEDIATE 事务内从该消费者连续水位的下一批起选择**未归档的连续
  区间**（至多 20 批）并保存租约与每批摘要；完成时在同一事务逐项核对摘要、
  写入确认、推进水位并释放租约（``held → completed``）——摘要变化、缺批次或
  租约过期整体拒绝、不推进。归档不得清除仍被 ``held`` 租约占用的明细；过期
  租约需显式回收（``held → revoked``）后区间才能重新领取。

设备异常隔离（核验发现摘要、序号或确认水位异常后的应急闸门）：

- ``device_isolation_events``：隔离/解除的**不可变事件**（设备、动作
  ``isolated``/``released``、幂等键、原因、事件时核对过的链头序号与摘要、
  解除时的核验范围与核验摘要、时刻）。设备当前状态由最新一条事件推导：
  ``isolated`` 为隔离中，``released`` 或无事件为正常。幂等键在
  （设备, 动作）内唯一（唯一索引兜底并发），同键同参数重试在该事件仍为
  最新事件时返回原事件；
- 隔离在单个 BEGIN IMMEDIATE 事务内核对请求提交的链头与当前链头一致后
  落事件；隔离期间上传批次、消费者确认、归档、租约领取/完成与连续批次
  判定在同一写事务入口检查并整体拒绝（409），查询、链核验与过期租约回收
  不受影响；解除在同一事务内重新核对链头未变化、范围核验为 ok 且无未完成
  租约，任一不满足整体拒绝。写锁串行化保证并发隔离/解除不越过状态。

首次启动时把旧版平铺 ``aids`` 表整体迁移为初始激活版本（source='migration'），
并给 ``judgments`` 表补充 ``library_version`` 列；同时为旧库的批次表补充
``replay_count`` / ``last_replayed_at`` / ``archived_at`` 列（明细列改为
可空以承载归档墓碑）、为链游标表补充最近事件三列与
``archived_through`` / ``archived_at`` 列（旧记录重放次数为零、归档水位
回填为 0，最近事件回填为链头批次的受理事件）。批次表补墓碑列需要
SQLite 表重建，迁移在单个事务内完成。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .batches import (
    canonical_payload,
    canonical_payload_of,
    compute_digest,
    merge_batch_observations,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS library_versions (
    version_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    status       TEXT NOT NULL CHECK (status IN ('draft', 'active', 'historical')),
    base_version INTEGER REFERENCES library_versions(version_id),
    source       TEXT NOT NULL DEFAULT 'api',
    created_at   REAL NOT NULL,
    activated_at REAL
);
CREATE TABLE IF NOT EXISTS library_version_aids (
    version_id INTEGER NOT NULL REFERENCES library_versions(version_id),
    aid_id     TEXT NOT NULL,
    period     REAL NOT NULL,
    flashes    TEXT NOT NULL,
    PRIMARY KEY (version_id, aid_id)
);
CREATE TABLE IF NOT EXISTS library_state (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    version_id INTEGER NOT NULL REFERENCES library_versions(version_id)
);
CREATE TABLE IF NOT EXISTS judgments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      REAL NOT NULL,
    status          TEXT NOT NULL,
    best_aid_id     TEXT,
    matched         INTEGER NOT NULL DEFAULT 0,
    missed          INTEGER NOT NULL DEFAULT 0,
    false_count     INTEGER NOT NULL DEFAULT 0,
    drift           REAL,
    library_version INTEGER,
    request         TEXT NOT NULL,
    response        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS device_chains (
    device_id           TEXT PRIMARY KEY,
    last_sequence       INTEGER NOT NULL,
    head_digest         TEXT NOT NULL,
    batch_count         INTEGER NOT NULL,
    updated_at          REAL NOT NULL,
    last_event_status   TEXT NOT NULL DEFAULT 'accepted'
        CHECK (last_event_status IN ('accepted', 'replayed')),
    last_event_sequence INTEGER NOT NULL DEFAULT 0,
    last_event_at       REAL NOT NULL DEFAULT 0.0,
    archived_through    INTEGER NOT NULL DEFAULT 0,
    archived_at         REAL
);
CREATE TABLE IF NOT EXISTS device_batches (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id         TEXT NOT NULL,
    sequence          INTEGER NOT NULL,
    batch_id          TEXT NOT NULL,
    previous_digest   TEXT NOT NULL,
    digest            TEXT NOT NULL,
    observation_count INTEGER NOT NULL,
    thresholds        TEXT,
    observations      TEXT,
    response          TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'accepted'
        CHECK (status IN ('accepted', 'archived')),
    replay_count      INTEGER NOT NULL DEFAULT 0,
    last_replayed_at  REAL,
    archived_at       REAL,
    created_at        REAL NOT NULL,
    UNIQUE (device_id, sequence),
    UNIQUE (device_id, batch_id)
);
CREATE TABLE IF NOT EXISTS device_consumers (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id      TEXT NOT NULL,
    consumer_id    TEXT NOT NULL,
    watermark      INTEGER NOT NULL DEFAULT 0,
    registered_at  REAL NOT NULL,
    UNIQUE (device_id, consumer_id)
);
CREATE TABLE IF NOT EXISTS device_batch_acks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id        TEXT NOT NULL,
    consumer_id      TEXT NOT NULL,
    sequence         INTEGER NOT NULL,
    batch_digest     TEXT NOT NULL,
    confirmed_at     REAL NOT NULL,
    UNIQUE (device_id, consumer_id, sequence)
);
CREATE TABLE IF NOT EXISTS replay_reports (
    report_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id      INTEGER NOT NULL REFERENCES library_versions(version_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    watermark       INTEGER NOT NULL,
    total_count     INTEGER NOT NULL,
    replayed_count  INTEGER NOT NULL,
    skipped_count   INTEGER NOT NULL,
    unchanged       INTEGER NOT NULL DEFAULT 0,
    match_gained    INTEGER NOT NULL DEFAULT 0,
    match_lost      INTEGER NOT NULL DEFAULT 0,
    winner_changed  INTEGER NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS replay_report_items (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id           INTEGER NOT NULL REFERENCES replay_reports(report_id),
    judgment_id         INTEGER NOT NULL,
    item_status         TEXT NOT NULL
        CHECK (item_status IN ('replayed', 'skipped')),
    classification      TEXT
        CHECK (classification IS NULL OR classification IN
               ('unchanged', 'match_gained', 'match_lost', 'winner_changed')),
    skip_reason         TEXT,
    old_status          TEXT,
    old_best_aid_id     TEXT,
    old_reason          TEXT,
    old_library_version INTEGER,
    old_best            TEXT,
    new_status          TEXT,
    new_best_aid_id     TEXT,
    new_reason          TEXT,
    new_best            TEXT,
    UNIQUE (report_id, judgment_id)
);
CREATE INDEX IF NOT EXISTS idx_replay_items_cursor
    ON replay_report_items (report_id, judgment_id);
CREATE TABLE IF NOT EXISTS batch_judgments (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key  TEXT NOT NULL UNIQUE,
    device_id        TEXT NOT NULL,
    from_sequence    INTEGER NOT NULL,
    to_sequence      INTEGER NOT NULL,
    library_version  INTEGER,
    observation_count INTEGER NOT NULL,
    thresholds       TEXT NOT NULL,
    observations     TEXT NOT NULL,
    judgment_id      INTEGER NOT NULL REFERENCES judgments(id),
    created_at       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batch_judgments_device
    ON batch_judgments (device_id, id);
CREATE TABLE IF NOT EXISTS batch_judgment_batches (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_judgment_id INTEGER NOT NULL REFERENCES batch_judgments(id),
    sequence          INTEGER NOT NULL,
    batch_id          TEXT NOT NULL,
    previous_digest   TEXT NOT NULL,
    digest            TEXT NOT NULL,
    observation_count INTEGER NOT NULL,
    UNIQUE (batch_judgment_id, sequence)
);
CREATE TABLE IF NOT EXISTS batch_judgment_review_reports (
    report_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key   TEXT NOT NULL UNIQUE,
    device_id         TEXT NOT NULL,
    batch_judgment_id INTEGER NOT NULL REFERENCES batch_judgments(id),
    status            TEXT NOT NULL
        CHECK (status IN ('unchanged', 'changed',
                          'source_unavailable', 'source_mismatch')),
    report            TEXT NOT NULL,
    created_at        REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bj_review_reports_judgment
    ON batch_judgment_review_reports (device_id, batch_judgment_id, report_id);
CREATE TABLE IF NOT EXISTS batch_leases (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id          TEXT NOT NULL,
    consumer_id        TEXT NOT NULL,
    idempotency_key    TEXT NOT NULL,
    status             TEXT NOT NULL
        CHECK (status IN ('held', 'completed', 'revoked')),
    from_sequence      INTEGER NOT NULL,
    to_sequence        INTEGER NOT NULL,
    watermark_start    INTEGER NOT NULL,
    lease_duration     INTEGER NOT NULL,
    granted_at         REAL NOT NULL,
    expires_at         REAL NOT NULL,
    completed_at       REAL,
    revoked_at         REAL
);
-- 同一消费者在同一设备上至多持有一个未完成租约（完成/回收后键可复用）
CREATE UNIQUE INDEX IF NOT EXISTS idx_batch_leases_active_consumer
    ON batch_leases (device_id, consumer_id) WHERE status = 'held';
-- 持约期间幂等键在 (设备, 消费者) 内唯一：同键重试返回原租约、他键 409
CREATE UNIQUE INDEX IF NOT EXISTS idx_batch_leases_active_key
    ON batch_leases (device_id, consumer_id, idempotency_key) WHERE status = 'held';
CREATE INDEX IF NOT EXISTS idx_batch_leases_device
    ON batch_leases (device_id, id);
CREATE TABLE IF NOT EXISTS batch_lease_items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    lease_id    INTEGER NOT NULL REFERENCES batch_leases(id),
    sequence    INTEGER NOT NULL,
    batch_digest TEXT NOT NULL,
    UNIQUE (lease_id, sequence)
);
CREATE TABLE IF NOT EXISTS device_isolation_events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id        TEXT NOT NULL,
    action           TEXT NOT NULL CHECK (action IN ('isolated', 'released')),
    idempotency_key  TEXT NOT NULL,
    reason           TEXT,
    head_sequence    INTEGER NOT NULL,
    head_digest      TEXT NOT NULL,
    verify_from      INTEGER,
    verify_to        INTEGER,
    verification     TEXT,
    created_at       REAL NOT NULL
);
-- 幂等键在 (设备, 动作) 内唯一：同键同参数重试返回原事件，他参占用 409
CREATE UNIQUE INDEX IF NOT EXISTS idx_isolation_events_action_key
    ON device_isolation_events (device_id, action, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_isolation_events_device
    ON device_isolation_events (device_id, id);
"""


class VersionNotFound(Exception):
    """引用的版本不存在。"""


class VersionStateError(Exception):
    """版本状态不允许该操作（如激活非草稿、以草稿为基线）。"""


class VersionConflict(Exception):
    """激活时草稿的 base_version 已不再是当前激活版本（乐观并发冲突）。"""


class ReplayConflict(Exception):
    """回放幂等键已被另一个版本占用（HTTP 409），detail 附占用方信息。"""

    def __init__(
        self,
        *,
        message: str,
        idempotency_key: str,
        occupied_by_version: int,
        occupied_by_report: int,
    ):
        super().__init__(message)
        self.detail = {
            "error": "replay_conflict",
            "reason": "idempotency_key_in_use",
            "message": message,
            "idempotency_key": idempotency_key,
            "occupied_by_version": occupied_by_version,
            "occupied_by_report": occupied_by_report,
        }


class BatchConflict(Exception):
    """批次上传冲突（HTTP 409）：载荷变化或链头过期，detail 附期望值。"""

    def __init__(
        self,
        *,
        reason: str,
        message: str,
        expected_sequence: int,
        expected_previous_digest: str,
        existing: dict | None = None,
    ):
        super().__init__(message)
        self.detail = {
            "error": "batch_conflict",
            "reason": reason,
            "message": message,
            "expected_sequence": expected_sequence,
            "expected_previous_digest": expected_previous_digest,
        }
        if existing is not None:
            self.detail["existing_batch"] = existing


class ConsumerConflict(Exception):
    """消费者确认冲突（HTTP 409）：摘要不符或消费者未登记，detail 附上下文。"""

    def __init__(self, *, reason: str, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": "consumer_conflict", "reason": reason, "message": message}
        self.detail.update(extra)


class BatchNotFound(Exception):
    """确认的批次在该设备上不存在（HTTP 404）。"""

    def __init__(self, device_id: str, sequence: int):
        super().__init__(f"batch {sequence} of device {device_id!r} not found")
        self.device_id = device_id
        self.sequence = sequence


class ArchiveConflict(Exception):
    """归档目标越过同一事务内核验的 safe_sequence（HTTP 409），detail 附水位上下文。"""

    def __init__(self, *, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": "archive_conflict", "message": message}
        self.detail.update(extra)


class BatchJudgmentConflict(Exception):
    """连续批次判定冲突（HTTP 409）：范围非法或幂等键已被其他范围/设备占用。"""

    def __init__(self, *, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": "batch_judgment_conflict", "message": message}
        self.detail.update(extra)


class BatchJudgmentNotFound(Exception):
    """复核目标的连续批次判定不存在或不属于该设备（HTTP 404）。"""

    def __init__(self, *, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": "batch_judgment_not_found", "message": message}
        self.detail.update(extra)


class ReviewReportConflict(Exception):
    """复核报告幂等键已被其他设备或判定目标占用（HTTP 409），detail 附占用方信息。"""

    def __init__(self, *, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": "review_report_conflict", "message": message}
        self.detail.update(extra)


class ChainVerificationNotFound(Exception):
    """链一致性核验目标不存在（HTTP 404）：设备无批次或请求范围内缺批次。"""

    def __init__(self, *, error: str, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": error, "message": message}
        self.detail.update(extra)


class LeaseNotFound(Exception):
    """租约不存在或不属于该设备/消费者（HTTP 404）。"""

    def __init__(self, *, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": "lease_not_found", "message": message}
        self.detail.update(extra)


class LeaseConflict(Exception):
    """批次处理租约冲突（HTTP 409），detail 附当前租约或逐项核对上下文。"""

    def __init__(self, *, reason: str, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": "lease_conflict", "reason": reason, "message": message}
        self.detail.update(extra)


class DeviceIsolated(Exception):
    """设备处于隔离状态（HTTP 409）：处理类写操作被拒绝，detail 附生效中的隔离事件。"""

    def __init__(self, *, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": "device_isolated", "message": message}
        self.detail.update(extra)


class IsolationConflict(Exception):
    """隔离/解除请求冲突（HTTP 409）：状态机不允许、链头不符、核验未过或租约未清。"""

    def __init__(self, *, reason: str, message: str, **extra):
        super().__init__(message)
        self.detail = {"error": "isolation_conflict", "reason": reason, "message": message}
        self.detail.update(extra)


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _diff_aids(base_aids: list[dict], new_aids: list[dict]) -> dict:
    """按航标编号比较两个快照，给出增/删/改（period 或 flashes 不同即为改）。"""
    base = {a["aid_id"]: a for a in base_aids}
    new = {a["aid_id"]: a for a in new_aids}
    return {
        "added": sorted(k for k in new if k not in base),
        "removed": sorted(k for k in base if k not in new),
        "modified": sorted(
            k for k in new.keys() & base.keys()
            if base[k]["period"] != new[k]["period"] or base[k]["flashes"] != new[k]["flashes"]
        ),
    }


class Database:
    """按操作开启连接的轻量封装：串行写由锁保证，多步写包在单个事务内。"""

    def __init__(self, path: str):
        self._path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with closing(self._connect()) as conn:
            conn.executescript(SCHEMA)
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._migrate(conn)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        # 写锁竞争时等待而非立即抛 database is locked，使
        # 「BEGIN IMMEDIATE 串行化写事务」在多连接（多 worker）下同样成立
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    @contextmanager
    def _write_tx(self):
        """串行化的单事务写：BEGIN IMMEDIATE … COMMIT，异常时 ROLLBACK。"""
        with self._lock, closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ---------------- 首次迁移 ----------------

    def _migrate(self, conn) -> None:
        tables = {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "aids" in tables:  # 旧版平铺特征库 → 初始激活版本
            rows = conn.execute(
                "SELECT aid_id, period, flashes FROM aids ORDER BY aid_id"
            ).fetchall()
            now = time.time()
            cur = conn.execute(
                "INSERT INTO library_versions (status, base_version, source, created_at, activated_at) "
                "VALUES ('active', NULL, 'migration', ?, ?)",
                (now, now),
            )
            vid = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO library_version_aids (version_id, aid_id, period, flashes) "
                "VALUES (?, ?, ?, ?)",
                [(vid, r["aid_id"], r["period"], r["flashes"]) for r in rows],
            )
            conn.execute("INSERT INTO library_state (id, version_id) VALUES (1, ?)", (vid,))
            conn.execute("DROP TABLE aids")
        cols = {r[1] for r in conn.execute("PRAGMA table_info(judgments)")}
        if "library_version" not in cols:
            conn.execute("ALTER TABLE judgments ADD COLUMN library_version INTEGER")
        self._migrate_device_tables(conn)

    def _migrate_device_tables(self, conn) -> None:
        """旧库兼容：批次表补重放/归档列，链表补最近事件与归档水位列。

        旧记录的重放次数为零（``last_replayed_at`` 为 NULL）、归档水位为 0
        （``archived_at`` 为 NULL）；链游标最近事件回填为当前链头批次的受理
        事件，与新批次受理后的游标含义保持一致。归档要求批次明细列
        （``observations``/``thresholds``）可空并新增 ``archived_at`` 墓碑
        列，旧库这两列为 NOT NULL 且无墓碑列，SQLite 不能直接放宽 NOT NULL，
        故按官方做法在同一事务内「建新表 → 拷数据 → 删旧表 → 改名」重建
        （(device_id, sequence) / (device_id, batch_id) 唯一约束随新表保留）。
        """
        batch_info = {
            r[1]: r for r in conn.execute("PRAGMA table_info(device_batches)")
        }
        need_rebuild = bool(batch_info) and (
            "archived_at" not in batch_info
            or batch_info["observations"][3]  # notnull 标志（PRAGMA 第 4 列）
            or batch_info["thresholds"][3]
        )
        if need_rebuild:
            # 先把旧库能直接 ALTER 补上的列补齐，重建时随数据一起搬过去
            if "replay_count" not in batch_info:
                conn.execute(
                    "ALTER TABLE device_batches ADD COLUMN "
                    "replay_count INTEGER NOT NULL DEFAULT 0"
                )
            if "last_replayed_at" not in batch_info:
                conn.execute(
                    "ALTER TABLE device_batches ADD COLUMN last_replayed_at REAL"
                )
            conn.execute(
                """
                CREATE TABLE device_batches__new (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id         TEXT NOT NULL,
                    sequence          INTEGER NOT NULL,
                    batch_id          TEXT NOT NULL,
                    previous_digest   TEXT NOT NULL,
                    digest            TEXT NOT NULL,
                    observation_count INTEGER NOT NULL,
                    thresholds        TEXT,
                    observations      TEXT,
                    response          TEXT NOT NULL,
                    status            TEXT NOT NULL DEFAULT 'accepted'
                        CHECK (status IN ('accepted', 'archived')),
                    replay_count      INTEGER NOT NULL DEFAULT 0,
                    last_replayed_at  REAL,
                    archived_at       REAL,
                    created_at        REAL NOT NULL,
                    UNIQUE (device_id, sequence),
                    UNIQUE (device_id, batch_id)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO device_batches__new
                    (id, device_id, sequence, batch_id, previous_digest, digest,
                     observation_count, thresholds, observations, response, status,
                     replay_count, last_replayed_at, archived_at, created_at)
                SELECT id, device_id, sequence, batch_id, previous_digest, digest,
                       observation_count, thresholds, observations, response, status,
                       replay_count, last_replayed_at, NULL, created_at
                FROM device_batches
                """
            )
            conn.execute("DROP TABLE device_batches")
            conn.execute("ALTER TABLE device_batches__new RENAME TO device_batches")

        chain_info = {
            r[1]: r for r in conn.execute("PRAGMA table_info(device_chains)")
        }
        migrated_chain = False
        if chain_info:
            if "last_event_status" not in chain_info:
                conn.execute(
                    "ALTER TABLE device_chains ADD COLUMN "
                    "last_event_status TEXT NOT NULL DEFAULT 'accepted'"
                )
                migrated_chain = True
            if "last_event_sequence" not in chain_info:
                conn.execute(
                    "ALTER TABLE device_chains ADD COLUMN "
                    "last_event_sequence INTEGER NOT NULL DEFAULT 0"
                )
                migrated_chain = True
            if "last_event_at" not in chain_info:
                conn.execute(
                    "ALTER TABLE device_chains ADD COLUMN "
                    "last_event_at REAL NOT NULL DEFAULT 0.0"
                )
                migrated_chain = True
            if "archived_through" not in chain_info:
                conn.execute(
                    "ALTER TABLE device_chains ADD COLUMN "
                    "archived_through INTEGER NOT NULL DEFAULT 0"
                )
            if "archived_at" not in chain_info:
                conn.execute(
                    "ALTER TABLE device_chains ADD COLUMN archived_at REAL"
                )
            if migrated_chain:
                # 旧链的最近事件即链头批次的首次受理：状态 accepted、序号为链头序号、
                # 时刻取该批次的 created_at
                conn.execute(
                    "UPDATE device_chains SET "
                    "last_event_status = 'accepted', "
                    "last_event_sequence = last_sequence, "
                    "last_event_at = COALESCE("
                    "  (SELECT b.created_at FROM device_batches b "
                    "   WHERE b.device_id = device_chains.device_id "
                    "   AND b.sequence = device_chains.last_sequence), 0.0)"
                )

    # ---------------- 特征库版本 ----------------

    @staticmethod
    def _aids_of(conn, version_id: int) -> list[dict]:
        rows = conn.execute(
            "SELECT aid_id, period, flashes FROM library_version_aids "
            "WHERE version_id = ? ORDER BY aid_id",
            (version_id,),
        ).fetchall()
        return [
            {"aid_id": r["aid_id"], "period": r["period"], "flashes": json.loads(r["flashes"])}
            for r in rows
        ]

    def _version_meta(self, conn, row) -> dict:
        vid = row["version_id"]
        base = row["base_version"]
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM library_version_aids WHERE version_id = ?", (vid,)
        ).fetchone()["c"]
        diff = None
        if base is not None:
            diff = _diff_aids(self._aids_of(conn, base), self._aids_of(conn, vid))
        return {
            "version_id": vid,
            "status": row["status"],
            "base_version": base,
            "source": row["source"],
            "aid_count": count,
            "created_at": _iso(row["created_at"]),
            "activated_at": _iso(row["activated_at"]) if row["activated_at"] is not None else None,
            "diff": diff,
        }

    def create_version(self, aids, *, base_version: int, source: str = "api") -> dict:
        """保存完整特征库为草稿版本；基线可以是当前激活版本，也可以是历史版本。"""
        rows = [(a.aid_id, a.period, json.dumps(a.flashes)) for a in aids]
        with self._write_tx() as conn:
            base = conn.execute(
                "SELECT status FROM library_versions WHERE version_id = ?", (base_version,)
            ).fetchone()
            if base is None:
                raise VersionNotFound(f"base_version {base_version} not found")
            if base["status"] == "draft":
                raise VersionStateError(
                    f"base_version {base_version} is still a draft; "
                    "base must be an activated (current or historical) version"
                )
            cur = conn.execute(
                "INSERT INTO library_versions (status, base_version, source, created_at) "
                "VALUES ('draft', ?, ?, ?)",
                (base_version, source, time.time()),
            )
            vid = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO library_version_aids (version_id, aid_id, period, flashes) "
                "VALUES (?, ?, ?, ?)",
                [(vid, aid_id, period, flashes) for aid_id, period, flashes in rows],
            )
        return self.get_version(vid)

    def activate_version(self, version_id: int) -> dict:
        """单事务激活：确认草稿的 base_version 仍是当前激活版本，然后原子切换。"""
        with self._write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM library_versions WHERE version_id = ?", (version_id,)
            ).fetchone()
            if row is None:
                raise VersionNotFound(f"version {version_id} not found")
            if row["status"] != "draft":
                raise VersionStateError(
                    f"version {version_id} is {row['status']}; only a draft can be activated"
                )
            state = conn.execute("SELECT version_id FROM library_state WHERE id = 1").fetchone()
            current = state["version_id"] if state is not None else None
            if row["base_version"] != current:
                raise VersionConflict(
                    f"base_version {row['base_version']} is no longer the active version "
                    f"(currently active: {current}); re-create the draft on top of the "
                    "current active version and retry"
                )
            now = time.time()
            conn.execute(
                "UPDATE library_versions SET status = 'historical' WHERE status = 'active'"
            )
            conn.execute(
                "UPDATE library_versions SET status = 'active', activated_at = ? "
                "WHERE version_id = ?",
                (now, version_id),
            )
            conn.execute(
                "INSERT INTO library_state (id, version_id) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET version_id = excluded.version_id",
                (version_id,),
            )
        return self.get_version(version_id)

    def publish_import(self, aids, *, replace: bool) -> tuple[int, dict]:
        """/library/import：基于当前激活版本合并/替换，单事务创建并直接激活新版本。"""
        with self._write_tx() as conn:
            state = conn.execute("SELECT version_id FROM library_state WHERE id = 1").fetchone()
            base_id = state["version_id"] if state is not None else None
            merged: dict[str, dict] = {}
            if not replace and base_id is not None:
                for a in self._aids_of(conn, base_id):
                    merged[a["aid_id"]] = a
            for a in aids:
                merged[a.aid_id] = {
                    "aid_id": a.aid_id,
                    "period": a.period,
                    "flashes": list(a.flashes),
                }
            now = time.time()
            conn.execute(
                "UPDATE library_versions SET status = 'historical' WHERE status = 'active'"
            )
            cur = conn.execute(
                "INSERT INTO library_versions (status, base_version, source, created_at, activated_at) "
                "VALUES ('active', ?, 'import', ?, ?)",
                (base_id, now, now),
            )
            vid = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO library_version_aids (version_id, aid_id, period, flashes) "
                "VALUES (?, ?, ?, ?)",
                [
                    (vid, a["aid_id"], a["period"], json.dumps(a["flashes"]))
                    for a in merged.values()
                ],
            )
            conn.execute(
                "INSERT INTO library_state (id, version_id) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET version_id = excluded.version_id",
                (vid,),
            )
        return len(aids), self.get_version(vid)

    def list_versions(self) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM library_versions ORDER BY version_id"
            ).fetchall()
            return [self._version_meta(conn, r) for r in rows]

    def get_version(self, version_id: int) -> dict | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM library_versions WHERE version_id = ?", (version_id,)
            ).fetchone()
            return self._version_meta(conn, row) if row is not None else None

    def get_version_aids(self, version_id: int) -> list[dict]:
        with closing(self._connect()) as conn:
            return self._aids_of(conn, version_id)

    def get_active_snapshot(self) -> tuple[int | None, list[dict]]:
        """/judge 请求开始时固定一次：同一读事务内取激活版本号与完整快照。"""
        with closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN")
                state = conn.execute(
                    "SELECT version_id FROM library_state WHERE id = 1"
                ).fetchone()
                vid = state["version_id"] if state is not None else None
                aids = self._aids_of(conn, vid) if vid is not None else []
            finally:
                conn.rollback()  # 只读事务，释放读锁
        return vid, aids

    @staticmethod
    def _active_snapshot_of(conn) -> tuple[int | None, list[dict]]:
        """在给定连接（可为写事务中的连接）内取当前激活版本号与完整快照。

        连续批次判定在写事务内调用：事务持写锁期间激活版本不可能被其他请求
        切换，故判定自请求开始固定的 ``library_version`` 与最终保存值严格
        同快照。
        """
        state = conn.execute(
            "SELECT version_id FROM library_state WHERE id = 1"
        ).fetchone()
        vid = state["version_id"] if state is not None else None
        return vid, Database._aids_of(conn, vid) if vid is not None else []

    # ---------------- 判定记录 ----------------

    def save_judgment(
        self,
        *,
        status: str,
        best_aid_id: str | None,
        matched: int,
        missed: int,
        false_count: int,
        drift: float | None,
        library_version: int | None,
        request: dict,
        response: dict,
    ) -> int:
        with self._write_tx() as conn:
            return self._insert_judgment(
                conn,
                status=status,
                best_aid_id=best_aid_id,
                matched=matched,
                missed=missed,
                false_count=false_count,
                drift=drift,
                library_version=library_version,
                request=request,
                response=response,
            )

    @staticmethod
    def _insert_judgment(
        conn,
        *,
        status: str,
        best_aid_id: str | None,
        matched: int,
        missed: int,
        false_count: int,
        drift: float | None,
        library_version: int | None,
        request: dict,
        response: dict,
    ) -> int:
        """在给定连接（可为其他写事务中的连接）内插入一条判定记录并返回其 id。"""
        cur = conn.execute(
            "INSERT INTO judgments "
            "(created_at, status, best_aid_id, matched, missed, false_count, drift, "
            "library_version, request, response) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                time.time(),
                status,
                best_aid_id,
                matched,
                missed,
                false_count,
                drift,
                library_version,
                json.dumps(request, ensure_ascii=False),
                json.dumps(response, ensure_ascii=False),
            ),
        )
        return int(cur.lastrowid)

    def list_judgments(self, limit: int = 50, offset: int = 0) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT id, created_at, status, best_aid_id, matched, missed, false_count, "
                "drift, library_version "
                "FROM judgments ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [
            {
                "id": r["id"],
                "created_at": _iso(r["created_at"]),
                "status": r["status"],
                "best_aid_id": r["best_aid_id"],
                "matched": r["matched"],
                "missed": r["missed"],
                "false": r["false_count"],
                "drift": r["drift"],
                "library_version": r["library_version"],
            }
            for r in rows
        ]

    def get_judgment(self, judgment_id: int) -> dict | None:
        with closing(self._connect()) as conn:
            r = conn.execute(
                "SELECT id, created_at, status, best_aid_id, matched, missed, false_count, "
                "drift, library_version, request, response "
                "FROM judgments WHERE id = ?",
                (judgment_id,),
            ).fetchone()
        if r is None:
            return None
        return {
            "id": r["id"],
            "created_at": _iso(r["created_at"]),
            "status": r["status"],
            "best_aid_id": r["best_aid_id"],
            "matched": r["matched"],
            "missed": r["missed"],
            "false": r["false_count"],
            "drift": r["drift"],
            "library_version": r["library_version"],
            "request": json.loads(r["request"]),
            "response": json.loads(r["response"]),
        }

    # ---------------- 现场设备批次链 ----------------

    def submit_batch(self, device_id: str, req) -> tuple[dict, bool]:
        """单事务提交批次：幂等/链头校验 → 追加记录/累加重放 → 更新设备游标。

        返回 ``(响应体, 是否新接受)``。相同载荷重放（摘要一致）在同一幂等事务
        内原子累加该批次 ``replay_count``、刷新其 ``last_replayed_at`` 并把设备
        游标的最近事件改为本次重放，但**不**新增批次、**不**推进链头、**不**
        改写首次受理响应；响应仍返回原结果并标记 ``replayed``。载荷变化或链头
        过期抛 :class:`BatchConflict`。写锁 + BEGIN IMMEDIATE 串行化并发提交，
        唯一约束兜底，不会形成分叉，并发重放也不会丢失计数。
        """
        digest = compute_digest(canonical_payload(device_id, req))
        prev = req.previous_digest or ""
        thresholds = {
            "tolerance": req.tolerance,
            "max_drift": req.max_drift,
            "max_missed": req.max_missed,
            "max_false": req.max_false,
        }
        observations = [
            {"local_id": o.local_id, "captured_at": o.captured_at}
            for o in req.observations
        ]
        now = time.time()
        with self._write_tx() as conn:
            self._assert_not_isolated(conn, device_id)
            row_bid = conn.execute(
                "SELECT * FROM device_batches WHERE device_id = ? AND batch_id = ?",
                (device_id, req.batch_id),
            ).fetchone()
            row_seq = conn.execute(
                "SELECT * FROM device_batches WHERE device_id = ? AND sequence = ?",
                (device_id, req.sequence),
            ).fetchone()
            chain = conn.execute(
                "SELECT * FROM device_chains WHERE device_id = ?", (device_id,)
            ).fetchone()
            last_seq = chain["last_sequence"] if chain is not None else 0
            head = chain["head_digest"] if chain is not None else ""

            if row_bid is not None or row_seq is not None:
                # 同一 batch_id 且同一 sequence 且摘要一致 → 相同载荷重放
                if (
                    row_bid is not None
                    and row_seq is not None
                    and row_bid["id"] == row_seq["id"]
                    and row_bid["digest"] == digest
                ):
                    # 幂等重放：原子累加计数并刷新批次与链游标的最近事件；
                    # 不新增记录、不推进链头、不改写首次受理响应（response 列）
                    conn.execute(
                        "UPDATE device_batches SET "
                        "replay_count = replay_count + 1, last_replayed_at = ? "
                        "WHERE id = ?",
                        (now, row_bid["id"]),
                    )
                    conn.execute(
                        "UPDATE device_chains SET "
                        "last_event_status = 'replayed', "
                        "last_event_sequence = ?, last_event_at = ? "
                        "WHERE device_id = ?",
                        (row_bid["sequence"], now, device_id),
                    )
                    body = json.loads(row_bid["response"])
                    body["status"] = "replayed"
                    return body, False
                existing = row_bid if row_bid is not None else row_seq
                raise BatchConflict(
                    reason="payload_changed",
                    message=(
                        f"batch_id/sequence already recorded with a different "
                        f"payload (existing sequence {existing['sequence']}, "
                        f"batch_id {existing['batch_id']!r})"
                    ),
                    expected_sequence=last_seq + 1,
                    expected_previous_digest=head,
                    existing={
                        "sequence": existing["sequence"],
                        "batch_id": existing["batch_id"],
                        "digest": existing["digest"],
                    },
                )

            if req.sequence != last_seq + 1 or prev != head:
                raise BatchConflict(
                    reason="stale_chain_head",
                    message=(
                        f"batch does not extend the chain head: got sequence "
                        f"{req.sequence} with previous_digest {prev!r}, expected "
                        f"sequence {last_seq + 1} with previous_digest {head!r}"
                    ),
                    expected_sequence=last_seq + 1,
                    expected_previous_digest=head,
                )

            body = {
                "status": "accepted",
                "device_id": device_id,
                "batch_id": req.batch_id,
                "sequence": req.sequence,
                "digest": digest,
                "previous_digest": prev,
                "observation_count": len(observations),
                "received_at": _iso(now),
                "chain": {
                    "head_sequence": req.sequence,
                    "head_digest": digest,
                    "next_sequence": req.sequence + 1,
                    "expected_previous_digest": digest,
                },
            }
            conn.execute(
                "INSERT INTO device_batches "
                "(device_id, sequence, batch_id, previous_digest, digest, "
                "observation_count, thresholds, observations, response, status, "
                "created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'accepted', ?)",
                (
                    device_id,
                    req.sequence,
                    req.batch_id,
                    prev,
                    digest,
                    len(observations),
                    json.dumps(thresholds, ensure_ascii=False),
                    json.dumps(observations, ensure_ascii=False),
                    json.dumps(body, ensure_ascii=False),
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO device_chains "
                "(device_id, last_sequence, head_digest, batch_count, updated_at, "
                "last_event_status, last_event_sequence, last_event_at) "
                "VALUES (?, ?, ?, 1, ?, 'accepted', ?, ?) "
                "ON CONFLICT(device_id) DO UPDATE SET "
                "last_sequence = excluded.last_sequence, "
                "head_digest = excluded.head_digest, "
                "batch_count = device_chains.batch_count + 1, "
                "updated_at = excluded.updated_at, "
                "last_event_status = 'accepted', "
                "last_event_sequence = excluded.last_event_sequence, "
                "last_event_at = excluded.last_event_at",
                (device_id, req.sequence, digest, now, req.sequence, now),
            )
        return body, True

    def get_device_cursor(self, device_id: str) -> dict:
        """设备批次链游标：链头位置、下一批期望值与最近一次事件（受理/重放）。

        ``last_event_status`` 为最近事件的 ``accepted``/``replayed``，
        ``last_event_sequence`` 为该事件所属批次序号，``last_event_at`` 为其
        时刻；未上传过的设备三者分别为 ``None``/``0``/``None``。
        """
        with closing(self._connect()) as conn:
            chain = conn.execute(
                "SELECT * FROM device_chains WHERE device_id = ?", (device_id,)
            ).fetchone()
        if chain is None:
            return {
                "device_id": device_id,
                "last_sequence": 0,
                "head_digest": "",
                "batch_count": 0,
                "next_sequence": 1,
                "expected_previous_digest": "",
                "updated_at": None,
                "last_event_status": None,
                "last_event_sequence": 0,
                "last_event_at": None,
                "archived_through": 0,
                "archived_at": None,
            }
        return {
            "device_id": device_id,
            "last_sequence": chain["last_sequence"],
            "head_digest": chain["head_digest"],
            "batch_count": chain["batch_count"],
            "next_sequence": chain["last_sequence"] + 1,
            "expected_previous_digest": chain["head_digest"],
            "updated_at": _iso(chain["updated_at"]),
            "last_event_status": chain["last_event_status"],
            "last_event_sequence": chain["last_event_sequence"],
            "last_event_at": _iso(chain["last_event_at"]),
            "archived_through": chain["archived_through"],
            "archived_at": _iso(chain["archived_at"]),
        }

    def list_device_batches(
        self, device_id: str, *, after_sequence: int = 0, limit: int = 50
    ) -> dict:
        """按 sequence 游标分页列出设备批次（升序）。

        每条返回首次受理事实 ``status='accepted'``（归档墓碑为
        ``'archived'``，不带明细）及与详情一致的
        ``replay_count`` / ``last_replayed_at``；页顶另返回设备当前
        ``archived_through``。
        """
        with closing(self._connect()) as conn:
            chain = conn.execute(
                "SELECT archived_through, archived_at FROM device_chains "
                "WHERE device_id = ?",
                (device_id,),
            ).fetchone()
            rows = conn.execute(
                "SELECT sequence, batch_id, previous_digest, digest, "
                "observation_count, status, replay_count, last_replayed_at, "
                "archived_at, created_at "
                "FROM device_batches "
                "WHERE device_id = ? AND sequence > ? ORDER BY sequence LIMIT ?",
                (device_id, after_sequence, limit + 1),
            ).fetchall()
        archived_through = chain["archived_through"] if chain is not None else 0
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = []
        for r in rows:
            item = {
                "sequence": r["sequence"],
                "batch_id": r["batch_id"],
                "previous_digest": r["previous_digest"],
                "digest": r["digest"],
                "observation_count": r["observation_count"],
                "status": r["status"],
                "archived": r["archived_at"] is not None,
                "replay_count": r["replay_count"],
                "last_replayed_at": _iso(r["last_replayed_at"]),
                "archived_at": _iso(r["archived_at"]),
                "created_at": _iso(r["created_at"]),
            }
            items.append(item)
        return {
            "device_id": device_id,
            "count": len(items),
            "archived_through": archived_through,
            "batches": items,
            "next_cursor": items[-1]["sequence"] if has_more and items else None,
        }

    def get_device_batch(self, device_id: str, sequence: int) -> dict | None:
        """单条批次详情：摘要、条数、首次受理状态与重放信息。

        未归档批次另含观测时间戳与 /judge 四项阈值；已归档批次为墓碑
        （``status='archived'``、``archived=true``），明细已清空，
        ``observations`` / ``thresholds`` 为 ``None``。
        """
        with closing(self._connect()) as conn:
            r = conn.execute(
                "SELECT * FROM device_batches WHERE device_id = ? AND sequence = ?",
                (device_id, sequence),
            ).fetchone()
        if r is None:
            return None
        archived = r["archived_at"] is not None
        return {
            "status": r["status"],
            "archived": archived,
            "replay_count": r["replay_count"],
            "last_replayed_at": _iso(r["last_replayed_at"]),
            "archived_at": _iso(r["archived_at"]),
            "device_id": r["device_id"],
            "sequence": r["sequence"],
            "batch_id": r["batch_id"],
            "digest": r["digest"],
            "previous_digest": r["previous_digest"],
            "observation_count": r["observation_count"],
            # 墓碑明细载荷已清空：观测与阈值不再返回
            "observations": None if archived else json.loads(r["observations"]),
            "thresholds": None if archived else json.loads(r["thresholds"]),
            "created_at": _iso(r["created_at"]),
        }

    # ---------------- 必需消费者登记 / 批次确认 / 归档水位 ----------------

    def register_consumer(self, device_id: str, consumer_id: str) -> tuple[dict, bool]:
        """登记设备的必需下游消费者；登记后不可删除。

        同一 ``(device_id, consumer_id)`` 重复登记幂等：返回既有登记信息并以
        ``created=False`` 标记。新登记 ``created=True``。设备本身无需预先存在
        （消费者可在任何批次上传之前登记）。在设备已有归档前缀之后登记的消费
        者，其水位从 ``archived_through`` 起步——这些批次的明细已被清除，
        无需也无法再逐个确认，其首个缺口直接指向归档前缀的下一批。
        """
        now = time.time()
        with self._write_tx() as conn:
            existing = conn.execute(
                "SELECT consumer_id, watermark, registered_at FROM device_consumers "
                "WHERE device_id = ? AND consumer_id = ?",
                (device_id, consumer_id),
            ).fetchone()
            chain = conn.execute(
                "SELECT archived_through, archived_at FROM device_chains "
                "WHERE device_id = ?",
                (device_id,),
            ).fetchone()
            archived_through = chain["archived_through"] if chain is not None else 0
            if existing is not None:
                return {
                    "status": "already_registered",
                    "device_id": device_id,
                    "consumer_id": consumer_id,
                    "watermark": existing["watermark"],
                    "archived_through": archived_through,
                    "registered_at": _iso(existing["registered_at"]),
                }, False
            conn.execute(
                "INSERT INTO device_consumers (device_id, consumer_id, watermark, registered_at) "
                "VALUES (?, ?, ?, ?)",
                (device_id, consumer_id, archived_through, now),
            )
            return {
                "status": "registered",
                "device_id": device_id,
                "consumer_id": consumer_id,
                "watermark": archived_through,
                "archived_through": archived_through,
                "registered_at": _iso(now),
            }, True

    @staticmethod
    def _consumer_statuses(conn, device_id: str, head_sequence: int = -1) -> list[dict]:
        """组装各消费者水位与首个缺口（水位之后的第一个未确认序号）。

        水位定义为「从 1 起连续确认的最大序号」，因此首个缺口恒为
        ``watermark + 1``；当已追平链头（``watermark == head_sequence``）时
        链上再无缺口，返回 ``None``。
        """
        consumers = conn.execute(
            "SELECT id, consumer_id, watermark, registered_at FROM device_consumers "
            "WHERE device_id = ? ORDER BY id",
            (device_id,),
        ).fetchall()
        statuses: list[dict] = []
        for c in consumers:
            watermark = c["watermark"]
            first_gap = None if 0 <= head_sequence <= watermark else watermark + 1
            statuses.append(
                {
                    "consumer_id": c["consumer_id"],
                    "watermark": watermark,
                    "first_gap": first_gap,
                    "registered_at": _iso(c["registered_at"]),
                }
            )
        return statuses

    def acknowledge_batch(
        self, device_id: str, consumer_id: str, sequence: int, batch_digest: str
    ) -> tuple[dict, bool]:
        """单事务保存消费者的稀疏确认并把其连续水位推进至首个缺口。

        允许乱序、并发及重复提交。仅接受已存在且摘要相符的批次：批次不存在
        返回 404；消费者未登记或摘要冲突返回 409（:class:`ConsumerConflict`）。
        相同确认（同一消费者/序号/摘要）幂等，HTTP 200 且 ``status=duplicate``；
        新确认 HTTP 201 且 ``status=confirmed``。

        水位在写事务内按最新稀疏确认整体重算（从当前水位的下一个序号起逐个
        找首个缺口），并发确认由写锁串行化，不会丢失推进。
        """
        now = time.time()
        with self._write_tx() as conn:
            self._assert_not_isolated(conn, device_id)
            consumer = conn.execute(
                "SELECT id, watermark FROM device_consumers "
                "WHERE device_id = ? AND consumer_id = ?",
                (device_id, consumer_id),
            ).fetchone()
            if consumer is None:
                raise ConsumerConflict(
                    reason="consumer_not_registered",
                    message=(
                        f"consumer {consumer_id!r} is not registered as a required "
                        f"consumer of device {device_id!r}; register it before "
                        "acknowledging batches"
                    ),
                    device_id=device_id,
                    consumer_id=consumer_id,
                )
            batch = conn.execute(
                "SELECT digest FROM device_batches WHERE device_id = ? AND sequence = ?",
                (device_id, sequence),
            ).fetchone()
            if batch is None:
                # 404 在路由层按 BatchNotFound 处理
                raise BatchNotFound(device_id, sequence)
            if batch["digest"] != batch_digest:
                raise ConsumerConflict(
                    reason="digest_mismatch",
                    message=(
                        f"batch_digest does not match batch {sequence} of device "
                        f"{device_id!r}: acknowledgment refused"
                    ),
                    device_id=device_id,
                    consumer_id=consumer_id,
                    sequence=sequence,
                    provided_digest=batch_digest,
                    expected_digest=batch["digest"],
                )

            existing = conn.execute(
                "SELECT batch_digest, confirmed_at FROM device_batch_acks "
                "WHERE device_id = ? AND consumer_id = ? AND sequence = ?",
                (device_id, consumer_id, sequence),
            ).fetchone()
            created = False
            if existing is None:
                # 唯一约束兜底并发：同一 (消费者, 序号) 只可能有一条确认，
                # 摘要不符的并发写入会在此处转为 digest_mismatch 409
                try:
                    conn.execute(
                        "INSERT INTO device_batch_acks "
                        "(device_id, consumer_id, sequence, batch_digest, confirmed_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (device_id, consumer_id, sequence, batch_digest, now),
                    )
                except sqlite3.IntegrityError:
                    existing = conn.execute(
                        "SELECT batch_digest, confirmed_at FROM device_batch_acks "
                        "WHERE device_id = ? AND consumer_id = ? AND sequence = ?",
                        (device_id, consumer_id, sequence),
                    ).fetchone()
                    if existing["batch_digest"] != batch_digest:
                        raise ConsumerConflict(
                            reason="digest_mismatch",
                            message=(
                                f"batch {sequence} already acknowledged with a different "
                                "digest by this consumer"
                            ),
                            device_id=device_id,
                            consumer_id=consumer_id,
                            sequence=sequence,
                            provided_digest=batch_digest,
                            expected_digest=existing["batch_digest"],
                        )
                else:
                    created = True

            # 在同一事务内基于最新稀疏确认把水位推进至首个缺口：
            # 从当前水位 + 1 起，连续存在的确认序号逐个越过，遇到缺口即停。
            watermark = consumer["watermark"]
            while True:
                nxt = conn.execute(
                    "SELECT 1 FROM device_batch_acks "
                    "WHERE device_id = ? AND consumer_id = ? AND sequence = ?",
                    (device_id, consumer_id, watermark + 1),
                ).fetchone()
                if nxt is None:
                    break
                watermark += 1
            if watermark != consumer["watermark"]:
                conn.execute(
                    "UPDATE device_consumers SET watermark = ? WHERE id = ?",
                    (watermark, consumer["id"]),
                )

            # 在同一事务内基于最新稀疏确认把水位推进至首个缺口：
            # 从当前水位 + 1 起，连续存在的确认序号逐个越过，遇到缺口即停。
            # 归档之后登记的消费者水位从 archived_through 起步，其首个缺口
            # 即归档前缀的下一批；归档区间内无需也无法再逐个确认。
            watermark = consumer["watermark"]
            while True:
                nxt = conn.execute(
                    "SELECT 1 FROM device_batch_acks "
                    "WHERE device_id = ? AND consumer_id = ? AND sequence = ?",
                    (device_id, consumer_id, watermark + 1),
                ).fetchone()
                if nxt is None:
                    break
                watermark += 1
            if watermark != consumer["watermark"]:
                conn.execute(
                    "UPDATE device_consumers SET watermark = ? WHERE id = ?",
                    (watermark, consumer["id"]),
                )

            confirmed_at = _iso(now if existing is None else existing["confirmed_at"])
            body = {
                "status": "confirmed" if created else "duplicate",
                "device_id": device_id,
                "consumer_id": consumer_id,
                "sequence": sequence,
                "batch_digest": batch_digest,
                "watermark": watermark,
                "confirmed_at": confirmed_at,
            }
            # safe_sequence / archived_through 在同一写事务内读出，
            # 与本次确认的水位推进严格同快照
            body.update(self._archive_status_fields(conn, device_id))
            return body, created

    @staticmethod
    def _held_lease_views(conn, device_id: str, now: float) -> list[dict]:
        """设备上仍占用批次的租约（``status='held'``，含已过期但尚未回收的）。

        过期但未显式回收的租约仍占用其区间：归档会被挡住，直到消费者显式
        回收（``held → revoked``）。
        """
        rows = conn.execute(
            "SELECT id, consumer_id, from_sequence, to_sequence, expires_at "
            "FROM batch_leases WHERE device_id = ? AND status = 'held' ORDER BY id",
            (device_id,),
        ).fetchall()
        return [
            {
                "lease_id": r["id"],
                "consumer_id": r["consumer_id"],
                "from_sequence": r["from_sequence"],
                "to_sequence": r["to_sequence"],
                "granted_expires_at": _iso(r["expires_at"]),
                "expired": now >= r["expires_at"],
            }
            for r in rows
        ]

    @staticmethod
    def _archive_status_fields(conn, device_id: str) -> dict:
        """在给定连接（可为写事务中的连接）内核验并组装归档水位字段。

        ``safe_sequence`` 取所有必需消费者水位的最小值；尚无消费者或任一消费
        者水位仍为 0 时为 0。``archived_through`` 取链游标上的已归档前缀右端
        （单调不退，初始 0）。``blocking_consumers`` 列出水位等于该最小值且
        尚未追平链头的消费者（空列表表示链上批次已全部可安全归档）。

        ``held_leases`` 列出仍占用批次的租约（含过期未回收），``lease_bound``
        为不侵犯任何在持租约的前提下可归档的最大序号（首个租约起点 - 1；无
        租约时为 ``None``，即只受 ``safe_sequence`` 约束）。
        """
        now = time.time()
        chain = conn.execute(
            "SELECT last_sequence, archived_through FROM device_chains "
            "WHERE device_id = ?",
            (device_id,),
        ).fetchone()
        head_sequence = chain["last_sequence"] if chain is not None else 0
        archived_through = chain["archived_through"] if chain is not None else 0
        statuses = Database._consumer_statuses(conn, device_id, head_sequence)
        if not statuses:
            safe_sequence = 0
            blocking: list[str] = []
        else:
            safe_sequence = min(s["watermark"] for s in statuses)
            blocking = (
                sorted(
                    s["consumer_id"]
                    for s in statuses
                    if s["watermark"] == safe_sequence
                )
                if safe_sequence < head_sequence
                else []
            )
        held_leases = Database._held_lease_views(conn, device_id, now)
        lease_bound = (
            min(lr["from_sequence"] for lr in held_leases) - 1
            if held_leases
            else None
        )
        return {
            "head_sequence": head_sequence,
            "consumer_count": len(statuses),
            "safe_sequence": safe_sequence,
            "first_gap": safe_sequence + 1,
            "blocking_consumers": blocking,
            "consumers": statuses,
            "archived_through": archived_through,
            "held_leases": held_leases,
            "lease_bound": lease_bound,
        }

    def get_archive_status(self, device_id: str) -> dict:
        """设备的多消费者归档状态：各水位、首个缺口、阻塞消费者与归档前缀。"""
        with closing(self._connect()) as conn:
            status = self._archive_status_fields(conn, device_id)
            archived_at = conn.execute(
                "SELECT archived_at FROM device_chains WHERE device_id = ?",
                (device_id,),
            ).fetchone()
        status["device_id"] = device_id
        status["archived_at"] = (
            _iso(archived_at["archived_at"]) if archived_at is not None else None
        )
        # 顶层字段顺序稳定：device_id 在最前
        return {
            "device_id": device_id,
            "head_sequence": status["head_sequence"],
            "consumer_count": status["consumer_count"],
            "safe_sequence": status["safe_sequence"],
            "first_gap": status["first_gap"],
            "blocking_consumers": status["blocking_consumers"],
            "consumers": status["consumers"],
            "archived_through": status["archived_through"],
            "archived_at": status["archived_at"],
            "held_leases": status["held_leases"],
            "lease_bound": status["lease_bound"],
        }

    def archive_device_prefix(self, device_id: str, through_sequence: int) -> tuple[dict, bool]:
        """归档连续前缀 ``1..through_sequence`` 的明细载荷（单事务）。

        目标不得越过**同一写事务内**核定的 ``safe_sequence``（全部必需消费者
        都已确认的连续前缀）；越界抛 :class:`ArchiveConflict`（HTTP 409），
        detail 携带当前 ``safe_sequence`` / ``archived_through`` /
        ``head_sequence`` / ``first_gap`` 与阻塞消费者。归档把目标范围内批次
        的 ``observations`` / ``thresholds`` 清空并置墓碑
        （``status='archived'``、``archived_at``），只保留 ``batch_id``、
        序号、前序/本批摘要、条数、首次受理时刻/响应与重放统计；链头摘要与
        后续上传不变。

        目标不超过已归档前缀时**幂等**（HTTP 200，``status=already_archived``，
        不刷新归档时刻、不改动重放统计）；新扩大前缀 HTTP 201
        （``status=archived``）。写锁 + BEGIN IMMEDIATE 与确认/登记/上传/租约
        共用同一串行点：并发确认、消费者登记与归档互不越过未确认批次，
        ``archived_through`` 只进不退。此外归档不得清除仍被在持租约
        （``held``，含过期未回收）占用的明细：目标范围与任一在持租约区间
        相交时抛 ``beyond_held_leases``（附 ``lease_bound`` 与阻塞租约清单）。
        """
        now = time.time()
        with self._write_tx() as conn:
            self._assert_not_isolated(conn, device_id)
            chain = conn.execute(
                "SELECT * FROM device_chains WHERE device_id = ?", (device_id,)
            ).fetchone()
            head_sequence = chain["last_sequence"] if chain is not None else 0
            archived_through = chain["archived_through"] if chain is not None else 0
            fields = self._archive_status_fields(conn, device_id)
            safe_sequence = fields["safe_sequence"]

            if through_sequence <= archived_through:
                # 重复或更小目标：幂等返回当前归档状态，不刷新 archived_at
                return self._archive_response(
                    device_id,
                    status="already_archived",
                    requested=through_sequence,
                    archived_through=archived_through,
                    archived_at=chain["archived_at"] if chain is not None else None,
                    fields=fields,
                ), False

            if through_sequence > safe_sequence:
                raise ArchiveConflict(
                    message=(
                        f"cannot archive through sequence {through_sequence}: it exceeds "
                        f"safe_sequence {safe_sequence} (all required consumers must "
                        f"acknowledge the contiguous prefix first)"
                    ),
                    reason="beyond_safe_sequence",
                    requested_through=through_sequence,
                    safe_sequence=safe_sequence,
                    archived_through=archived_through,
                    head_sequence=head_sequence,
                    first_gap=fields["first_gap"],
                    blocking_consumers=fields["blocking_consumers"],
                )

            # 归档不得清除仍被租约占用的明细：任一 held 租约（含过期但尚未
            # 显式回收的）的区间起点落在目标范围内即拒绝。过期租约需由所属
            # 消费者显式回收（revoked）后才能继续归档。
            blocking_leases = [
                lr
                for lr in fields["held_leases"]
                if lr["from_sequence"] <= through_sequence
            ]
            if blocking_leases:
                raise ArchiveConflict(
                    message=(
                        f"cannot archive through sequence {through_sequence}: "
                        f"{len(blocking_leases)} held lease(s) still occupy batches in "
                        "the range; expired leases must be explicitly reclaimed first"
                    ),
                    reason="beyond_held_leases",
                    requested_through=through_sequence,
                    safe_sequence=safe_sequence,
                    archived_through=archived_through,
                    head_sequence=head_sequence,
                    lease_bound=fields["lease_bound"],
                    blocking_leases=blocking_leases,
                )

            # 通过 <= safe_sequence 保证目标范围内每个批次都存在
            # （1..safe_sequence 必为已上传的连续前缀）。
            cur = conn.execute(
                "UPDATE device_batches SET "
                "observations = NULL, thresholds = NULL, "
                "status = 'archived', archived_at = ? "
                "WHERE device_id = ? AND sequence > ? AND sequence <= ?",
                (now, device_id, archived_through, through_sequence),
            )
            archived_count = cur.rowcount
            if chain is None:
                # 极端情况下无链行（安全水位必为 0，目标 >=1 不可能走到这里；
                # 兜底插入，保持 archived_through 与实际墓碑一致）
                conn.execute(
                    "INSERT INTO device_chains "
                    "(device_id, last_sequence, head_digest, batch_count, updated_at, "
                    "archived_through, archived_at) "
                    "VALUES (?, 0, '', 0, ?, ?, ?)",
                    (device_id, now, through_sequence, now),
                )
            else:
                conn.execute(
                    "UPDATE device_chains SET archived_through = ?, archived_at = ? "
                    "WHERE device_id = ?",
                    (through_sequence, now, device_id),
                )
            fields = self._archive_status_fields(conn, device_id)
            body = self._archive_response(
                device_id,
                status="archived",
                requested=through_sequence,
                archived_through=through_sequence,
                archived_at=now,
                fields=fields,
                archived_count=archived_count,
            )
            return body, True

    @staticmethod
    def _archive_response(
        device_id: str,
        *,
        status: str,
        requested: int,
        archived_through: int,
        archived_at: float | None,
        fields: dict,
        archived_count: int = 0,
    ) -> dict:
        return {
            "status": status,
            "device_id": device_id,
            "requested_through": requested,
            "archived_through": archived_through,
            "archived_at": _iso(archived_at),
            "newly_archived_count": archived_count,
            "head_sequence": fields["head_sequence"],
            "safe_sequence": fields["safe_sequence"],
            "first_gap": fields["safe_sequence"] + 1,
            "blocking_consumers": fields["blocking_consumers"],
            "consumers": fields["consumers"],
            "held_leases": fields["held_leases"],
            "lease_bound": fields["lease_bound"],
        }

    # ---------------- 草稿回放报告 ----------------

    @staticmethod
    def _replay_summary(report_row) -> dict:
        """组装回放报告摘要（不含逐条明细，明细走游标分页接口）。"""
        return {
            "report_id": report_row["report_id"],
            "version_id": report_row["version_id"],
            "idempotency_key": report_row["idempotency_key"],
            "watermark_judgment_id": report_row["watermark"],
            "created_at": _iso(report_row["created_at"]),
            "total_judgments": report_row["total_count"],
            "replayed_count": report_row["replayed_count"],
            "skipped_count": report_row["skipped_count"],
            "changes": {
                "unchanged": report_row["unchanged"],
                "match_gained": report_row["match_gained"],
                "match_lost": report_row["match_lost"],
                "winner_changed": report_row["winner_changed"],
            },
        }

    def create_replay_report(
        self, version_id: int, idempotency_key: str, runner
    ) -> tuple[dict, bool]:
        """为草稿创建回放报告；整个流程（冻结水位 → 重放 → 落库）在单个
        BEGIN IMMEDIATE 事务内完成。

        - 目标版本必须存在且为 ``draft``（不存在抛 :class:`VersionNotFound`，
          非草稿抛 :class:`VersionStateError`）；
        - ``idempotency_key`` 全局唯一：同版本同键重试返回原报告
          （``created=False``，HTTP 200）；键已被**其他版本**占用抛
          :class:`ReplayConflict`（HTTP 409）。写锁串行化 + 唯一约束兜底，
          并发请求不会生成重复报告；
        - 开始时冻结当前最大 ``judgment_id`` 作为水位，只重放水位内的记录：
          请求携带内联 ``library`` 的记录按原因跳过，载荷缺字段的历史记录
          同样跳过；其余沿用原观测与四项阈值，由调用方注入的 ``runner``
          （草稿快照, 待回放记录）执行与 /judge 相同的判定算法。

        回放结果只写入 ``replay_reports`` / ``replay_report_items``，
        不写入 ``judgments``。
        """
        from .replay import (
            SKIP_INLINE_LIBRARY,
            SKIP_MISSING_INPUT,
            classify_change,
            has_replay_input,
            is_inline_judgment,
        )

        now = time.time()
        with self._write_tx() as conn:
            version = conn.execute(
                "SELECT status FROM library_versions WHERE version_id = ?",
                (version_id,),
            ).fetchone()
            if version is None:
                raise VersionNotFound(f"version {version_id} not found")
            if version["status"] != "draft":
                raise VersionStateError(
                    f"version {version_id} is {version['status']}; "
                    "replay reports can only be created for a draft"
                )

            existing = conn.execute(
                "SELECT * FROM replay_reports WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["version_id"] == version_id:
                    return self._replay_summary(existing), False
                raise ReplayConflict(
                    message=(
                        f"idempotency_key {idempotency_key!r} is already used by a "
                        f"replay report of version {existing['version_id']}"
                    ),
                    idempotency_key=idempotency_key,
                    occupied_by_version=existing["version_id"],
                    occupied_by_report=existing["report_id"],
                )

            # 冻结水位：事务已持写锁，后续 /judge 的写入在本事务提交后才能进行，
            # 新判定的 id 必然大于该水位，不会混入本次报告。
            watermark_row = conn.execute(
                "SELECT COALESCE(MAX(id), 0) AS m FROM judgments"
            ).fetchone()
            watermark = int(watermark_row["m"])

            judgment_rows = conn.execute(
                "SELECT id, status, best_aid_id, library_version, request, response "
                "FROM judgments WHERE id <= ? ORDER BY id",
                (watermark,),
            ).fetchall()

            to_replay: list[dict] = []
            rows_by_id: dict[int, sqlite3.Row] = {}
            skipped: list[tuple[int, str, sqlite3.Row]] = []
            for r in judgment_rows:
                request = json.loads(r["request"])
                if is_inline_judgment(request):
                    skipped.append((r["id"], SKIP_INLINE_LIBRARY, r))
                elif not has_replay_input(request):
                    skipped.append((r["id"], SKIP_MISSING_INPUT, r))
                else:
                    rows_by_id[r["id"]] = r
                    to_replay.append({"judgment_id": r["id"], "request": request})

            outcomes = runner(self._aids_of(conn, version_id), to_replay)
            outcomes_by_id = {o["judgment_id"]: o for o in outcomes}

            try:
                cur = conn.execute(
                    "INSERT INTO replay_reports "
                    "(version_id, idempotency_key, watermark, total_count, replayed_count, "
                    "skipped_count, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        version_id,
                        idempotency_key,
                        watermark,
                        len(judgment_rows),
                        len(to_replay),
                        len(skipped),
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                # 并发兜底：写锁通常已把同键请求串行化到上面的 SELECT；
                # 若仍有并发事务抢先占用该键，按幂等/跨版本冲突处理
                rival = conn.execute(
                    "SELECT * FROM replay_reports WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if rival is not None and rival["version_id"] == version_id:
                    return self._replay_summary(rival), False
                raise ReplayConflict(
                    message=(
                        f"idempotency_key {idempotency_key!r} is already used by a "
                        f"replay report of version {rival['version_id'] if rival else '?'}"
                    ),
                    idempotency_key=idempotency_key,
                    occupied_by_version=int(rival["version_id"]) if rival else version_id,
                    occupied_by_report=int(rival["report_id"]) if rival else 0,
                )
            report_id = int(cur.lastrowid)

            for judgment_id, skip_reason, r in skipped:
                conn.execute(
                    "INSERT INTO replay_report_items "
                    "(report_id, judgment_id, item_status, skip_reason, "
                    "old_status, old_best_aid_id, old_reason, old_library_version) "
                    "VALUES (?, ?, 'skipped', ?, ?, ?, ?, ?)",
                    (
                        report_id,
                        judgment_id,
                        skip_reason,
                        r["status"],
                        r["best_aid_id"],
                        None,
                        r["library_version"],
                    ),
                )

            class_counts = {
                "unchanged": 0,
                "match_gained": 0,
                "match_lost": 0,
                "winner_changed": 0,
            }
            for rec in to_replay:
                r = rows_by_id[rec["judgment_id"]]
                old_response = json.loads(r["response"])
                old_best = old_response.get("best")
                old_reason = old_response.get("reason")
                # 裁决状态/最佳编号以 judgments 权威列为准，证据以保存的首次响应为准
                old_status = r["status"]
                outcome = outcomes_by_id[rec["judgment_id"]]
                new_best = outcome["best"]
                new_status = outcome["status"]
                classification = classify_change(
                    old_status, old_best, new_status, new_best
                )
                class_counts[classification] += 1
                conn.execute(
                    "INSERT INTO replay_report_items "
                    "(report_id, judgment_id, item_status, classification, "
                    "old_status, old_best_aid_id, old_reason, old_library_version, "
                    "old_best, new_status, new_best_aid_id, new_reason, new_best) "
                    "VALUES (?, ?, 'replayed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        report_id,
                        rec["judgment_id"],
                        classification,
                        old_status,
                        r["best_aid_id"],
                        old_reason,
                        r["library_version"],
                        json.dumps(old_best, ensure_ascii=False),
                        new_status,
                        new_best["aid_id"] if new_best is not None else None,
                        outcome["reason"],
                        json.dumps(new_best, ensure_ascii=False),
                    ),
                )

            conn.execute(
                "UPDATE replay_reports SET unchanged = ?, match_gained = ?, "
                "match_lost = ?, winner_changed = ? WHERE report_id = ?",
                (
                    class_counts["unchanged"],
                    class_counts["match_gained"],
                    class_counts["match_lost"],
                    class_counts["winner_changed"],
                    report_id,
                ),
            )
            report = conn.execute(
                "SELECT * FROM replay_reports WHERE report_id = ?", (report_id,)
            ).fetchone()
            summary = self._replay_summary(report)
        return summary, True

    def get_replay_report(self, version_id: int, report_id: int) -> tuple[int, dict] | None:
        """读取指定版本下的回放报告摘要；不属于该版本或不存在返回 None。

        返回 ``(version_id, summary)``。
        """
        with closing(self._connect()) as conn:
            r = conn.execute(
                "SELECT * FROM replay_reports WHERE report_id = ? AND version_id = ?",
                (report_id, version_id),
            ).fetchone()
        if r is None:
            return None
        return int(r["version_id"]), self._replay_summary(r)

    def list_replay_changes(
        self,
        version_id: int,
        report_id: int,
        *,
        after_judgment_id: int = 0,
        limit: int = 50,
        classification: str | None = None,
        include_skipped: bool = False,
    ) -> dict | None:
        """按原 judgment_id 游标分页返回回放明细（升序）。

        默认只返回已重放记录（可按四分类之一过滤）；``include_skipped=true``
        时改返回被跳过记录（此时不允许再按 classification 过滤）。报告不
        属于该版本或不存在时返回 None。
        """
        with closing(self._connect()) as conn:
            report = conn.execute(
                "SELECT 1 FROM replay_reports WHERE report_id = ? AND version_id = ?",
                (report_id, version_id),
            ).fetchone()
            if report is None:
                return None
            if include_skipped:
                rows = conn.execute(
                    "SELECT * FROM replay_report_items "
                    "WHERE report_id = ? AND item_status = 'skipped' "
                    "AND judgment_id > ? ORDER BY judgment_id LIMIT ?",
                    (report_id, after_judgment_id, limit + 1),
                ).fetchall()
            elif classification is not None:
                rows = conn.execute(
                    "SELECT * FROM replay_report_items "
                    "WHERE report_id = ? AND item_status = 'replayed' "
                    "AND classification = ? AND judgment_id > ? "
                    "ORDER BY judgment_id LIMIT ?",
                    (report_id, classification, after_judgment_id, limit + 1),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM replay_report_items "
                    "WHERE report_id = ? AND item_status = 'replayed' "
                    "AND judgment_id > ? ORDER BY judgment_id LIMIT ?",
                    (report_id, after_judgment_id, limit + 1),
                ).fetchall()

        has_more = len(rows) > limit
        rows = rows[:limit]
        items: list[dict] = []
        for r in rows:
            if r["item_status"] == "skipped":
                items.append(
                    {
                        "judgment_id": r["judgment_id"],
                        "item_status": "skipped",
                        "skip_reason": r["skip_reason"],
                        "old_status": r["old_status"],
                        "old_best_aid_id": r["old_best_aid_id"],
                        "old_library_version": r["old_library_version"],
                    }
                )
            else:
                items.append(
                    {
                        "judgment_id": r["judgment_id"],
                        "item_status": "replayed",
                        "classification": r["classification"],
                        "old": {
                            "status": r["old_status"],
                            "best_aid_id": r["old_best_aid_id"],
                            "reason": r["old_reason"],
                            "library_version": r["old_library_version"],
                            "best": json.loads(r["old_best"]),
                        },
                        "new": {
                            "status": r["new_status"],
                            "best_aid_id": r["new_best_aid_id"],
                            "reason": r["new_reason"],
                            "best": json.loads(r["new_best"]),
                        },
                    }
                )
        return {
            "report_id": report_id,
            "version_id": version_id,
            "classification": classification,
            "include_skipped": include_skipped,
            "count": len(items),
            "changes": items,
            "next_cursor": items[-1]["judgment_id"] if has_more and items else None,
        }

    # ---------------- 连续批次归档前巡检判定 ----------------

    def create_batch_judgment(
        self,
        device_id: str,
        idempotency_key: str,
        from_sequence: int,
        to_sequence: int,
        runner,
    ) -> tuple[dict, bool]:
        """对设备连续批次区间创建一次归档前巡检判定（单事务，幂等）。

        整个流程在单个 BEGIN IMMEDIATE 事务内完成，写锁与上传/归档/激活共用
        同一串行点：

        1. 幂等键检查：相同键 + 相同设备/范围重试返回原判定
           （``created=False``，HTTP 200）；相同键但设备或范围不同抛
           :class:`BatchJudgmentConflict`（HTTP 409）；
        2. 区间校验：``from_sequence..to_sequence`` 内每个序号都必须存在、
           且明细仍在（非墓碑）。范围含墓碑抛 ``range_archived``，含缺批次
           抛 ``range_not_continuous``——两种情况整体拒绝，不落任何记录；
        3. 各批保存的 /judge 四项阈值必须完全一致，否则抛
           ``thresholds_mismatch``（同样整体拒绝）；
        4. 在同一事务内固定当前激活特征库版本与快照（持锁期间版本不可能被
           切换），按 ``(captured_at, sequence, local_id)`` 合并观测，由
           调用方注入的 ``runner`` 复用与 /judge 完全相同的判定算法；
        5. 判定结果写入 ``judgments``（可被 /judgments 与草稿回放复核），
           再保存 ``batch_judgments``（幂等键、设备、序号范围、
           ``library_version``、合并观测与阈值、judgment_id）与各批摘要
           （``batch_judgment_batches``）作为不可变来源。
        """
        now = time.time()
        with self._write_tx() as conn:
            self._assert_not_isolated(conn, device_id)
            existing = conn.execute(
                "SELECT id FROM batch_judgments WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                saved = self._get_batch_judgment(conn, int(existing["id"]))
                if (
                    saved is not None
                    and saved["device_id"] == device_id
                    and saved["from_sequence"] == from_sequence
                    and saved["to_sequence"] == to_sequence
                ):
                    return saved, False
                raise BatchJudgmentConflict(
                    message=(
                        f"idempotency_key {idempotency_key!r} is already used by a "
                        f"batch judgment of device {saved['device_id']!r} range "
                        f"{saved['from_sequence']}..{saved['to_sequence']}"
                    ),
                    reason="idempotency_key_in_use",
                    idempotency_key=idempotency_key,
                    occupied_by_batch_judgment_id=saved["batch_judgment_id"],
                    occupied_device_id=saved["device_id"],
                    occupied_from_sequence=saved["from_sequence"],
                    occupied_to_sequence=saved["to_sequence"],
                )

            rows = conn.execute(
                "SELECT sequence, batch_id, previous_digest, digest, "
                "observation_count, thresholds, observations, archived_at "
                "FROM device_batches "
                "WHERE device_id = ? AND sequence >= ? AND sequence <= ? "
                "ORDER BY sequence",
                (device_id, from_sequence, to_sequence),
            ).fetchall()
            chain = conn.execute(
                "SELECT last_sequence FROM device_chains WHERE device_id = ?",
                (device_id,),
            ).fetchone()
            head_sequence = chain["last_sequence"] if chain is not None else 0

            by_seq = {r["sequence"]: r for r in rows}
            missing = [
                s for s in range(from_sequence, to_sequence + 1) if s not in by_seq
            ]
            if missing:
                raise BatchJudgmentConflict(
                    message=(
                        f"cannot judge range {from_sequence}..{to_sequence}: "
                        f"missing batch sequence(s) {', '.join(map(str, missing))}"
                    ),
                    reason="range_not_continuous",
                    device_id=device_id,
                    from_sequence=from_sequence,
                    to_sequence=to_sequence,
                    head_sequence=head_sequence,
                    missing_sequences=missing,
                )
            archived = [
                r["sequence"]
                for r in rows
                if r["archived_at"] is not None or r["observations"] is None
            ]
            if archived:
                raise BatchJudgmentConflict(
                    message=(
                        f"cannot judge range {from_sequence}..{to_sequence}: "
                        f"batch sequence(s) {', '.join(map(str, archived))} are "
                        "archived tombstones without observations"
                    ),
                    reason="range_archived",
                    device_id=device_id,
                    from_sequence=from_sequence,
                    to_sequence=to_sequence,
                    archived_sequences=archived,
                )

            thresholds = json.loads(rows[0]["thresholds"])
            mismatched: list[int] = []
            for r in rows[1:]:
                if json.loads(r["thresholds"]) != thresholds:
                    mismatched.append(r["sequence"])
            if mismatched:
                raise BatchJudgmentConflict(
                    message=(
                        "all batches in the range must share the exact same /judge "
                        "thresholds (tolerance/max_drift/max_missed/max_false); "
                        f"sequence(s) {', '.join(map(str, mismatched))} differ from "
                        f"sequence {from_sequence}"
                    ),
                    reason="thresholds_mismatch",
                    device_id=device_id,
                    from_sequence=from_sequence,
                    to_sequence=to_sequence,
                    reference_sequence=from_sequence,
                    mismatched_sequences=mismatched,
                )

            batch_details = [
                {
                    "sequence": r["sequence"],
                    "observations": json.loads(r["observations"]),
                }
                for r in rows
            ]
            merged = merge_batch_observations(batch_details)
            timestamps = [o["captured_at"] for o in merged]

            # 持写锁固定激活版本：判定与保存之间不可能发生版本切换
            library_version, aids = self._active_snapshot_of(conn)
            request_payload, response_payload = runner(
                timestamps, thresholds, library_version, aids
            )

            # 保存点：若唯一约束兜底发现同键已被并发请求占用（多 worker 下
            # 可能越过进程内写锁串行），回滚本请求已写的 judgments 行，
            # 绝不留下孤儿判定或半个范围
            conn.execute("SAVEPOINT batch_judgment_insert")
            judgment_id = self._insert_judgment(
                conn,
                status=response_payload["status"],
                best_aid_id=response_payload["best"]
                and response_payload["best"]["aid_id"],
                matched=response_payload["best"]["matched"]
                if response_payload["best"] is not None
                else 0,
                missed=response_payload["best"]["missed"]
                if response_payload["best"] is not None
                else 0,
                false_count=response_payload["best"]["false"]
                if response_payload["best"] is not None
                else 0,
                drift=response_payload["best"]["drift"]
                if response_payload["best"] is not None
                else None,
                library_version=library_version,
                request=request_payload,
                response=response_payload,
            )
            response_payload["judgment_id"] = judgment_id

            try:
                cur = conn.execute(
                    "INSERT INTO batch_judgments "
                    "(idempotency_key, device_id, from_sequence, to_sequence, "
                    "library_version, observation_count, thresholds, observations, "
                    "judgment_id, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        idempotency_key,
                        device_id,
                        from_sequence,
                        to_sequence,
                        library_version,
                        len(merged),
                        json.dumps(thresholds, ensure_ascii=False),
                        json.dumps(merged, ensure_ascii=False),
                        judgment_id,
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                # 唯一约束兜底并发同键：写锁通常已串行化到上面的 SELECT；
                # 多 worker 下若仍有并发事务抢先占用该键，撤销本请求写入
                conn.execute("ROLLBACK TO SAVEPOINT batch_judgment_insert")
                rival = conn.execute(
                    "SELECT * FROM batch_judgments WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                saved = self._get_batch_judgment(conn, int(rival["id"]))
                if (
                    saved is not None
                    and saved["device_id"] == device_id
                    and saved["from_sequence"] == from_sequence
                    and saved["to_sequence"] == to_sequence
                ):
                    return saved, False
                raise BatchJudgmentConflict(
                    message=(
                        f"idempotency_key {idempotency_key!r} is already used by a "
                        f"batch judgment of device {saved['device_id']!r} range "
                        f"{saved['from_sequence']}..{saved['to_sequence']}"
                    ),
                    reason="idempotency_key_in_use",
                    idempotency_key=idempotency_key,
                    occupied_by_batch_judgment_id=saved["batch_judgment_id"],
                    occupied_device_id=saved["device_id"],
                    occupied_from_sequence=saved["from_sequence"],
                    occupied_to_sequence=saved["to_sequence"],
                )
            conn.execute("RELEASE SAVEPOINT batch_judgment_insert")
            batch_judgment_id = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO batch_judgment_batches "
                "(batch_judgment_id, sequence, batch_id, previous_digest, digest, "
                "observation_count) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        batch_judgment_id,
                        r["sequence"],
                        r["batch_id"],
                        r["previous_digest"],
                        r["digest"],
                        r["observation_count"],
                    )
                    for r in rows
                ],
            )
            saved = self._get_batch_judgment(conn, batch_judgment_id)
        return saved, True

    @staticmethod
    def _batch_judgment_batches(conn, batch_judgment_id: int) -> list[dict]:
        rows = conn.execute(
            "SELECT sequence, batch_id, previous_digest, digest, observation_count "
            "FROM batch_judgment_batches WHERE batch_judgment_id = ? ORDER BY sequence",
            (batch_judgment_id,),
        ).fetchall()
        return [
            {
                "sequence": r["sequence"],
                "batch_id": r["batch_id"],
                "previous_digest": r["previous_digest"],
                "digest": r["digest"],
                "observation_count": r["observation_count"],
            }
            for r in rows
        ]

    def _get_batch_judgment(self, conn, batch_judgment_id: int) -> dict | None:
        """在给定连接内组装一次连续批次判定的完整不可变来源。"""
        r = conn.execute(
            "SELECT * FROM batch_judgments WHERE id = ?", (batch_judgment_id,)
        ).fetchone()
        if r is None:
            return None
        judgment = conn.execute(
            "SELECT response FROM judgments WHERE id = ?", (r["judgment_id"],)
        ).fetchone()
        judgment_payload = (
            json.loads(judgment["response"]) if judgment is not None else None
        )
        if judgment_payload is not None:
            # judgments 保存的是回填 id 前的响应（同 GET /judgments/{id}），
            # 这里补上来源判定 id 便于直接复核
            judgment_payload["judgment_id"] = r["judgment_id"]
        return {
            "batch_judgment_id": r["id"],
            "idempotency_key": r["idempotency_key"],
            "device_id": r["device_id"],
            "from_sequence": r["from_sequence"],
            "to_sequence": r["to_sequence"],
            "library_version": r["library_version"],
            "observation_count": r["observation_count"],
            "thresholds": json.loads(r["thresholds"]),
            "observations": json.loads(r["observations"]),
            "source_judgment_id": r["judgment_id"],
            "created_at": _iso(r["created_at"]),
            "batches": self._batch_judgment_batches(conn, r["id"]),
            "judgment": judgment_payload,
        }

    def get_batch_judgment(self, device_id: str, batch_judgment_id: int) -> dict | None:
        """查询一次连续批次判定的完整来源；不属于该设备或不存在返回 None（404）。"""
        with closing(self._connect()) as conn:
            found = self._get_batch_judgment(conn, batch_judgment_id)
        if found is None or found["device_id"] != device_id:
            return None
        return found

    def list_batch_judgments(
        self, device_id: str, *, after_id: int = 0, limit: int = 50
    ) -> dict:
        """按 id 游标分页列出设备的连续批次判定（升序，仅摘要，不含判定证据）。"""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM batch_judgments "
                "WHERE device_id = ? AND id > ? ORDER BY id LIMIT ?",
                (device_id, after_id, limit + 1),
            ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = [
            {
                "batch_judgment_id": r["id"],
                "idempotency_key": r["idempotency_key"],
                "device_id": r["device_id"],
                "from_sequence": r["from_sequence"],
                "to_sequence": r["to_sequence"],
                "library_version": r["library_version"],
                "observation_count": r["observation_count"],
                "thresholds": json.loads(r["thresholds"]),
                "source_judgment_id": r["judgment_id"],
                "created_at": _iso(r["created_at"]),
            }
            for r in rows
        ]
        return {
            "device_id": device_id,
            "count": len(items),
            "batch_judgments": items,
            "next_cursor": items[-1]["batch_judgment_id"] if has_more and items else None,
        }

    # ---------------- 连续批次判定复核报告（只读快照复核 + 幂等报告） ----------------

    # 复核问题清单中属于「摘要不符」（source_mismatch）的 issue_code；
    # 其余 issue_code 均属「来源缺失」（source_unavailable）
    _REVIEW_MISMATCH_CODES = frozenset({
        "digest_mismatch",
        "digest_recompute_mismatch",
        "previous_digest_mismatch",
        "previous_chain_broken",
        "thresholds_mismatch",
        "batch_id_mismatch",
        "observation_count_mismatch",
        "observations_mismatch",
    })

    def create_batch_judgment_review_report(
        self,
        device_id: str,
        batch_judgment_id: int,
        idempotency_key: str,
        runner,
    ) -> tuple[dict, bool]:
        """复核一次连续批次判定并保存可幂等的复核报告。

        分三步，复核本身**只读**：

        1. 幂等键只读检查：相同键 + 相同设备/判定目标直接返回原报告
           （``created=False``，HTTP 200）；相同键被其他设备或判定目标占用
           抛 :class:`ReviewReportConflict`（HTTP 409）；
        2. **单个只读 SQLite 事务**（``PRAGMA query_only`` 兜底）内固定快照，
           由 :meth:`_review_batch_judgment` 逐批核对当前批次链，全部明细
           可用且摘要、前序链、阈值一致时用保存版本的特征库重跑判定——
           纯计算，**不写入** ``judgments``；
        3. 写事务落报告（只写 ``batch_judgment_review_reports``）；幂等键
           唯一约束兜底多 worker 并发同键，撞键时回读占用方报告按第 1 步
           同样规则裁决。
        """
        with closing(self._connect()) as conn:
            resolved = self._review_key_resolution(
                conn, idempotency_key, device_id, batch_judgment_id
            )
        if resolved is not None:
            return resolved, False

        with closing(self._connect()) as conn:
            conn.execute("PRAGMA query_only = ON")  # 只读兜底：复核绝不写数据
            try:
                conn.execute("BEGIN")  # 只读事务：以下全部读取固定于同一快照
                report_body = self._review_batch_judgment(
                    conn, device_id, batch_judgment_id, runner
                )
            finally:
                conn.rollback()  # 只读事务，释放读锁

        now = time.time()
        with self._write_tx() as conn:
            try:
                cur = conn.execute(
                    "INSERT INTO batch_judgment_review_reports "
                    "(idempotency_key, device_id, batch_judgment_id, status, "
                    "report, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        idempotency_key,
                        device_id,
                        batch_judgment_id,
                        report_body["status"],
                        json.dumps(report_body, ensure_ascii=False),
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                # 唯一约束兜底并发同键：回读占用方，同目标返回原报告、否则 409
                resolved = self._review_key_resolution(
                    conn, idempotency_key, device_id, batch_judgment_id
                )
                if resolved is None:  # 防御：非键占用的约束错误不应发生
                    raise
                return resolved, False
            saved = self._get_review_report(conn, int(cur.lastrowid))
        return saved, True

    def _review_key_resolution(
        self, conn, idempotency_key: str, device_id: str, batch_judgment_id: int
    ) -> dict | None:
        """幂等键裁决：键未使用返回 None；同设备同判定目标返回原报告；
        键被其他设备或判定目标占用抛 :class:`ReviewReportConflict`。"""
        row = conn.execute(
            "SELECT * FROM batch_judgment_review_reports WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        if row is None:
            return None
        if row["device_id"] == device_id and row["batch_judgment_id"] == batch_judgment_id:
            return self._review_report_view(row)
        raise ReviewReportConflict(
            message=(
                f"idempotency_key {idempotency_key!r} is already used by a review "
                f"report of device {row['device_id']!r} batch judgment "
                f"{row['batch_judgment_id']}"
            ),
            reason="idempotency_key_in_use",
            idempotency_key=idempotency_key,
            occupied_by_report_id=row["report_id"],
            occupied_device_id=row["device_id"],
            occupied_batch_judgment_id=row["batch_judgment_id"],
        )

    def _review_batch_judgment(
        self, conn, device_id: str, batch_judgment_id: int, runner
    ) -> dict:
        """在只读事务连接内复核一次连续批次判定（快照由调用方的事务保证）。

        固定 ``batch_judgments`` 不可变记录、各批摘要与保存的
        ``library_version``，逐批核对当前批次链（已归档墓碑只核对摘要骨架、
        没有明细）；只有所有明细可用且摘要、前序链和阈值一致时，才用保存
        版本的特征库由 ``runner`` 重跑判定（与 /judge 同一算法）并逐字段
        比较新旧裁决。纯读取 + 纯计算：不写任何表，尤其不写 ``judgments``。
        """
        _issue = self._verify_issue
        bj = conn.execute(
            "SELECT * FROM batch_judgments WHERE id = ?", (batch_judgment_id,)
        ).fetchone()
        if bj is None or bj["device_id"] != device_id:
            raise BatchJudgmentNotFound(
                message=(
                    f"batch judgment {batch_judgment_id} of device "
                    f"{device_id!r} not found"
                ),
                device_id=device_id,
                batch_judgment_id=batch_judgment_id,
            )
        saved_batches = self._batch_judgment_batches(conn, batch_judgment_id)
        saved_thresholds = json.loads(bj["thresholds"])
        saved_observations = json.loads(bj["observations"])
        library_version = bj["library_version"]

        jrow = conn.execute(
            "SELECT response FROM judgments WHERE id = ?", (bj["judgment_id"],)
        ).fetchone()
        saved_response = json.loads(jrow["response"]) if jrow is not None else None

        issues: list[dict] = []
        if saved_response is None:
            issues.append(
                _issue(
                    "judgment_record_missing", None, "present", "missing",
                    judgment_id=bj["judgment_id"],
                )
            )

        # ---- 逐批核对当前批次（已归档墓碑可核对摘要骨架，但没有明细）----
        current: dict[int, sqlite3.Row] = {}
        archived_count = 0
        for sb in saved_batches:
            seq = sb["sequence"]
            row = conn.execute(
                "SELECT batch_id, previous_digest, digest, observation_count, "
                "thresholds, observations, archived_at FROM device_batches "
                "WHERE device_id = ? AND sequence = ?",
                (device_id, seq),
            ).fetchone()
            if row is None:
                issues.append(_issue("batch_missing", seq, "present", "missing"))
                continue
            current[seq] = row
            if row["batch_id"] != sb["batch_id"]:
                issues.append(
                    _issue("batch_id_mismatch", seq, sb["batch_id"], row["batch_id"])
                )
            if row["digest"] != sb["digest"]:
                issues.append(
                    _issue("digest_mismatch", seq, sb["digest"], row["digest"])
                )
            if row["previous_digest"] != sb["previous_digest"]:
                issues.append(
                    _issue(
                        "previous_digest_mismatch",
                        seq,
                        sb["previous_digest"],
                        row["previous_digest"],
                    )
                )
            if row["observation_count"] != sb["observation_count"]:
                issues.append(
                    _issue(
                        "observation_count_mismatch",
                        seq,
                        sb["observation_count"],
                        row["observation_count"],
                    )
                )
            if row["archived_at"] is not None or row["observations"] is None:
                # 墓碑：摘要骨架已核对，明细已清空 → 来源缺失，不能重跑
                archived_count += 1
                issues.append(
                    _issue("batch_archived", seq, "details_present", "archived")
                )
                continue
            # 明细仍在：阈值必须与判定保存值一致；并按保存明细重算规范摘要，
            # 防止明细被改写后连同 digest 一起伪造
            thresholds = json.loads(row["thresholds"])
            if thresholds != saved_thresholds:
                issues.append(
                    _issue(
                        "thresholds_mismatch", seq, saved_thresholds, thresholds
                    )
                )
            recomputed = compute_digest(
                canonical_payload_of(
                    device_id,
                    row["batch_id"],
                    seq,
                    json.loads(row["observations"]),
                    thresholds,
                )
            )
            if recomputed != row["digest"]:
                issues.append(
                    _issue(
                        "digest_recompute_mismatch", seq, recomputed, row["digest"]
                    )
                )

        # ---- 前序链：范围内相邻批次链接 + 区间首批的链锚（from_sequence-1）----
        ordered = [sb["sequence"] for sb in saved_batches]
        for prev_seq, cur_seq in zip(ordered, ordered[1:]):
            prev_row, cur_row = current.get(prev_seq), current.get(cur_seq)
            if (
                prev_row is not None
                and cur_row is not None
                and cur_row["previous_digest"] != prev_row["digest"]
            ):
                issues.append(
                    _issue(
                        "previous_chain_broken",
                        cur_seq,
                        prev_row["digest"],
                        cur_row["previous_digest"],
                    )
                )
        if ordered and ordered[0] > 1 and ordered[0] in current:
            anchor = conn.execute(
                "SELECT digest FROM device_batches "
                "WHERE device_id = ? AND sequence = ?",
                (device_id, ordered[0] - 1),
            ).fetchone()
            if anchor is None:
                issues.append(
                    _issue(
                        "previous_chain_anchor_missing",
                        ordered[0] - 1,
                        "present",
                        "missing",
                    )
                )
            elif anchor["digest"] != current[ordered[0]]["previous_digest"]:
                issues.append(
                    _issue(
                        "previous_chain_broken",
                        ordered[0],
                        anchor["digest"],
                        current[ordered[0]]["previous_digest"],
                    )
                )

        # ---- 保存版本的特征库必须仍在（版本不可变、从不删除；防御性核对）----
        if library_version is not None:
            version_row = conn.execute(
                "SELECT 1 AS x FROM library_versions WHERE version_id = ?",
                (library_version,),
            ).fetchone()
            if version_row is None:
                issues.append(
                    _issue(
                        "library_version_missing", None, "present", "missing",
                        library_version=library_version,
                    )
                )

        saved_evidence = (
            self._review_evidence(saved_response, judgment_id=bj["judgment_id"])
            if saved_response is not None
            else None
        )
        differences: list[dict] = []
        current_evidence = None
        mismatch = any(
            i["issue_code"] in self._REVIEW_MISMATCH_CODES for i in issues
        )
        unavailable = not mismatch and bool(issues)
        if mismatch:
            status = "source_mismatch"
        elif unavailable:
            status = "source_unavailable"
        else:
            # 所有明细可用且摘要、前序链、阈值一致：用保存版本的特征库重跑
            aids = (
                self._aids_of(conn, library_version)
                if library_version is not None
                else []
            )
            merged = merge_batch_observations(
                [
                    {
                        "sequence": seq,
                        "observations": json.loads(current[seq]["observations"]),
                    }
                    for seq in ordered
                ]
            )
            if merged != saved_observations:
                # 防御：摘要全部相符时合并观测必然等于保存值；不等说明保存的
                # 判定来源本身被改写，按摘要不符处理（不重跑）
                first_diff = next(
                    (
                        i
                        for i, (a, b) in enumerate(zip(saved_observations, merged))
                        if a != b
                    ),
                    None,
                )
                extra = {}
                if first_diff is not None:
                    extra = {
                        "first_difference_index": first_diff,
                        "saved_observation": saved_observations[first_diff],
                        "current_observation": merged[first_diff],
                    }
                issues.append(
                    _issue(
                        "observations_mismatch",
                        None,
                        {"observation_count": len(saved_observations)},
                        {"observation_count": len(merged)},
                        **extra,
                    )
                )
                status = "source_mismatch"
            else:
                current_response = runner(
                    [o["captured_at"] for o in merged],
                    saved_thresholds,
                    library_version,
                    aids,
                )
                differences = self._review_differences(
                    saved_response, current_response
                )
                current_evidence = self._review_evidence(
                    current_response, judgment_id=None
                )
                status = "changed" if differences else "unchanged"

        return {
            "status": status,
            "source_judgment_id": bj["judgment_id"],
            "from_sequence": bj["from_sequence"],
            "to_sequence": bj["to_sequence"],
            "library_version": library_version,
            "checked_batches": len(saved_batches),
            "archived_batches": archived_count,
            "issue_count": len(issues),
            "issues": issues,
            "differences": differences,
            "saved": saved_evidence,
            "current": current_evidence,
        }

    @staticmethod
    def _review_evidence(response: dict, *, judgment_id) -> dict:
        """一侧裁决证据：状态、原因、最佳航标编号与完整最佳候选（同 /judge best）。"""
        best = response.get("best")
        evidence = {
            "status": response.get("status"),
            "reason": response.get("reason"),
            "best_aid_id": best.get("aid_id") if best is not None else None,
            "best": best,
        }
        if judgment_id is not None:
            evidence["judgment_id"] = judgment_id
        return evidence

    @staticmethod
    def _review_differences(saved_response: dict, current_response: dict) -> list[dict]:
        """逐字段比较保存的判定响应与重跑响应，返回差异字段清单（空 = 未变）。"""
        diffs: list[dict] = []

        def cmp(field, old, new):
            if old != new:
                diffs.append({"field": field, "saved": old, "current": new})

        cmp("status", saved_response.get("status"), current_response.get("status"))
        cmp("reason", saved_response.get("reason"), current_response.get("reason"))
        saved_best = saved_response.get("best") or {}
        current_best = current_response.get("best") or {}
        for key in ("aid_id", "matched", "missed", "false", "offset", "drift"):
            cmp(f"best.{key}", saved_best.get(key), current_best.get(key))
        return diffs

    @staticmethod
    def _review_report_view(row) -> dict:
        body = json.loads(row["report"])
        return {
            "report_id": row["report_id"],
            "idempotency_key": row["idempotency_key"],
            "device_id": row["device_id"],
            "batch_judgment_id": row["batch_judgment_id"],
            "created_at": _iso(row["created_at"]),
            **body,
        }

    def _get_review_report(self, conn, report_id: int) -> dict | None:
        row = conn.execute(
            "SELECT * FROM batch_judgment_review_reports WHERE report_id = ?",
            (report_id,),
        ).fetchone()
        return self._review_report_view(row) if row is not None else None

    def get_batch_judgment_review_report(
        self, device_id: str, batch_judgment_id: int, report_id: int
    ) -> dict | None:
        """查询单份复核报告；不属于该设备/判定或不存在返回 None（404）。"""
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM batch_judgment_review_reports WHERE report_id = ?",
                (report_id,),
            ).fetchone()
            if (
                row is None
                or row["device_id"] != device_id
                or row["batch_judgment_id"] != batch_judgment_id
            ):
                return None
            return self._review_report_view(row)

    def list_batch_judgment_review_reports(
        self, device_id: str, batch_judgment_id: int, *, after_id: int = 0, limit: int = 50
    ) -> dict | None:
        """按 report_id 游标分页列出某次判定的复核报告（升序，仅摘要）。

        判定不存在或不属于该设备返回 None（404）。
        """
        with closing(self._connect()) as conn:
            owner = conn.execute(
                "SELECT device_id FROM batch_judgments WHERE id = ?",
                (batch_judgment_id,),
            ).fetchone()
            if owner is None or owner["device_id"] != device_id:
                return None
            rows = conn.execute(
                "SELECT * FROM batch_judgment_review_reports "
                "WHERE device_id = ? AND batch_judgment_id = ? AND report_id > ? "
                "ORDER BY report_id LIMIT ?",
                (device_id, batch_judgment_id, after_id, limit + 1),
            ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = []
        for r in rows:
            body = json.loads(r["report"])
            items.append(
                {
                    "report_id": r["report_id"],
                    "idempotency_key": r["idempotency_key"],
                    "device_id": r["device_id"],
                    "batch_judgment_id": r["batch_judgment_id"],
                    "status": body["status"],
                    "source_judgment_id": body["source_judgment_id"],
                    "library_version": body["library_version"],
                    "checked_batches": body["checked_batches"],
                    "archived_batches": body["archived_batches"],
                    "issue_count": body["issue_count"],
                    "difference_count": len(body["differences"]),
                    "created_at": _iso(r["created_at"]),
                }
            )
        return {
            "device_id": device_id,
            "batch_judgment_id": batch_judgment_id,
            "count": len(items),
            "review_reports": items,
            "next_cursor": items[-1]["report_id"] if has_more and items else None,
        }

    # ---------------- 设备链一致性核验（只读快照，不改数据） ----------------

    @staticmethod
    def _verify_issue(issue_code: str, sequence, expected, actual, **extra) -> dict:
        """组装一条核验问题：issue_code / sequence / expected / actual + 附加上下文。"""
        item = {
            "issue_code": issue_code,
            "sequence": sequence,
            "expected": expected,
            "actual": actual,
        }
        item.update(extra)
        return item

    def verify_chain(
        self, device_id: str, from_sequence: int, to_sequence: int
    ) -> dict:
        """在**单个只读 SQLite 事务**（固定快照）内核验设备批次链一致性。

        并发上传、确认或归档期间，本次核验的全部读取都来自事务开始时固定的
        同一快照；``PRAGMA query_only`` 兜底保证核验绝不修改任何数据（发现
        问题也只报告、不修复）。设备无任何批次抛 ``device_not_found``；请求
        范围内缺批次（含右端越过链头）抛 ``batches_not_found``——均为
        :class:`ChainVerificationNotFound`（HTTP 404）。
        """
        with closing(self._connect()) as conn:
            conn.execute("PRAGMA query_only = ON")  # 只读兜底：核验绝不写数据
            try:
                conn.execute("BEGIN")  # 只读事务：以下全部读取固定于同一快照
                result = self._verify_chain(
                    conn, device_id, from_sequence, to_sequence
                )
            finally:
                conn.rollback()  # 只读事务，释放读锁
        return result

    def _verify_chain(
        self, conn, device_id: str, from_sequence: int, to_sequence: int
    ) -> dict:
        """在给定只读事务连接内执行核验（快照由调用方的事务保证）。

        结构核对覆盖**整条链**（代价低、只读骨架）：序号连续性、
        ``previous_digest`` 链、链游标（``batch_count`` / ``last_sequence`` /
        ``head_digest``）、``archived_through`` 墓碑边界与墓碑骨架、全部
        消费者确认摘要与水位、全部连续批次判定来源摘要。规范 SHA-256 重算
        只针对**请求范围内**的未归档批次（归档批次明细已清空，只核对保存的
        摘要经链式引用互相印证及墓碑边界）。
        """
        _issue = self._verify_issue
        issues: list[dict] = []

        skeletons = conn.execute(
            "SELECT sequence, previous_digest, digest, status, archived_at, "
            "(observations IS NULL) AS observations_cleared, "
            "(thresholds IS NULL) AS thresholds_cleared "
            "FROM device_batches WHERE device_id = ? ORDER BY sequence",
            (device_id,),
        ).fetchall()
        if not skeletons:
            raise ChainVerificationNotFound(
                error="device_not_found",
                message=f"device {device_id!r} has no batches; nothing to verify",
                device_id=device_id,
            )
        by_seq = {r["sequence"]: r for r in skeletons}
        head_sequence = skeletons[-1]["sequence"]
        batch_total = len(skeletons)

        # 请求范围必须完整落在已存在的批次上：范围内缺批次（含右端越过链头）
        # → 404，missing_sequences 给出缺失序号示例（至多 50 条）
        in_range = [
            r["sequence"]
            for r in skeletons
            if from_sequence <= r["sequence"] <= to_sequence
        ]
        missing_count = (to_sequence - from_sequence + 1) - len(in_range)
        if missing_count > 0:
            present = set(in_range)
            gaps: list[int] = []
            for s in range(from_sequence, min(to_sequence, head_sequence) + 1):
                if s not in present:
                    gaps.append(s)
                    if len(gaps) >= 50:
                        break
            if len(gaps) < 50 and to_sequence > head_sequence:
                # 右端越过链头：链头之上的序号全部缺失
                for s in range(max(head_sequence + 1, from_sequence), to_sequence + 1):
                    gaps.append(s)
                    if len(gaps) >= 50:
                        break
            raise ChainVerificationNotFound(
                error="batches_not_found",
                message=(
                    f"cannot verify range {from_sequence}..{to_sequence} of device "
                    f"{device_id!r}: {missing_count} batch sequence(s) missing"
                ),
                device_id=device_id,
                from_sequence=from_sequence,
                to_sequence=to_sequence,
                head_sequence=head_sequence,
                missing_sequences=gaps,
                missing_count=missing_count,
            )

        # ---- 序号连续性：链上必须是 1..head_sequence 无缺口 ----
        prev = 0
        for r in skeletons:
            for gap in range(prev + 1, r["sequence"]):
                issues.append(_issue("sequence_gap", gap, "present", "missing"))
            prev = r["sequence"]

        # ---- previous_digest 链：每批保存的前序摘要必须等于上一批保存的摘要 ----
        prev_digest = ""
        for r in skeletons:
            if r["previous_digest"] != prev_digest:
                issues.append(
                    _issue(
                        "previous_digest_mismatch",
                        r["sequence"],
                        prev_digest,
                        r["previous_digest"],
                    )
                )
            prev_digest = r["digest"]

        # ---- 链游标：batch_count / last_sequence / head_digest / archived_through ----
        chain = conn.execute(
            "SELECT * FROM device_chains WHERE device_id = ?", (device_id,)
        ).fetchone()
        if chain is None:
            issues.append(_issue("chain_cursor_missing", None, "present", "missing"))
            archived_through = 0
        else:
            if chain["last_sequence"] != head_sequence:
                issues.append(
                    _issue(
                        "last_sequence_mismatch",
                        None,
                        head_sequence,
                        chain["last_sequence"],
                    )
                )
            if chain["batch_count"] != batch_total:
                issues.append(
                    _issue(
                        "batch_count_mismatch", None, batch_total, chain["batch_count"]
                    )
                )
            head_row = by_seq.get(chain["last_sequence"])
            if head_row is None:
                issues.append(
                    _issue(
                        "head_batch_missing",
                        chain["last_sequence"],
                        "present",
                        "missing",
                    )
                )
            elif chain["head_digest"] != head_row["digest"]:
                issues.append(
                    _issue(
                        "head_digest_mismatch",
                        chain["last_sequence"],
                        head_row["digest"],
                        chain["head_digest"],
                    )
                )
            archived_through = chain["archived_through"]
            if archived_through > head_sequence:
                issues.append(
                    _issue(
                        "archived_through_beyond_head",
                        None,
                        head_sequence,
                        archived_through,
                    )
                )

        # ---- archived_through 墓碑边界与墓碑骨架（全链）----
        for r in skeletons:
            is_tombstone = r["archived_at"] is not None
            should_be_tombstone = r["sequence"] <= archived_through
            if is_tombstone != should_be_tombstone:
                issues.append(
                    _issue(
                        "tombstone_boundary_violation",
                        r["sequence"],
                        "archived" if should_be_tombstone else "accepted",
                        "archived" if is_tombstone else "accepted",
                    )
                )
            if (r["status"] == "archived") != is_tombstone:
                issues.append(
                    _issue(
                        "tombstone_status_mismatch",
                        r["sequence"],
                        "archived" if is_tombstone else "accepted",
                        r["status"],
                    )
                )
            if is_tombstone and not (
                r["observations_cleared"] and r["thresholds_cleared"]
            ):
                issues.append(
                    _issue(
                        "tombstone_details_present",
                        r["sequence"],
                        "cleared",
                        "present",
                    )
                )

        # ---- 范围内未归档批次：重算规范 SHA-256 并核对 observation_count ----
        range_rows = conn.execute(
            "SELECT sequence, batch_id, digest, observation_count, thresholds, "
            "observations, archived_at FROM device_batches "
            "WHERE device_id = ? AND sequence BETWEEN ? AND ? ORDER BY sequence",
            (device_id, from_sequence, to_sequence),
        ).fetchall()
        recomputed_batches = 0
        archived_batches = 0
        for r in range_rows:
            if r["archived_at"] is not None:
                # 归档批次：明细已清空，不重算；保存的摘要由链式引用与墓碑边界覆盖
                archived_batches += 1
                continue
            if r["thresholds"] is None or r["observations"] is None:
                # 非墓碑却缺明细：无法重算，按明细缺失报告
                issues.append(
                    _issue(
                        "batch_details_missing", r["sequence"], "present", "missing"
                    )
                )
                continue
            thresholds = json.loads(r["thresholds"])
            observations = json.loads(r["observations"])
            digest = compute_digest(
                canonical_payload_of(
                    device_id,
                    r["batch_id"],
                    r["sequence"],
                    observations,
                    thresholds,
                )
            )
            recomputed_batches += 1
            if digest != r["digest"]:
                issues.append(
                    _issue("digest_mismatch", r["sequence"], digest, r["digest"])
                )
            if r["observation_count"] != len(observations):
                issues.append(
                    _issue(
                        "observation_count_mismatch",
                        r["sequence"],
                        len(observations),
                        r["observation_count"],
                    )
                )

        # ---- 消费者确认摘要与连续水位 ----
        consumers = conn.execute(
            "SELECT consumer_id, watermark FROM device_consumers "
            "WHERE device_id = ? ORDER BY id",
            (device_id,),
        ).fetchall()
        ack_rows = conn.execute(
            "SELECT a.consumer_id AS consumer_id, a.sequence AS sequence, "
            "a.batch_digest AS batch_digest, b.digest AS current_digest "
            "FROM device_batch_acks a "
            "LEFT JOIN device_batches b "
            "ON b.device_id = a.device_id AND b.sequence = a.sequence "
            "WHERE a.device_id = ? ORDER BY a.consumer_id, a.sequence",
            (device_id,),
        ).fetchall()
        acked_by_consumer: dict[str, set[int]] = {}
        for a in ack_rows:
            acked_by_consumer.setdefault(a["consumer_id"], set()).add(a["sequence"])
            ctx = {"consumer_id": a["consumer_id"]}
            if a["current_digest"] is None:
                issues.append(
                    _issue(
                        "ack_batch_missing", a["sequence"], "present", "missing", **ctx
                    )
                )
            elif a["batch_digest"] != a["current_digest"]:
                issues.append(
                    _issue(
                        "ack_digest_mismatch",
                        a["sequence"],
                        a["current_digest"],
                        a["batch_digest"],
                        **ctx,
                    )
                )
        for c in consumers:
            cid = c["consumer_id"]
            watermark = c["watermark"]
            if watermark > head_sequence:
                issues.append(
                    _issue(
                        "watermark_beyond_head",
                        None,
                        head_sequence,
                        watermark,
                        consumer_id=cid,
                    )
                )
            if watermark < archived_through:
                issues.append(
                    _issue(
                        "watermark_below_archived_through",
                        None,
                        archived_through,
                        watermark,
                        consumer_id=cid,
                    )
                )
            # 水位必须已按当前稀疏确认推进到首个缺口（从存储水位起连续重算）
            acked = acked_by_consumer.get(cid, set())
            expected_watermark = watermark
            while expected_watermark + 1 in acked:
                expected_watermark += 1
            if expected_watermark != watermark:
                issues.append(
                    _issue(
                        "watermark_not_advanced",
                        watermark + 1,
                        expected_watermark,
                        watermark,
                        consumer_id=cid,
                    )
                )

        # ---- 连续批次判定来源：保存的各批骨架必须与当前批次一致 ----
        source_rows = conn.execute(
            "SELECT jb.batch_judgment_id AS judgment_id, jb.sequence AS sequence, "
            "jb.batch_id AS batch_id, jb.previous_digest AS previous_digest, "
            "jb.digest AS digest, jb.observation_count AS observation_count, "
            "b.batch_id AS cur_batch_id, b.previous_digest AS cur_previous_digest, "
            "b.digest AS cur_digest, b.observation_count AS cur_observation_count "
            "FROM batch_judgment_batches jb "
            "JOIN batch_judgments j ON j.id = jb.batch_judgment_id "
            "LEFT JOIN device_batches b "
            "ON b.device_id = j.device_id AND b.sequence = jb.sequence "
            "WHERE j.device_id = ? ORDER BY jb.batch_judgment_id, jb.sequence",
            (device_id,),
        ).fetchall()
        for s in source_rows:
            ctx = {"batch_judgment_id": s["judgment_id"]}
            if s["cur_digest"] is None:
                issues.append(
                    _issue(
                        "judgment_source_batch_missing",
                        s["sequence"],
                        "present",
                        "missing",
                        **ctx,
                    )
                )
                continue
            if s["digest"] != s["cur_digest"]:
                issues.append(
                    _issue(
                        "judgment_source_digest_mismatch",
                        s["sequence"],
                        s["cur_digest"],
                        s["digest"],
                        **ctx,
                    )
                )
            if s["previous_digest"] != s["cur_previous_digest"]:
                issues.append(
                    _issue(
                        "judgment_source_previous_digest_mismatch",
                        s["sequence"],
                        s["cur_previous_digest"],
                        s["previous_digest"],
                        **ctx,
                    )
                )
            if s["batch_id"] != s["cur_batch_id"]:
                issues.append(
                    _issue(
                        "judgment_source_batch_id_mismatch",
                        s["sequence"],
                        s["cur_batch_id"],
                        s["batch_id"],
                        **ctx,
                    )
                )
            if s["observation_count"] != s["cur_observation_count"]:
                issues.append(
                    _issue(
                        "judgment_source_observation_count_mismatch",
                        s["sequence"],
                        s["cur_observation_count"],
                        s["observation_count"],
                        **ctx,
                    )
                )

        judgment_count = conn.execute(
            "SELECT COUNT(*) AS c FROM batch_judgments WHERE device_id = ?",
            (device_id,),
        ).fetchone()["c"]
        # 快照上限：核验时刻从批次表推导的链上界（游标值若不一致已在问题清单中）
        snapshot = {
            "head_sequence": head_sequence,
            "head_digest": by_seq[head_sequence]["digest"],
            "batch_count": batch_total,
            "archived_through": archived_through,
            "safe_sequence": min((c["watermark"] for c in consumers), default=0),
            "consumer_count": len(consumers),
            "batch_judgment_count": judgment_count,
        }
        return {
            "device_id": device_id,
            "from_sequence": from_sequence,
            "to_sequence": to_sequence,
            "ok": not issues,
            "issue_count": len(issues),
            "issues": issues,
            "checked_batches": len(range_rows),
            "recomputed_batches": recomputed_batches,
            "archived_batches": archived_batches,
            "checked_acknowledgments": len(ack_rows),
            "checked_judgment_sources": len(source_rows),
            "snapshot": snapshot,
        }

    # ---------------- 已登记消费者：批次处理租约 ----------------

    @staticmethod
    def _lease_state(row, now: float) -> str:
        """租约对外状态：``completed`` / ``revoked`` 直接取存储状态；
        ``held`` 再按到期时刻细分为 ``held`` 与 ``expired``。"""
        if row["status"] != "held":
            return row["status"]
        return "held" if now < row["expires_at"] else "expired"

    def _lease_payload(self, conn, row, *, include_batches: bool, now: float | None = None) -> dict:
        """组装一条租约：摘要字段 + 可选的逐批明细（含观测明细与阈值）。

        租约占用期间批次明细不会被归档：明细缺失（理论上不应发生）时
        ``observations`` / ``thresholds`` 为 ``None`` 并以 ``archived=true``
        标注。
        """
        now = time.time() if now is None else now
        payload = {
            "lease_id": row["id"],
            "device_id": row["device_id"],
            "consumer_id": row["consumer_id"],
            "idempotency_key": row["idempotency_key"],
            "status": self._lease_state(row, now),
            "from_sequence": row["from_sequence"],
            "to_sequence": row["to_sequence"],
            "batch_count": row["to_sequence"] - row["from_sequence"] + 1,
            "watermark_start": row["watermark_start"],
            "lease_duration_seconds": row["lease_duration"],
            "granted_at": _iso(row["granted_at"]),
            "expires_at": _iso(row["expires_at"]),
            "completed_at": _iso(row["completed_at"]),
            "revoked_at": _iso(row["revoked_at"]),
        }
        if include_batches:
            items = conn.execute(
                "SELECT i.sequence AS sequence, i.batch_digest AS lease_digest, "
                "b.batch_id AS batch_id, b.previous_digest AS previous_digest, "
                "b.digest AS digest, b.observation_count AS observation_count, "
                "b.observations AS observations, b.thresholds AS thresholds, "
                "b.archived_at AS archived_at "
                "FROM batch_lease_items i "
                "LEFT JOIN device_batches b "
                "ON b.device_id = ? AND b.sequence = i.sequence "
                "WHERE i.lease_id = ? ORDER BY i.sequence",
                (row["device_id"], row["id"]),
            ).fetchall()
            payload["batches"] = [
                {
                    "sequence": r["sequence"],
                    "batch_id": r["batch_id"],
                    "previous_digest": r["previous_digest"],
                    "digest": r["digest"],
                    "lease_digest": r["lease_digest"],
                    "observation_count": r["observation_count"],
                    "archived": r["archived_at"] is not None,
                    "observations": (
                        None if r["observations"] is None
                        else json.loads(r["observations"])
                    ),
                    "thresholds": (
                        None if r["thresholds"] is None
                        else json.loads(r["thresholds"])
                    ),
                }
                for r in items
            ]
        return payload

    @staticmethod
    def _held_lease_idempotency_payload(row, payload: dict) -> dict:
        return {
            "status": "held",
            "lease": payload,
        }

    def claim_lease(
        self,
        device_id: str,
        consumer_id: str,
        max_batches: int,
        lease_duration_seconds: int,
        idempotency_key: str,
    ) -> tuple[dict, bool]:
        """为已登记消费者领取批次处理租约（单事务，幂等）。

        在单个 BEGIN IMMEDIATE 事务内：

        1. 消费者必须已在该设备登记，否则 :class:`LeaseConflict`
           （``consumer_not_registered``，409）；
        2. 该消费者已有 ``held`` 租约时：相同幂等键重试返回原租约
           （``created=False``，HTTP 200，租约可能已过期但尚未回收）；其他
           幂等键返回 409——租约未过期为 ``active_lease_exists``，已过期为
           ``lease_expired``（需先显式回收）。同一批区间因此不可能分给同一
           消费者的两个租约（写锁 + held 部分唯一索引兜底）；
        3. 从该消费者连续水位（``watermark``）的下一批起，沿序号取连续存在
           且**未归档**的批次，直到首个缺口、墓碑或 ``max_batches``（≤20）。
           没有可领取的批次时不创建租约，返回
           ``status="nothing_to_claim"``（HTTP 200）；
        4. 保存租约（起止序号、领取时水位、有效期/到期时刻）及每批摘要，
           返回逐批观测明细与阈值。
        """
        now = time.time()
        with self._write_tx() as conn:
            self._assert_not_isolated(conn, device_id)
            consumer = conn.execute(
                "SELECT id, watermark FROM device_consumers "
                "WHERE device_id = ? AND consumer_id = ?",
                (device_id, consumer_id),
            ).fetchone()
            if consumer is None:
                raise LeaseConflict(
                    reason="consumer_not_registered",
                    message=(
                        f"consumer {consumer_id!r} is not registered as a required "
                        f"consumer of device {device_id!r}; register it before "
                        "claiming a lease"
                    ),
                    device_id=device_id,
                    consumer_id=consumer_id,
                )

            held = conn.execute(
                "SELECT * FROM batch_leases "
                "WHERE device_id = ? AND consumer_id = ? AND status = 'held'",
                (device_id, consumer_id),
            ).fetchone()
            if held is not None:
                lease = self._lease_payload(
                    conn, held, include_batches=True, now=now
                )
                if held["idempotency_key"] == idempotency_key:
                    # 同键重试（租约可能已过期但尚未回收）：原租约原样返回
                    return self._held_lease_idempotency_payload(held, lease), False
                expired = now >= held["expires_at"]
                raise LeaseConflict(
                    reason="lease_expired" if expired else "active_lease_exists",
                    message=(
                        f"consumer {consumer_id!r} already holds lease {held['id']} on "
                        f"device {device_id!r}"
                        + (" which has expired; reclaim it before claiming a new one"
                           if expired else
                           "; complete or wait for it to expire before claiming again")
                    ),
                    device_id=device_id,
                    consumer_id=consumer_id,
                    active_lease_id=held["id"],
                    active_lease=lease,
                    expires_at=_iso(held["expires_at"]),
                    expired=expired,
                )

            watermark_start = consumer["watermark"]
            rows = conn.execute(
                "SELECT sequence, digest, archived_at FROM device_batches "
                "WHERE device_id = ? AND sequence > ? "
                "ORDER BY sequence LIMIT ?",
                (device_id, watermark_start, max_batches),
            ).fetchall()
            chosen: list[sqlite3.Row] = []
            expected = watermark_start + 1
            for r in rows:
                if r["sequence"] != expected:
                    break
                # 归档墓碑（明细已清空）不进入租约：水位之后的连续可处理区间
                # 到墓碑前为止。正常流程下安全水位会先挡住归档，但回收/并发
                # 场景下以此处的显式判断为准。
                if r["archived_at"] is not None:
                    break
                chosen.append(r)
                expected += 1

            if not chosen:
                head = conn.execute(
                    "SELECT last_sequence FROM device_chains WHERE device_id = ?",
                    (device_id,),
                ).fetchone()
                return {
                    "status": "nothing_to_claim",
                    "device_id": device_id,
                    "consumer_id": consumer_id,
                    "idempotency_key": idempotency_key,
                    "watermark": watermark_start,
                    "head_sequence": head["last_sequence"] if head is not None else 0,
                    "lease": None,
                }, False

            from_sequence = chosen[0]["sequence"]
            to_sequence = chosen[-1]["sequence"]
            expires_at = now + lease_duration_seconds
            try:
                cur = conn.execute(
                    "INSERT INTO batch_leases "
                    "(device_id, consumer_id, idempotency_key, status, from_sequence, "
                    "to_sequence, watermark_start, lease_duration, granted_at, expires_at) "
                    "VALUES (?, ?, ?, 'held', ?, ?, ?, ?, ?, ?)",
                    (
                        device_id,
                        consumer_id,
                        idempotency_key,
                        from_sequence,
                        to_sequence,
                        watermark_start,
                        lease_duration_seconds,
                        now,
                        expires_at,
                    ),
                )
            except sqlite3.IntegrityError:
                # 多 worker 下越过进程写锁的并发兜底：重读在持租约，
                # 按「同键幂等 / 他键 409」同一语义处理
                rival = conn.execute(
                    "SELECT * FROM batch_leases "
                    "WHERE device_id = ? AND consumer_id = ? AND status = 'held'",
                    (device_id, consumer_id),
                ).fetchone()
                if rival is not None and rival["idempotency_key"] == idempotency_key:
                    lease = self._lease_payload(
                        conn, rival, include_batches=True, now=now
                    )
                    return self._held_lease_idempotency_payload(rival, lease), False
                expired = rival is not None and now >= rival["expires_at"]
                raise LeaseConflict(
                    reason="lease_expired" if expired else "active_lease_exists",
                    message=(
                        f"consumer {consumer_id!r} already holds a lease on device "
                        f"{device_id!r}; concurrent claims are serialized"
                    ),
                    device_id=device_id,
                    consumer_id=consumer_id,
                    active_lease_id=int(rival["id"]) if rival is not None else None,
                    expires_at=_iso(rival["expires_at"]) if rival is not None else None,
                    expired=expired,
                )
            lease_id = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO batch_lease_items (lease_id, sequence, batch_digest) "
                "VALUES (?, ?, ?)",
                [(lease_id, r["sequence"], r["digest"]) for r in chosen],
            )
            row = conn.execute(
                "SELECT * FROM batch_leases WHERE id = ?", (lease_id,)
            ).fetchone()
            payload = self._lease_payload(conn, row, include_batches=True, now=now)
        return {"status": "leased", "lease": payload}, True

    @staticmethod
    def _advance_consumer_watermark(conn, device_id: str, consumer_row) -> int:
        """在写事务内按最新稀疏确认把消费者水位推进至首个缺口。"""
        watermark = consumer_row["watermark"]
        while True:
            nxt = conn.execute(
                "SELECT 1 FROM device_batch_acks "
                "WHERE device_id = ? AND consumer_id = ? AND sequence = ?",
                (device_id, consumer_row["consumer_id"], watermark + 1),
            ).fetchone()
            if nxt is None:
                break
            watermark += 1
        if watermark != consumer_row["watermark"]:
            conn.execute(
                "UPDATE device_consumers SET watermark = ? WHERE id = ?",
                (watermark, consumer_row["id"]),
            )
        return watermark

    def complete_lease(
        self, device_id: str, lease_id: int, consumer_id: str, items: list
    ) -> dict:
        """完成租约：逐项核对摘要 → 写确认、推进水位、释放租约（单事务）。

        整体拒绝（不写入任何确认、不推进水位、租约保持 ``held``）的情形：

        - 租约不存在或不属于该设备/消费者：:class:`LeaseNotFound`（404）；
        - 租约已完成（``lease_already_completed``）或已回收
          （``lease_reclaimed``），或消费者不符（``lease_consumer_mismatch``）：
          409；
        - 租约已过期（``lease_expired``）：409，需显式回收后重新领取；
        - 提交项的序号集合/顺序与租约批次不一致（``items_mismatch``，附期望
          与缺失/多余序号）、某批当前摘要与租约保存摘要不一致
          （``digest_changed``，附序列、租约摘要与当前摘要）：409。

        全部通过后：每批确认以 ``INSERT OR IGNORE`` 幂等写入（消费者可能已
        经手工确认过其中批次），再把连续水位推进到首个缺口，租约置
        ``completed``。返回租约详情与同快照的归档水位字段。
        """
        now = time.time()
        provided = [(item.sequence, item.batch_digest) for item in items]
        with self._write_tx() as conn:
            self._assert_not_isolated(conn, device_id)
            row = conn.execute(
                "SELECT * FROM batch_leases WHERE id = ?", (lease_id,)
            ).fetchone()
            if row is None or row["device_id"] != device_id:
                raise LeaseNotFound(
                    message=(
                        f"lease {lease_id} of device {device_id!r} not found"
                    ),
                    device_id=device_id,
                    lease_id=lease_id,
                )
            if row["consumer_id"] != consumer_id:
                raise LeaseConflict(
                    reason="lease_consumer_mismatch",
                    message=(
                        f"lease {lease_id} belongs to consumer {row['consumer_id']!r}, "
                        f"not {consumer_id!r}"
                    ),
                    device_id=device_id,
                    lease_id=lease_id,
                    consumer_id=consumer_id,
                    owner_consumer_id=row["consumer_id"],
                )
            if row["status"] == "completed":
                raise LeaseConflict(
                    reason="lease_already_completed",
                    message=f"lease {lease_id} is already completed",
                    device_id=device_id,
                    lease_id=lease_id,
                    completed_at=_iso(row["completed_at"]),
                )
            if row["status"] == "revoked":
                raise LeaseConflict(
                    reason="lease_reclaimed",
                    message=(
                        f"lease {lease_id} has been reclaimed after expiry; claim a "
                        "new lease for the batches"
                    ),
                    device_id=device_id,
                    lease_id=lease_id,
                    revoked_at=_iso(row["revoked_at"]),
                )
            if now >= row["expires_at"]:
                raise LeaseConflict(
                    reason="lease_expired",
                    message=(
                        f"lease {lease_id} expired at {_iso(row['expires_at'])}; "
                        "reclaim it explicitly before the batches can be leased again"
                    ),
                    device_id=device_id,
                    lease_id=lease_id,
                    expires_at=_iso(row["expires_at"]),
                )

            saved = conn.execute(
                "SELECT sequence, batch_digest FROM batch_lease_items "
                "WHERE lease_id = ? ORDER BY sequence",
                (lease_id,),
            ).fetchall()
            saved_seqs = [r["sequence"] for r in saved]
            provided_seqs = [s for s, _ in provided]
            if provided_seqs != saved_seqs:
                saved_set, provided_set = set(saved_seqs), set(provided_seqs)
                raise LeaseConflict(
                    reason="items_mismatch",
                    message=(
                        "completion items must cover exactly the leased batches in "
                        f"order: expected {saved_seqs}, got {provided_seqs}"
                    ),
                    device_id=device_id,
                    lease_id=lease_id,
                    expected_sequences=saved_seqs,
                    missing_sequences=sorted(saved_set - provided_set),
                    unexpected_sequences=sorted(provided_set - saved_set),
                )

            saved_digest = {r["sequence"]: r["batch_digest"] for r in saved}
            for sequence, digest in provided:
                lease_digest = saved_digest[sequence]
                batch = conn.execute(
                    "SELECT digest FROM device_batches "
                    "WHERE device_id = ? AND sequence = ?",
                    (device_id, sequence),
                ).fetchone()
                current_digest = batch["digest"] if batch is not None else None
                # 逐项核对：批次必须仍存在，且提交摘要、租约冻结摘要、批次当前
                # 摘要三者一致。任一不符整体拒绝，不写确认、不推进水位。
                if current_digest is None:
                    raise LeaseConflict(
                        reason="batch_missing",
                        message=(
                            f"batch {sequence} leased by lease {lease_id} no longer "
                            "exists; completion rejected wholesale without advancing "
                            "the watermark"
                        ),
                        device_id=device_id,
                        lease_id=lease_id,
                        sequence=sequence,
                        provided_digest=digest,
                        lease_digest=lease_digest,
                    )
                if digest != lease_digest or current_digest != lease_digest:
                    raise LeaseConflict(
                        reason="digest_changed",
                        message=(
                            f"batch {sequence} digest does not match the digest frozen "
                            "by the lease; completion rejected wholesale without "
                            "advancing the watermark"
                        ),
                        device_id=device_id,
                        lease_id=lease_id,
                        sequence=sequence,
                        provided_digest=digest,
                        lease_digest=lease_digest,
                        current_digest=current_digest,
                    )

            # 逐项核对通过：幂等写入每批确认（INSERT OR IGNORE 兼容手工确认），
            # 推进该消费者连续水位，再释放租约——全部在同一事务，任一失败整体回滚
            consumer = conn.execute(
                "SELECT id, consumer_id, watermark FROM device_consumers "
                "WHERE device_id = ? AND consumer_id = ?",
                (device_id, consumer_id),
            ).fetchone()
            for sequence, digest in provided:
                conn.execute(
                    "INSERT OR IGNORE INTO device_batch_acks "
                    "(device_id, consumer_id, sequence, batch_digest, confirmed_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (device_id, consumer_id, sequence, digest, now),
                )
            watermark = self._advance_consumer_watermark(conn, device_id, consumer)
            conn.execute(
                "UPDATE batch_leases SET status = 'completed', completed_at = ? "
                "WHERE id = ?",
                (now, lease_id),
            )
            row = conn.execute(
                "SELECT * FROM batch_leases WHERE id = ?", (lease_id,)
            ).fetchone()
            lease = self._lease_payload(conn, row, include_batches=True, now=now)
            body = {
                "status": "completed",
                "lease": lease,
                "watermark": watermark,
                "confirmed_count": len(provided),
                "completed_at": _iso(now),
            }
            body.update(self._archive_status_fields(conn, device_id))
            return body

    def recover_lease(
        self, device_id: str, lease_id: int, consumer_id: str
    ) -> tuple[dict, bool]:
        """显式回收过期租约（``held → revoked``），回收后区间可重新领取。

        - 租约不存在或不属于该设备：:class:`LeaseNotFound`（404）；
        - 消费者不符（``lease_consumer_mismatch``）、租约已完成
          （``lease_already_completed``）：409；
        - 租约尚未过期（``lease_not_expired``）：409，附到期时刻；
        - 已经回收（幂等）：HTTP 200、``status=already_reclaimed``，不刷新
          回收时刻。
        """
        now = time.time()
        with self._write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM batch_leases WHERE id = ?", (lease_id,)
            ).fetchone()
            if row is None or row["device_id"] != device_id:
                raise LeaseNotFound(
                    message=f"lease {lease_id} of device {device_id!r} not found",
                    device_id=device_id,
                    lease_id=lease_id,
                )
            if row["consumer_id"] != consumer_id:
                raise LeaseConflict(
                    reason="lease_consumer_mismatch",
                    message=(
                        f"lease {lease_id} belongs to consumer {row['consumer_id']!r}, "
                        f"not {consumer_id!r}"
                    ),
                    device_id=device_id,
                    lease_id=lease_id,
                    consumer_id=consumer_id,
                    owner_consumer_id=row["consumer_id"],
                )
            if row["status"] == "completed":
                raise LeaseConflict(
                    reason="lease_already_completed",
                    message=f"lease {lease_id} is already completed",
                    device_id=device_id,
                    lease_id=lease_id,
                    completed_at=_iso(row["completed_at"]),
                )
            if row["status"] == "revoked":
                lease = self._lease_payload(conn, row, include_batches=False, now=now)
                return {
                    "status": "already_reclaimed",
                    "lease": lease,
                    "reclaimed_at": _iso(row["revoked_at"]),
                }, False
            if now < row["expires_at"]:
                raise LeaseConflict(
                    reason="lease_not_expired",
                    message=(
                        f"lease {lease_id} is still held until "
                        f"{_iso(row['expires_at'])}; wait for expiry or complete it"
                    ),
                    device_id=device_id,
                    lease_id=lease_id,
                    expires_at=_iso(row["expires_at"]),
                )
            conn.execute(
                "UPDATE batch_leases SET status = 'revoked', revoked_at = ? WHERE id = ?",
                (now, lease_id),
            )
            row = conn.execute(
                "SELECT * FROM batch_leases WHERE id = ?", (lease_id,)
            ).fetchone()
            lease = self._lease_payload(conn, row, include_batches=False, now=now)
            return {
                "status": "reclaimed",
                "lease": lease,
                "reclaimed_at": _iso(now),
            }, True

    def get_lease(self, device_id: str, lease_id: int) -> dict | None:
        """查询单条租约详情（逐批摘要与观测明细）；不属于该设备或不存在返回 None。"""
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM batch_leases WHERE id = ?", (lease_id,)
            ).fetchone()
            if row is None or row["device_id"] != device_id:
                return None
            return self._lease_payload(conn, row, include_batches=True)

    def list_leases(
        self,
        device_id: str,
        *,
        after_id: int = 0,
        limit: int = 50,
        consumer_id: str | None = None,
        status: str | None = None,
    ) -> dict:
        """按 id 游标分页列出设备的批次处理租约（升序，仅摘要，不含逐批明细）。

        ``status`` 可取 ``held`` / ``expired`` / ``completed`` / ``revoked``：
        其中 ``expired`` 匹配存储状态为 ``held`` 但已过到期时刻的租约，
        ``held`` 只匹配仍在有效期的在持租约。
        """
        sql = "SELECT * FROM batch_leases WHERE device_id = ? AND id > ?"
        params: list = [device_id, after_id]
        if consumer_id is not None:
            sql += " AND consumer_id = ?"
            params.append(consumer_id)
        held_filter = status in ("held", "expired")
        if held_filter:
            # held/expired 都是存储状态 'held'：由到期时刻在内存中区分。
            # 每个消费者至多一条 held，该集合天然很小，取出后内存过滤
            sql += " AND status = 'held' ORDER BY id"
        elif status in ("completed", "revoked"):
            sql += " AND status = ? ORDER BY id LIMIT ?"
            params.extend([status, limit + 1])
        else:
            sql += " ORDER BY id LIMIT ?"
            params.append(limit + 1)
        with closing(self._connect()) as conn:
            rows = conn.execute(sql, params).fetchall()
        now = time.time()
        if held_filter:
            leases = [
                self._lease_summary(r, now)
                for r in rows
                if self._lease_state(r, now) == status
            ]
            # held 集合无 SQL LIMIT：游标 after_id 已保证单调，取一页即可
            has_more = len(leases) > limit
            leases = leases[:limit]
            next_cursor = leases[-1]["lease_id"] if has_more else None
        else:
            has_more = len(rows) > limit
            rows = rows[:limit]
            leases = [self._lease_summary(r, now) for r in rows]
            next_cursor = rows[-1]["id"] if has_more and rows else None
        return {
            "device_id": device_id,
            "count": len(leases),
            "leases": leases,
            "next_cursor": next_cursor,
        }

    @staticmethod
    def _lease_summary(row, now: float) -> dict:
        return {
            "lease_id": row["id"],
            "device_id": row["device_id"],
            "consumer_id": row["consumer_id"],
            "idempotency_key": row["idempotency_key"],
            "status": Database._lease_state(row, now),
            "from_sequence": row["from_sequence"],
            "to_sequence": row["to_sequence"],
            "batch_count": row["to_sequence"] - row["from_sequence"] + 1,
            "watermark_start": row["watermark_start"],
            "lease_duration_seconds": row["lease_duration"],
            "granted_at": _iso(row["granted_at"]),
            "expires_at": _iso(row["expires_at"]),
            "completed_at": _iso(row["completed_at"]),
            "revoked_at": _iso(row["revoked_at"]),
        }

    # ---------------- 设备异常隔离（应急闸门，不可变事件） ----------------

    @staticmethod
    def _isolation_event_view(row) -> dict:
        """组装一条隔离/解除事件（不可变；解除事件附核验范围与核验摘要）。"""
        return {
            "event_id": row["id"],
            "device_id": row["device_id"],
            "action": row["action"],
            "reason": row["reason"],
            "idempotency_key": row["idempotency_key"],
            "head_sequence": row["head_sequence"],
            "head_digest": row["head_digest"],
            "verify_from": row["verify_from"],
            "verify_to": row["verify_to"],
            "verification": (
                json.loads(row["verification"]) if row["verification"] else None
            ),
            "created_at": _iso(row["created_at"]),
        }

    @staticmethod
    def _latest_isolation_event(conn, device_id: str):
        """设备最新一条隔离事件；设备状态由它推导（isolated=隔离中）。"""
        return conn.execute(
            "SELECT * FROM device_isolation_events WHERE device_id = ? "
            "ORDER BY id DESC LIMIT 1",
            (device_id,),
        ).fetchone()

    def _assert_not_isolated(self, conn, device_id: str) -> None:
        """处理类写操作入口检查（写事务内）：设备隔离中抛 :class:`DeviceIsolated`。

        上传批次、消费者确认、归档、租约领取/完成与连续批次判定共用本检查；
        查询、链核验与过期租约回收不经过这里，隔离期间仍可用。
        """
        row = self._latest_isolation_event(conn, device_id)
        if row is not None and row["action"] == "isolated":
            raise DeviceIsolated(
                message=(
                    f"device {device_id!r} is isolated since "
                    f"{_iso(row['created_at'])}: {row['reason']}"
                ),
                device_id=device_id,
                isolation_event_id=row["id"],
                isolation_reason=row["reason"],
                isolated_at=_iso(row["created_at"]),
                head_sequence=row["head_sequence"],
                head_digest=row["head_digest"],
            )

    def _isolation_key_resolution(
        self, conn, device_id: str, action: str, idempotency_key: str, params: dict
    ):
        """同动作同键事件处理：``('proceed', None)`` / ``('retry', row)`` /
        ``('conflict', row)``。

        同键同参数且该事件仍是最新事件（设备状态仍由它决定）时为安全重试；
        其余同键情形（参数不同，或状态已随后续事件推进）一律视为键已占用。
        """
        existing = conn.execute(
            "SELECT * FROM device_isolation_events "
            "WHERE device_id = ? AND action = ? AND idempotency_key = ?",
            (device_id, action, idempotency_key),
        ).fetchone()
        if existing is None:
            return "proceed", None
        latest = self._latest_isolation_event(conn, device_id)
        match = all(existing[k] == v for k, v in params.items())
        if match and latest is not None and latest["id"] == existing["id"]:
            return "retry", existing
        return "conflict", existing

    def _isolation_key_conflict(
        self, device_id: str, idempotency_key: str, existing
    ) -> IsolationConflict:
        return IsolationConflict(
            reason="idempotency_key_in_use",
            message=(
                f"idempotency_key {idempotency_key!r} is already used by "
                f"isolation event {existing['id']} of device {device_id!r}; "
                "use a new key for a new isolation action"
            ),
            device_id=device_id,
            idempotency_key=idempotency_key,
            occupied_by_event=self._isolation_event_view(existing),
        )

    @staticmethod
    def _chain_head_of(conn, device_id: str) -> tuple[int, str]:
        """设备当前链头（无链行时为 0 / 空串，与游标接口的空设备语义一致）。"""
        chain = conn.execute(
            "SELECT last_sequence, head_digest FROM device_chains WHERE device_id = ?",
            (device_id,),
        ).fetchone()
        if chain is None:
            return 0, ""
        return chain["last_sequence"], chain["head_digest"]

    def isolate_device(
        self,
        device_id: str,
        head_sequence: int,
        head_digest: str,
        reason: str,
        idempotency_key: str,
    ) -> tuple[dict, bool]:
        """隔离设备（单事务，幂等）：核对链头并记录不可变隔离事件。

        - 相同幂等键且相同参数（链头序号/摘要/原因）重试，且该事件仍是设备
          最新事件时返回原事件（``created=False``，HTTP 200）；键已被占用
          （参数不同或状态已推进）抛 ``idempotency_key_in_use``（409）；
        - 设备已在隔离中（最新事件为 ``isolated``）抛 ``already_isolated``
          （409，附生效中的隔离事件）；
        - 提交的链头与同一事务内读出的当前链头不一致抛 ``stale_chain_head``
          （409，附服务端当前链头）；无批次设备的链头为 ``0`` / 空串；
        - 通过后在同一事务插入不可变 ``isolated`` 事件；写锁 + BEGIN
          IMMEDIATE 串行化并发隔离/解除，唯一索引兜底多 worker 并发，状态
          机不会被并发请求越过。
        """
        now = time.time()
        params = {
            "head_sequence": head_sequence,
            "head_digest": head_digest,
            "reason": reason,
        }
        with self._write_tx() as conn:
            verdict, existing = self._isolation_key_resolution(
                conn, device_id, "isolated", idempotency_key, params
            )
            if verdict == "retry":
                return {
                    "status": "isolated",
                    "device_id": device_id,
                    "isolation": self._isolation_event_view(existing),
                }, False
            if verdict == "conflict":
                raise self._isolation_key_conflict(device_id, idempotency_key, existing)

            latest = self._latest_isolation_event(conn, device_id)
            if latest is not None and latest["action"] == "isolated":
                raise IsolationConflict(
                    reason="already_isolated",
                    message=(
                        f"device {device_id!r} is already isolated by event "
                        f"{latest['id']} since {_iso(latest['created_at'])}"
                    ),
                    device_id=device_id,
                    current_isolation=self._isolation_event_view(latest),
                )

            cur_seq, cur_digest = self._chain_head_of(conn, device_id)
            if cur_seq != head_sequence or cur_digest != head_digest:
                raise IsolationConflict(
                    reason="stale_chain_head",
                    message=(
                        f"submitted chain head (sequence {head_sequence}, digest "
                        f"{head_digest!r}) does not match the current chain head of "
                        f"device {device_id!r} (sequence {cur_seq}, digest "
                        f"{cur_digest!r}); re-read the cursor and retry"
                    ),
                    device_id=device_id,
                    expected_head_sequence=cur_seq,
                    expected_head_digest=cur_digest,
                    provided_head_sequence=head_sequence,
                    provided_head_digest=head_digest,
                )

            try:
                cur = conn.execute(
                    "INSERT INTO device_isolation_events "
                    "(device_id, action, idempotency_key, reason, head_sequence, "
                    "head_digest, created_at) "
                    "VALUES (?, 'isolated', ?, ?, ?, ?, ?)",
                    (device_id, idempotency_key, reason, head_sequence, head_digest, now),
                )
            except sqlite3.IntegrityError:
                # 多 worker 下越过进程写锁的并发兜底：重读同键事件按同一语义处理
                verdict, existing = self._isolation_key_resolution(
                    conn, device_id, "isolated", idempotency_key, params
                )
                if verdict == "retry":
                    return {
                        "status": "isolated",
                        "device_id": device_id,
                        "isolation": self._isolation_event_view(existing),
                    }, False
                raise self._isolation_key_conflict(device_id, idempotency_key, existing)
            row = conn.execute(
                "SELECT * FROM device_isolation_events WHERE id = ?",
                (cur.lastrowid,),
            ).fetchone()
            return {
                "status": "isolated",
                "device_id": device_id,
                "isolation": self._isolation_event_view(row),
            }, True

    def release_device_isolation(
        self,
        device_id: str,
        from_sequence: int,
        to_sequence: int,
        idempotency_key: str,
    ) -> tuple[dict, bool]:
        """解除设备隔离（单事务，幂等）：链头未变 + 范围核验 ok + 无未完成租约。

        在同一 BEGIN IMMEDIATE 事务内依次核对（任一不满足整体拒绝，设备保持
        隔离，不落任何事件）：

        1. 设备必须处于隔离中，否则抛 ``not_isolated``（409）；
        2. 当前链头必须与隔离事件记录的链头一致，否则抛
           ``chain_head_changed``（409，附记录值与当前值）；
        3. 设备有批次时对 ``from_sequence..to_sequence`` 执行与
           ``verify_chain`` 相同的只读核验：范围缺批次抛
           ``verification_range_invalid``，结果非 ok 抛 ``verification_failed``
           （附问题清单）；设备无批次时空链视为 ok，跳过范围核验；
        4. 设备上不得有未完成租约（``held``，含过期未回收），否则抛
           ``leases_outstanding``（409，附租约清单）——过期租约可在隔离期间
           显式回收后重试解除。

        全部通过后记录不可变 ``released`` 事件（含核验范围与核验摘要），设备
        恢复原流程。相同幂等键且相同范围的重试在该事件仍为最新事件时返回原
        事件（HTTP 200）；键已被占用抛 ``idempotency_key_in_use``（409）。
        """
        now = time.time()
        params = {"verify_from": from_sequence, "verify_to": to_sequence}
        with self._write_tx() as conn:
            verdict, existing = self._isolation_key_resolution(
                conn, device_id, "released", idempotency_key, params
            )
            if verdict == "retry":
                return {
                    "status": "released",
                    "device_id": device_id,
                    "release": self._isolation_event_view(existing),
                }, False
            if verdict == "conflict":
                raise self._isolation_key_conflict(device_id, idempotency_key, existing)

            latest = self._latest_isolation_event(conn, device_id)
            if latest is None or latest["action"] != "isolated":
                raise IsolationConflict(
                    reason="not_isolated",
                    message=(
                        f"device {device_id!r} is not isolated; nothing to release"
                    ),
                    device_id=device_id,
                    last_event=(
                        self._isolation_event_view(latest)
                        if latest is not None
                        else None
                    ),
                )
            isolating = latest

            cur_seq, cur_digest = self._chain_head_of(conn, device_id)
            if (
                cur_seq != isolating["head_sequence"]
                or cur_digest != isolating["head_digest"]
            ):
                raise IsolationConflict(
                    reason="chain_head_changed",
                    message=(
                        f"chain head of device {device_id!r} changed while isolated: "
                        f"recorded at isolation (sequence "
                        f"{isolating['head_sequence']}, digest "
                        f"{isolating['head_digest']!r}) != current (sequence "
                        f"{cur_seq}, digest {cur_digest!r}); investigate before "
                        "releasing"
                    ),
                    device_id=device_id,
                    isolation_head_sequence=isolating["head_sequence"],
                    isolation_head_digest=isolating["head_digest"],
                    current_head_sequence=cur_seq,
                    current_head_digest=cur_digest,
                )

            verification = None
            if cur_seq >= 1:
                # 与 verify_chain 相同的只读核验，在解除事务的同一快照内执行
                try:
                    result = self._verify_chain(
                        conn, device_id, from_sequence, to_sequence
                    )
                except ChainVerificationNotFound as exc:
                    raise IsolationConflict(
                        reason="verification_range_invalid",
                        message=str(exc),
                        device_id=device_id,
                        verification=exc.detail,
                    )
                if not result["ok"]:
                    raise IsolationConflict(
                        reason="verification_failed",
                        message=(
                            f"chain verification of range "
                            f"{from_sequence}..{to_sequence} on device "
                            f"{device_id!r} reported {result['issue_count']} "
                            "issue(s); resolve them before releasing the isolation"
                        ),
                        device_id=device_id,
                        from_sequence=from_sequence,
                        to_sequence=to_sequence,
                        issue_count=result["issue_count"],
                        issues=result["issues"],
                    )
                verification = {
                    "from_sequence": result["from_sequence"],
                    "to_sequence": result["to_sequence"],
                    "ok": result["ok"],
                    "checked_batches": result["checked_batches"],
                    "recomputed_batches": result["recomputed_batches"],
                    "archived_batches": result["archived_batches"],
                    "checked_acknowledgments": result["checked_acknowledgments"],
                    "checked_judgment_sources": result["checked_judgment_sources"],
                    "snapshot": result["snapshot"],
                }

            outstanding = self._held_lease_views(conn, device_id, now)
            if outstanding:
                raise IsolationConflict(
                    reason="leases_outstanding",
                    message=(
                        f"{len(outstanding)} lease(s) still outstanding on device "
                        f"{device_id!r}; wait for expiry and reclaim them before "
                        "releasing the isolation"
                    ),
                    device_id=device_id,
                    outstanding_leases=outstanding,
                )

            try:
                cur = conn.execute(
                    "INSERT INTO device_isolation_events "
                    "(device_id, action, idempotency_key, reason, head_sequence, "
                    "head_digest, verify_from, verify_to, verification, created_at) "
                    "VALUES (?, 'released', ?, NULL, ?, ?, ?, ?, ?, ?)",
                    (
                        device_id,
                        idempotency_key,
                        cur_seq,
                        cur_digest,
                        from_sequence,
                        to_sequence,
                        json.dumps(verification, ensure_ascii=False)
                        if verification is not None
                        else None,
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                # 多 worker 并发兜底：同键事件已被抢先记录，按同一语义处理
                verdict, existing = self._isolation_key_resolution(
                    conn, device_id, "released", idempotency_key, params
                )
                if verdict == "retry":
                    return {
                        "status": "released",
                        "device_id": device_id,
                        "release": self._isolation_event_view(existing),
                    }, False
                raise self._isolation_key_conflict(device_id, idempotency_key, existing)
            row = conn.execute(
                "SELECT * FROM device_isolation_events WHERE id = ?",
                (cur.lastrowid,),
            ).fetchone()
            return {
                "status": "released",
                "device_id": device_id,
                "release": self._isolation_event_view(row),
            }, True

    def get_isolation_status(self, device_id: str) -> dict:
        """设备隔离状态查询：是否隔离中、生效中的隔离事件、最近事件与事件总数。"""
        with closing(self._connect()) as conn:
            latest = self._latest_isolation_event(conn, device_id)
            count = conn.execute(
                "SELECT COUNT(*) AS c FROM device_isolation_events WHERE device_id = ?",
                (device_id,),
            ).fetchone()["c"]
        isolated = latest is not None and latest["action"] == "isolated"
        return {
            "device_id": device_id,
            "isolated": isolated,
            "status": "isolated" if isolated else "normal",
            "current_isolation": (
                self._isolation_event_view(latest) if isolated else None
            ),
            "last_event": (
                self._isolation_event_view(latest) if latest is not None else None
            ),
            "event_count": count,
        }

    def list_isolation_events(
        self, device_id: str, *, after_id: int = 0, limit: int = 50
    ) -> dict:
        """按 id 游标分页列出设备的隔离/解除事件（升序，不可变历史）。"""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM device_isolation_events "
                "WHERE device_id = ? AND id > ? ORDER BY id LIMIT ?",
                (device_id, after_id, limit + 1),
            ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = [self._isolation_event_view(r) for r in rows]
        return {
            "device_id": device_id,
            "count": len(items),
            "events": items,
            "next_cursor": items[-1]["event_id"] if has_more and items else None,
        }
