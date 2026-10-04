# 航标闪光巡检判定服务（navlight-inspector）

基于 FastAPI + SQLite 的航标巡检判定服务。接口接收闪光观测时间戳、航标特征库
（编号、周期、周期内闪光时刻）以及时间容差、最大线性钟漂、漏记/误记上限，
联合估计观测时钟的**相位与线性钟漂**，对跨多个周期的观测序列做**循环对齐**
（周期边界不会被误判为缺口），并按固定顺序裁决最佳航标，返回逐事件对应关系
与未匹配记录；无合格候选时给出超限原因。

`/patrols/analyze` 把一次巡检的多个观测窗口与计划航线**整体核对**：先为每个
窗口计算全部合格航标候选，再用动态规划把窗口一对一全局分配到严格递增的航点
（可跳过窗口或航点，禁止逐窗局部最佳），返回分配及候选证据、未分配窗口与
漏巡航点。

服务端特征库以**不可变版本**发布：草稿提交完整特征库并基于 `base_version`
计算增删改差异；激活在单个 SQLite 事务内确认基线仍是当前版本后原子切换，
冲突返回 409；历史版本不可改，可作为回退时新草稿的来源。

草稿**上线前**可创建**回放报告**（`POST /library/versions/{id}/replay-reports`）：
请求携带 `idempotency_key`，开始时冻结当前最大 `judgment_id` 作为水位，只
重放水位内**使用服务端特征库**的判定记录（内联库记录按原因跳过）；每条
记录沿用原观测与 `/judge` 四项阈值，改用草稿快照执行同一判定算法，按
`unchanged` / `match_gained` / `match_lost` / `winner_changed` 四分类并保存
旧、新最佳候选证据。相同键重试返回原报告（HTTP 200），键被其他版本占用
返回 409，写锁加唯一约束保证并发不会生成重复报告；回放只读判定、**不写入
`judgments`**，新判定也不会混入既有报告。

现场设备通过 `POST /field-devices/{device_id}/batches` 以**链式批次**上传观测：
每批含从 1 递增的 `sequence`、设备内唯一的 `batch_id`、引用上一批摘要的
`previous_digest` 和 1～100 条按 `(captured_at, local_id)` 递增的观测；服务端
只保存观测时间戳与 `/judge` 四项阈值（不判定），并对排除摘要字段的规范化载荷
计算 SHA-256 作为批次摘要。校验、追加记录、更新设备链头在**单个 SQLite 事务**
内完成：相同载荷重放在幂等事务内原子累加 `replay_count` 并更新游标最近事件
（不新增批次、不推进链头、不改写首次受理响应），返回原结果（`status=replayed`），
载荷变化或链头过期返回 409 及期望值，并发提交与并发重放都不会形成分叉或丢计数。

同一台设备的批次会被**多个下游处理方**消费（如归档、分析、备份）。各处理方
先登记为设备的**必需消费者**（登记后不可删除），再以
`consumer_id` + `sequence` + `batch_digest` 确认自己已处理完的批次——允许乱序、
并发与重复提交，仅接受已存在且摘要相符的批次，相同确认幂等，摘要冲突返回 409。
每个消费者在**同一 SQLite 事务**内保存稀疏确认并把连续水位推进到首个缺口；
设备的 `safe_sequence` 取全部必需消费者水位的最小值，即所有处理方都已确认、
**可安全归档的连续前缀**（尚无消费者或任一消费者未确认任何批次时为 0）。
状态接口列出各消费者水位、首个缺口与阻塞消费者；确认只追加记录，**不删除原批次**。

设备可调用 `POST /field-devices/{device_id}/archive` 按 `through_sequence`
归档**连续前缀**：目标不得超过同一写事务内核验的 `safe_sequence`，越界返回
409 并附当前水位与阻塞消费者。归档清空目标范围批次的观测与阈值明细，批次成为
**墓碑**（`status=archived`），只保留 `batch_id`、序号、前序/本批摘要、条数、
首次受理时刻/响应与重放统计；链头摘要与后续上传不变。重复或更小目标幂等，
`archived_through` 只进不退。已归档批次仍可按摘要接受**同载荷重放**并累计重放
次数（载荷变化仍 409）；归档之后登记的新消费者从 `archived_through` 起步，
其首个缺口直接指向归档前缀的下一批。

已登记消费者可领取**批次处理租约**（`POST …/batch-leases`）：请求提交
`consumer_id`、至多 20 个批次的 `max_batches`、1～3600 秒的
`lease_duration_seconds` 与幂等键，服务在**单个 SQLite 写事务**内从该消费者
连续水位的下一批起选择**未归档的连续区间**，保存租约及每批摘要并返回逐批
观测明细。同一消费者存在未过期租约时，相同幂等键重试返回原租约（HTTP 200），
其他键返回 409——同一区间不会分给两个租约。完成租约时逐项核对摘要，在同一
事务写入确认、推进水位并释放租约；摘要变化、缺批次或租约过期**整体拒绝且不
推进**。归档不得清除仍被租约占用的明细（过期租约也需显式回收后才释放），
回收（`…/recover`）之后区间可重新领取。另有租约列表与详情查询。

连续批次在归档前可形成一次**可复核的巡检判定**
（`POST /field-devices/{device_id}/batch-judgments`）：请求携带客户端幂等键与
起止 `sequence`，服务在同一写事务开始时固定当前激活特征库版本，确认区间内
**序号连续、批次明细仍在（无墓碑、无缺批次）、各批保存的四项阈值完全一致**，
再按 `(captured_at, sequence, local_id)` 合并全部观测并复用现有 `/judge` 规则
判定，保存 `judgment_id` 及设备、序号范围、各批摘要与 `library_version` 作为
**不可变来源**。相同键重试返回原判定（HTTP 200）；相同键改范围或换设备返回
409；范围含墓碑、缺批次或阈值不一致时**整体拒绝且不落判定**。写锁串行化保证
归档或上传并发不会使已保存结果引用半个范围；批次随后归档，已保存的判定来源
仍可完整查询。

已保存的批次判定可随时复核
（`POST …/batch-judgments/{id}/review-reports`）：请求携带幂等键，服务在
**单个只读 SQLite 事务**内固定该判定的不可变记录、各批摘要与保存的
`library_version`，逐批核对当前批次链（已归档墓碑可核对摘要骨架但没有
明细）。只有所有明细可用且摘要、前序链、阈值一致时，才用**保存版本**的
特征库重跑 `/judge` 判定算法：一致返回 `unchanged`，不一致返回 `changed`
并给出差异字段与原/新候选证据；来源缺失或摘要不符返回
`source_unavailable` / `source_mismatch` 及问题清单，**不写入
`judgments`**。相同键重试返回原报告，改用其他设备或判定目标返回 409。

设备离线补传、归档或数据库迁移之后，运维可用
`POST /field-devices/{device_id}/verify-chain` 对批次链做**一致性核验**，确认
批次链仍可作为判定来源：提交设备编号与序号范围，服务在**单个只读 SQLite 事务**
内固定快照，逐批重算范围内未归档批次的规范 SHA-256 并与保存摘要比对，核对
`previous_digest` 链、序号连续性、`batch_count`/`last_sequence`/`head_digest`、
`archived_through` 墓碑边界、消费者确认摘要与连续水位，以及连续批次判定
（`batch_judgment_batches`）保存的来源摘要；归档批次只核对保存的摘要与边界。
返回快照上限、`ok` 与问题清单（`issue_code`/`sequence`/`expected`/`actual`）；
并发上传、确认或归档期间结果来自同一快照，核验**不修改任何数据**。

核验发现摘要、序号或确认水位异常时，运维可先**隔离**设备
（`POST /field-devices/{device_id}/isolation`）阻止继续处理、避免扩大损坏：
请求提交当前链头序号、链头摘要、原因与幂等键，服务在**同一 SQLite 写事务**
内核对链头并记录**不可变隔离事件**。隔离期间上传批次、消费者确认、归档、
租约领取/完成与批次判定均返回 409（`device_isolated`），查询、链核验与过期
租约回收仍可用。核验修复后调用 `POST …/isolation/release` **解除隔离**：请求
携带核验范围与幂等键，服务在同一事务内重新核对**链头未变化**、**范围核验为
ok** 且**无未完成租约**，任一不满足整体拒绝（设备保持隔离）；全部通过才记录
不可变解除事件并恢复原流程。并发隔离/解除由写锁串行化、不会越过状态；
`GET …/isolation` 查询当前状态，`GET …/isolation/events` 分页查询不可变事件
历史。

## 快速开始

单条命令启动（唯一前置要求：Docker 与 Docker Compose）：

```bash
docker compose up --build
```

访问地址：

| 地址 | 说明 |
| --- | --- |
| http://localhost:8000 | 服务信息 |
| http://localhost:8000/docs | Swagger 交互式 API 文档 |
| http://localhost:8000/health | 健康检查 |

停止并清理：`docker compose down`（加 `-v` 同时删除数据卷）。

## 配置项

全部通过环境变量配置（见 `docker-compose.yml` 中的 `environment`）：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `NAVLIGHT_DB_PATH` | `/app/data/navlight.db`（容器内） | SQLite 数据库文件路径 |
| `NAVLIGHT_GRID_MAX_STEPS` | `201` | 钟漂网格搜索最大步数 |
| `NAVLIGHT_SEED_TOP_K` | `5` | 进入细化阶段的相位/钟漂种子数 |

端口映射在 `docker-compose.yml` 的 `ports` 中修改（默认 `8000:8000`）。
SQLite 数据通过命名卷 `navlight-data` 持久化。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/library/versions` | 创建版本草稿：提交完整特征库 + `base_version`，返回增删改差异 |
| `GET` | `/library/versions` | 列出全部版本（`draft` / `active` / `historical`） |
| `GET` | `/library/versions/{id}` | 版本详情（元数据 + 完整特征库快照） |
| `GET` | `/library/versions/{id}/diff` | 查询版本相对 `base_version` 的增删改 |
| `POST` | `/library/versions/{id}/activate` | 激活草稿：单事务校验基线并原子切换，冲突返回 409 |
| `POST` | `/library/versions/{id}/replay-reports` | 为草稿创建回放报告（幂等键；冻结判定水位、四分类、保存新旧证据；重试 200，跨版本占用 409） |
| `GET` | `/library/versions/{id}/replay-reports/{rid}` | 查询回放报告摘要（水位与四分类计数） |
| `GET` | `/library/versions/{id}/replay-reports/{rid}/changes` | 按原 `judgment_id` 游标分页查询变更明细（可按分类过滤、可查跳过记录） |
| `POST` | `/library/import` | 导入 JSON 特征库（兼容旧接口；基于当前版本原子创建并激活新版本） |
| `GET` | `/library` | 查看当前激活特征库（含 `library_version`） |
| `POST` | `/judge` | 判定观测序列，结果落库（响应与记录保存 `library_version`） |
| `POST` | `/patrols/analyze` | 巡检整体核对：观测窗口全局分配到计划航线航点（固定激活版本，不落库） |
| `GET` | `/judgments` | 查询判定记录（`limit`/`offset` 分页，按时间倒序） |
| `GET` | `/judgments/{id}` | 查询单条判定记录详情（含完整请求与响应） |
| `POST` | `/field-devices/{device_id}/batches` | 上传设备观测批次：链式追加、只保存不判定；重放返回原结果，冲突 409 |
| `GET` | `/field-devices/{device_id}/cursor` | 查询设备批次链游标（链头摘要、下一批期望值与最近事件状态/序号/时刻） |
| `GET` | `/field-devices/{device_id}/batches` | 按 `after_sequence` 游标分页列出设备批次（首次受理状态 + 重放计数） |
| `GET` | `/field-devices/{device_id}/batches/{sequence}` | 查询单条批次详情（未归档：观测时间戳与 `/judge` 四项阈值；已归档：墓碑骨架，明细为 `null`） |
| `POST` | `/field-devices/{device_id}/consumers` | 登记设备的必需下游消费者（不可删除；重复登记幂等；归档后登记从 `archived_through` 起步） |
| `POST` | `/field-devices/{device_id}/acknowledgments` | 消费者确认已处理批次（`consumer_id`/`sequence`/`batch_digest`；乱序/并发/重复安全，冲突 409） |
| `GET` | `/field-devices/{device_id}/archive-status` | 查询安全归档水位：`safe_sequence`、`archived_through`、首个缺口、各消费者水位与阻塞消费者 |
| `POST` | `/field-devices/{device_id}/archive` | 归档连续前缀：清空 `sequence ≤ through_sequence` 的明细载荷（目标须 ≤ 同事务 `safe_sequence` 且不侵犯在持租约，越界 409；重复/更小目标幂等） |
| `POST` | `/field-devices/{device_id}/batch-leases` | 领取批次处理租约（`consumer_id`/`max_batches≤20`/`lease_duration_seconds 1~3600`/幂等键；单从未归档连续区间选批并保存每批摘要；同键重试 200、他键或过期 409） |
| `GET` | `/field-devices/{device_id}/batch-leases` | 分页查询租约摘要（`after_id` 游标，可按 `consumer_id` 与 `status=held/expired/completed/revoked` 过滤） |
| `GET` | `/field-devices/{device_id}/batch-leases/{id}` | 查询单条租约详情（状态、起止序号与逐批摘要、观测明细） |
| `POST` | `/field-devices/{device_id}/batch-leases/{id}/complete` | 完成租约：逐项核对摘要，同事务写确认、推进水位、置 completed（摘要变化/缺批次/过期/已回收整体 409 且不推进） |
| `POST` | `/field-devices/{device_id}/batch-leases/{id}/recover` | 显式回收过期租约（held → revoked），回收后区间可重新领取（未过期/已完成 409，重复回收幂等） |
| `POST` | `/field-devices/{device_id}/batch-judgments` | 对连续批次区间创建归档前巡检判定（幂等键；固定激活库版本，合并观测复用 `/judge`；重试 200，改范围/设备 409，含墓碑/缺批次/阈值不一致整体拒绝） |
| `GET` | `/field-devices/{device_id}/batch-judgments` | 分页查询设备的连续批次判定（`after_id` 游标 + `limit`） |
| `GET` | `/field-devices/{device_id}/batch-judgments/{id}` | 查询单次连续批次判定详情（不可变来源：序号范围、各批摘要、合并观测、阈值、`library_version`、判定结果） |
| `POST` | `/field-devices/{device_id}/batch-judgments/{id}/review-reports` | 复核一次连续批次判定（幂等键；只读快照逐批核对来源，一致时用保存版本特征库重跑：`unchanged`/`changed`/`source_unavailable`/`source_mismatch`，不写 `judgments`；重试 200，改设备/判定 409） |
| `GET` | `/field-devices/{device_id}/batch-judgments/{id}/review-reports` | 分页查询一次判定的复核报告（`after_id` 游标 + `limit`，摘要） |
| `GET` | `/field-devices/{device_id}/batch-judgments/{id}/review-reports/{rid}` | 查询单份复核报告详情（问题清单、差异字段与原/新候选证据） |
| `POST` | `/field-devices/{device_id}/verify-chain` | 核验设备批次链一致性（只读快照：重算范围内未归档批次摘要，核对链式引用/序号连续性/链游标/墓碑边界/消费者确认与水位/判定来源摘要；不改数据，空设备或缺批次 404） |
| `POST` | `/field-devices/{device_id}/isolation` | 隔离设备（提交当前链头序号/摘要 + 原因 + 幂等键；同事务核对链头并记录不可变事件；隔离期间上传/确认/归档/租约领取与完成/批次判定 409；同键重试 200，链头不符或已隔离 409） |
| `POST` | `/field-devices/{device_id}/isolation/release` | 解除隔离（提交核验范围 + 幂等键；同事务核对链头未变、范围核验 ok、无未完成租约，全部通过记录解除事件并恢复原流程；任一不满足 409 整体拒绝） |
| `GET` | `/field-devices/{device_id}/isolation` | 查询设备隔离状态（是否隔离中、生效中的隔离事件、最近事件与事件总数） |
| `GET` | `/field-devices/{device_id}/isolation/events` | 分页查询设备隔离/解除事件历史（`after_id` 游标，不可变，按 id 升序） |

### 1. 导入特征库（原子发布新版本）

```bash
curl -s -X POST http://localhost:8000/library/import \
  -H 'Content-Type: application/json' \
  -d @samples/library.json
```

特征库条目格式：`{"aid_id": "CN-001", "period": 10.0, "flashes": [0.0, 1.5]}`，
其中 `flashes` 为周期内闪光时刻（秒，取值 `[0, period)`）。

`mode=merge` 按编号并入当前版本，`mode=replace` 清空重导。每次导入都基于
当前激活版本在**单个事务**内创建并直接激活一个新版本（`source="import"`），
旧版本转为 `historical`；响应中的 `version` 字段即新版本元数据（含差异）。

### 2. 特征库版本发布（草稿 → 激活）

```bash
# ① 创建草稿：提交完整特征库 + base_version（当前激活版本或历史版本）
curl -s -X POST http://localhost:8000/library/versions \
  -H 'Content-Type: application/json' \
  -d '{"base_version": 1, "aids": [
        {"aid_id": "CN-001", "period": 10.0, "flashes": [0.0, 1.5]},
        {"aid_id": "CN-009", "period": 6.0,  "flashes": [0.0]}
      ]}'

# ② 查询差异（创建响应中已带 diff，此处可随时复查）
curl -s http://localhost:8000/library/versions/2/diff

# ③ 激活草稿
curl -s -X POST http://localhost:8000/library/versions/2/activate
```

**版本模型**：

- 每个版本保存**完整特征库快照**，创建后不可变；状态机为
  `draft → active → historical`，任一时刻至多一个 `active`；
- `base_version` 可以是当前激活版本，也可以是历史版本（回退场景），
  但不能是草稿；基线不存在返回 404；
- 差异按航标编号计算：`added`（新增）、`removed`（删除）、
  `modified`（`period` 或 `flashes` 变化），创建时即算出并随版本保存，
  可通过 `GET /library/versions/{id}/diff` 复查。

**激活与冲突处理（409）**：

激活在**单个 SQLite 事务**内完成：先确认草稿的 `base_version` 仍是当前激活
版本，再把旧激活版本转为 `historical`、新草稿转为 `active` 并切换激活指针，
三步原子生效。若创建草稿后有其他版本被激活（基线过期），激活返回
`409 Conflict`，草稿保持 `draft` 状态不变。处理方式：以**当前激活版本**为
`base_version` 重新提交内容相同的草稿（服务端会基于新基线重新计算差异），
再激活即可。

**回退流程**：

历史版本不可变，可直接作为新草稿的内容来源：

```bash
# 查看历史版本完整快照
curl -s http://localhost:8000/library/versions/1

# 用历史快照在当前激活版本之上重建草稿并激活，即完成回退
curl -s -X POST http://localhost:8000/library/versions \
  -H 'Content-Type: application/json' \
  -d '{"base_version": <当前激活版本号>, "aids": [<历史版本的完整 aids>]}'
curl -s -X POST http://localhost:8000/library/versions/<新草稿号>/activate
```

注意：以历史版本为 `base_version` 创建的草稿在激活时同样会因基线过期返回
409——回退的草稿必须基于**当前激活版本**创建（内容可以来自任意历史版本）。

### 2.5 草稿回放报告（上线前评估会怎样改写既有判定）

草稿激活前，可基于该草稿对历史判定做一次**离线重放**，回答「这个库一旦
上线，过去的判定会有多少改变、分别变成什么」。整个流程（冻结水位 → 逐条
重放 → 落报告）在单个 SQLite 写事务内完成：

1. 开始时冻结当前最大 `judgment_id` 作为**水位**：只重放 `id ≤ 水位` 的
   记录；重放期间及之后新产生的判定 `id` 必然更大，**不会混入**本报告；
2. 只重放**使用服务端特征库**的判定（`library_source=stored`）。请求携带
   内联 `library` 的记录与服务端版本无关，以 `item_status=skipped`、
   `skip_reason=inline_library` 跳过（可通过 `include_skipped=true` 查看）；
3. 每条记录**沿用原观测与四项阈值**（`tolerance` / `max_drift` /
   `max_missed` / `max_false`），只把特征库整体替换为草稿完整快照，复用与
   `/judge` 完全相同的现有判定算法；
4. 新旧裁决结果按四类归档，并保存**旧、新最佳候选证据**（与 `/judge` 的
   `best` 结构相同：相位、钟漂、逐事件、漏记、未匹配观测；无匹配侧为
   `null`，并保留该侧 `reason`）：

| 分类 | 含义 |
| --- | --- |
| `unchanged` | 新旧均无合格候选，或最佳候选仍是同一航标 |
| `match_gained` | 旧判定无合格候选，草稿下得到匹配 |
| `match_lost` | 旧判定有匹配，草稿下变为无合格候选 |
| `winner_changed` | 新旧均有匹配，但最佳航标编号改变 |

```bash
# ① 在当前激活版本之上准备草稿（不激活）
curl -s -X POST http://localhost:8000/library/versions \
  -H 'Content-Type: application/json' \
  -d '{"base_version": 1, "aids": [
        {"aid_id": "CN-001", "period": 10.0, "flashes": [0.0, 1.5]},
        {"aid_id": "CN-009", "period": 6.0,  "flashes": [0.0]}
      ]}'
# → 记下草稿 version_id（假设为 2）

# ② 创建回放报告（携带客户端生成的幂等键；网络重试请复用同一键）
curl -s -X POST http://localhost:8000/library/versions/2/replay-reports \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key": "draft-2-eval-2026-09-30"}'
```

```json
{
  "report_id": 1,
  "version_id": 2,
  "idempotency_key": "draft-2-eval-2026-09-30",
  "watermark_judgment_id": 128,
  "created_at": "2026-09-30T06:00:00+00:00",
  "total_judgments": 130,
  "replayed_count": 122,
  "skipped_count": 8,
  "changes": {
    "unchanged": 114,
    "match_gained": 3,
    "match_lost": 4,
    "winner_changed": 1
  }
}
```

- **幂等**：相同 `idempotency_key` 重试（同版本）返回**原报告**，HTTP 200、
  `report_id`/水位/计数/创建时刻均不变，且不会重复计算或重复落库；
- **跨版本占用 409**：同一个键已被**另一版本**的报告使用时返回
  `409 Conflict`，`detail` 给出 `occupied_by_version` /
  `occupied_by_report`，换一个键即可；
- **并发安全**：写锁串行化 + `idempotency_key` 全局唯一约束兜底，同键并发
  请求恰好一份 201，其余得到同一份 200，绝不生成重复报告；
- 只有 `draft` 版本可创建报告（版本不存在 404、非草稿 400）；草稿随后被
  激活或转为历史不影响已生成报告的查阅。

**变更明细按原 `judgment_id` 游标分页**（升序，`after_judgment_id` 传上一页
的 `next_cursor`），只关心改变时按分类过滤：

```bash
BASE=http://localhost:8000/library/versions/2/replay-reports/1

# 首页（每页 50 条）
curl -s "$BASE/changes?limit=50"
# 下一页
curl -s "$BASE/changes?limit=50&after_judgment_id=<上一页 next_cursor>"
# 只看「最佳航标改变」与「失去匹配」
curl -s "$BASE/changes?classification=winner_changed"
curl -s "$BASE/changes?classification=match_lost"
# 查看哪些历史记录被跳过（内联库）
curl -s "$BASE/changes?include_skipped=true"
```

每条变更同时给出旧、新两侧的裁决结论与完整候选证据，便于直接对比：

```json
{
  "report_id": 1,
  "version_id": 2,
  "count": 1,
  "next_cursor": null,
  "changes": [
    {
      "judgment_id": 57,
      "item_status": "replayed",
      "classification": "winner_changed",
      "old": {
        "status": "matched",
        "best_aid_id": "CN-001",
        "reason": null,
        "library_version": 1,
        "best": {"aid_id": "CN-001", "drift": 0.00012, "matched": 58, "…": "…"}
      },
      "new": {
        "status": "matched",
        "best_aid_id": "CN-009",
        "reason": null,
        "best": {"aid_id": "CN-009", "drift": -0.00003, "matched": 60, "…": "…"}
      }
    }
  ]
}
```

回放是**只读评估**：结果只写入 `replay_reports` /
`replay_report_items`，绝不写入 `judgments`；既有判定记录、响应与
`/judgments` 查询完全不受影响。确认草稿影响可接受后，再按 §2 的流程激活。

**首次迁移**：服务启动时若发现旧版平铺 `aids` 表，会在单个事务内将其整体
转为初始激活版本（`version_id=1`，`source="migration"`）并补充判定记录的
`library_version` 列；同时为旧版设备批次表补充 `replay_count` /
`last_replayed_at` / `archived_at` 列（既有批次重放次数为零、
`last_replayed_at` 为空、未归档；明细列放宽为可空以承载归档墓碑，旧库经
「建新表 → 拷数据 → 改名」在单事务内重建，唯一约束保留）、为设备链游标表
补充 `last_event_status` / `last_event_sequence` / `last_event_at` 列
（回填为链头批次的受理事件）与 `archived_through` / `archived_at` 列
（回填为 0 / 空）。全新部署则无初始版本，首次 `/library/import` 即创建
版本 1。

### 3. 判定观测序列

生成一份合成观测（含钟漂、噪声、2 次漏记、2 次误记）并提交判定：

```bash
python3 samples/generate_request.py > /tmp/judge.json
curl -s -X POST http://localhost:8000/judge \
  -H 'Content-Type: application/json' -d @/tmp/judge.json | python3 -m json.tool
```

请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `observations` | `float[]` | 闪光观测时间戳（Unix 秒） |
| `tolerance` | `float>0` | 时间容差（秒），匹配残差上限 |
| `max_drift` | `float∈[0,1)` | 最大线性钟漂（无量纲，s/s） |
| `max_missed` | `int≥0` | 漏记上限 |
| `max_false` | `int≥0` | 误记上限 |
| `library` | 可选 | 内联特征库；缺省时在请求开始固定当前激活版本，整个判定只读该快照 |

响应要点：

- `status`：`matched` / `no_candidate`；无合格候选时 `reason` 写明超限原因；
- `library_source` / `library_version`：`stored` 时记录本次判定固定的激活版本号
  （请求开始即固定，计算期间发生版本切换也不受影响），`inline` 时为 `null`；
- `best`：最佳合格候选，含 `offset`（相位）、`drift`（钟漂）、`matched`/`missed`/`false`
  计数、`events`（逐事件对应关系：观测时刻、期望真时、预测观测时刻、周期序号、
  周期内闪光时刻、残差）、`missed_flashes`（漏记明细）、
  `unmatched_observations`（误记/未匹配观测）；
- `candidates`：全部候选按裁决顺序排列的摘要（含各自 `violations`）；
- `judgment_id`：判定记录 ID，可用于 `GET /judgments/{id}` 回溯；
  判定记录同样保存 `library_version`，可按版本号精确复现当时使用的特征库。

### 4. 巡检整体核对（多窗口 ↔ 计划航线）

生成一份示例巡检请求（3 个航点、3 个观测窗口：1 个噪声窗口、1 个漏巡航点）
并提交整体核对：

```bash
python3 samples/generate_patrol_request.py > /tmp/patrol.json
curl -s -X POST http://localhost:8000/patrols/analyze \
  -H 'Content-Type: application/json' -d @/tmp/patrol.json | python3 -m json.tool
```

请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `waypoints` | `object[]`（≤50） | 计划航线航点，按航线顺序排列；`waypoint_id` 请求内唯一，`aid_id` 必须存在于请求开始时固定的激活特征库版本（否则 400） |
| `windows` | `object[]`（≤20） | 观测窗口，按采集顺序排列；每个窗口含 `observations`（闪光观测时间戳，Unix 秒） |
| `tolerance` | `float>0` | 时间容差（秒），同 `/judge` |
| `max_drift` | `float∈[0,1)` | 最大线性钟漂（无量纲，s/s），同 `/judge` |
| `max_missed` / `max_false` | `int≥0` | 漏记/误记上限，同 `/judge` |

处理流程：先为每个窗口用与 `/judge` 相同的联合估计计算航线上全部**合格航标
候选**，再用**动态规划**把窗口一对一分配到严格递增的航点（可跳过窗口或航点，
禁止逐窗采用局部最佳）。

响应要点：

- `status`：`assigned` / `no_assignment`；没有分配时 `reason` 写明原因
  （无航点、无观测窗口，或所有 (窗口, 航点) 对均无合格候选——附裁决键最小
  候选的违规原因）；
- `library_version`：请求开始即固定的激活特征库版本，计算期间发生版本切换
  也不受影响；
- `assignments`：分配结果及候选证据——窗口索引、航点索引/编号、航标编号，
  以及与 `/judge` 的 `best` 结构相同的候选详情（相位、钟漂、逐事件对应关系、
  漏记明细、未匹配观测）；
- `unassigned_windows`：未分配窗口（索引与观测数）；
- `missed_waypoints`：漏巡航点——首末已分配航点之间未被分配的航点
  （首个已分配航点之前、末个之后未覆盖的航点不计入）；
- `window_candidates`：每个窗口的全部合格航标候选摘要（按 `/judge` 裁决键
  排序，`waypoint_indices` 标注该候选可用于航线上哪些航点），即参与全局
  分配的候选证据。

**分配裁决顺序**（动态规划求全局最优，依次比较，前者优先）：

1. 已分配窗口最多；
2. 首末已分配航点之间的漏巡点最少；
3. 已分配候选的漏记与误记合计最少；
4. 匹配残差总和最小；
5. 钟漂绝对值总和最小；
6. 航点索引序列字典序最小（另以窗口索引序列作稳定性补充，保证结果确定可复现）。

### 5. 查询判定记录

```bash
curl -s http://localhost:8000/judgments?limit=10
curl -s http://localhost:8000/judgments/1
```

### 6. 现场设备批次链（断网续传与冲突恢复）

现场设备把观测按**批次链**上传：每台设备一条链，`sequence` 从 1 递增，
`previous_digest` 引用上一批摘要（首批为空串 `""`）。服务端只保存观测时间戳
与 `/judge` 四项阈值（`tolerance`/`max_drift`/`max_missed`/`max_false`），
**不做判定**；判定仍通过 `/judge` 或 `/patrols/analyze` 显式发起。

**请求体**：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `batch_id` | `string`（1~128） | 客户端生成的批次编号，设备内唯一 |
| `sequence` | `int≥1` | 批次序号，从 1 递增，必须等于链头序号 + 1 |
| `previous_digest` | `string` | 上一批摘要（64 位十六进制）；首批为空串 `""` |
| `observations` | `object[]`（1~100） | 观测，按 `(captured_at, local_id)` 严格递增；`captured_at` 为采集时刻（Unix 秒），`local_id` 为设备本地编号 |
| `tolerance` | `float>0` | 时间容差（秒），同 `/judge` |
| `max_drift` | `float∈[0,1)` | 最大线性钟漂（无量纲，s/s），同 `/judge` |
| `max_missed` / `max_false` | `int≥0` | 漏记/误记上限，同 `/judge` |

**批次摘要（SHA-256）**：服务端对**排除摘要字段**（`previous_digest`）的规范化
载荷计算摘要——字段为 `batch_id`、`device_id`、`sequence`、`observations`
（`captured_at`/`local_id`）与四项阈值；JSON 序列化采用 **UTF-8 键序**
（键按 UTF-8 序排序）与**紧凑分隔符**（`,`/`:`，无空白），UTF-8 编码后取
SHA-256 十六进制小写。客户端可按同一规则独立复算：

```python
import hashlib, json
payload = {
    "batch_id": "b-001", "device_id": "dev-01", "sequence": 1,
    "observations": [{"captured_at": 1700000000.0, "local_id": "o001"}],
    "tolerance": 0.5, "max_drift": 0.0005, "max_missed": 3, "max_false": 3,
}
blob = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                  separators=(",", ":")).encode("utf-8")
digest = hashlib.sha256(blob).hexdigest()
```

**上传与链式追加**：

```bash
# 首批：previous_digest 为空
curl -s -X POST http://localhost:8000/field-devices/dev-01/batches \
  -H 'Content-Type: application/json' -d '{
    "batch_id": "b-001", "sequence": 1, "previous_digest": "",
    "observations": [
      {"local_id": "o001", "captured_at": 1700000000.0},
      {"local_id": "o002", "captured_at": 1700000000.5}
    ],
    "tolerance": 0.5, "max_drift": 0.0005, "max_missed": 3, "max_false": 3
  }'
# → 201 {"status": "accepted", "digest": "49740f…", "chain": {"next_sequence": 2,
#        "expected_previous_digest": "49740f…", …}, …}

# 后续批次：previous_digest 引用上一批摘要（即响应中的 digest）
curl -s -X POST http://localhost:8000/field-devices/dev-01/batches \
  -H 'Content-Type: application/json' -d '{
    "batch_id": "b-002", "sequence": 2,
    "previous_digest": "49740fb763c71461617735f601caeb89cc114aabc76e5dcd79ec05045767016a",
    "observations": [{"local_id": "o003", "captured_at": 1700000001.0}],
    "tolerance": 0.5, "max_drift": 0.0005, "max_missed": 3, "max_false": 3
  }'
```

校验、追加记录、更新设备链头在**单个 SQLite 事务**内完成；写串行化加
`(device_id, sequence)`、`(device_id, batch_id)` 唯一约束兜底，并发提交
不会形成分叉（同一序号只会有一个批次被接受，其余收到 409）。

**幂等重放**：以相同载荷重发同一 `batch_id`（或同一序号）时，服务端返回
**原结果**，仅 `status` 标记为 `replayed`（HTTP 200）；新接受的批次为
`status=accepted`（HTTP 201）。重放在**幂等事务**内完成两件持久化工作：
原子累加该批次的 `replay_count`、刷新其 `last_replayed_at`，并把设备游标的
最近事件改为本次重放；同时保证**三不**——不新增批次、不推进链头
（`last_sequence`/`head_digest`/`batch_count` 不变）、不改写首次受理响应。
并发重放由写锁 + `BEGIN IMMEDIATE` 串行化，计数用
`replay_count = replay_count + 1` 原子累加，不会丢计数。

批次的**列表与详情**始终保留 `status=accepted` 作为首次受理事实（不因重放而
变成 `replayed`），并额外返回含义相同的两个字段：

| 字段 | 说明 |
| --- | --- |
| `status` | 首次受理事实，恒为 `accepted`（重放只在当次上传响应中标记为 `replayed`） |
| `replay_count` | 相同载荷的累计重放次数；从未重放为 `0`（旧库迁移的既有批次也为 0） |
| `last_replayed_at` | 最近一次重放时刻（UTC ISO-8601）；从未重放为 `null` |

**游标**在链头信息之外另返回最近一次事件（受理或重放）的三个字段；新批次
受理后最近事件为该批次的 `accepted`，重放后改为被重放批次的 `replayed`：

| 字段 | 说明 |
| --- | --- |
| `last_event_status` | 最近事件类型：`accepted` / `replayed`；空链设备为 `null` |
| `last_event_sequence` | 最近事件所属批次序号（重放历史批次时为该批次序号，而非链头序号）；空链为 `0` |
| `last_event_at` | 最近事件时刻（UTC ISO-8601）；空链为 `null` |

注意 `updated_at` 只在链头被新批次推进时变化，重放不更新它；要判断「最近
一次设备交互」请使用 `last_event_*`。上传响应（`accepted`/`replayed`）与
三个查询接口的字段分工明确、含义一致，重试安全。

**断网续传**：设备离线期间在本地缓存批次；恢复联网后先查游标，从
`next_sequence` 与 `expected_previous_digest` 续传，已送达的批次按幂等
重放处理，不会产生重复记录：

```bash
# 查询服务端链头：下一批该用哪个 sequence 与 previous_digest
curl -s http://localhost:8000/field-devices/dev-01/cursor
# → {"last_sequence": 2, "head_digest": "5e7aa3…", "next_sequence": 3,
#    "expected_previous_digest": "5e7aa3…", "batch_count": 2,
#    "last_event_status": "accepted", "last_event_sequence": 2,
#    "last_event_at": "…", …}

# 断网期间未确认的批次原样重发：若服务端已接受，返回原结果且 status=replayed
curl -s -X POST http://localhost:8000/field-devices/dev-01/batches \
  -H 'Content-Type: application/json' -d '<与上次完全相同的请求体>'
# → 200 {"status": "replayed", "digest": "5e7aa3…", …}
```

**冲突恢复（409）**：响应 `detail` 携带期望值，设备据此对齐链头后重发：

```json
{
  "detail": {
    "error": "batch_conflict",
    "reason": "stale_chain_head",
    "message": "batch does not extend the chain head: …",
    "expected_sequence": 3,
    "expected_previous_digest": "5e7aa3…"
  }
}
```

- `reason=stale_chain_head`（链头过期）：`sequence` 或 `previous_digest` 不是
  当前链头的后继。用 `expected_sequence` 与 `expected_previous_digest` 重建
  批次重发；若本地链与服务端不一致，以 `GET /field-devices/{device_id}/batches`
  分页核对服务端已接受的批次；
- `reason=payload_changed`（载荷变化）：同一 `batch_id` 或序号已以**不同**载荷
  记录。`detail.existing_batch` 给出既有批次的 `sequence`/`batch_id`/`digest`，
  可用 `GET /field-devices/{device_id}/batches/{sequence}` 核对内容；确认已
  送达后跳到下一批，切勿以新载荷覆盖原编号。

**游标与批次查询**：

```bash
# 分页列出批次（after_sequence 为上一页响应的 next_cursor）
# 页顶带 archived_through；每条批次带 status（accepted/archived 墓碑）、
# replay_count、last_replayed_at
curl -s "http://localhost:8000/field-devices/dev-01/batches?limit=2"
curl -s "http://localhost:8000/field-devices/dev-01/batches?after_sequence=2&limit=2"

# 单条批次详情：未归档时返回观测时间戳、/judge 四项阈值与重放计数；
# 已归档时为墓碑（status=archived、archived=true），observations/thresholds 为 null
curl -s http://localhost:8000/field-devices/dev-01/batches/2
```

### 7. 多处理方确认与安全归档水位（safe_sequence）

同一台设备的批次会被多个下游处理方消费。服务端为每台设备维护必需消费者
登记、稀疏确认与连续水位，给出**全部处理方都确认完毕、可安全归档的连续
前缀**：

- **登记必需消费者**（只增不删）：
  `POST /field-devices/{device_id}/consumers`，请求体 `{"consumer_id": "..."}`。
  登记后该消费者即参与 `safe_sequence` 最小值计算；重复登记同一
  `consumer_id` 幂等（HTTP 200，`status=already_registered`）。消费者可在任何
  批次上传之前登记；
- **确认批次**：`POST /field-devices/{device_id}/acknowledgments`，请求体为
  `consumer_id`、`sequence`、`batch_digest`（即上传响应中的 `digest`）。
  允许**乱序、并发、重复**提交：
  - 仅接受**已存在且摘要相符**的批次：批次不存在返回 404，消费者未登记或
    摘要冲突返回 409（`reason=digest_mismatch`，`detail.expected_digest`
    给出服务端保存的摘要）；
  - 相同确认幂等：新确认 HTTP 201（`status=confirmed`），重复确认 HTTP 200
    （`status=duplicate`，保留首次 `confirmed_at`）；
- **水位推进**：每个消费者的确认是稀疏集合；保存确认与把连续水位
  （`watermark`：从 1 起连续确认到的最大序号）推进至**首个缺口**在**同一个
  SQLite 事务**内完成，写锁 + `BEGIN IMMEDIATE` 串行化并发确认，推进不会
  丢失、不会回退；
- **安全归档水位**：`safe_sequence = min(所有必需消费者的 watermark)`。
  尚无消费者，或任一消费者尚未确认任何批次时为 `0`；只有前缀
  `1..safe_sequence` 可安全归档。确认只追加记录，**不删除任何原批次**。

**多处理方确认示例**（两个处理方 `archiver` 与 `ml-pipeline` 各跑各的，
乱序确认互不影响）：

```bash
DEV=http://localhost:8000/field-devices/dev-01

# ① 登记两个必需消费者（登记后不可删除；重复登记幂等）
curl -s -X POST $DEV/consumers -H 'Content-Type: application/json' \
  -d '{"consumer_id": "archiver"}'
# → 201 {"status": "registered", "consumer_id": "archiver", "watermark": 0,
#        "archived_through": 0, ...}
curl -s -X POST $DEV/consumers -H 'Content-Type: application/json' \
  -d '{"consumer_id": "ml-pipeline"}'

# ② 设备照常上传链式批次（见上一节），取每批响应中的 digest
#    下面的 $D1 / $D2 分别为 seq1 / seq2 上传响应中的 digest
D1=…; D2=…

# ③ 处理方各自确认（携带 consumer_id / sequence / batch_digest）
#    乱序也安全：archiver 先确认 seq2 不会推进水位（seq1 是缺口）
curl -s -X POST $DEV/acknowledgments -H 'Content-Type: application/json' \
  -d "{\"consumer_id\": \"archiver\", \"sequence\": 2, \"batch_digest\": \"$D2\"}"
# → 201 {"status": "confirmed", "watermark": 0, "safe_sequence": 0,
#        "archived_through": 0, ...}
curl -s -X POST $DEV/acknowledgments -H 'Content-Type: application/json' \
  -d "{\"consumer_id\": \"archiver\", \"sequence\": 1, \"batch_digest\": \"$D1\"}"
# → 201 {"status": "confirmed", "watermark": 2, "safe_sequence": 0,
#        "archived_through": 0, ...}
#    archiver 已到 2，但 ml-pipeline 尚未确认 → 设备 safe_sequence 仍是 0

# 相同确认重复提交幂等（重试安全）：HTTP 200，status=duplicate
curl -s -X POST $DEV/acknowledgments -H 'Content-Type: application/json' \
  -d "{\"consumer_id\": \"archiver\", \"sequence\": 1, \"batch_digest\": \"$D1\"}"

# 摘要不符 → 409，detail 给出服务端保存的 expected_digest
curl -s -X POST $DEV/acknowledgments -H 'Content-Type: application/json' \
  -d "{\"consumer_id\": \"archiver\", \"sequence\": 1, \"batch_digest\": \"00…00\"}"
# → 409 {"detail": {"error": "consumer_conflict", "reason": "digest_mismatch",
#        "sequence": 1, "provided_digest": "00…00", "expected_digest": "<D1>"}}

# ④ ml-pipeline 也确认 seq1 后，连续前缀推进到 1
curl -s -X POST $DEV/acknowledgments -H 'Content-Type: application/json' \
  -d "{\"consumer_id\": \"ml-pipeline\", \"sequence\": 1, \"batch_digest\": \"$D1\"}"
# → 201 {"status": "confirmed", "watermark": 1, "safe_sequence": 1, ...}
```

**状态查询**：`GET /field-devices/{device_id}/archive-status` 返回链头、
`safe_sequence`、首个缺口、阻塞消费者与每个消费者的水位/缺口：

```bash
curl -s $DEV/archive-status
```

```json
{
  "device_id": "dev-01",
  "head_sequence": 2,
  "consumer_count": 2,
  "safe_sequence": 1,
  "first_gap": 2,
  "blocking_consumers": ["ml-pipeline"],
  "consumers": [
    {"consumer_id": "archiver", "watermark": 2, "first_gap": null,
     "registered_at": "…"},
    {"consumer_id": "ml-pipeline", "watermark": 1, "first_gap": 2,
     "registered_at": "…"}
  ],
  "archived_through": 0,
  "archived_at": null
}
```

| 字段 | 说明 |
| --- | --- |
| `safe_sequence` | 所有必需消费者水位的最小值；可安全归档的连续前缀 `1..safe_sequence`。无消费者或任一消费者水位为 0 时为 `0` |
| `first_gap` | 安全前缀之后的首个待归档序号（恒为 `safe_sequence + 1`） |
| `blocking_consumers` | 水位等于该最小值且尚未追平链头的消费者（按编号排序）；全部追平或链上无批次时为空列表 |
| `consumers[].watermark` | 该消费者从 1 起连续确认到的最大序号（乱序确认只填空缺，不越过首个缺口）；归档之后才登记的消费者从 `archived_through` 起步 |
| `consumers[].first_gap` | 该消费者水位之后的首个缺口；已追平链头时为 `null`；归档后新登记的消费者首个缺口为 `archived_through + 1` |
| `archived_through` | 已归档连续前缀的右端序号（只进不退，初始 `0`） |
| `archived_at` | 最近一次归档时刻（UTC ISO-8601）；从未归档为 `null` |

归档作业轮询该接口并归档 `sequence <= safe_sequence` 的批次；`consumers`
与 `blocking_consumers` 可用于定位卡在哪个处理方。确认记录始终保留；归档
只清空批次明细载荷（见下节），不删除批次骨架。

### 8. 归档连续前缀（清除已被全部必需消费者处理的明细载荷）

当全部必需消费者都已确认某个连续前缀（`safe_sequence > archived_through`），
设备可调用归档接口清空这些批次的明细载荷，回收存储：

```bash
DEV=http://localhost:8000/field-devices/dev-01

# 归档连续前缀 1..through_sequence（上例 safe_sequence=1）
curl -s -X POST $DEV/archive -H 'Content-Type: application/json' \
  -d '{"through_sequence": 1}'
```

```json
{
  "status": "archived",
  "device_id": "dev-01",
  "requested_through": 1,
  "archived_through": 1,
  "archived_at": "2026-10-01T06:00:00+00:00",
  "newly_archived_count": 1,
  "head_sequence": 2,
  "safe_sequence": 1,
  "first_gap": 2,
  "blocking_consumers": ["ml-pipeline"],
  "consumers": [ …同 archive-status… ]
}
```

- **归档内容**：目标范围内批次的 `observations` 与四项阈值被清空，批次成为
  墓碑（`status="archived"`、`archived=true`、带 `archived_at`），只保留
  `batch_id`、`sequence`、`previous_digest`、`digest`、`observation_count`、
  `created_at`、首次受理响应快照与 `replay_count` / `last_replayed_at`。
  链头摘要、`batch_count` 与后续上传完全不变；
- **越界冲突（409）**：`through_sequence` 不得超过**同一写事务内**核定的
  `safe_sequence`。越界返回 409，`detail` 给出当前水位与阻塞消费者，作业
  应等阻塞消费者追平后重试同一目标：

```json
{
  "detail": {
    "error": "archive_conflict",
    "reason": "beyond_safe_sequence",
    "message": "cannot archive through sequence 3: it exceeds safe_sequence 1 …",
    "requested_through": 3,
    "safe_sequence": 1,
    "archived_through": 0,
    "head_sequence": 2,
    "first_gap": 2,
    "blocking_consumers": ["ml-pipeline"]
  }
}
```

- **幂等**：重复相同目标或更小的目标返回 HTTP 200、
  `status="already_archived"`，`archived_through` 与 `archived_at` 均不变，
  `newly_archived_count=0`；归档水位**只进不退**；
- **并发安全**：归档与批次确认、消费者登记、批次上传共用同一写锁串行点
  （`BEGIN IMMEDIATE`）：并发确认不会让归档越过未确认批次，登记新消费者
  后归档水位也不会越过它的水位；
- **已归档批次仍可重放**：设备断网恢复后重发已归档批次，服务端按保存的
  摘要判定——同载荷返回原结果（`status=replayed`）并原子累加
  `replay_count`、刷新 `last_replayed_at`，墓碑与链头不变；载荷变化仍
  返回 409（`reason=payload_changed`，附既有 `digest`）；
- **已归档批次的确认幂等**：对墓碑批次以正确摘要确认返回 200
  `duplicate`，错误摘要返回 409（`expected_digest` 仍可取自墓碑）；确认
  不使任何水位回退；
- **归档后登记的新消费者**：水位直接初始化为当前 `archived_through`，
  无需逐个确认明细已清空的旧批次，其 `first_gap` 指向归档前缀的下一批；
  它一旦落后链头，`safe_sequence` 会回落到其水位（但永不低于
  `archived_through`），归档随之被挡住直到它追平。

列表、详情、游标与 `archive-status` 均返回 `archived_through`；墓碑在列表
与详情中均明确标注 `status="archived"`（另有布尔字段 `archived`）。

### 8.5 消费者批次处理租约（领取 → 处理 → 完成，区间不重复分发）

下游消费者（如多个并行处理进程）此前只能各自挑批次、自行确认，容易重复读取
同一设备链。已登记消费者可领取**批次处理租约**：服务在单个写事务内把一段
**未归档的连续区间**交给该消费者并冻结每批摘要，消费者处理完再一次性核对、
确认并推进水位。区间在租约存活期间不会被再次分发，归档也不会清除其明细。

**领取租约**：`POST /field-devices/{device_id}/batch-leases`

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `consumer_id` | `string` | 已登记的必需消费者编号（未登记 409） |
| `max_batches` | `int`（1~20） | 本次最多领取批数；实际从水位的下一批起取连续区间 |
| `lease_duration_seconds` | `int`（1~3600） | 租约有效期（秒） |
| `idempotency_key` | `string`（1~128） | 客户端幂等键；网络重试请复用同一键 |

服务在**同一个 SQLite 写事务**（`BEGIN IMMEDIATE`）内：读取该消费者连续水位
`watermark` → 从 `watermark + 1` 起沿序号取**连续存在且未归档**的批次，直到
首个缺口、墓碑或 `max_batches` → 保存租约（起止序号、领取时水位、到期时刻）
与每批摘要。写锁与部分唯一索引保证同一消费者至多持有一个在持租约，**同一区间
不会分给两个租约**。

```bash
DEV=http://localhost:8000/field-devices/dev-01

# ① 消费者登记（沿用必需消费者登记接口）
curl -s -X POST $DEV/consumers -H 'Content-Type: application/json' \
  -d '{"consumer_id": "worker-a"}'

# ② 领取：最多 20 批、租约 900 秒
curl -s -X POST $DEV/batch-leases -H 'Content-Type: application/json' -d '{
    "consumer_id": "worker-a",
    "max_batches": 20,
    "lease_duration_seconds": 900,
    "idempotency_key": "worker-a-2026-10-03T08:00Z"
  }'
```

```json
{
  "status": "leased",
  "lease": {
    "lease_id": 1,
    "device_id": "dev-01",
    "consumer_id": "worker-a",
    "idempotency_key": "worker-a-2026-10-03T08:00Z",
    "status": "held",
    "from_sequence": 1,
    "to_sequence": 3,
    "batch_count": 3,
    "watermark_start": 0,
    "lease_duration_seconds": 900,
    "granted_at": "2026-10-03T08:00:00+00:00",
    "expires_at": "2026-10-03T08:15:00+00:00",
    "completed_at": null,
    "revoked_at": null,
    "batches": [
      {"sequence": 1, "batch_id": "b-001", "previous_digest": "",
       "digest": "49740f…", "lease_digest": "49740f…", "observation_count": 4,
       "archived": false,
       "observations": [{"local_id": "o000", "captured_at": 1700000000.0}, "…"],
       "thresholds": {"tolerance": 0.5, "max_drift": 0.0005, "max_missed": 3, "max_false": 3}},
      {"sequence": 2, "…": "…"},
      {"sequence": 3, "…": "…"}
    ]
  }
}
```

- 水位之后暂无可领取批次（已追平链头，或下一批是墓碑/缺口）时不创建租约，
  返回 HTTP 200、`status="nothing_to_claim"`、`lease=null`（可反复请求，
  不占用幂等键）；
- **幂等与持约冲突**：同一消费者已有在持租约时——相同幂等键重试返回原租约
  （HTTP 200，`lease_id`/区间/授予时刻均不变，即使改了 `max_batches` 或
  TTL）；其他幂等键返回 **409**：

```bash
# 相同键重试：安全拿回原租约（200）
curl -s -X POST $DEV/batch-leases -H 'Content-Type: application/json' -d '{
    "consumer_id": "worker-a", "max_batches": 20,
    "lease_duration_seconds": 900, "idempotency_key": "worker-a-2026-10-03T08:00Z"}'

# 换一个键（或另一个进程也来领取）：409 active_lease_exists
curl -s -X POST $DEV/batch-leases -H 'Content-Type: application/json' -d '{
    "consumer_id": "worker-a", "max_batches": 20,
    "lease_duration_seconds": 900, "idempotency_key": "stale-retry-001"}'
```

```json
{
  "detail": {
    "error": "lease_conflict",
    "reason": "active_lease_exists",
    "message": "consumer 'worker-a' already holds lease 1 on device 'dev-01'; …",
    "device_id": "dev-01",
    "consumer_id": "worker-a",
    "active_lease_id": 1,
    "expires_at": "2026-10-03T08:15:00+00:00",
    "expired": false,
    "active_lease": {"lease_id": 1, "from_sequence": 1, "to_sequence": 3, "…": "…"}
  }
}
```

租约到期后他键返回 409、`reason="lease_expired"`（`expired=true`，附原租约）；
相同键的网络重试即使在过期后仍可取回原租约（`status="expired"`）。不同消费者
之间允许领取重叠区间——安全归档仍以全部消费者的共同确认为准。

**完成租约**：`POST …/batch-leases/{lease_id}/complete`，请求体为
`consumer_id` 与租约内每批的 `{sequence, batch_digest}`。服务在**同一写事务**
内逐项核对：提交项的序号集合与顺序必须与租约完全一致，每个摘要必须等于租约
冻结的摘要。全部通过后才幂等写入各批确认（兼容租约期间的手工确认）、把连续
水位推进至首个缺口，并把租约置为 `completed`（区间释放）。

```bash
# ③ 处理完三批后完成（digest 取自领取响应中各批的 digest）
curl -s -X POST $DEV/batch-leases/1/complete -H 'Content-Type: application/json' -d '{
    "consumer_id": "worker-a",
    "items": [
      {"sequence": 1, "batch_digest": "49740f…"},
      {"sequence": 2, "batch_digest": "5e7aa3…"},
      {"sequence": 3, "batch_digest": "9c10bd…"}
    ]
  }'
```

```json
{
  "status": "completed",
  "lease": {"lease_id": 1, "status": "completed", "from_sequence": 1,
            "to_sequence": 3, "completed_at": "…", "…": "…"},
  "watermark": 3,
  "confirmed_count": 3,
  "completed_at": "2026-10-03T08:01:30+00:00",
  "safe_sequence": 3,
  "first_gap": 4,
  "blocking_consumers": [],
  "consumers": ["…同 archive-status…"],
  "held_leases": [],
  "lease_bound": null
}
```

任一核对失败都**整体拒绝（409），不写任何确认、不推进水位，租约保持 held**：

| `reason` | 触发条件 | detail 关键字段 |
| --- | --- | --- |
| `items_mismatch` | 序号缺失、多余或顺序不符 | `expected_sequences` / `missing_sequences` / `unexpected_sequences` |
| `digest_changed` | 某批摘要与租约冻结值不同（批次内容已变） | `sequence` / `provided_digest` / `lease_digest` / `current_digest` |
| `lease_expired` | 租约已过到期时刻（处理超时） | `expires_at` |
| `lease_reclaimed` | 租约已被显式回收 | `revoked_at` |
| `lease_already_completed` | 重复完成 | `completed_at` |
| `lease_consumer_mismatch` | `consumer_id` 不是租约所有者 | `owner_consumer_id` |

完成成功后区间立即释放，下一次领取从新水位（示例中为 4）继续。

**过期回收**：过期租约仍占用其区间（防止处理进程其实还在跑），必须由所属消费
者显式回收后才能重新领取，归档也会继续被它挡住：

```bash
# 过期后回收（未过期返回 409 lease_not_expired；重复回收幂等 200 already_reclaimed）
curl -s -X POST $DEV/batch-leases/1/recover -H 'Content-Type: application/json' \
  -d '{"consumer_id": "worker-a"}'
# → 201 {"status": "reclaimed", "lease": {"status": "revoked", …}, "reclaimed_at": "…"}
# 随后即可用新幂等键重新领取 1..3
```

**租约与归档的关系**：归档除受 `safe_sequence` 约束外，**不得清除仍被租约
占用的明细**——只要目标范围内存在任何 `held` 租约（含已过期但尚未回收的），
归档返回 409、`reason="beyond_held_leases"`，附 `lease_bound`（不侵犯租约的
前提下可归档到的最大序号）与 `blocking_leases`（`lease_id` / `consumer_id` /
区间 / 是否过期）。`GET …/archive-status` 与归档响应都带 `held_leases` 与
`lease_bound`（无在持租约时为 `null`）。

**租约查询**：

```bash
# 分页摘要（after_id 游标；可按 consumer_id 与状态过滤）
curl -s "$DEV/batch-leases?limit=20"
curl -s "$DEV/batch-leases?consumer_id=worker-a&status=held"      # 在有效期的在持租约
curl -s "$DEV/batch-leases?status=expired"                        # 已过期未回收
curl -s "$DEV/batch-leases?status=completed"

# 单条详情：状态、起止序号、watermark_start、各批摘要与观测明细
curl -s "$DEV/batch-leases/1"
```

租约状态：`held`（在有效期内持有）、`expired`（已过期、尚未回收，仍占用
区间）、`completed`（已完成并释放）、`revoked`（过期后显式回收）。租约只是
**领取与核对**机制：消费者也可以继续使用 §7 的逐条确认接口（完成租约时会幂等
并入这些确认）。

### 9. 连续批次归档前巡检判定（一次可复核的判定来源）

现场设备上传的连续批次在**归档之前**，可按设备与起止 `sequence` 形成一次
可复核的巡检判定——把多批观测合并为一条序列，用与 `/judge` 完全相同的规则
裁决，并把区间、各批摘要、合并观测、阈值、激活库版本与底层判定 id 固化为
**不可变来源**；批次随后归档（明细被清空）也不影响已保存判定的复核。

**请求体**：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `idempotency_key` | `string`（1~128） | 客户端生成的幂等键（全局唯一；网络重试复用同一键） |
| `from_sequence` | `int≥1` | 判定区间起始批次序号（含），不得大于 `to_sequence` |
| `to_sequence` | `int≥1` | 判定区间结束批次序号（含） |

服务在**同一个写事务**内串行完成校验、固定版本、判定与落库：

1. **幂等键**：相同键 + 相同设备/范围重试直接返回原判定
   （HTTP 200，`batch_judgment_id` / `source_judgment_id` / `created_at`
   均不变）；相同键被**其他设备或其他范围**占用时返回 409，`detail` 给出
   `occupied_device_id` / `occupied_from_sequence` /
   `occupied_to_sequence` / `occupied_by_batch_judgment_id`；
2. **区间前置校验**（任一不满足即整体 409 拒绝，**不落任何判定**）：
   - `range_not_continuous`：区间内存在缺失序号，`detail.missing_sequences`
     与 `head_sequence` 列出缺口与当前链头；
   - `range_archived`：区间内有序号已是归档墓碑（明细不在），
     `detail.archived_sequences` 列出墓碑序号；
   - `thresholds_mismatch`：各批保存的四项阈值
     （`tolerance` / `max_drift` / `max_missed` / `max_false`）不完全一致，
     `detail.reference_sequence` / `mismatched_sequences` 给出基准与分歧批次；
3. **固定激活版本**：在持写锁的事务内读取当前激活特征库版本与完整快照，
   判定与保存之间不可能发生版本切换；响应与记录中的 `library_version`
   即本次判定固定的版本（无激活版本时为 `null`，按无合格候选处理，仍落判定）；
4. **合并观测**：把各批观测按 `(captured_at, sequence, local_id)` 全局排序
   合并——不同批次同一时刻的观测以序号区分先后，同一批内保持采集顺序；
   判定只取时间戳序列，合并后的每条观测同时保留来源 `sequence` 以便复核；
5. **复用 /judge**：以各批共同的四项阈值执行与 `/judge` 完全相同的判定
   算法（`library_source="stored"`），结果写入 `judgments`（可在
   `GET /judgments/{id}` 与草稿回放报告中复核），再保存本次连续批次判定的
   不可变来源。

```bash
DEV=http://localhost:8000/field-devices/dev-01

# 先确认批次 1..3 已上传（观测跨批连续），然后对区间创建判定
curl -s -X POST $DEV/batch-judgments \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key": "dev-01-1-3-2026-10-01",
       "from_sequence": 1, "to_sequence": 3}'
```

```json
{
  "batch_judgment_id": 1,
  "idempotency_key": "dev-01-1-3-2026-10-01",
  "device_id": "dev-01",
  "from_sequence": 1,
  "to_sequence": 3,
  "library_version": 2,
  "observation_count": 12,
  "thresholds": {"tolerance": 0.5, "max_drift": 0.0005, "max_missed": 3, "max_false": 3},
  "observations": [
    {"captured_at": 1700000000.0, "sequence": 1, "local_id": "o000"},
    {"captured_at": 1700000000.5, "sequence": 1, "local_id": "o001"},
    {"captured_at": 1700000001.0, "sequence": 2, "local_id": "o004"}
  ],
  "source_judgment_id": 17,
  "created_at": "2026-10-01T06:00:00+00:00",
  "batches": [
    {"sequence": 1, "batch_id": "b-001", "previous_digest": "",
     "digest": "49740f…", "observation_count": 4},
    {"sequence": 2, "batch_id": "b-002", "previous_digest": "49740f…",
     "digest": "5e7aa3…", "observation_count": 4},
    {"sequence": 3, "batch_id": "b-003", "previous_digest": "5e7aa3…",
     "digest": "9c10bd…", "observation_count": 4}
  ],
  "judgment": {
    "status": "matched",
    "reason": null,
    "library_source": "stored",
    "library_version": 2,
    "observation_count": 12,
    "parameters": {"tolerance": 0.5, "max_drift": 0.0005, "max_missed": 3, "max_false": 3},
    "best": {"aid_id": "CN-001", "matched": 12, "missed": 0, "false": 0, "…": "…"},
    "candidates": [ … ],
    "judgment_id": 17
  }
}
```

- **重试幂等**：同一 `idempotency_key` 原样重发返回 HTTP 200 与同一份结果，
  不会重复判定或重复落库；
- **改范围/换设备 409**：同一键更换 `from_sequence` / `to_sequence` 或设备
  时返回 409（`reason=idempotency_key_in_use`），换用新键即可；
- **并发不引用半个范围**：判定与批次上传、前缀归档共用同一写锁串行点
  （`BEGIN IMMEDIATE`）+ 幂等键唯一约束兜底。判定进行中的归档必须等其提交
  后才能清空明细；判定进行中的上传要么尚未进入区间（整体 409 拒绝），
  要么完整进入区间（覆盖全范围）。任何已保存判定都满足
  `observation_count == Σ batches[].observation_count`，绝不会只引用半个
  范围；同键并发请求恰好一份 201，其余得到同一份 200；
- **归档后仍可复核**：区间内批次之后被归档为墓碑，不改变已保存的
  `observations` / `batches` / `judgment`，按 id 仍可查询完整来源；底层
  判定行也可经 `source_judgment_id` 在 `GET /judgments/{id}` 回溯，并可被
  草稿回放报告重放评估。

**结果查询**：

```bash
# 分页列表（摘要：序号范围、阈值、library_version、source_judgment_id）
curl -s "$DEV/batch-judgments?limit=20"
curl -s "$DEV/batch-judgments?after_id=1&limit=20"

# 单次详情：不可变来源（各批摘要 + 合并观测 + 完整判定证据）
curl -s "$DEV/batch-judgments/1"
# 不属于该设备的 id 返回 404
```

### 9.5 连续批次判定复核报告（确认已保存判定仍能从原始链数据重现）

设备离线补传、归档或数据库迁移之后，运维需要确认已保存的批次判定**仍能从
当前批次链数据重现**。`POST …/batch-judgments/{id}/review-reports` 对一次
连续批次判定做**只读复核**并保存可幂等的复核报告：服务在**单个只读
SQLite 事务**（`PRAGMA query_only` 兜底）内固定快照，读取该判定的不可变
记录、各批摘要与保存的 `library_version`，逐批核对当前批次链；复核与
重跑**不写入 `judgments`**，只把报告写入复核报告表。

**请求体**：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `idempotency_key` | `string`（1~128） | 客户端生成的幂等键（全局唯一；网络重试复用同一键） |

**复核流程**（全部读取来自事务开始时固定的同一快照）：

1. 固定 `batch_judgment_id` 的不可变来源：序号范围、各批摘要
   （`batch_id` / `previous_digest` / `digest` / 条数）、合并观测、四项
   阈值与保存的 `library_version`；
2. **逐批核对当前批次**：批次缺失 → `batch_missing`；已归档墓碑只核对
   摘要骨架（明细已清空 → `batch_archived`）；明细仍在的批次核对保存
   摘要、前序摘要、`batch_id`、条数与四项阈值，并按保存明细**重算规范
   SHA-256**（防止明细被改写后连同摘要一起伪造）；另核对范围内相邻批次
   的链式引用与区间首批的链锚（`from_sequence - 1` 批次的当前摘要）；
3. **只有所有明细可用且摘要、前序链、阈值一致**时，才用**保存版本**的
   特征库（`library_version` 对应的不可变快照，与当前激活版本无关）重跑
   与 `/judge` 完全相同的判定算法，并逐字段比较新旧裁决（`status` /
   `reason` / `best.aid_id` / `best.matched` / `best.missed` /
   `best.false` / `best.offset` / `best.drift`）。

**结论**（`status`）：

| 结论 | 含义 |
| --- | --- |
| `unchanged` | 来源核对全部通过，重跑结果与保存判定一致 |
| `changed` | 来源核对全部通过，但重跑结果与保存判定不一致；`differences` 给出差异字段的原/新值，`saved` / `current` 给出两侧候选证据 |
| `source_unavailable` | 来源缺失：批次不存在、批次已归档为墓碑、底层判定记录或保存的特征库版本缺失、前序链锚缺失；附 `issues` 清单，不重跑 |
| `source_mismatch` | 摘要/前序链/阈值与保存来源不符；附 `issues` 清单，不重跑（与缺失并存时以不符为准） |

```bash
DEV=http://localhost:8000/field-devices/dev-01

# 对判定 1 创建复核报告（网络重试请复用同一幂等键）
curl -s -X POST $DEV/batch-judgments/1/review-reports \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key": "review-2026-10-04-01"}' | python3 -m json.tool
```

```json
{
  "report_id": 1,
  "idempotency_key": "review-2026-10-04-01",
  "device_id": "dev-01",
  "batch_judgment_id": 1,
  "created_at": "2026-10-04T06:00:00+00:00",
  "status": "unchanged",
  "source_judgment_id": 17,
  "from_sequence": 1,
  "to_sequence": 3,
  "library_version": 2,
  "checked_batches": 3,
  "archived_batches": 0,
  "issue_count": 0,
  "issues": [],
  "differences": [],
  "saved": {
    "judgment_id": 17,
    "status": "matched",
    "reason": null,
    "best_aid_id": "CN-001",
    "best": {"aid_id": "CN-001", "matched": 12, "…": "…"}
  },
  "current": {
    "status": "matched",
    "reason": null,
    "best_aid_id": "CN-001",
    "best": {"aid_id": "CN-001", "matched": 12, "…": "…"}
  }
}
```

`changed` 时 `differences` 逐项给出差异字段与原/新值（`saved` 为库中
保存的判定响应，`current` 为从当前链数据重跑的结果）：

```json
{
  "status": "changed",
  "differences": [
    {"field": "best.aid_id", "saved": "CN-009", "current": "CN-001"},
    {"field": "best.matched", "saved": 3, "current": 12}
  ],
  "saved":    {"status": "matched", "best_aid_id": "CN-009", "best": {"…": "…"}},
  "current":  {"status": "matched", "best_aid_id": "CN-001", "best": {"…": "…"}}
}
```

`source_unavailable` / `source_mismatch` 时 `issues` 逐条给出
`issue_code` / `sequence` / `expected` / `actual`，`current` 为 `null`：

| `issue_code` | 类别 | 含义 |
| --- | --- | --- |
| `batch_missing` | 缺失 | 来源批次已不存在 |
| `batch_archived` | 缺失 | 批次已归档为墓碑：摘要骨架已核对，明细不可得 |
| `judgment_record_missing` | 缺失 | 底层 `judgments` 记录缺失 |
| `library_version_missing` | 缺失 | 保存的特征库版本缺失 |
| `previous_chain_anchor_missing` | 缺失 | 区间首批的链锚（`from_sequence - 1`）缺失 |
| `digest_mismatch` / `digest_recompute_mismatch` | 不符 | 当前摘要 ≠ 保存摘要 / 明细重算 ≠ 当前摘要 |
| `previous_digest_mismatch` / `previous_chain_broken` | 不符 | 前序摘要 ≠ 保存值 / 链式引用断裂 |
| `thresholds_mismatch` | 不符 | 批次四项阈值 ≠ 判定保存的阈值 |
| `batch_id_mismatch` / `observation_count_mismatch` | 不符 | `batch_id` / 条数与保存来源不符 |
| `observations_mismatch` | 不符 | 逐批摘要全部相符但合并观测 ≠ 保存来源（防御性核对） |

- **幂等**：相同 `idempotency_key` + 相同设备/判定重试返回**原报告**
  （HTTP 200，`report_id` / 结论 / `created_at` 均不变，即使库已随后
  变化也不会重新复核）；相同键改用**其他设备或其他判定目标**返回
  `409 idempotency_key_in_use`，`detail` 给出占用方
  `occupied_by_report_id` / `occupied_device_id` /
  `occupied_batch_judgment_id`；目标判定不存在返回 404（不占键）；
- **并发安全**：幂等键全局唯一约束兜底，同键并发请求恰好一份 201，
  其余得到同一份 200；
- **只读复核**：复核在只读快照内读取与重跑，不写 `judgments`、不改
  批次/链/消费者任何数据；设备隔离期间仍可使用（与 `verify-chain`
  同属核验类接口）。

**报告查询**：

```bash
# 分页列出某次判定的复核报告（摘要：结论、来源判定 id、库版本、问题/差异计数）
curl -s "$DEV/batch-judgments/1/review-reports?limit=20"
curl -s "$DEV/batch-judgments/1/review-reports?after_id=1&limit=20"

# 单份报告详情（问题清单、差异字段与原/新候选证据）
curl -s "$DEV/batch-judgments/1/review-reports/1"
```

### 10. 设备链一致性核验（离线补传 / 归档 / 迁移后的运维核对）

设备离线补传、前缀归档或数据库迁移之后，运维需要确认批次链仍可作为判定
来源。`POST /field-devices/{device_id}/verify-chain` 在**单个只读 SQLite
事务**中固定快照，对链做一次全面一致性核验——核验**只读不改数据**，发现
问题只报告、不修复。

**请求体**：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `from_sequence` | `int≥1` | 核验区间起始批次序号（含），不得大于 `to_sequence` |
| `to_sequence` | `int≥1` | 核验区间结束批次序号（含） |

```bash
DEV=http://localhost:8000/field-devices/dev-01

# 核验批次 1..10（范围内每个序号都必须已存在）
curl -s -X POST $DEV/verify-chain \
  -H 'Content-Type: application/json' \
  -d '{"from_sequence": 1, "to_sequence": 10}' | python3 -m json.tool
```

```json
{
  "device_id": "dev-01",
  "from_sequence": 1,
  "to_sequence": 10,
  "ok": true,
  "issue_count": 0,
  "issues": [],
  "checked_batches": 10,
  "recomputed_batches": 7,
  "archived_batches": 3,
  "checked_acknowledgments": 14,
  "checked_judgment_sources": 4,
  "snapshot": {
    "head_sequence": 12,
    "head_digest": "9c10bd…",
    "batch_count": 12,
    "archived_through": 3,
    "safe_sequence": 9,
    "consumer_count": 2,
    "batch_judgment_count": 2
  }
}
```

**核验内容**（全部在事务开始时固定的同一快照内读取）：

- **范围内未归档批次**：按保存的观测与四项阈值**重算规范 SHA-256**（与上传
  时相同的规范化规则），与保存的 `digest` 比对；并核对 `observation_count`
  与实际观测条数；
- **归档批次（墓碑）**：明细已清空，**不重算**，只核对保存的摘要经
  `previous_digest` 链式引用与相邻批次互相印证，以及墓碑边界与骨架；
- **`previous_digest` 链**（全链）：每批保存的前序摘要必须等于上一批保存的
  摘要，首批必须为空串；
- **序号连续性**（全链）：链上序号必须从 1 连续到链头，缺口逐序号报出；
- **链游标**：`batch_count` / `last_sequence` / `head_digest` 必须与批次表
  推导值一致，`archived_through` 不得越过链头；
- **墓碑边界**（全链）：`sequence ≤ archived_through` 必须为墓碑（明细已
  清空、状态一致），其后必须不是墓碑；
- **消费者**：每条确认保存的 `batch_digest` 必须等于该批次当前摘要；水位
  不得越过链头、不得低于 `archived_through`，且必须已按稀疏确认推进到
  首个缺口（乱序确认形成的合法缺口不报问题）；
- **连续批次判定来源**：`batch_judgment_batches` 保存的各批
  `digest` / `previous_digest` / `batch_id` / `observation_count` 必须与
  当前批次一致。

结构核对（链式引用、序号、游标、边界、确认、判定来源）覆盖**整条链**；
规范 SHA-256 重算只针对**请求范围内**的未归档批次。`snapshot` 给出核验
时刻从批次表推导的**快照上限**（`head_sequence`/`head_digest`）与归档、
水位上下文——即使只核验子区间，也能看到当时整条链延伸到哪里。

**问题清单**：`ok=false` 时 `issues` 逐条给出
`issue_code` / `sequence` / `expected` / `actual`（消费者问题附
`consumer_id`，判定来源问题附 `batch_judgment_id`；`expected` 为核验
推导值、`actual` 为库中保存值；链级问题的 `sequence` 为 `null`）：

```json
{
  "ok": false,
  "issue_count": 2,
  "issues": [
    {"issue_code": "digest_mismatch", "sequence": 4,
     "expected": "49740f…", "actual": "00ab3d…"},
    {"issue_code": "watermark_not_advanced", "sequence": 6,
     "expected": 8, "actual": 5, "consumer_id": "ml-pipeline"}
  ]
}
```

| `issue_code` | 含义 |
| --- | --- |
| `digest_mismatch` | 范围内未归档批次重算摘要 ≠ 保存摘要 |
| `observation_count_mismatch` | 保存的条数 ≠ 实际观测条数 |
| `batch_details_missing` | 非墓碑批次缺观测/阈值明细，无法重算 |
| `previous_digest_mismatch` | 保存的前序摘要 ≠ 上一批保存的摘要（首批应为空串） |
| `sequence_gap` | 链上序号缺口（范围外缺口也报出） |
| `batch_count_mismatch` / `last_sequence_mismatch` / `head_digest_mismatch` | 链游标与批次表推导值不一致 |
| `head_batch_missing` / `chain_cursor_missing` | 游标指向的链头批次不存在 / 链游标行缺失 |
| `archived_through_beyond_head` | 归档水位越过链头 |
| `tombstone_boundary_violation` | 批次归档状态与 `archived_through` 边界不符 |
| `tombstone_status_mismatch` / `tombstone_details_present` | 墓碑骨架不一致：状态列与归档时刻矛盾 / 明细未清空 |
| `ack_digest_mismatch` / `ack_batch_missing` | 确认摘要与批次当前摘要不符 / 确认指向不存在的批次 |
| `watermark_beyond_head` / `watermark_below_archived_through` / `watermark_not_advanced` | 水位越过链头 / 低于归档水位 / 未按稀疏确认推进到首个缺口 |
| `judgment_source_digest_mismatch` 等 | 连续批次判定来源保存的摘要（或 `previous_digest`/`batch_id`/`observation_count`）与当前批次不一致；来源批次缺失为 `judgment_source_batch_missing` |

**错误响应（4xx）**：

- `422`：范围非法（`from_sequence < 1`、`from_sequence > to_sequence` 等，
  由请求校验给出）；
- `404 device_not_found`：设备没有任何批次（空设备）；
- `404 batches_not_found`：范围内缺批次（含右端越过链头），`detail` 附
  `head_sequence`、`missing_sequences`（缺失序号示例，至多 50 条）与
  `missing_count`。

**并发与只读保证**：核验在单个只读事务（`PRAGMA query_only` 兜底）内完成，
并发上传、确认或归档期间，一次核验的全部读取都来自事务开始时固定的同一
快照——每个写操作本身单事务原子提交，故核验结果恒为某一时刻的完整一致
状态；核验不重写任何表，发现问题后数据库保持原样，可重复核验比对。

### 11. 设备异常隔离与解除（核验发现异常后的应急闸门）

链核验（上一节）发现摘要、序号或确认水位异常时，运维需要**先阻止继续处理**
以免扩大损坏：设备还在上传、消费者还在确认、归档还在推进，都会让异常扩散。
`POST /field-devices/{device_id}/isolation` 提供可幂等的**隔离**能力，
`POST /field-devices/{device_id}/isolation/release` 提供带核验闸门的**解除**
能力；每次隔离/解除都记录为**不可变事件**，可随时审计。

**隔离请求体**：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `head_sequence` | `int≥0` | 当前链头序号（无批次设备为 0；取自考核时的 `GET …/cursor`） |
| `head_digest` | `string` | 当前链头摘要（64 位十六进制；无批次设备为空串） |
| `reason` | `string` | 隔离原因（运维备注，随事件不可变保存） |
| `idempotency_key` | `string` | 客户端幂等键（设备内按动作唯一；网络重试复用同一键） |

```bash
DEV=http://localhost:8000/field-devices/dev-01

# ① 核验发现异常（例如 digest_mismatch），先读取当前链头
curl -s $DEV/cursor | python3 -m json.tool
# → {"last_sequence": 12, "head_digest": "9c10bd…", …}

# ② 隔离设备：提交链头 + 原因 + 幂等键（同一写事务核对链头并记录不可变事件）
curl -s -X POST $DEV/isolation \
  -H 'Content-Type: application/json' \
  -d '{"head_sequence": 12, "head_digest": "9c10bd…",
       "reason": "verify-chain 报 digest_mismatch at seq 7",
       "idempotency_key": "iso-20261004-01"}' | python3 -m json.tool
```

```json
{
  "status": "isolated",
  "device_id": "dev-01",
  "isolation": {
    "event_id": 1,
    "action": "isolated",
    "reason": "verify-chain 报 digest_mismatch at seq 7",
    "idempotency_key": "iso-20261004-01",
    "head_sequence": 12,
    "head_digest": "9c10bd…",
    "verify_from": null,
    "verify_to": null,
    "verification": null,
    "created_at": "2026-10-04T06:00:00+00:00"
  }
}
```

- 链头已被其他请求推进（`head_sequence`/`head_digest` 不再匹配）→
  `409 stale_chain_head`，`detail` 附服务端当前链头，重读游标后重试；
- 相同幂等键 + 相同参数重试 → `200` 返回原事件（安全重试）；
- 设备已在隔离中（换了幂等键）→ `409 already_isolated`，附生效中的隔离事件；
- 幂等键已被占用（参数不同，或该事件已随后续解除/再隔离推进）→
  `409 idempotency_key_in_use`。

**隔离期间**：以下处理类接口对隔离设备一律返回 `409 device_isolated`
（`detail` 附隔离事件 id、原因、时刻与链头）——

- `POST …/batches`（上传批次，含相同载荷重放）；
- `POST …/acknowledgments`（消费者确认）；
- `POST …/archive`（归档连续前缀）；
- `POST …/batch-leases`（租约领取）与 `POST …/batch-leases/{id}/complete`
  （租约完成）；
- `POST …/batch-judgments`（连续批次判定）。

仍可使用：全部查询接口、`POST …/verify-chain`（链核验）、
`POST …/batch-judgments/{id}/review-reports`（判定复核，只读核验类）、
`POST …/batch-leases/{id}/recover`（回收已过期租约，解除闸门前清场）、
消费者登记，以及隔离状态/事件查询。其他设备不受任何影响。

```bash
# ③ 隔离期间上传被挡住（409 device_isolated）
curl -s -X POST $DEV/batches -H 'Content-Type: application/json' -d '{…}'
# → {"detail": {"error": "device_isolated",
#      "message": "device 'dev-01' is isolated since …: verify-chain 报 …",
#      "isolation_event_id": 1, "head_sequence": 12, "head_digest": "9c10bd…", …}}

# ④ 随时查询隔离状态与不可变事件历史
curl -s $DEV/isolation | python3 -m json.tool
# → {"isolated": true, "status": "isolated",
#    "current_isolation": {"event_id": 1, …}, "last_event": {…}, "event_count": 1}
curl -s "$DEV/isolation/events?limit=50" | python3 -m json.tool
```

**解除请求体**：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `from_sequence` | `int≥1` | 解除前核验区间起始批次序号（含） |
| `to_sequence` | `int≥1` | 解除前核验区间结束批次序号（含） |
| `idempotency_key` | `string` | 客户端幂等键（与隔离键相互独立命名空间） |

解除在同一写事务内依次核对三道闸门，**任一不满足整体拒绝、设备保持隔离**：

1. **链头未变化**：当前链头必须与隔离事件记录的一致，否则
   `409 chain_head_changed`（附记录值与当前值）——隔离期间链头被改动说明
   存在绕过闸门的写入，必须先调查；
2. **范围核验为 ok**：对 `from_sequence..to_sequence` 执行与
   `verify-chain` 完全相同的只读核验；范围缺批次
   `409 verification_range_invalid`，核验非 ok `409 verification_failed`
   并附完整问题清单（设备无批次时空链视为 ok，跳过范围核验）；
3. **无未完成租约**：设备上不得有 `held` 租约（含过期未回收），否则
   `409 leases_outstanding` 附租约清单——过期租约可在隔离期间显式回收
   （`…/recover`）后重试解除。

```bash
# ⑤ 修复数据并重新核验确认 ok 后，解除隔离（核验范围 + 新幂等键）
curl -s -X POST $DEV/isolation/release \
  -H 'Content-Type: application/json' \
  -d '{"from_sequence": 1, "to_sequence": 12,
       "idempotency_key": "rel-20261004-01"}' | python3 -m json.tool
```

```json
{
  "status": "released",
  "device_id": "dev-01",
  "release": {
    "event_id": 2,
    "action": "released",
    "reason": null,
    "idempotency_key": "rel-20261004-01",
    "head_sequence": 12,
    "head_digest": "9c10bd…",
    "verify_from": 1,
    "verify_to": 12,
    "verification": {
      "from_sequence": 1,
      "to_sequence": 12,
      "ok": true,
      "checked_batches": 12,
      "recomputed_batches": 9,
      "archived_batches": 3,
      "checked_acknowledgments": 24,
      "checked_judgment_sources": 4,
      "snapshot": {"head_sequence": 12, "head_digest": "9c10bd…", …}
    },
    "created_at": "2026-10-04T06:30:00+00:00"
  }
}
```

解除事件把**核验范围与核验摘要**（含快照上限）一并不可变保存，事后可审计
「这次解除是基于哪次核验放行的」。相同幂等键 + 相同范围重试返回原事件
（HTTP 200）；设备未在隔离中返回 `409 not_isolated`。解除成功后上传、
确认、归档、租约与批次判定全部恢复原流程；之后可用新幂等键再次隔离。

**并发与状态机**：隔离/解除与上传、确认、归档、租约共用同一写锁串行点
（BEGIN IMMEDIATE + 唯一索引兜底多 worker），并发隔离只有一个成功（其余
`409 already_isolated`），并发解除只有一个成功（其余 `409 not_isolated`），
隔离与解除并发时状态机不会被越过——每个请求看到的都是前一请求提交后的
最新状态。

## 判定模型与裁决规则

**观测模型**：观测时钟相对真时存在线性钟漂与固定相位差，

```
t_obs ≈ (1 + drift) · T_true + c
```

航标闪光真时时刻为 `T = ψ + n·period + f_j`（ψ 相位，n 周期序号，f_j 周期内闪光时刻）。

**估计流程**：

1. 在 `[-max_drift, +max_drift]` 内自适应网格搜索钟漂（步长保证整个观测跨度上
   的残差变化不超过容差 1/4）；
2. 每个钟漂下用 `(t/s − f_j) mod period` 循环投票估计相位（滑动窗口 + 圆周均值，
   周期边界 0 与 period 相接处的聚集不会被切断）；
3. 取投票数最高的种子，交替执行「循环匹配 → 约束最小二乘」细化至收敛。

**循环对齐与计数约定**：

- 期望闪光序列按周期连续逐周期生成，跨周期边界不产生伪缺口；
- 观测与期望一对一匹配（残差 ≤ `tolerance`）。重叠容差窗下**不按残差贪心**
  （贪心会抢占共享期望闪光、压低配对数，虚增漏记/误记），而是求最优匹配，
  依次按以下三级规则裁决：
  1. **配对数最大**；
  2. 配对数相同时，**配对残差绝对值总和最小**；
  3. 总残差仍相同时，按配对的**（观测索引, 期望索引）序列字典序**最小裁决，
     保证结果确定可复现。

  相位/钟漂迭代细化与最终候选构建使用同一匹配规则；逐事件对应关系、
  漏记、未匹配记录均由该匹配结果导出；
- **漏记**：仅统计首个与末个匹配闪光之间的期望闪光缺失——观测起止处的
  不完整周期不计入；
- **误记**：未匹配到任何期望闪光的观测。

**候选裁决顺序**（依次比较，前者小者胜）：

1. 漏记 + 误记总数；
2. 匹配残差总和；
3. 钟漂绝对值；
4. 航标编号（字典序）。

候选合格条件：`missed ≤ max_missed` 且 `false ≤ max_false` 且 `|drift| ≤ max_drift`；
最佳候选为合格候选中裁决键最小者。

## 本地开发

```bash
pip install -r requirements-dev.txt
pytest                       # 运行测试（算法 + 巡检分配 + API + 批次链 + 多消费者确认 + 前缀归档 + 消费者批次处理租约 + 草稿回放 + 连续批次判定 + 判定复核报告 + 链一致性核验 + 异常隔离共 236 例）
uvicorn app.main:app --reload   # 本地起服务，默认使用 ./data/navlight.db
```

## 项目结构

```
app/
  main.py    FastAPI 路由与应用工厂
  align.py   相位-钟漂联合估计与循环对齐核心算法
  patrol.py  巡检整体核对：窗口合格候选 + 全局最优分配动态规划
  replay.py  草稿回放：用草稿快照沿用原观测/阈值重放历史判定并四分类
  batches.py 现场设备批次：规范化载荷与 SHA-256 链式摘要；跨批观测合并
  models.py  请求数据模型（Pydantic）
  db.py      SQLite 持久层（不可变特征库版本 + 判定记录 + 草稿回放报告 +
             设备批次链 + 多消费者稀疏确认与安全归档水位 + 连续前缀归档墓碑 +
             消费者批次处理租约（领取/完成/回收、租约占用归档闸门）+
             连续批次归档前巡检判定不可变来源 + 只读快照判定复核报告 +
             只读快照链一致性核验 +
             设备异常隔离不可变事件与处理接口闸门，含首次迁移）
  config.py  环境变量配置
samples/
  library.json                示例特征库
  generate_request.py         合成示例判定请求
  generate_patrol_request.py  合成示例巡检核对请求
tests/                  pytest 测试
Dockerfile / docker-compose.yml
```
