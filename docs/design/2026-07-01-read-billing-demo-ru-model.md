# Read Billing Demo RU 模型与校准统计面设计

- Author(s): TBD
- Discussion PR: TBD
- Tracking Issue: TBD

## 目录

* [简介](#简介)
* [背景和目标](#背景和目标)
* [当前代码边界](#当前代码边界)
* [模型和支持范围](#模型和支持范围)
* [推荐统计面](#推荐统计面)
* [采集和生命周期](#采集和生命周期)
* [查询和权重校准方法](#查询和权重校准方法)
* [初始权重](#初始权重)
* [性能和容量控制](#性能和容量控制)
* [兼容性和迁移计划](#兼容性和迁移计划)
* [测试设计](#测试设计)
* [备选方案](#备选方案)
* [风险和未决问题](#风险和未决问题)

## 简介

本文描述 read billing demo 的 SELECT 读请求 RU 模型，以及用于 workload 权重校准的 TiDB-native 统计面。

模型把读请求执行过程拆成有限数量的 operator class。每个 operator class 都按版本管理权重，权重作用在无系数的 base units 上：

```text
preview_ru =
    fixed_events * fixed_weight
  + input_rows   * row_weight
  + input_bytes  * byte_weight
```

第一版是校准 demo，不是最终生产 billing 语义。它和现有 RU v2 resource-control 上报保持隔离，并分成两个输出面：

1. `EXPLAIN ANALYZE FORMAT='RU'`：单语句诊断输出，显示 base units、weight 和 preview RU。
2. statement-summary-associated statistics tables：workload 级无系数统计面，用于离线重新套任意 weights，不依赖 Prometheus scrape，也不打印 per-SQL log。

Prometheus `tidb_read_billing_demo_*` metrics 可以继续作为 bounded observability 辅助，但不能作为 workload testers 的必需接口；校准契约以 SQL 查询面为准。

## 背景和目标

现有 RU v2 主要是 statement-level billing 逻辑，不适合回答两个问题：

1. 一条 SELECT 语句里的 TiDB / TiKV 哪些 executor 消耗了 RU；
2. 在 workload 下，如何收集无系数、可重放的原始输入，用真实压测数据回归出更合理的 weights。

read billing demo 的目标是：

1. 让单条语句可以通过 `EXPLAIN ANALYZE FORMAT='RU'` 看到 operator-level 的 base units、weight 和 preview RU；
2. 让 workload 可以通过 SQL 查询收集 `site/op_class/unit` 维度的 `fixed_events`、`input_rows`、`input_bytes` 等基础量；
3. 让 testers 可以在 workload 结束后直接替换 weights 并离线重算 RU，不必重跑 workload；
4. 让 unsupported、unknown input、statement error、aggregation overflow 等失败或降级状态有可查询位置；
5. 在 weights 还没有校准完成前，避免把 demo 结果混入现有 RU v2 billing 或 resource-control consumption 上报。

非目标：

- 不把 per-SQL log 作为校准接口；
- 不要求 downstream testers 搭 Prometheus 或依赖 metrics retention；
- 不在第一版覆盖 TiFlash、MPP、IndexMerge 或复杂 storage path；
- 不把当前 `v1` weights 解释成生产校准值。

## 当前代码边界

当前实现已经有以下锚点：

- `pkg/sessionctx/vardef/tidb_vars.go`、`pkg/sessionctx/variable/session.go`、`pkg/sessionctx/variable/sysvar.go` 定义 `tidb_enable_read_billing_demo`，默认 OFF。
- `pkg/executor/adapter.go::FinishExecuteStmt` 在 statement 完成时调用 `plannercore.RecordReadBillingDemoForStatement`，随后 `SummaryStmt` 把返回的 read-billing summary 放进 `stmtsummary.StmtExecInfo`。
- `pkg/session/session.go::recordReadBillingDemoEarlyError` 覆盖 compile / resolve / pre-completion error 等没有正常走到 `FinishExecuteStmt` 的错误出口。
- `pkg/planner/core/explain_ru.go::buildReadBillingDemoResult` 生成 frozen result，包含 statement status、operator status 和 operator base units。
- `pkg/planner/core/explain_ru.go::recordReadBillingDemoResult` 当前会写 bounded Prometheus metrics。
- `pkg/planner/core/explain_ru.go::summarizeReadBillingDemoBaseUnits` 当前只把成功 statement 的 base units 汇总成三个 scalar totals。
- `pkg/util/stmtsummary/statement_summary.go`、`pkg/util/stmtsummary/reader.go`、`pkg/infoschema/tables.go` 当前在 statement summary v1 路径暴露三列：
  - `SUM_READ_BILLING_DEMO_FIXED_EVENTS`
  - `SUM_READ_BILLING_DEMO_INPUT_ROWS`
  - `SUM_READ_BILLING_DEMO_INPUT_BYTES`
- `pkg/util/stmtsummary/v2/record.go`、`pkg/util/stmtsummary/v2/column.go` 当前在 persistent statement summary v2 路径保持同样三列。

三列 totals 可以作为 convenience summary，但它们丢失了 `site/op_class/unit/input_source/input_side` 维度，无法支持 per-opclass weight calibration。因此本设计推荐新增维度化 SQL 查询面，并从同一个 statement summary 生命周期里聚合。

## 模型和支持范围

### 支持范围和安全 gate

demo 只接受 side-effect-free SELECT：

- 普通 `SELECT`；
- set operation，前提是所有叶子都是 side-effect-free SELECT；
- prepared statement / `EXECUTE`，前提是 resolved prepared target 是可支持的 side-effect-free SELECT。

以下语句或路径会被拒绝、跳过或 fail closed：

- 非 SELECT；
- `SELECT ... INTO`；
- locking SELECT；
- 带副作用的函数，例如 lock 函数、sequence mutation、`LAST_INSERT_ID(expr)` 和 `SLEEP`；
- internal / restricted SQL；
- TiFlash、MPP exchange、IndexMerge 和未支持的 operator。

当出现 unsupported、unknown input 或 statement error 时，demo 必须记录 status/reason，但不能上报 partial base units 或 partial preview RU。

### Base-unit result

每个 supported 且实际执行的 billable operator 至少产生一个：

```text
fixed_events = 1
```

大多数 operator 还会产生：

- `input_rows`：该 operator 消耗的行数或 key 数；
- `input_bytes`：该 operator 消耗的 byte-shaped 输入。

每个 operator 结果都有以下身份字段：

- `site`：`tidb`、`tikv`，statement-level status 用 `statement`；
- `op_class`：用于解析权重的有限 class；
- `operator_kind`：真实 plan/operator 名称，只用于观测和调参，不参与 weight lookup；
- `model_version`：当前统计模型版本，当前是 `v1`；
- `weight_version`：当前 weight table 版本，当前是 `v1`。

权重按下面的 key 解析：

```text
site + op_class + weight_version
```

因此 TiDB 和 TiKV 即使共享类似的 opclass 名称，例如 `filter_eval`，也可以使用不同权重。

### TiKV Cop Executor 模型

TiKV 侧不是只按 scan 计费，而是把 cop executor 也纳入模型。第一版包括：

| TiKV opclass | 覆盖的 operator 形态 | base-unit 输入 |
|---|---|---|
| `kv_range_scan` | table/index range scan | `input_rows = ScanDetail.TotalKeys`；`input_bytes = ScanDetail.ProcessedKeysSize` 优先，缺失时回退 `TotalKeys * row_width` |
| `kv_point_lookup` | point get / batch point get | point lookup work；当前 variable input 基于 runtime rows，fixed event 覆盖 operator setup |
| `filter_eval` | selection | 直接 cop child runtime rows 和 modeled row width |
| `projection_eval` | projection | 直接 cop child runtime rows 和 modeled row width |
| `row_limit` | limit | 直接 cop child runtime rows 和 modeled row width |
| `bounded_topn` | TopN | 直接 cop child runtime rows 和 modeled row width |
| `agg_hash` | hash aggregation | aggregation input rows 和 modeled input bytes |
| `agg_stream` | stream aggregation | aggregation input rows 和 modeled input bytes |

range scan 的 byte 输入优先使用 `ProcessedKeysSize`，因为 scan 成本不仅和输出行数有关，还和 key/value bytes、MVCC 数据移动、row decode 有关。如果 runtime 只有 `TotalKeys` 而没有 processed size，则回退为：

```text
input_bytes = TotalKeys * row_width
```

如果是 mock-store 或其他没有 scan detail 的路径，则回退为：

```text
input_rows  = actRows
input_bytes = actRows * row_width
input_source = runtime_act_rows
```

这个 fallback 会通过 `input_source` 明确标出，避免把 runtime rows 伪装成 scan detail。

### TiDB Root Executor 模型

TiDB 侧覆盖 root executor CPU、row movement、reader materialization、join 和 local overlay：

| TiDB opclass | 覆盖的 operator 形态 | base-unit 输入 |
|---|---|---|
| `filter_eval` | root selection | local child rows 和 modeled input bytes |
| `projection_eval` | root projection | local child rows 和 modeled input bytes |
| `row_limit` | root limit | local child rows 和 modeled input bytes |
| `bounded_topn` | root TopN | local child rows 和 modeled input bytes |
| `full_ordering` | sort | local child rows 和 modeled input bytes |
| `window_eval` | window executor | local child rows 和 modeled input bytes |
| `agg_hash` | hash aggregation | aggregation input rows 和 modeled input bytes |
| `agg_stream` | stream aggregation | aggregation input rows 和 modeled input bytes |
| `join_hash` | hash join | build/probe 两侧 input rows 和 bytes |
| `join_merge` | merge join | left/right 两侧 input rows 和 bytes |
| `join_lookup` | index lookup join family | left/right 两侧 input rows 和 bytes |
| `reader_receive` | table/index reader receive | runtime rows 和 modeled received bytes |
| `lookup_reader` | index lookup reader orchestration | runtime rows 和 modeled bytes |
| `overlay_reader` | union scan / local overlay | runtime rows 和 modeled bytes |
| `metadata_reader` | memory / cluster metadata reads | runtime rows 和 modeled bytes |

Join 的 side 会通过 `input_side` 保留下来：

- hash join：`build`、`probe`；
- merge join / lookup join：`left`、`right`；
- 其他 operator：`all`。

## 推荐统计面

推荐新增 statement-summary-associated virtual tables，按 statement summary 的 digest/window/history 生命周期聚合 read billing demo 明细。表名前缀和现有 statement summary 保持一致：

- `INFORMATION_SCHEMA.STATEMENTS_SUMMARY_READ_BILLING_DEMO_BASE_UNITS`
- `INFORMATION_SCHEMA.STATEMENTS_SUMMARY_HISTORY_READ_BILLING_DEMO_BASE_UNITS`
- `INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_READ_BILLING_DEMO_BASE_UNITS`
- `INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY_READ_BILLING_DEMO_BASE_UNITS`
- `INFORMATION_SCHEMA.STATEMENTS_SUMMARY_READ_BILLING_DEMO_STATUS`
- `INFORMATION_SCHEMA.STATEMENTS_SUMMARY_HISTORY_READ_BILLING_DEMO_STATUS`
- `INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_READ_BILLING_DEMO_STATUS`
- `INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY_READ_BILLING_DEMO_STATUS`

### Base-unit table schema

每一行表示一个 statement summary window 内、一个 digest/plan_digest 下的一组 base-unit key 聚合。

| Column | Type | 含义 |
|---|---|---|
| `SUMMARY_BEGIN_TIME` | timestamp | statement summary window begin |
| `SUMMARY_END_TIME` | timestamp | statement summary window end |
| `STMT_TYPE` | varchar | statement type |
| `SCHEMA_NAME` | varchar | schema |
| `DIGEST` | varchar | SQL digest |
| `DIGEST_TEXT` | blob | normalized SQL，沿用 statement summary 截断策略 |
| `PLAN_DIGEST` | varchar | plan digest；point get 可由 lazy plan digest 补齐 |
| `RESOURCE_GROUP` | varchar | resource group |
| `MODEL_VERSION` | varchar | read billing model version |
| `WEIGHT_VERSION` | varchar | 生成这些 base units 时匹配的 weight version |
| `SITE` | varchar | `tidb` / `tikv` |
| `OP_CLASS` | varchar | bounded opclass |
| `OPERATOR_KIND` | varchar | bounded plan/operator kind |
| `UNIT` | varchar | `fixed_events` / `input_rows` / `input_bytes` |
| `INPUT_SOURCE` | varchar | `scan_detail` / `runtime_act_rows` |
| `INPUT_SIDE` | varchar | `all` / `build` / `probe` / `left` / `right` |
| `ROW_WIDTH_SOURCE` | varchar | `plan_stats` / `schema_type_width` / `schema_fallback` / `operator_helper` |
| `VALUE` | double unsigned | 该 key 的 base-unit total |
| `SAMPLE_COUNT` | bigint unsigned | 汇入该 key 的 unit samples 数 |
| `ROW_WIDTH_SUM` | double unsigned | row-width sample sum，用于诊断 modeled bytes |
| `AVG_ROW_WIDTH` | double unsigned | `ROW_WIDTH_SUM / SAMPLE_COUNT` |

`DIGEST_TEXT` 不是 key，只是查询便利字段。校准时推荐按 `DIGEST`、`PLAN_DIGEST`、`SITE`、`OP_CLASS`、`UNIT` 等机器字段聚合。

### Status table schema

每一行表示一个 statement summary window 内、一个 digest/plan_digest 下的一组 statement/operator status 聚合。

| Column | Type | 含义 |
|---|---|---|
| `SUMMARY_BEGIN_TIME` | timestamp | statement summary window begin |
| `SUMMARY_END_TIME` | timestamp | statement summary window end |
| `STMT_TYPE` | varchar | statement type |
| `SCHEMA_NAME` | varchar | schema |
| `DIGEST` | varchar | SQL digest |
| `DIGEST_TEXT` | blob | normalized SQL |
| `PLAN_DIGEST` | varchar | plan digest |
| `RESOURCE_GROUP` | varchar | resource group |
| `MODEL_VERSION` | varchar | read billing model version |
| `WEIGHT_VERSION` | varchar | current weight version at accounting time |
| `SITE` | varchar | `statement` / `tidb` / `tikv` |
| `OP_CLASS` | varchar | statement or operator opclass |
| `OPERATOR_KIND` | varchar | statement or plan/operator kind |
| `STATUS` | varchar | `success` / `unsupported` / `unknown_input` / `error` / `ok` |
| `REASON` | varchar | bounded reason，例如 `unsupported_mpp`、`missing_scan_detail`、`statement_error` |
| `COUNT` | bigint unsigned | 该 status key 的出现次数 |

失败 statement 只写 status table，不写 base-unit table。成功 statement 写一条 statement-level `success` status，并为每个 billable operator 写 operator-level `ok` status。

### 为什么不用 statement summary 单表 JSON 列

JSON/encoded column 的写入成本可以较低，但对校准不够直接：

- testers 需要额外 JSON 解析才能 group by `site/op_class/unit`；
- SQL 里直接套 weights、按 digest/window 过滤和 join 会变复杂；
- 历史持久化中的 schema 演进、旧 JSON 兼容和 partial parse failure 更难测试；
- 失败 status 和 base units 混在一个 encoded blob 里，容易再次变成不可直接观测的状态。

窄表把 row explosion 放到查询阶段，而不是把复杂性塞进单列解析；同时它复用 statement summary 的 digest/window/LRU/history 控制。

## 采集和生命周期

### Statement completion path

后续实现应把 `RecordReadBillingDemoForStatement` 的返回值从三个 totals 扩成结构化 snapshot，例如：

```go
type ReadBillingDemoStatementStats struct {
    ModelVersion  string
    WeightVersion string
    Statuses      []ReadBillingDemoStatusSample
    BaseUnits     []ReadBillingDemoBaseUnitSample
    Totals        ReadBillingDemoBaseUnitSummary
}
```

其中 `Totals` 从 `BaseUnits` 派生，用于继续填充现有三列；`Statuses` 和 `BaseUnits` 则进入新表所需的维度化聚合。

采集顺序：

1. statement 执行结束或早期错误出口触发 read billing demo hook；
2. `buildReadBillingDemoResult` 生成 frozen result；
3. 如果 result status 是 success：
   - 为 statement 写 `success` status；
   - 为 billable operator 写 operator `ok` status；
   - 把 operator units 展平成 base-unit samples；
   - 从 base-unit samples 派生三列 totals；
4. 如果 result status 不是 success：
   - 为 statement 或 failed operator 写 status/reason；
   - 不写任何 base-unit samples；
   - 三列 totals 保持 0；
5. 正常执行结束路径由 `ExecStmt.SummaryStmt` 把结构化 snapshot 挂到 `StmtExecInfo`；
6. statement summary v1/v2 通过同一个 `Add`/`Merge` 路径聚合到当前 window 和 history；
7. 早期错误路径不能假设一定有 `ExecStmt.SummaryStmt`。`session.recordReadBillingDemoEarlyError` 需要在调用 `RecordReadBillingDemoForStatement` 后，把 status-only snapshot 通过一个 read-billing 专用 helper 写入同一套 statement-summary-associated status 聚合，例如 `stmtsummaryv2.AddReadBillingDemoStatusOnly(...)`。该 helper 只记录 status table 所需字段，`PLAN_DIGEST` 可为空，不写 base units，不影响普通 statement summary latency/RU 等通用列。

### Statement summary v1

v1 in-memory path 位于 `pkg/util/stmtsummary/statement_summary.go`：

- `stmtSummaryStats` 新增 bounded in-memory maps：
  - `ReadBillingDemoBaseUnitAggs map[ReadBillingDemoBaseUnitKey]ReadBillingDemoBaseUnitAgg`
  - `ReadBillingDemoStatusAggs map[ReadBillingDemoStatusKey]ReadBillingDemoStatusAgg`
- `newStmtSummaryStats`、`stmtSummaryStats.add`、`ReadBillingDemo...Merge` 负责把 `StmtExecInfo` 的 samples 合并进去；
- status-only early error helper 使用同样的 key 构造和 window 选择逻辑，但只 upsert status map；如果 statement digest 不可用，则使用空 digest/空 plan digest 聚合到当前 window，并保留 bounded reason；
- `stmtSummaryReader` 新增面向 base-unit/status 表的 reader，遍历同一个 current/history/cumulative window 后展开 rows；
- `STATEMENTS_SUMMARY_READ_BILLING_DEMO_*` 表沿用 statement summary 的 auth 行为：无 `PROCESS` privilege 时只能看到自己的 statement 聚合。

### Statement summary v2

v2 persistent path 位于 `pkg/util/stmtsummary/v2`：

- `StmtRecord` 新增 JSON-stable entry slices，而不是 `map[struct]...` 这类不能稳定 marshal/unmarshal 的字段：
  - `ReadBillingDemoBaseUnitAggs []ReadBillingDemoBaseUnitAggEntry`
  - `ReadBillingDemoStatusAggs []ReadBillingDemoStatusAggEntry`
- 每个 entry 直接携带 key 字段和 aggregate value；`StmtRecord.Add` / `StmtRecord.Merge` 可以用临时 canonical string key 或小规模线性 upsert 合并后再保持 entry slice 排序；
- `MemReader` 从当前 `StmtRecord` 展开新表 rows；
- `HistoryReader` 读取历史 JSON log 时，如果旧记录没有这些字段，则 entry slices 为 nil，新表返回 0 行，不影响旧 log 解析；
- evicted aggregate record 也合并 read billing entries。对校准而言，evicted row 只作为覆盖率/容量诊断，不应用来拟合精细 weights，因为 digest/plan 维度已经被合并。

### Current 和 history 表

当前表读取未持久化窗口，history 表读取内存当前窗口加已持久化历史，与现有 `STATEMENTS_SUMMARY` / `STATEMENTS_SUMMARY_HISTORY` 语义保持一致。

如果 user 需要完整 workload run，推荐：

1. 开启 statement summary；
2. 确认 `tidb_enable_read_billing_demo = ON`；
3. 记录 workload 起止时间；
4. 从 `CLUSTER_STATEMENTS_SUMMARY_HISTORY_READ_BILLING_DEMO_*` 用 `SUMMARY_BEGIN_TIME` / `SUMMARY_END_TIME` 过滤；
5. 同时查 status 表确认失败和 overflow 比例。

## 查询和权重校准方法

### 开启采集

```sql
SET GLOBAL tidb_enable_read_billing_demo = ON;
```

建议只在校准 workload 的 session 或测试集群中开启。生产默认仍然是 OFF。

### 第一步：检查覆盖率和失败状态

先看 statement/operator status：

```sql
SELECT
    status,
    reason,
    SUM(count) AS count
FROM information_schema.cluster_statements_summary_history_read_billing_demo_status
WHERE summary_begin_time >= TIMESTAMP '2026-07-02 10:00:00'
  AND summary_end_time   <= TIMESTAMP '2026-07-02 11:00:00'
  AND model_version = 'v1'
GROUP BY status, reason
ORDER BY count DESC;
```

如果 `unsupported`、`unknown_input`、`error` 或 `aggregation_overflow` 占比高，说明 base-unit 数据不能代表整个 workload，需要先收敛覆盖范围或降低维度压力。

进一步按 operator 看原因：

```sql
SELECT
    site,
    op_class,
    operator_kind,
    status,
    reason,
    SUM(count) AS count
FROM information_schema.cluster_statements_summary_history_read_billing_demo_status
WHERE summary_begin_time >= TIMESTAMP '2026-07-02 10:00:00'
  AND summary_end_time   <= TIMESTAMP '2026-07-02 11:00:00'
  AND model_version = 'v1'
GROUP BY site, op_class, operator_kind, status, reason
ORDER BY count DESC;
```

### 第二步：收集 base units

按 `site/op_class/unit/source/side` 聚合 base units：

```sql
SELECT
    site,
    op_class,
    unit,
    input_source,
    input_side,
    SUM(value) AS value
FROM information_schema.cluster_statements_summary_history_read_billing_demo_base_units
WHERE summary_begin_time >= TIMESTAMP '2026-07-02 10:00:00'
  AND summary_end_time   <= TIMESTAMP '2026-07-02 11:00:00'
  AND model_version = 'v1'
GROUP BY site, op_class, unit, input_source, input_side
ORDER BY site, op_class, unit, input_source, input_side;
```

这个查询是校准模型的主要输入，相当于得到无系数特征矩阵：

```text
X[site, op_class, unit] = 当前时间窗口内观测到的 base-unit total
```

如果需要区分 plan shape，可以保留 `DIGEST` 和 `PLAN_DIGEST`：

```sql
SELECT
    digest,
    plan_digest,
    site,
    op_class,
    unit,
    SUM(value) AS value
FROM information_schema.cluster_statements_summary_history_read_billing_demo_base_units
WHERE summary_begin_time >= TIMESTAMP '2026-07-02 10:00:00'
  AND summary_end_time   <= TIMESTAMP '2026-07-02 11:00:00'
  AND model_version = 'v1'
GROUP BY digest, plan_digest, site, op_class, unit;
```

### 第三步：检查 row-width evidence

row width 是模型 factor，不是 runtime 真实 byte sample。拟合前需要检查它是否稳定：

```sql
SELECT
    site,
    op_class,
    row_width_source,
    SUM(sample_count) AS samples,
    SUM(row_width_sum) / NULLIF(SUM(sample_count), 0) AS avg_row_width
FROM information_schema.cluster_statements_summary_history_read_billing_demo_base_units
WHERE summary_begin_time >= TIMESTAMP '2026-07-02 10:00:00'
  AND summary_end_time   <= TIMESTAMP '2026-07-02 11:00:00'
  AND unit = 'input_bytes'
  AND model_version = 'v1'
GROUP BY site, op_class, row_width_source
ORDER BY site, op_class, row_width_source;
```

如果 `schema_fallback` 占比高，或某些 opclass 的 row width 异常，拟合出来的 byte weight 可能是在补偿 row-width 估计误差，而不是真实 byte cost。

### 第四步：用任意 weights 重算 RU

testers 可以把新 weights 放到 CTE、临时表或外部 notebook。SQL 侧的最小形态如下：

```sql
WITH observed AS (
    SELECT site, op_class, unit, SUM(value) AS value
    FROM information_schema.cluster_statements_summary_history_read_billing_demo_base_units
    WHERE summary_begin_time >= TIMESTAMP '2026-07-02 10:00:00'
      AND summary_end_time   <= TIMESTAMP '2026-07-02 11:00:00'
      AND model_version = 'v1'
    GROUP BY site, op_class, unit
),
weights(site, op_class, fixed_weight, row_weight, byte_weight) AS (
    SELECT 'tikv', 'kv_range_scan', 0.070, 0.000045, 0.000020 UNION ALL
    SELECT 'tidb', 'projection_eval', 0.020, 0.000020, 0.000004
)
SELECT
    SUM(CASE observed.unit
        WHEN 'fixed_events' THEN observed.value * weights.fixed_weight
        WHEN 'input_rows'   THEN observed.value * weights.row_weight
        WHEN 'input_bytes'  THEN observed.value * weights.byte_weight
        ELSE 0
    END) AS recalculated_ru
FROM observed
JOIN weights
  ON observed.site = weights.site
 AND observed.op_class = weights.op_class;
```

因为统计面保留的是 base units，重算时只需要替换 `weights` 数据，不需要重新跑 workload。

### 第五步：拟合新 weights

对每个校准窗口构造：

```text
y_window = 观测目标成本
X_window = 按 site/opclass/unit 聚合的 base-unit totals
```

`y_window` 可以选择：

- workload 窗口内 TiDB/TiKV CPU time；
- 现有 RU consumption，用于兼容性对比；
- benchmark 手工标注成本；
- 混合目标，例如 CPU + IO-normalized scan cost。

推荐使用非负线性模型：

```text
minimize || X * w - y ||^2
subject to w >= 0
```

拟合规则建议：

1. 按 `site/op_class` 分别拟合 `fixed`、`row`、`byte` 三类系数。
2. 保持权重非负。
3. 使用 hold-out workload window 验证预测误差。
4. 默认保留符合源码成本形态的单调关系，除非数据强烈反证，例如：
   - `agg_hash.row >= agg_stream.row`；
   - `bounded_topn.row >= row_limit.row`；
   - `kv_range_scan.byte >= filter_eval.byte`；
   - `join_hash.row >= join_merge.row`。
5. 拟合后的新权重应该发布为新的 `weight_version`，不要静默改变 `v1` 的含义。

## 初始权重

当前 `v1` weights 是 demo calibration seed，不是 production-calibrated billing constants。它们来自源码成本形态和相对排序：

- scan 同时受 row/key 数和 bytes 影响，比纯 expression eval 更重；
- hash aggregation 和 hash join 需要维护 hash map / hash table / aggregate state，row weight 更高；
- stream aggregation 和 merge join 依赖有序输入，通常低于 hash 版本；
- limit 是最轻的 row-shaped operator；
- TopN 需要 order expression 和 heap maintenance，比 limit 重；
- TiDB reader class 对 bytes 更敏感，因为它负责接收、materialize chunk 和协调 lookup；
- TiDB 与 TiKV 即使 opclass 名称相同，也运行不同代码路径，因此权重分开设置。

当前 `v1` 权重如下：

| site | opclass | fixed_weight | row_weight | byte_weight | 初始设置原因 |
|---|---|---:|---:|---:|---|
| `tikv` | `kv_range_scan` | 0.070 | 0.000045 | 0.000020 | range iterator、MVCC/key-value movement、row decode 和 scan limiter 使 scan 同时对 row/key 和 bytes 敏感。 |
| `tikv` | `kv_point_lookup` | 0.045 | 0.000030 | 0.000012 | point get 有 per-key dispatch 和 decode，但没有 range iterator loop。 |
| `tikv` | `filter_eval` | 0.020 | 0.000040 | 0.000006 | RPN predicate eval 主要按 logical rows 消耗 CPU，byte movement 权重较低。 |
| `tikv` | `projection_eval` | 0.020 | 0.000030 | 0.000006 | projection 会执行表达式或 materialize column reference，row 成本低于 filter。 |
| `tikv` | `row_limit` | 0.010 | 0.000008 | 0.000002 | 主要是 pass-through counter / gate，是最轻的 executor。 |
| `tikv` | `bounded_topn` | 0.060 | 0.000075 | 0.000012 | order expression eval 加 bounded heap maintenance。 |
| `tikv` | `agg_hash` | 0.080 | 0.000100 | 0.000014 | group expression eval、hash group lookup/insert、aggregate state update。 |
| `tikv` | `agg_stream` | 0.060 | 0.000065 | 0.000010 | ordered group compare 加 state update，不构建 hash table。 |
| `tidb` | `filter_eval` | 0.020 | 0.000030 | 0.000005 | TiDB chunk 上的 expression eval，低于 scan 和 hash 类 operator。 |
| `tidb` | `projection_eval` | 0.020 | 0.000020 | 0.000004 | projection / materialization，per-row 成本较低。 |
| `tidb` | `row_limit` | 0.010 | 0.000006 | 0.000001 | 轻量 pass-through control。 |
| `tidb` | `bounded_topn` | 0.060 | 0.000060 | 0.000010 | heap / order expression work，比 full sort 范围小。 |
| `tidb` | `full_ordering` | 0.080 | 0.000070 | 0.000012 | full sort 需要处理完整输入。 |
| `tidb` | `window_eval` | 0.070 | 0.000070 | 0.000010 | partition / order / frame state maintenance。 |
| `tidb` | `agg_hash` | 0.070 | 0.000085 | 0.000012 | hash grouping 和 aggregate state update。 |
| `tidb` | `agg_stream` | 0.050 | 0.000055 | 0.000008 | ordered streaming aggregate，低于 hash agg。 |
| `tidb` | `join_hash` | 0.110 | 0.000115 | 0.000020 | build/probe hash table work，是 TiDB 侧最高 row cost 之一。 |
| `tidb` | `join_merge` | 0.090 | 0.000075 | 0.000012 | ordered comparison / merge，低于 hash join。 |
| `tidb` | `join_lookup` | 0.120 | 0.000120 | 0.000020 | lookup task orchestration 加 join-side work，固定成本和 row 成本都较高。 |
| `tidb` | `reader_receive` | 0.040 | 0.000025 | 0.000014 | network/result receive 和 chunk materialization，对 bytes 敏感。 |
| `tidb` | `lookup_reader` | 0.070 | 0.000045 | 0.000016 | index/table two-phase lookup reader coordination。 |
| `tidb` | `overlay_reader` | 0.050 | 0.000035 | 0.000012 | UnionScan/local overlay merge。 |
| `tidb` | `metadata_reader` | 0.020 | 0.000008 | 0.000002 | metadata / memory-table read，故意设得较轻。 |

## 性能和容量控制

### 低开销原则

新统计面必须满足：

- `tidb_enable_read_billing_demo` 默认 OFF，关闭时不构造 read billing result，也不写 statement summary maps；
- 开启后仍只在 statement end 聚合一次，不打印 per-SQL log；
- base-unit/status key 由 bounded enum-like 字段组成，不包含 SQL text、table name、index name、region、store address 或 plan id；
- `DIGEST_TEXT` 和 `PLAN_DIGEST` 复用 statement summary 已有字段，不进入 read-billing map key；
- 读表时才把 map 展开成多行，写入时保持 map 聚合，不为每次执行追加 unbounded slice；
- cluster 表沿用现有 cluster memtable fan-out，不引入新的后台 scrape。

### 单 digest/window map 上限

statement summary 已有 `max-stmt-count`、`refresh-interval`、`history-size` 和 LRU eviction，但这些只限制 digest/window 数量。read billing 还需要限制单 digest/window 内的维度 key 数。

建议第一版使用保守上限：

```text
max_base_unit_keys_per_record = 256
max_status_keys_per_record    = 128
```

当超过上限：

- status table 增加 `status='unknown_input'`、`reason='aggregation_overflow'` 的 reserved row；
- 超出上限的新 base-unit key 不再写入 base-unit table，避免把不完整特征矩阵伪装成完整数据；
- 现有已聚合 key 继续累加；
- 三列 totals 仍只从已接收 base-unit samples 派生，并且用户必须通过 status 表判断该窗口是否可用于拟合。

overflow row 必须永远可见，不能被 status map 自身的上限挤掉。实现时预留两个不计入 `max_status_keys_per_record` 的 fixed buckets：

```text
statement/statement/statement unknown_input aggregation_overflow
statement/statement/statement unknown_input status_aggregation_overflow
```

如果 base-unit map 超限，写 `aggregation_overflow` bucket。如果 status map 对普通 status key 超限，写 `status_aggregation_overflow` bucket。reserved buckets 只累加 count，不触发新的 overflow，也不写 base units。

这个策略牺牲超高维异常 workload 的完整性，但保证整体性能和内存不会被单个 digest 放大。

### 估算内存上界

粗略上界：

```text
max_records = stmt-summary.max-stmt-count * (history-size 或 current window)
per_record_read_billing_keys <= 256 + 128
```

实际 key 数通常远低于上限，因为第一版 opclass、unit、input_source 和 input_side 都是有限集合。典型 SELECT digest/window 只会产生数十个 key。

## 兼容性和迁移计划

1. 保留 `SUM_READ_BILLING_DEMO_FIXED_EVENTS`、`SUM_READ_BILLING_DEMO_INPUT_ROWS`、`SUM_READ_BILLING_DEMO_INPUT_BYTES` 三列作为 convenience totals。
2. 文档和后续测试明确：三列不能用于 per-opclass calibration，因为它们丢失维度。
3. 新 base-unit/status 表成为 workload calibration 的主接口。
4. Prometheus `tidb_read_billing_demo_*` metrics 可继续保留为 optional observability，但不作为 testers 必需依赖。
5. v2 persisted statement summary 的 JSON 结构新增 read-billing maps；旧日志没有这些字段时读出来为空，不需要 migration。
6. 如果后续模型稳定并准备生产化，可以再决定是否移除 demo 三列或把 demo 表改名为非 demo 表；第一版不做破坏性删除。

## 测试设计

后续实现 loop 至少需要补以下测试。

### Planner / calculator tests

- `RecordReadBillingDemoForStatement` 返回结构化 status/base-unit snapshot；
- success path 只为 billable operators 产生 base units；
- unsupported、unknown-input、error path 不产生 base units，但产生 status/reason；
- normal finish path 通过 `StmtExecInfo` 写 status/base units，early error path 通过 status-only helper 写 SQL-visible status；
- `aggregation_overflow` 和 `status_aggregation_overflow` reserved buckets 可见且不会写 partial unknown key；
- current three-column totals 从 base-unit samples 派生，与现有测试保持兼容。

### Statement summary v1 tests

- `stmtSummaryStats.add` 能按 `site/op_class/unit/input_source/input_side/row_width_source` 聚合 value；
- status-only early error helper 能写入 current window 的 status table，且不写 base-unit rows；
- `Merge` / evicted aggregate 能合并 read billing maps；
- `STATEMENTS_SUMMARY_READ_BILLING_DEMO_BASE_UNITS` current/history rows 与 digest/window 对齐；
- `STATEMENTS_SUMMARY_READ_BILLING_DEMO_STATUS` 能看到 failed billing；
- base-unit/status map 超限时 reserved overflow buckets 不受 cap 影响；
- auth 行为与 statement summary 保持一致。

### Statement summary v2 tests

- `StmtRecord.Add` / `Merge` 聚合 JSON-stable read billing entry slices；
- current reader 展开 base-unit/status rows；
- history reader 能读取新 JSON 字段；
- old history JSON 没有 read-billing 字段时返回 0 行且不报错；
- entry slice marshal/unmarshal 后 key 字段和值不丢失，排序保持 deterministic；
- evicted aggregate 包含 read billing maps，但测试中明确它不适合精细拟合。

### Information schema / cluster table tests

- 新表注册、列顺序、cluster `INSTANCE` 列；
- digest/time predicate extractor 复用 statement summary extractor；
- `CLUSTER_*` 表能查询多个 TiDB instance 的 rows；
- 默认 OFF 时新表无 read-billing rows；
- 开启 demo 后可以用 SQL 直接按 `site/op_class/unit` 重算 RU。

### 性能验证

- targeted benchmark 比较 demo OFF、demo ON + scalar totals、demo ON + dimension maps；
- 验证正常 workload 下 statement end 额外开销不显著；
- 验证高维异常 workload 触发 overflow 后内存和写入开销保持 bounded。

## 备选方案

### 继续只用 Prometheus metrics

不采用。metrics label shape 可以保存 `site/op_class/unit`，但 downstream testers 明确不希望依赖 Prometheus；metrics retention、scrape interval 和 workload 窗口对齐也会增加校准流程复杂度。

### 每条 SQL 打日志

不采用。per-SQL log 会显著影响 QPS，也会把校准流程变成日志采集和解析问题。

### 只扩展现有 statement summary 三列

不采用。三列 totals 丢失 `site/op_class/unit`，只能重算一个全局粗粒度模型，不能直接优化不同 opclass 的 weights。

### 在 statement summary 单表里放 JSON/encoded column

暂不采用。它减少表数量，但把校准所需的 group-by 和 join 复杂度推给用户，也让 failed billing status 不够直接。可以作为后续低表面面积方案重新评估。

### 每个 operator kind 一个 weight

不采用。这样会显著提高模型和统计面复杂度，也会让校准更脆弱。当前保留 `operator_kind` 用于观测，但 weight 绑定在有限 opclass 上。

### workload 统计面直接上报 preview RU

不采用。preview RU 是加权结果，会遮住校准需要的 base-unit 原始证据。workload 表上报 base units；单语句调试通过 `EXPLAIN ANALYZE FORMAT='RU'` 看 preview RU。

## 风险和未决问题

### 风险

- 新表会增加 statement summary 内存占用；必须用单 digest/window key 上限和 overflow status 控制。
- v2 persistent history 会扩大 statement summary JSON record；需要 benchmark 和 history-reader 兼容测试。
- row-width factor 仍然是 modeled value，如果大量来自 `schema_fallback`，byte weight 可能补偿 row-width 误差。
- evicted aggregate 会丢 digest/plan 细节，不适合作为精细校准输入；用户需要查询 status 和 evicted 表判断窗口质量。
- 当前 point lookup miss 的 variable row cost 证据仍有限；没有 requested-key 证据时主要由 fixed event 覆盖。

### 未决问题

- 第一轮生产化拟合应该选什么目标信号：CPU、现有 RU、latency-normalized cost，还是混合目标？
- point lookup 是否需要接入 requested key count，使 key miss 也能产生 variable row cost？
- scan bytes 后续是否应该拆成 total key size、processed key size、value size 等多个 bounded units？
- `aggregation_overflow` 的默认上限是否需要暴露为配置，还是作为 demo 常量即可？
- TiFlash、MPP、IndexMerge 何时从 fail-closed 进入 supported opclasses？
