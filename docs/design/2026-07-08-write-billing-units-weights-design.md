# TiDB 写入计费 Unit 和初始权重设计

- Author(s): Codex
- Discussion PR: N/A
- Tracking Issue: N/A

## 目录

* [简介](#简介)
* [目标和非目标](#目标和非目标)
* [当前代码边界](#当前代码边界)
* [模型边界](#模型边界)
* [Taxonomy](#taxonomy)
* [Base Units](#base-units)
* [覆盖语句](#覆盖语句)
* [初始权重](#初始权重)
* [采集和暴露面](#采集和暴露面)
* [失败和降级状态](#失败和降级状态)
* [避免重复计费](#避免重复计费)
* [实现期 instrumentation 需求](#实现期-instrumentation-需求)
* [测试设计](#测试设计)
* [风险和未决问题](#风险和未决问题)

## 简介

本文描述 TiDB 侧写入工作的 billing demo 设计，重点回答写入 executor 应该如何拆分 `unit` 和 `weight`。这是设计文档，不包含产品代码实现。

设计沿用 read billing demo 的基本方向：先暴露无系数 base units，再通过 `site/op_class/unit/weight_version` 离线套权重。不要只输出最终加权 RU，因为 workload 校准需要保留足够维度来重算不同权重。

写入侧 preview RU 推荐使用同一种可扩展公式形态：

```text
preview_ru = sum(unit_value * weight(site, op_class, unit, weight_version))
```

其中 `unit` 是有界枚举，不把表名、索引名、SQL 文本、region、store 或 plan id 放进权重 key。`operator_kind` 可以保留用于诊断，但不参与权重解析。

## 目标和非目标

目标：

1. 覆盖 TiDB 侧主要 DML 写入 executor：`INSERT`、`INSERT ... SELECT`、`INSERT ... ON DUPLICATE KEY UPDATE`、`REPLACE`、`UPDATE`、`DELETE`、batch DML，以及 `LOAD DATA` 的 TiDB 解析和提交 worker 路径。
2. 明确 `site`、`op_class`、`unit`、`operator_kind` 的 bounded taxonomy。
3. 为每个 opclass 定义 base units、初始权重或相对权重顺序。
4. 区分哪些 unit 可以从现有 executor / statement context / runtime stats 得到，哪些需要新增 instrumentation。
5. 明确 `INSERT ... SELECT`、`UPDATE`、`DELETE` 等混合读写语句如何避免重复计算读路径和写路径。
6. 明确 TiDB 写准备、表达式求值、table/index mutation、transaction/2PC orchestration、TiKV storage write cost 的边界。
7. 让 failed / unsupported / unknown accounting 有 SQL 可查询 status。

非目标：

- 不改变当前 RU v2 resource-control 生产上报。
- 不把 storage-side TiKV/TiFlash write charging 纳入本设计的第一版权重；TiKV write keys / write bytes 只作为边界和未来校准参考。
- 不把 commit / 2PC latency 当作 TiDB executor CPU 的替代输入。
- 不用 `affectedRows` 直接代表物理写入行数。
- 不要求第一版支持所有 DDL backfill、temporary index merge、FK cascade 的完整成本；这些路径必须能给出 status。

## 当前代码边界

写入入口和关键路径如下：

- `pkg/executor/insert_common.go`
  - `InsertValues` 保存 INSERT / REPLACE / LOAD DATA 共享状态。
  - `insertRows` 处理 `INSERT/REPLACE ... VALUES` 和 `SET`。
  - `insertRowsFromSelect` 处理 `INSERT/REPLACE ... SELECT`。
  - `evalRow`、`fastEvalRow`、`getRow`、`fillRow` 完成表达式求值、cast、default、generated column、auto id 和 rowid 填充。
  - `batchCheckAndInsert` 处理 `IGNORE` / `LOAD DATA IGNORE` / `LOAD DATA REPLACE` 的 duplicate 检查。
  - `addRecordWithAutoIDHint` 调用 table 层 `AddRecord`，并更新 SQL 语义 row counters。
- `pkg/executor/insert.go`
  - `InsertExec.exec` 分派普通 insert、insert ignore 和 ODKU。
  - `batchUpdateDupRows` 构造 duplicate check keys、prefetch，并对冲突行走 update path。
  - `doDupRowUpdate` 复用 `updateRecord`。
- `pkg/executor/replace.go`
  - `ReplaceExec.exec` 先构造 duplicate check keys，再删除冲突旧行并插入新行。
  - `replaceRow`、`removeIndexRow` 负责 handle / unique-key 冲突处理。
- `pkg/executor/update.go` 和 `pkg/executor/write.go`
  - `UpdateExec.updateRows` 从 child executor 读取待更新行。
  - `composeNewRow` / `fastComposeNewRow` 执行 assignment 表达式和 cast。
  - `updateRecord` 比较 old/new row，处理 no-op、handle changed、generated/on-update-now、FK、`RemoveRecord + AddRecord` 或 `UpdateRecord`。
- `pkg/executor/delete.go`
  - `deleteSingleTableByChunk` / `deleteMultiTablesByChunk` 从 child executor 读取待删行。
  - `removeRow` 调用 table 层 `RemoveRecord` 并处理 FK。
- `pkg/executor/load_data.go`
  - `LoadDataExec.Next` 进入 local / remote load。
  - `LoadDataWorker.load` 创建 reader、encode worker 和 commit worker pipeline。
  - `encodeWorker.readOneBatchRows` / `parserData2TableData` 做 parser 输出读取、列映射、user variable、`SET` 表达式、cast/default/generated column。
  - `commitWorker.checkAndInsertOneBatch` 按 worker task 复用 insert / duplicate-check 写入路径；不要把每个 task 等同为单独的 statement commit。
- `pkg/table/tables/tables.go` 和 `pkg/table/tables/index.go`
  - `TableCommon.AddRecord` 构造 record key/value、写 memBuffer、创建索引。
  - `TableCommon.UpdateRecord` 重建受影响索引、编码新 row、写 memBuffer。
  - `TableCommon.RemoveRecord` delete record key 和 index keys。
  - `index.create` / `index.Delete` 生成 index key/value，做 unique check、temp index double-write、assertion 和 memBuffer mutation。
- `pkg/sessionctx/stmtctx/stmtctx.go`
  - `RecordRows`、`CopiedRows`、`UpdatedRows`、`DeletedRows`、`TouchedRows`、`AffectedRows` 是 MySQL 兼容语义计数，不是物理写入成本。
- `pkg/executor/adapter.go` 和 `pkg/util/execdetails/ruv2_metrics.go`
  - 当前 RU v2 从 `CommitDetails.WriteKeys` / `WriteSize` 汇总写入相关指标，但显式事务、batch DML 和 pipelined DML 的归因边界不同于 executor-local base units。

## 模型边界

写 billing demo 第一版按 `site` 分成三层：

| Site | 含义 | 第一版动作 |
|---|---|---|
| `statement` | statement-level status | 只写 status |
| `tidb` | TiDB executor、row prepare、table/index mutation 构造、memBuffer mutation、LOAD DATA decode | 计入 base units |
| `tikv` | storage read/write、lock、2PC、raft/write bytes | 第一版不直接计入 TiDB 写 base units；可作为独立 storage billing 或 status/reference |

TiDB 写计费只覆盖 TiDB 本地工作。下面这些工作不应在 TiDB write opclass 中计入：

- `INSERT ... SELECT`、`UPDATE`、`DELETE` child plan 的 scan/filter/join/read executor 成本。它们应由 read billing demo 继续计入。
- duplicate / FK check 过程中实际落到 TiKV 的 `txn.Get`、`txn.BatchGet`、snapshot iterator 的 storage read 成本。
- pessimistic lock acquisition、`txn.LockKeys`、prewrite、commit、resolve lock、region grouping、local latch、raftstore write。
- TiKV / TiFlash storage engine write amplification。

TiDB 写侧可以记录 duplicate probe keys、mutation keys、mutation bytes 等无系数输入，但这些 unit 的权重必须解释为 TiDB 本地 key construction / encoding / memBuffer work，而不是 storage RPC 或 2PC 成本。

## Taxonomy

### Site

第一版使用：

- `statement`
- `tidb`

保留未来扩展：

- `tikv`
- `tiflash`

### Op Class

第一版推荐的 TiDB 写 opclasses：

| Op class | 主要覆盖 | 代码锚点 |
|---|---|---|
| `write_row_prepare` | VALUES/SELECT/LOAD DATA 输入行到 table row 的表达式求值、cast、default、generated column、auto id/rowid/on-update-now | `InsertValues.evalRow`、`fastEvalRow`、`getRow`、`fillRow`、`UpdateExec.composeNewRow`、`parserData2TableData` |
| `write_duplicate_probe` | 构造 handle / unique index probe keys，批量 duplicate 检查的 TiDB 本地循环和 key handling | `getKeysNeedCheck`、`prefetchUniqueIndices`、`batchCheckAndInsert`、`batchUpdateDupRows`、`ReplaceExec.replaceRow` |
| `write_row_compare` | UPDATE / ODKU old/new row 比较、changed/no-op 判断、handle changed 判断 | `updateRecord` |
| `write_row_mutation` | record key/value encode，record key set/delete，row-level memBuffer mutation | `TableCommon.AddRecord`、`UpdateRecord`、`RemoveRecord` |
| `write_index_mutation` | index key/value encode，index set/delete，unique-index memBuffer checks，MV index expansion，temp-index double write | `TableCommon.addIndices`、`rebuildUpdateRecordIndices`、`removeRowIndices`、`index.create`、`index.Delete` |
| `write_foreign_key` | FK check/cascade 的 TiDB 本地 row/key preparation | `FKCheckExec`、`FKCascadeExec`、`checkRows`、`onRemoveRowForFK` |
| `write_txn_orchestrate` | batch DML statement commit boundary、`NewTxnInStmt`、`txn.MayFlush` 调用等 TiDB 事务编排事件 | `doBatchInsert`、`doBatchDelete`、`txn.MayFlush` callers |
| `load_data_decode` | LOAD DATA 文件行解析、列映射、user vars、SET 表达式进入 row prepare 前的 TiDB 本地处理 | `LoadDataWorker.load`、`encodeWorker.readOneBatchRows`、`parserData2TableData` |

`operator_kind` 是 bounded diagnostic 维度，建议使用具体执行路径而不是表名或索引名：

- `insert_values`
- `insert_select`
- `insert_on_duplicate`
- `replace`
- `update`
- `delete`
- `load_data`
- `batch_insert`
- `batch_delete`
- `table_add_record`
- `table_update_record`
- `table_remove_record`
- `index_create`
- `index_delete`
- `foreign_key_check`
- `foreign_key_cascade`
- `txn_may_flush`
- `batch_dml_stmt_commit`

### Unit

第一版推荐 unit 枚举：

| Unit | 含义 |
|---|---|
| `fixed_events` | opclass 发生次数；用于保留固定开销 |
| `input_rows` | opclass 消耗的逻辑输入行数 |
| `input_cells` | 被表达式、cast、default、compare 处理的列值数量；可近似 row * column |
| `input_bytes` | TiDB 本地处理的 row/file/value byte-shaped 输入 |
| `probe_keys` | duplicate / FK / unchanged-lock 等准备或检查的 key 数 |
| `probe_key_bytes` | probe keys 的 key bytes |
| `mutation_keys` | TiDB 写入 memBuffer 的 record/index set/delete key 数 |
| `mutation_bytes` | TiDB 构造并写入 memBuffer 的 key/value bytes；delete 只计 key bytes |
| `changed_rows` | 发生真实 row mutation 的逻辑行数 |
| `unchanged_rows` | UPDATE/ODKU/REPLACE 中比较后没有真实 row mutation 的行数 |
| `conflict_rows` | duplicate probe 找到冲突并进入 ignore/update/replace 分支的行数 |
| `batch_events` | batch DML / LOAD DATA commit worker / statement transaction boundary 事件 |

这些 units 都是 coefficient-free。下游校准可以选择只使用其中一部分；TiDB 仍应保留所有可验证维度。

## Base Units

### `write_row_prepare`

覆盖：

- INSERT VALUES / SET 的表达式求值和 cast。
- INSERT SELECT 从 child row 到 table row 的 cast/default/generated/auto id。
- UPDATE assignment 表达式、cast、generated/on-update-now。
- LOAD DATA 行解析后的列映射、SET 表达式、cast/default/generated。

Base units：

| Unit | 来源 | 需要新增 instrumentation |
|---|---|---|
| `fixed_events` | 每次 executor 或 worker batch 发生 | 是 |
| `input_rows` | `InsertValues.rowCount` delta、`StmtCtx.RecordRows`、UPDATE child rows、LOAD DATA batch count | 部分需要 |
| `input_cells` | insert/update 当前已有 `rowsColMultiply` 口径，LOAD DATA 可用 parsed columns | 需要保存为维度化样本 |
| `input_bytes` | `types.EstimatedMemUsage`、LOAD DATA raw bytes、row datum estimated size | 需要 |

`StmtCtx.AffectedRows` 不可用于这里，因为 ODKU、REPLACE、CLIENT_FOUND_ROWS 和 FK trigger 会改变 MySQL 语义。

### `write_duplicate_probe`

覆盖：

- `getKeysNeedCheck` 为每个 row 生成 handle key 和 unique index keys。
- `prefetchUniqueIndices` / `prefetchConflictedOldRows` 的 TiDB key collection。
- `batchCheckAndInsert`、ODKU、REPLACE 中逐 key duplicate handling。

Base units：

| Unit | 来源 | 需要新增 instrumentation |
|---|---|---|
| `fixed_events` | 每个 duplicate-check batch | 是 |
| `input_rows` | 被检查的 row 数 | `toBeCheckedRows` 长度可得 |
| `probe_keys` | handle key + unique keys + temp-index fallback keys | 是 |
| `probe_key_bytes` | probe key len sum | 是 |
| `conflict_rows` | duplicate path 找到冲突的 row 数 | 是 |

注意：实际 `txn.Get` / `BatchGet` 的 storage read cost 不在本 opclass 权重里。若未来 TiKV read/write billing 覆盖这些请求，应通过 storage site 或独立 opclass 处理。

### `write_row_compare`

覆盖：

- UPDATE 和 ODKU 中 old/new row 比较。
- no-op update / unchanged replace 判断。
- handle changed 判断。

Base units：

| Unit | 来源 | 需要新增 instrumentation |
|---|---|---|
| `fixed_events` | 每个 update/ODKU compare path | 是 |
| `input_rows` | 进入 `updateRecord` 的 row 数 | 是 |
| `input_cells` | `len(t.Cols())` 或 writable columns | 是 |
| `changed_rows` | `updateRecord` changed=true | 是 |
| `unchanged_rows` | `updateRecord` changed=false 或 identical replace | 是 |

`unchanged_rows` 保留为 base unit，是因为 no-op UPDATE 仍有 TiDB 比较和 lock-key preparation 成本，但不能被当成 row mutation。

### `write_row_mutation`

覆盖：

- `AddRecord` record key/value encode 和 memBuffer set。
- `UpdateRecord` record value re-encode 和 memBuffer set。
- `RemoveRecord` record key delete。
- handle changed UPDATE 中的 remove + add 两段 mutation。
- REPLACE 中删除旧 row 后插入新 row。

Base units：

| Unit | 来源 | 需要新增 instrumentation |
|---|---|---|
| `fixed_events` | `AddRecord` / `UpdateRecord` / `RemoveRecord` 调用 | 是 |
| `input_rows` | row mutation 数，remove + add 分开计 | 是 |
| `input_cells` | encoded / checked writable columns | 是 |
| `mutation_keys` | record key set/delete 数 | 是 |
| `mutation_bytes` | record key bytes + encoded row value bytes；delete 只计 key bytes | 是 |

`CommitDetails.WriteKeys` / `WriteSize` 不适合作为第一版来源：它们发生在 commit/2PC 边界，显式事务会偏向 `COMMIT` 语句，pipelined DML 还存在 flush 归并口径风险。

### `write_index_mutation`

覆盖：

- AddRecord 创建 writable secondary indexes。
- UpdateRecord 删除旧 index、创建新 index，跳过 untouched index 的优化。
- RemoveRecord 删除 deletable indexes。
- unique index 的 local/memBuffer 检查和 assertion 设置。
- MV index expansion。
- DDL 状态下的 temp index / double-write。

Base units：

| Unit | 来源 | 需要新增 instrumentation |
|---|---|---|
| `fixed_events` | 每次进入 index mutation path | 是 |
| `input_rows` | 参与 index mutation 的 row 数 | 是 |
| `input_cells` | index columns + affected columns + partial-index condition columns | 是 |
| `mutation_keys` | 实际 memBuffer set/delete 的 index key 数；temp-index double-write 分开计 | 是 |
| `mutation_bytes` | index key bytes + index value bytes；delete 只计 key bytes | 是 |
| `probe_keys` | unique index existence check keys | 是 |
| `probe_key_bytes` | unique index check key bytes | 是 |

不要把 `index_name` 放入维度。索引数量和索引 key bytes 已经反映写放大；`operator_kind=index_create/index_delete` 足够诊断路径。

### `write_foreign_key`

覆盖：

- FK check rows/key preparation。
- FK cascade 调用前后的 TiDB 本地 row preparation。

Base units：

| Unit | 来源 | 需要新增 instrumentation |
|---|---|---|
| `fixed_events` | 每个 FK checker/cascade executor | 是 |
| `input_rows` | 被检查或 cascade 的 rows | 部分可从 FK runtime stats 推导 |
| `probe_keys` | FK check keys | 是 |
| `probe_key_bytes` | FK key bytes | 是 |

第一版可先对 simple FK check 计 base units；复杂 cascade 若无法完整归因，应写 status `unsupported` / `unsupported_fk_cascade`，不能发 partial units。

### `write_txn_orchestrate`

覆盖：

- batch insert/delete 每批 `StmtCommit` + `NewTxnInStmt`。
- `txn.MayFlush` 调用事件。
- LOAD DATA commit worker batch 事件。

Base units：

| Unit | 来源 | 需要新增 instrumentation |
|---|---|---|
| `fixed_events` | 事务编排事件 | 是 |
| `batch_events` | batch DML commit boundary / LOAD DATA worker task 数 | 是 |
| `input_rows` | batch 中 rows | 可从 batch count 得到 |

本 opclass 只表示 TiDB 事务编排固定成本，不包含 prewrite/commit/lock 的 storage/2PC 成本。

### `load_data_decode`

覆盖：

- 文件行读取和 parser 输出。
- column list、user variables、`SET` 表达式。
- restrictive / non-restrictive load 的 warning/error handling。

Base units：

| Unit | 来源 | 需要新增 instrumentation |
|---|---|---|
| `fixed_events` | 每个 load stream / batch | 是 |
| `input_rows` | parsed rows | 可从 controller/encoder batch count 得到 |
| `input_cells` | parsed fields + SET expressions | 是 |
| `input_bytes` | raw file bytes 或 parser consumed bytes | 是 |

后续插入阶段继续复用 `write_row_prepare`、`write_duplicate_probe`、`write_row_mutation`、`write_index_mutation`。

## 覆盖语句

### INSERT VALUES / SET

推荐发出：

- `write_row_prepare`
- `write_row_mutation`
- `write_index_mutation`
- `write_txn_orchestrate`，仅 batch DML、pipelined flush 或显式需要 statement transaction boundary 时

普通 INSERT 的 duplicate check 如果走 lazy check，只在 `write_index_mutation` 中记录 unique index key construction / memBuffer flags；不要把 future prewrite constraint check 算进 TiDB executor。

### INSERT ... SELECT

读路径由 read billing 计费：

- child SELECT 的 scan/filter/join/aggregation 继续使用 read billing units。
- write billing 从 `insertRowsFromSelect` 从 child chunk 取出的 rows 开始，只计 row prepare 和 table/index mutation。
- `input_source` 建议记录为 `child_output_rows`，避免把 child executor 的 input rows 再计一次。

### INSERT IGNORE

推荐发出：

- `write_row_prepare`
- `write_duplicate_probe`
- 对未冲突 rows 发出 `write_row_mutation` / `write_index_mutation`
- 对冲突 rows 发出 `conflict_rows`，但不发 mutation units

### INSERT ... ON DUPLICATE KEY UPDATE

推荐发出：

- `write_row_prepare`，对 candidate insert rows
- `write_duplicate_probe`
- 对非冲突 rows 发出 insert mutation units
- 对冲突 rows 发出 `write_row_compare`
- 对 changed duplicate rows 发出 update mutation units
- 对 unchanged duplicate rows 发出 `unchanged_rows`，不发 row/index mutation units

ODKU 的 `AffectedRows += 2` 是 MySQL 语义，不能作为 mutation row 数。

### REPLACE

REPLACE 的 physical effect 应拆成 duplicate probe、delete mutation、insert mutation：

- candidate row 发出 `write_row_prepare`
- duplicate path 发出 `write_duplicate_probe`
- 每个真正被删除的旧 row 发出 delete-flavored `write_row_mutation` / `write_index_mutation`
- 每个真正插入的新 row 发出 insert-flavored `write_row_mutation` / `write_index_mutation`
- identical row 不发 mutation units，只发 `unchanged_rows`

这和 `AffectedRows` 的 1/2 语义分离。

### UPDATE

读路径由 read billing 计费：

- child executor 找 matched rows 的 scan/join/filter 成本属于 read side。
- write billing 从 `UpdateExec.updateRows` 读取到 matched row 后开始。

推荐发出：

- `write_row_prepare`，assignment/cast/generated/on-update-now
- `write_row_compare`
- handle unchanged 且 changed：`UpdateRecord` 作为 update-flavored row/index mutation
- handle changed：`RemoveRecord + AddRecord` 拆成 delete + insert mutation
- no-op update：只发 compare units 和 `unchanged_rows`

### DELETE

读路径由 read billing 计费：

- child executor 找待删 rows 的成本属于 read side。
- write billing 从 `DeleteExec.deleteOneRow` / `removeRow` 开始。

推荐发出：

- `write_row_mutation`，record key delete
- `write_index_mutation`，deletable index delete
- batch delete 额外发 `write_txn_orchestrate`

Multi-table DELETE 应按每个实际删除的 table row 发 mutation units，但不把 table id 纳入维度。

### Batch DML

`tidb_batch_insert` / `tidb_batch_delete` 会在单条 SQL 内多次 `StmtCommit` 并 `NewTxnInStmt`。base units 应仍归属于原始 digest/window：

- row/index units 累加到原 statement。
- `write_txn_orchestrate.batch_events` 记录 batch boundary 次数。
- 不新增 batch number 维度，避免高基数。

### LOAD DATA

LOAD DATA 首版建议覆盖两个阶段：

1. `load_data_decode`：文件 decode、列映射、user variables、SET expression。
2. insert-like write path：按 `ERROR` / `IGNORE` / `REPLACE` 复用 INSERT / duplicate / REPLACE 的 units。

远端对象存储读取、网络下载和 decompression worker 是否属于 TiDB billing 需要单独决定。若没有可靠 byte evidence，先发 status `unknown_input` / `missing_load_data_bytes`，不要估算。

## 初始权重

以下权重只用于 demo preview 和离线试算，不代表生产校准结果。数值选择原则：

- `fixed_events` 只覆盖小额固定成本；
- `input_cells` 捕捉表达式、cast、比较的 per-cell CPU；
- `probe_keys` / `probe_key_bytes` 只覆盖 TiDB key construction 和 local handling，不覆盖 TiKV read RPC；
- `mutation_keys` / `mutation_bytes` 覆盖 TiDB row/index encode 和 memBuffer set/delete，不覆盖 storage write；
- index mutation 通常比 row mutation 更贵，因为它包含 index value/key generation、partial/MV/unique 处理；
- duplicate probe、row compare、LOAD DATA decode 根据源码中的额外逻辑给予更高 per-row/per-key 权重。

推荐 seed weights，单位是 preview RU per unit：

| Site | Op class | `fixed_events` | `input_rows` | `input_cells` | `input_bytes` | `probe_keys` | `probe_key_bytes` | `mutation_keys` | `mutation_bytes` | `changed_rows` | `unchanged_rows` | `conflict_rows` | `batch_events` |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `tidb` | `write_row_prepare` | 0.030 | 0.000020 | 0.000006 | 0.000001 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| `tidb` | `write_duplicate_probe` | 0.040 | 0.000015 | 0 | 0 | 0.000025 | 0.000001 | 0 | 0 | 0 | 0 | 0.000040 | 0 |
| `tidb` | `write_row_compare` | 0.030 | 0.000015 | 0.000005 | 0.000001 | 0 | 0 | 0 | 0 | 0.000010 | 0.000006 | 0 | 0 |
| `tidb` | `write_row_mutation` | 0.050 | 0.000020 | 0.000004 | 0.000001 | 0 | 0 | 0.000035 | 0.000004 | 0.000010 | 0 | 0 | 0 |
| `tidb` | `write_index_mutation` | 0.060 | 0.000015 | 0.000006 | 0.000001 | 0.000020 | 0.000001 | 0.000050 | 0.000005 | 0 | 0 | 0 | 0 |
| `tidb` | `write_foreign_key` | 0.050 | 0.000020 | 0.000004 | 0.000001 | 0.000030 | 0.000001 | 0 | 0 | 0 | 0 | 0 | 0 |
| `tidb` | `write_txn_orchestrate` | 0.020 | 0.000003 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.010 |
| `tidb` | `load_data_decode` | 0.060 | 0.000025 | 0.000007 | 0.000002 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

相对顺序：

```text
write_index_mutation
  > write_row_mutation
  > write_duplicate_probe ~= load_data_decode
  > write_row_prepare ~= write_row_compare
  > write_txn_orchestrate
```

这个顺序来自当前源码工作量：

- `index.create` / `index.Delete` 需要 index value/key generation、MV expansion、unique/temp-index 处理和 memBuffer flags。
- `AddRecord` / `UpdateRecord` / `RemoveRecord` 需要 row encoding、constraint check、assertion 和 record key mutation。
- duplicate probe 在 `getKeysNeedCheck`、`prefetchUniqueIndices`、`batchCheckAndInsert` 中构造多组 keys 并循环处理冲突。
- row prepare 和 row compare 主要是 expression/cast/default/generated/compare 的 per-row/per-cell CPU。
- transaction orchestration 的本地固定开销存在，但 storage/2PC 成本不在本 opclass。

## 采集和暴露面

推荐新增与 read billing demo 平行的写侧开关和 SQL 面：

- `tidb_enable_write_billing_demo`，默认 OFF。
- `INFORMATION_SCHEMA.STATEMENTS_SUMMARY_WRITE_BILLING_DEMO_BASE_UNITS`
- `INFORMATION_SCHEMA.STATEMENTS_SUMMARY_HISTORY_WRITE_BILLING_DEMO_BASE_UNITS`
- `INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_WRITE_BILLING_DEMO_BASE_UNITS`
- `INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY_WRITE_BILLING_DEMO_BASE_UNITS`
- `INFORMATION_SCHEMA.STATEMENTS_SUMMARY_WRITE_BILLING_DEMO_STATUS`
- `INFORMATION_SCHEMA.STATEMENTS_SUMMARY_HISTORY_WRITE_BILLING_DEMO_STATUS`
- `INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_WRITE_BILLING_DEMO_STATUS`
- `INFORMATION_SCHEMA.CLUSTER_STATEMENTS_SUMMARY_HISTORY_WRITE_BILLING_DEMO_STATUS`

Base-unit table columns 应沿用 read billing demo 的主要维度，并把 read-only 的 `ROW_WIDTH_SOURCE` 泛化为 `WIDTH_SOURCE` 或保留兼容命名：

| Column | 含义 |
|---|---|
| statement summary window / digest / plan digest / resource group columns | 沿用 statement summary |
| `MODEL_VERSION` | write billing model version |
| `WEIGHT_VERSION` | 生成样本时使用的 weight version |
| `SITE` | `tidb` |
| `OP_CLASS` | bounded opclass |
| `OPERATOR_KIND` | bounded diagnostic kind |
| `UNIT` | bounded unit |
| `INPUT_SOURCE` | `executor_counter` / `stmtctx_counter` / `runtime_stats` / `mem_buffer_delta` / `encoded_bytes` / `load_data_parser` |
| `INPUT_SIDE` | `all`；必要时可用 `old` / `new` / `delete` / `insert`，但不要放 table/index name |
| `VALUE` | unit total |
| `SAMPLE_COUNT` | sample count |

Status table 应和 read billing demo 一致，支持 statement-level 和 operator-level status。

Prometheus 可以新增 bounded observability metrics：

- `tidb_write_billing_demo_statements_total`
- `tidb_write_billing_demo_operator_status_total`
- `tidb_write_billing_demo_base_units_total`

但 workload 校准契约应以 SQL 查询面为准，不能要求 testers 依赖 Prometheus retention。

## 失败和降级状态

推荐 status：

| Status | 含义 |
|---|---|
| `success` | statement-level accounting 完整 |
| `ok` | operator-level accounting 完整 |
| `unsupported` | 语句或路径明确不支持 |
| `unknown_input` | 必要输入缺失，不能安全估算 |
| `error` | statement 执行失败 |

推荐 reason：

| Reason | 触发场景 |
|---|---|
| `statement_error` | executor 返回错误 |
| `missing_plan` | 没有可归因 plan |
| `missing_stmtctx` | statement context 不可用 |
| `missing_runtime_stats` | 必要 runtime stats 不可用 |
| `missing_mutation_stats` | table/index mutation 没有 instrumentation |
| `missing_encoded_bytes` | bytes unit 需要的 encoded length 不可用 |
| `missing_load_data_bytes` | LOAD DATA 无可靠 input bytes |
| `unsupported_statement` | 非目标语句 |
| `unsupported_fk_cascade` | FK cascade 无法完整归因 |
| `unsupported_temporary_table` | temporary table 口径不同且未支持 |
| `unsupported_temp_index_merge` | DDL temp-index merge / double-write 无法完整归因 |
| `unsupported_internal_sql` | restricted/internal SQL |
| `aggregation_overflow` | base-unit key cap overflow |
| `status_aggregation_overflow` | status key cap overflow |

失败或 unsupported statement 不写 base units。若某个必需 opclass 缺失输入，应 fail closed：写 status，跳过该 statement 的所有 base units，避免 partial units 被误用于校准。

## 避免重复计费

1. 读写混合语句：
   - child SELECT / scan / join / filter 由 read billing 负责；
   - write billing 只从 DML executor 消费 child rows 后开始；
   - `input_source=child_output_rows` 可标明边界。
2. duplicate / FK storage probes：
   - TiDB write side 只计 key construction 和本地 loop；
   - `txn.Get` / `BatchGet` / snapshot scan 成本属于 storage/read path；
   - 不把 probe RPC latency 或 processed keys 乘入 TiDB write weights。
3. row/index mutation 与 commit details：
   - `mutation_keys` / `mutation_bytes` 是 TiDB 构造并放入 memBuffer 的 key/value；
   - `CommitDetails.WriteKeys` / `WriteSize` 是 2PC mutation 口径，显式事务可能归到 COMMIT；
   - 两者不能同时作为同一层权重输入。
4. transaction orchestration：
   - `write_txn_orchestrate` 只计本地 fixed/batch event；
   - prewrite/commit/lock/resolve-lock/local-latch 不属于本 opclass。
5. affected rows：
   - `AffectedRows` 只用于 SQL 兼容展示；
   - physical mutation rows 必须从 `AddRecord` / `UpdateRecord` / `RemoveRecord` 或新增 counters 得到。

## 实现期 Instrumentation 需求

可直接复用或改造的现有输入：

| 输入 | 当前来源 | 限制 |
|---|---|---|
| statement type / digest / plan digest / resource group | statement summary | 可复用 |
| `RecordRows` / `CopiedRows` / `UpdatedRows` / `DeletedRows` / `TouchedRows` | `StatementContext` | SQL 语义，不够物理 |
| `rowsColMultiply` | `InsertValues.rowsColMultiply`、`UpdateExec.rowsColMultiplyForPreparedRow`、delete row/column count | 当前只进 RU v2 scalar counter，需要结构化保存 |
| insert/update runtime stats | `InsertRuntimeStat`、`updateRuntimeStats` | time/RPC 诊断，不是 base-unit source |
| commit write keys/size | `CommitDetails` / RU v2 metrics | storage/2PC 边界，不适合作为 TiDB executor-local source |

需要新增：

1. Statement-scoped `WriteBillingDemoStats`，与 read billing stats 类似，包含 status samples 和 base-unit samples。
2. Table/index mutation instrumentation：
   - record set/delete key count；
   - record key bytes；
   - encoded row value bytes；
   - index set/delete key count；
   - index key/value bytes；
   - temp-index double-write count。
3. Duplicate probe instrumentation：
   - per row handle key count；
   - unique key count；
   - probe key bytes；
   - conflict rows；
   - temp-index fallback probes。
4. Row prepare / compare instrumentation：
   - input rows；
   - input cells；
   - estimated input bytes；
   - changed / unchanged row counts。
5. LOAD DATA instrumentation：
   - parser consumed bytes；
   - parsed rows / fields；
   - SET expression cells；
   - restrictive/non-restrictive skipped rows。
6. Statement summary v1/v2 aggregation maps and JSON-stable v2 entry slices，保留 overflow status。

## 测试设计

后续实现应至少覆盖：

1. INSERT VALUES：
   - 无索引、普通二级索引、unique index、common handle、auto id。
   - 验证 row prepare、row mutation、index mutation units。
2. INSERT SELECT：
   - 验证 read side 和 write side 不重复计算 child input rows。
3. INSERT IGNORE：
   - 无冲突、handle 冲突、unique 冲突。
   - 冲突 rows 只产生 duplicate probe / conflict units，不产生 mutation units。
4. ODKU：
   - 无冲突插入、冲突更新、冲突 no-op。
   - `AffectedRows` 与 mutation rows 分离。
5. REPLACE：
   - identical row、replace one row、多个 unique key 触发多次 remove。
6. UPDATE：
   - no-op、普通 update、handle changed update、multi-table update。
7. DELETE：
   - single-table、multi-table、batch delete。
8. LOAD DATA：
   - ERROR / IGNORE / REPLACE 三种 duplicate mode；
   - local 和 remote reader 的 input bytes 缺失时 status 行。
9. FK：
   - simple FK check；
   - unsupported cascade status-only。
10. Statement summary：
   - v1 current/history、v2 persistent current/history、cluster table；
   - base-unit aggregation overflow 和 status aggregation overflow；
   - error / unsupported / unknown-input 不写 base units。
11. RU v2 隔离：
   - 开启 write billing demo 不改变 resource-control RU 上报；
   - `CommitDetails.WriteKeys/WriteSize` 不被误当成 TiDB write base units。

## 风险和未决问题

1. `mutation_bytes` 的最佳口径需要实现时固定：建议用 TiDB 实际构造的 key bytes + value bytes，而不是 memBuffer memory footprint。
2. 显式事务中一条 DML 产生 mutation，另一条 COMMIT 提交。若未来要 storage write billing，需要单独解决语句归因；本文只解决 TiDB executor-local write preparation。
3. Pipelined DML 当前 commit details 可能无法完整反映 flushed keys/size。不要用该口径验证 TiDB write units。
4. Temporary table、temp index merge、DDL backfill 期间的 double-write 口径复杂，第一版可以 status-only 或以 bounded units 记录 double-write count。
5. FK cascade 可能递归触发多条 row mutation。第一版应先保证不漏报 status，再决定是否把 cascade 作为独立 nested write opclass。
6. `LOAD DATA` 的文件读取、解压、对象存储网络是否属于 TiDB write billing 需要产品层确认；本设计只覆盖 parser/decode 和 insert-like write path。
