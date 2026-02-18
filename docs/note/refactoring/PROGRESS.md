# Refactoring Progress Tracker

Last updated: 2026-02-18 (exhaust_physical_plans.go decomposition)

## Benchmark Validation (2026-02-17)

### Micro-benchmarks (benchstat, p=0.008, n=5)
- **Vectorized string functions**: -24% to -59% faster (SUBSTR, LOCATE, CHAR_LENGTH)
  - Substring2ArgsUTF8 Vec: -24.4%, Substring3ArgsUTF8 Vec: -38.5%
  - Locate3ArgsUTF8 Vec: -48.3% to -59.5%, Locate2ArgsUTF8 Vec: -7.6% to -15.0%
  - CharLengthUTF8 NonVec: -20.8%
  - Geomean: **-23.9% sec/op, -5.2% B/op, -4.3% allocs/op**
- **reflect.TypeOf caching**: 10.0ns → 5.65ns (**1.8x faster**)
- **Chunk iterator reuse**: negligible (compiler inlines allocation)

### go-ycsb macro (8 threads, 20k ops, 10k records, unistore)
- Workload A (50/50 read/update): ~50k OPS, no regression
- Workload C (100% read): ~78k OPS, no regression

## Status Legend

- `[ ]` Not started
- `[~]` In progress
- `[x]` Completed
- `[-]` Skipped / Won't do

---

## P0 - Critical (Do First)

- [x] **executor/builder.go split** - Replace 103-case switch with registry/factory pattern
  - File: `pkg/executor/builder.go` (6,222 lines → 1,082 lines, 83% reduction)
  - Target: Split into per-operator-type builder files with auto-registration
  - [x] Phase 1: Extract `buildMemTable` (292 lines) → `builder_memtable.go`
  - [x] Phase 1: Extract reader builders (1,652 lines) → `builder_reader.go`
  - [x] Phase 2: Extract analyze builders (376 lines) → `builder_analyze.go`
  - [x] Phase 2: Extract join builders (706 lines) → `builder_join.go`
  - [x] Phase 3: Extract DDL/admin builders (474 lines) → `builder_ddl_admin.go`
  - [x] Phase 3: Extract sort/window builders (411 lines) → `builder_sort_window.go`
  - [x] Phase 4: Extract agg/project builders (244 lines) → `builder_agg_project.go`
  - [x] Phase 4: Extract CTE/misc builders (315 lines) → `builder_cte_misc.go`
  - [x] Phase 4: Extract statement builders (601 lines) → `builder_stmt.go`
  - [x] Phase 4: Extract UnionScan builders (236 lines) → `builder_union_scan.go`

- [ ] **Hash Join V1 deprecation** - Complete V2 for all join types, remove V1
  - Files: `pkg/executor/join/hash_join_v1.go` (1,458 lines), `hash_join_v2.go` (1,538 lines)
  - Target: Single implementation with full join type coverage

- [x] **Hot-path allocation fixes** - Pool iterators, pre-allocate slices
  - [x] `pkg/executor/select.go:259` - chunk iterator recreation per Next() → cached on struct
  - [-] `pkg/executor/adapter.go:122-157` - ResultField slice per Fields() call (already cached, no action needed)
  - [x] `pkg/executor/select.go:728-729` - selected slice re-init → reuse backing array
  - [x] `pkg/executor/internal/exec/executor.go:457` - reflect.TypeOf().String() per Next() → cached in sync.Map
  - [x] `pkg/expression/builtin_string_vec.go` - 17 []rune allocations replaced with utf8 operations
  - [x] `pkg/expression/builtin_string.go` - 16 []rune allocations replaced with utf8 operations
  - [x] `pkg/expression/builtin_encryption.go` - 1 []rune allocation replaced

- [x] **Panic elimination in production code** - Replace with error returns
  - [x] 8 runtime panics replaced: builder.go, analyze.go, index_merge_reader.go,
        aggfuncs/func_value.go, encode.go, rule_partition_processor.go, txn_info.go, summary.go
  - [-] `pkg/kv/key.go` - 6 panics are intentional interface contract assertions (Handle)
  - [-] `pkg/session/txnmanager.go` - panic is inside failpoint (test-only)
  - [-] `pkg/server/http_status.go` - startup-time assertions (acceptable)

## P1 - High Priority

- [x] **session.go decomposition** - Extract transaction, variable, privilege management
  - File: `pkg/session/session.go` (5,558 → 1,087 lines, 80% reduction)
  - Target: <1000 lines per file with clear sub-package boundaries
  - [x] `session_txn.go` (1,086 lines) - transaction commit/rollback, retry logic, txn context, pipelined DML
  - [x] `session_bootstrap.go` (955 lines) - bootstrap, session factory, domain init, session creation
  - [x] `session_auth.go` (413 lines) - authentication, privilege validation
  - [x] `session_execute.go` (836 lines) - statement execution, prepared stmts
  - [x] `session_parse.go` (546 lines) - SQL parsing, process info, internal exec
  - [x] `session_restricted.go` (287 lines) - restricted SQL execution
  - [x] `session_logging.go` (387 lines) - query logging, metrics, telemetry
  - [x] `session_states.go` (212 lines) - encode/decode session states
  - [x] `session_sysvar.go` (157 lines) - system variable management

- [x] **domain.go decomposition** - Extract subsystem managers
  - File: `pkg/domain/domain.go` (3,023 → 997 lines, 67% reduction)
  - Target: Composable service managers
  - [x] `domain_stats.go` (723 lines) - statistics worker lifecycle, historical stats, analyze workers
  - [x] `domain_privilege.go` (467 lines) - privilege events, binding management, sysvar cache, notifications
  - [x] `domain_serverid.go` (356 lines) - server ID acquisition, renewal, keeper loop
  - [x] `domain_workers.go` (285 lines) - telemetry, plan cache, TTL, workload learning workers
  - [x] `domain_disttask.go` (208 lines) - distributed task framework loop
  - [x] `domain_infra.go` (192 lines) - log backup advancer, replica read check loop

- [x] **Planner-executor dependency break** - Remove executor imports from planner
  - Moved `pkg/executor/join/joinversion` → `pkg/util/joinversion`
  - Updated all 11 Go importers; BUILD.bazel files need `make bazel_prepare`

- [ ] **Cost model unification** - Deprecate one of ver1/ver2
  - Files: `pkg/planner/core/plan_cost_ver1.go`, `plan_cost_ver2.go`
  - Target: Single cost model

- [x] **SessionVars decomposition** - Split 350+ field mega-struct
  - File: `pkg/sessionctx/variable/session.go` (3,853 lines)
  - Target: Grouped sub-structs by concern
  - [x] Phase 1: Extracted `TiFlashVars` (27 fields) and `CostModelFactors` (28 fields) as embedded sub-structs
  - [x] Phase 2: Extracted `PlanCacheVars` (13 fields) and `OptimizerVars` (32 fields) as embedded sub-structs
  - [x] Phase 3a: Extracted `StatsVars` (16 statistics-related fields) as embedded sub-struct
  - [x] Phase 3b: Extracted `TransactionVars` (16 transaction-related fields) as embedded sub-struct
  - [x] Phase 4: Extracted `ExecutionVars` (11 execution-related fields) as embedded sub-struct

- [x] **planbuilder.go decomposition** - Extract statement-specific plan builders
  - File: `pkg/planner/core/planbuilder.go` (6,518 → 2,116 lines, 68% reduction)
  - Target: Focused files per statement type
  - [x] `planbuilder_analyze.go` (1,296 lines) - ANALYZE statement handlers
  - [x] `planbuilder_insert.go` (884 lines) - INSERT/LOAD/IMPORT handlers
  - [x] `planbuilder_admin.go` (570 lines) - ADMIN statements, index lookup
  - [x] `planbuilder_show.go` (539 lines) - SHOW/Simple/Grant/Revoke
  - [x] `planbuilder_split.go` (449 lines) - split/distribute, stats lock
  - [x] `planbuilder_ddl.go` (368 lines) - DDL privilege checks
  - [x] `planbuilder_bind.go` (343 lines) - SQL binding operations
  - [x] `planbuilder_explain.go` (247 lines) - EXPLAIN/TRACE statements

- [x] **logical_plan_builder.go decomposition** - Extract functional builder groups
  - File: `pkg/planner/core/logical_plan_builder.go` (7,362 → 3,897 lines, 47% reduction)
  - Target: Focused files per functional area
  - [x] `logical_plan_builder_window.go` (753 lines) - window function builders
  - [x] `logical_plan_builder_dml.go` (903 lines) - UPDATE/DELETE plan builders
  - [x] `logical_plan_builder_cte.go` (368 lines) - CTE plan builders
  - [x] `logical_plan_builder_setops.go` (448 lines) - UNION/INTERSECT/EXCEPT builders
  - [x] `logical_plan_builder_join.go` (394 lines) - JOIN plan builders
  - [x] `logical_plan_builder_datasource.go` (804 lines) - data source, view, memtable builders

- [x] **ddl/partition.go decomposition** - Extract partition operation handlers
  - File: `pkg/ddl/partition.go` (5,358 → 3,155 lines, 41% reduction)
  - Target: Focused files per partition operation
  - [x] `partition_reorganize.go` (1,185 lines) - onReorganizePartition, reorgPartitionWorker, helpers
  - [x] `partition_exchange.go` (725 lines) - onExchangeTablePartition, checkExchange*, buildCheckSQL*
  - [x] `partition_truncate.go` (411 lines) - onTruncateTablePartition, replaceTruncatePartitions

- [x] **executor/infoschema_reader.go decomposition** - Extract retriever types
  - File: `pkg/executor/infoschema_reader.go` (4,213 → 2,990 lines, 29% reduction)
  - Target: Focused files per retriever/functional area
  - [x] `infoschema_reader_lock.go` (492 lines) - tidbTrxTable, dataLockWaits, deadlocks retrievers
  - [x] `infoschema_reader_tiflash.go` (277 lines) - TiFlashSystemTableRetriever
  - [x] `infoschema_reader_resource.go` (569 lines) - attributes, policies, resources, keywords, plan cache

- [x] **ddl/index.go decomposition** - Extract index operation handlers
  - File: `pkg/ddl/index.go` (4,140 → 2,691 lines, 35% reduction)
  - Target: Focused files per index operation area
  - [x] `index_dist_task.go` (418 lines) - distributed task execution, param tuning, row size estimation
  - [x] `index_backfill_worker.go` (619 lines) - add-index txn worker, batch unique check, ingest write
  - [x] `index_columnar.go` (261 lines) - TiFlash columnar index creation and progress monitoring
  - [x] `index_analyze.go` (289 lines) - post-index-creation analyze workflow

- [x] **executor/simple.go decomposition** - Extract user and role management
  - File: `pkg/executor/simple.go` (3,198 → 1,064 lines, 67% reduction)
  - Target: Focused files per functional area
  - [x] `simple_user.go` (1,296 lines) - user creation, alteration, password management
  - [x] `simple_role.go` (917 lines) - role grant/revoke, set default/active role, rename/drop user

- [x] **executor/show.go decomposition** - Extract create statements and region ops
  - File: `pkg/executor/show.go` (2,825 → 1,412 lines, 50% reduction)
  - Target: Focused files per functional area
  - [x] `show_create.go` (714 lines) - SHOW CREATE TABLE/VIEW/DATABASE/SEQUENCE/POLICY/RESOURCE GROUP
  - [x] `show_region.go` (771 lines) - regions, distributions, import jobs, session states, builtins

- [x] **planner/core/find_best_task.go decomposition** - Extract property matching and scan conversion
  - File: `pkg/planner/core/find_best_task.go` (3,030 → 1,118 lines, 63% reduction)
  - Target: Focused files per functional area
  - [x] `find_best_task_property.go` (1,056 lines) - skyline pruning, candidate comparison, property matching
  - [x] `find_best_task_scan.go` (915 lines) - index/table/point-get scan conversion

- [x] **planner/core/exhaust_physical_plans.go decomposition** - Extract MPP join, hints, and index join inner
  - File: `pkg/planner/core/exhaust_physical_plans.go` (2,892 → 1,399 lines, 52% reduction)
  - Target: Focused files per functional area
  - [x] `exhaust_physical_plans_index_join_inner.go` (741 lines) - inner side task building for index joins
  - [x] `exhaust_physical_plans_hints.go` (382 lines) - hint recording, prefer-task logic, force/filter hints
  - [x] `exhaust_physical_plans_mpp_join.go` (367 lines) - MPP/TiFlash BCJ and shuffle join enumeration

- [ ] **DDL schema version lock** - Reduce global mutex scope
  - File: `pkg/ddl/ddl.go:387-445`
  - Target: Per-job or fine-grained locking

## P2 - Medium Priority

- [ ] **pkg/util/ reorganization** - Re-organize 111+ subdirectories
- [x] **String function rune optimization** - Use utf8 index-based operations
- [ ] **Goroutine pool for executors** - Aggregate, sort, projection worker pools
- [ ] **Datum type optimization** - Eliminate interface{} fallback, use typed union
- [ ] **Expression clone generation** - Auto-generate all Clone() methods
- [ ] **Aggregate executor map reduction** - Reduce O(P*F) map proliferation
- [ ] **Concurrent hash map adaptive sharding** - Replace fixed ShardCount=320
- [ ] **Statistics memoization in planner** - Cache selectivity calculations

## P3 - Low Priority

- [ ] **Remove old cascades code** - `pkg/planner/cascades/old/`
- [ ] **Executor interface split** - Lifecycle, Execution, Debug interfaces
- [ ] **Context propagation** - Replace 5,266 Background()/TODO() calls
- [x] **DDL executor.go split** - `pkg/ddl/executor.go` (7,201 → 1,986 lines, 72% reduction)
- [ ] **sessionctx.Context interface** - Split 76-method interface
- [ ] **nolint audit** - Investigate 738 suppressed warnings

---

## Completed Items

(Move items here when done, with date and PR link)

- [x] **SelectLockExec iterator caching** - 2026-02-17 - Cache `chunk.Iterator4Chunk` on struct to avoid heap allocation per `Next()` call
- [x] **SelectionExec selected slice reuse** - 2026-02-17 - Reuse `[]bool` backing array across `Open()`/`Close()` cycles instead of re-allocating
- [x] **exec.Next reflect.TypeOf caching** - 2026-02-17 - Cache `reflect.TypeOf(e).String()+".Next"` in `sync.Map` to avoid per-call reflection + string concatenation
- [x] **String function rune optimization** - 2026-02-17 - Replace 27 `[]rune(str)` allocations with `utf8.RuneCountInString` and `utf8.DecodeRuneInString` for zero-copy operations in LEFT, RIGHT, LOCATE, SUBSTR, INSERT, MID, LPAD, RPAD, CHAR_LENGTH
- [x] **Planner panic elimination** - 2026-02-17 - Replace `panic("unreachable")` with error return in `exhaustPhysicalPlans`
- [x] **Planner-executor dependency break** - 2026-02-17 - Move `joinversion` package from `executor/join/joinversion` to `util/joinversion`, eliminating planner→executor import
- [x] **Runtime panic elimination (8 panics)** - 2026-02-17 - Replace panics in builder.go, analyze.go, index_merge_reader.go, aggfuncs/func_value.go, encode.go, rule_partition_processor.go, txn_info.go, summary.go
- [x] **Additional rune optimizations (6 patterns)** - 2026-02-17 - SUBSTRING non-vec, INSERT non-vec, Quote, WeightString, ValidatePasswordStrength
- [x] **SessionVars decomposition phase 1** - 2026-02-17 - Extract `TiFlashVars` (27 MPP/TiFlash fields) and `CostModelFactors` (28 cost factor fields) into embedded sub-structs, reducing SessionVars from 315 to ~260 direct fields
- [x] **executor/builder.go split phases 1-2** - 2026-02-17 - Extract into 4 files: `builder_memtable.go` (324 lines), `builder_reader.go` (1,710 lines), `builder_analyze.go` (416 lines), `builder_join.go` (740 lines), reducing builder.go from 6,223 to 3,172 lines (49% reduction)
- [x] **SessionVars decomposition phase 2** - 2026-02-17 - Extract `PlanCacheVars` (13 plan cache fields) and `OptimizerVars` (32 optimizer fields) into embedded sub-structs
- [x] **executor/builder.go split phase 3** - 2026-02-17 - Extract `builder_ddl_admin.go` (474 lines) and `builder_sort_window.go` (411 lines), reducing builder.go from 3,172 to 2,360 lines (62% total reduction)
- [x] **executor/builder.go split phase 4** - 2026-02-17 - Extract `builder_agg_project.go` (244 lines), `builder_cte_misc.go` (315 lines), `builder_stmt.go` (601 lines), and `builder_union_scan.go` (236 lines), reducing builder.go from 2,360 to 1,082 lines (83% total reduction). Split complete: 10 builder files total.
- [x] **SessionVars Phase 3a (StatsVars)** - 2026-02-17 - Extract 16 statistics-related fields (EnableFastAnalyze, AnalyzeVersion, RegardNULLAsPoint, etc.) into embedded `StatsVars` sub-struct
- [x] **SessionVars Phase 3b (TransactionVars)** - 2026-02-17 - Extract 16 transaction-related fields (RetryLimit, LockWaitTimeout, TxnScope, EnableAsyncCommit, etc.) into embedded `TransactionVars` sub-struct
- [x] **session.go decomposition** - 2026-02-17 - Split into 9 focused files: `session_txn.go` (1,086), `session_bootstrap.go` (955), `session_execute.go` (836), `session_parse.go` (546), `session_auth.go` (413), `session_logging.go` (387), `session_restricted.go` (287), `session_states.go` (212), `session_sysvar.go` (157). Reduced session.go from 5,558 to 1,087 lines (80% reduction).
- [x] **SessionVars Phase 4 (ExecutionVars)** - 2026-02-17 - Extract 11 execution-related fields (DMLBatchSize, BatchInsert, BatchDelete, BatchCommit, BulkDMLEnabled, EnableChunkRPC, EnablePaging, EnableReuseChunk, MaxExecutionTime, SelectLimit, StoreBatchSize) into embedded `ExecutionVars` sub-struct. Total: 7 sub-structs, ~143 fields organized.
- [x] **DDL executor.go decomposition** - 2026-02-17 - Split into 8 focused files: `executor_partition.go` (1,084 lines), `executor_index.go` (967 lines), `executor_column.go` (472 lines), `executor_create.go` (565 lines), `executor_table.go` (532 lines), `executor_misc.go` (543 lines), `executor_schema.go` (680 lines), `executor_table_props.go` (683 lines). Reduced executor.go from 7,201 to 1,986 lines (72% reduction).
- [x] **domain.go decomposition** - 2026-02-18 - Split into 6 focused files: `domain_stats.go` (723), `domain_privilege.go` (467), `domain_serverid.go` (356), `domain_workers.go` (285), `domain_disttask.go` (208), `domain_infra.go` (192). Reduced domain.go from 3,023 to 997 lines (67% reduction).
- [x] **planbuilder.go decomposition** - 2026-02-18 - Split into 8 focused files: `planbuilder_analyze.go` (1,296), `planbuilder_insert.go` (884), `planbuilder_admin.go` (570), `planbuilder_show.go` (539), `planbuilder_split.go` (449), `planbuilder_ddl.go` (368), `planbuilder_bind.go` (343), `planbuilder_explain.go` (247). Reduced planbuilder.go from 6,518 to 2,116 lines (68% reduction).
- [x] **logical_plan_builder.go decomposition** - 2026-02-18 - Split into 6 focused files: `logical_plan_builder_window.go` (753), `logical_plan_builder_dml.go` (903), `logical_plan_builder_cte.go` (368), `logical_plan_builder_setops.go` (448), `logical_plan_builder_join.go` (394), `logical_plan_builder_datasource.go` (804). Reduced logical_plan_builder.go from 7,362 to 3,897 lines (47% reduction).
- [x] **ddl/partition.go decomposition** - 2026-02-18 - Split into 3 focused files: `partition_reorganize.go` (1,185), `partition_exchange.go` (725), `partition_truncate.go` (411). Reduced partition.go from 5,358 to 3,155 lines (41% reduction).
- [x] **executor/infoschema_reader.go decomposition** - 2026-02-18 - Split into 3 focused files: `infoschema_reader_lock.go` (492), `infoschema_reader_tiflash.go` (277), `infoschema_reader_resource.go` (569). Reduced infoschema_reader.go from 4,213 to 2,990 lines (29% reduction).
- [x] **ddl/index.go decomposition** - 2026-02-18 - Split into 4 focused files: `index_dist_task.go` (418), `index_backfill_worker.go` (619), `index_columnar.go` (261), `index_analyze.go` (289). Reduced index.go from 4,140 to 2,691 lines (35% reduction).
- [x] **executor/simple.go decomposition** - 2026-02-18 - Split into 2 focused files: `simple_user.go` (1,296), `simple_role.go` (917). Reduced simple.go from 3,198 to 1,064 lines (67% reduction).
- [x] **executor/show.go decomposition** - 2026-02-18 - Split into 2 focused files: `show_create.go` (714), `show_region.go` (771). Reduced show.go from 2,825 to 1,412 lines (50% reduction).
- [x] **planner/core/find_best_task.go decomposition** - 2026-02-18 - Split into 2 focused files: `find_best_task_property.go` (1,056), `find_best_task_scan.go` (915). Reduced find_best_task.go from 3,030 to 1,118 lines (63% reduction).
- [x] **planner/core/exhaust_physical_plans.go decomposition** - 2026-02-18 - Split into 3 focused files: `exhaust_physical_plans_index_join_inner.go` (741), `exhaust_physical_plans_hints.go` (382), `exhaust_physical_plans_mpp_join.go` (367). Reduced exhaust_physical_plans.go from 2,892 to 1,399 lines (52% reduction).
