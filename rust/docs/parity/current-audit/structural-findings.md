# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-23; the [JSON register](structural-findings.json) holds the same
rows. A repaired finding is removed, not kept. The method and the live probes behind the rows
are in [structural-audit-2026-11-23.md](structural-audit-2026-11-23.md).

**9 unresolved: 9 open, 0 partial.**

A row states the remaining gap only.

## Unresolved


### Structure: Go's shape before more ports

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| T05 | open | Go `pkg/planner/optimize.go` (`Optimize`, `optimizeNoCache`, `optimize`, `buildLogicalPlan`, `buildAndOptimizeLogicalPlanRound` and the alternative-round predicates, `OptimizeForForeignKeyCascade`, `queryPlanCost`, `genBriefPlanWithSCtx`) has no planner-side owner: it lives in the executor. Rust: `tidb-executor/src/driver/planner_bridge.rs` (`planner_optimized_query_with_allocators`, `build_logical_plan_and_do_optimize`, `build_and_optimize_logical_plan_round`), `tidb-executor/src/explain.rs`. | `pkg/planner/optimize.go` |
| T09 | open | `SessionVars`' typed fields are not on the session variables: the executor re-parses the variable table into a per-generation snapshot (66 `SessionVars` fields) instead of each variable's `SetSession` hook maintaining a field. Rust: `tidb-executor/src/stmt_ctx_build.rs` (the statement context's session-variable half). | `pkg/sessionctx/variable/session.go`, `sysvar.go` `SetSession` hooks |
| T14 | open | A transaction's writes are not Go's `LazyTxn` over the store transaction's MemDB: executors stage them in the session's `MutationBuffer`, and an autocommit statement publishes the buffer at commit through `ClusterTransactions::commit_with_schema_lease`. Go activates the `kv.Transaction` (or its pending future) before the statement writes and commits that transaction; pipelined DML (T01) flushes that MemDB while the statement runs. Rust: `tidb-session/src/txn_manager.rs` (`buffer`, `commit_autocommit_buffer`), `tidb-domain-core/src/cluster_transactions.rs`. | `pkg/session/txn.go` (`LazyTxn`), `pkg/session/session.go` (`PrepareTSFuture`, `NewTxn`), `pkg/sessiontxn/isolation` |

### System tables and startup

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Privileges, wire and administration

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### DDL

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Planner and bindings

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| T06 | open | Partition pruning is not Go's `partitionProcessor`: Go prunes over the conditions in the logical rule (`processRangePartition` / `processListPartition` / `processHashOrKeyPartition`, `rangePruner`, `listPartitionPruner`); Rust prunes from ranger intervals through an executor-side callback, with LIST COLUMNS locating in the executor. Rust: `tidb-planner-rule/src/lib.rs` (`pruned_ids_from_ranger`), `tidb-executor/src/driver/planner_bridge.rs` (`partition_pruning`, `locate_list_columns_*`), `tidb-planner-core-rule/src/rule_partition_processor.rs` asks `ctx.partition_pruning`. | `pkg/planner/core/rule/rule_partition_processor.go` |
| T07 | open | Planner code owned by the executor: `pb_to_plan.go` (`PBPlanBuilder`), the plan-cache utilities and cacheable checker, and `rule_collect_plan_stats.go`'s statistics loading live in `tidb-executor`. Rust: `tidb-executor/src/coprocessor.rs`, `tidb-executor/src/driver/access.rs`, `tidb-executor/src/compiler/plan_cacheable_checker.rs`, `compiler/non_prepared_plan_cache.rs`, `driver/catalog_ctx.rs`. | `pkg/planner/core/pb_to_plan.go`, `plan_cache_utils.go`, `plan_cacheable_checker.go`, `rule/rule_collect_plan_stats.go` |
| T08 | open | Go's `expressionRewriter` (`pkg/planner/core/expression_rewriter.go`) and `ScalarSubQueryExpr` are split into the expression crate instead of the planner. Rust: `tidb-expr/src/rewriter.rs`, `tidb-expr/src/simple_expr.rs`, `tidb-expr/src/scalar_subquery.rs`. | `pkg/planner/core/expression_rewriter.go`, `scalar_subq_expression.go` |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| T01 | open | Pipelined DML is not ported: `tidb_dml_type='bulk'` runs a standard transaction; Go's `usePipelinedDmlOrWarn` (fallback warnings for temporary tables, `tidb_constraint_check_in_place=ON`, explicit transactions, ...) and the pipelined transaction are missing. A Go server reports `pipelined:true` in `tidb_last_txn_info` after a bulk INSERT; this one reports `false` and no warnings. Depends on T14: the statement's writes have to land in a pipelined store transaction. Rust: `tidb-session/src/lib.rs` hard-codes `"pipelined":false`; no `use_pipelined_dml_or_warn`. | `pkg/session/session.go` `usePipelinedDmlOrWarn`, `NewTxn` pipelined path; `pkg/store/driver/txn` |
| T10 | open | Go `pkg/executor` code is implemented as `Session` methods in the session crate: `ExecStmt` (`adapter.go`), `ResetContextOfStmt` and the select-lock / warning handling (`select.go`), `PlanReplayerExec` / `PlanReplayerLoadInfo` (`plan_replayer.go`), parts of `SimpleExec` (`simple.go`), `DDLExec` (`ddl.go`), `ShowExec` (`show.go`), the prepared-statement executors (`prepared.go`), `ImportIntoExec`, `CompactTableTiFlashExec`, `TraceExec`, `LoadStatsExec`, `CancelDDLJobsExec` and more. In Go they are executors over `sessionctx.Context`. Rust: `tidb-session/src/plan_replayer.rs`, `dispatch.rs`, `observation.rs`, `statement_lifecycle.rs`, `ddl_service.rs`, `import_into.rs`, `show.rs`, `domain/cluster_session_node/mod.rs` (counted from their `Go \`X\`` doc claims on `pkg/executor` symbols). | `pkg/executor` (`adapter.go`, `select.go`, `plan_replayer.go`, `simple.go`, `ddl.go`, `show.go`, `prepared.go`, ...) |
| T13 | open | Go's `PointGetExecutor` and `BatchPointGetExec` (`point_get.go`, `batch_point_get.go`) are not ported as executors: point and batch-point reads, and index range scans with row lookup, are `access_path` source executors over the Rust `TableStorage` seam (`MemStorage` / `ClusterTableStorage`), with the reads and locks in the storage layer. Rust: `tidb-executor/src/access_path.rs`, `tidb-store-driver-txn/src/cluster_table_storage.rs` (`PointGetExecutor.get` / `lockKeyIfNeeded`), `tidb-table/src/tables/index_entries.rs`. | `pkg/executor/point_get.go`, `pkg/executor/batch_point_get.go` |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Statistics

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
