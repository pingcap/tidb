# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-23; the [JSON register](structural-findings.json) holds the same
rows. A repaired finding is removed, not kept. The method and the live probes behind the rows
are in [structural-audit-2026-11-23.md](structural-audit-2026-11-23.md).

**6 unresolved: 6 open, 0 partial.**

A row states the remaining gap only.

## Unresolved


### Structure: Go's shape before more ports

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| T05 | open | Go `pkg/planner/optimize.go`'s statement entry (`Optimize`, `optimizeNoCache`: binding match, fast plan, plan cache) and `buildLogicalPlan`'s `PlanBuilder` setup, `OptimizeForForeignKeyCascade`, `queryPlanCost`, `genBriefPlanWithSCtx` and `calculatePlanDigestFunc` live in the executor. (`optimize`'s rounds, `buildAndOptimizeLogicalPlanRound` and the alternative-round predicates are `tidb-planner/src/planner_optimize.rs`.) Rust: `tidb-executor/src/driver.rs`, `driver/dml.rs` (statement entry), `driver/planner_bridge.rs` (`QueryBuilderParts::build`, `planner_built_query_with_allocators`), `tidb-executor/src/explain.rs` (`query_plan_cost`, `gen_brief_plan`, `calculate_plan_digest`), `foreign_key.rs`. | `pkg/planner/optimize.go` |
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
| T07 | open | Planner code owned by the executor: the prepared and non-prepared plan cache's statement and key (`plan_cache.go` / `plan_cache_utils.go`: `PlanCacheStmt`, `NewPlanCacheKey`, `GeneratePlanCacheStmtWithAST`) live in `tidb-executor`. (`pb_to_plan.go` is `tidb-planner/src/pb_to_plan.rs`; the non-prepared cacheability walk and `GetParamSQLFromAST` are `tidb-planner/src/plan_cacheable_checker.rs`; `RequestLoadStats` / `SyncWaitStatsLoad` are `tidb-planner-core-rule/src/rule_collect_plan_stats.rs`.) Rust: `tidb-executor/src/driver/access.rs` (`PreparedSelectPlan`, `PlanCacheStmtInfo`, `PreparedPlanCacheEnvironment`), `driver/plan_cache.rs`. | `pkg/planner/core/plan_cache.go`, `plan_cache_utils.go` |
| T08 | open | Go's `expressionRewriter` (`pkg/planner/core/expression_rewriter.go`) and `ScalarSubQueryExpr` are split into the expression crate instead of the planner. Rust: `tidb-expr/src/rewriter.rs`, `tidb-expr/src/simple_expr.rs`, `tidb-expr/src/scalar_subquery.rs`. | `pkg/planner/core/expression_rewriter.go`, `scalar_subq_expression.go` |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| T01 | open | Pipelined DML is not ported: `tidb_dml_type='bulk'` runs a standard transaction; Go's `usePipelinedDmlOrWarn` (fallback warnings for temporary tables, `tidb_constraint_check_in_place=ON`, explicit transactions, ...) and the pipelined transaction are missing. A Go server reports `pipelined:true` in `tidb_last_txn_info` after a bulk INSERT; this one reports `false` and no warnings. Depends on T14: the statement's writes have to land in a pipelined store transaction. Rust: `tidb-session/src/lib.rs` hard-codes `"pipelined":false`; no `use_pipelined_dml_or_warn`. | `pkg/session/session.go` `usePipelinedDmlOrWarn`, `NewTxn` pipelined path; `pkg/store/driver/txn` |
| T10 | open | Go `pkg/executor` code is implemented as `Session` methods in the session crate: `ExecStmt` (`adapter.go`), `ResetContextOfStmt` and the select-lock / warning handling (`select.go`), `PlanReplayerExec` / `PlanReplayerLoadInfo` (`plan_replayer.go`), parts of `SimpleExec` (`simple.go`), `DDLExec` (`ddl.go`), `ShowExec` (`show.go`), the prepared-statement executors (`prepared.go`), `ImportIntoExec`, `CompactTableTiFlashExec`, `TraceExec`, `LoadStatsExec`, `CancelDDLJobsExec` and more. In Go they are executors over `sessionctx.Context`. Rust: `tidb-session/src/plan_replayer.rs`, `dispatch.rs`, `observation.rs`, `statement_lifecycle.rs`, `ddl_service.rs`, `import_into.rs`, `show.rs`, `domain/cluster_session_node/mod.rs` (counted from their `Go \`X\`` doc claims on `pkg/executor` symbols). | `pkg/executor` (`adapter.go`, `select.go`, `plan_replayer.go`, `simple.go`, `ddl.go`, `show.go`, `prepared.go`, ...) |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Statistics

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
