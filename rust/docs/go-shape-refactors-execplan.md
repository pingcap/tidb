# Close the 2026-11-23 structural audit rows (T01–T14)

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the repository root; this plan must be maintained according to it.

## Purpose / Big Picture

The symbol-level audit of 2026-11-23 (`rust/docs/parity/current-audit/structural-audit-2026-11-23.md`) scored every symbol of the 451 Go packages `cmd/tidb-server` links and registered fourteen places where the Rust code does not have Go's shape (`structural-findings.md`). After this plan each of those Go owners has one Rust owner in the matching layer: the planner owns `pkg/planner` code, the executor owns `pkg/executor` code, the session variables own `SessionVars`' fields, and a transaction's writes land in the store transaction's MemDB.

## Progress

- [x] (2026-11-23) T02 performance_schema `getRows`, T03 `tidb_auth_token`, T04 `logutil/consistency`, T11 analyze-job lifecycle, T12 `ConnectionVerification`, T13 PointGet / BatchPointGet executors.
- [x] (2026-11-23) T09: `tidb-variable/src/vars_typed.rs` holds 81 `SessionVars` fields, each maintained by its variable's `SetSession` hook; the statement snapshot copies fields instead of parsing variables.
- [x] (2026-11-23) T06: Go's `PartitionProcessor` and pruners live in `tidb-planner-core-rule/src/rule_partition_processor/`; `DataSource.table` is Go's `ds.Table`; the executor's pruning callback is gone.
- [x] (2026-11-24) T05: Go `planner.Optimize` is the planner's: `statement_optimize.rs` (`NewPlanBuilder().Init` as `BuilderParts`, `buildLogicalPlan`, the rounds, the query entry with `TryFastPlan`, `ScalarSubqueryEvalCtx`), `statement_plan.rs` (the other statement entries, `queryPlanCost`), `dml_optimize.rs` (UPDATE/DELETE, `OptimizeForForeignKeyCascade`, FK trigger plans), `do_optimize.rs` (`DoOptimize`), `binding_match.rs` (the binding step; `matchSQLBindingCore` is tidb-bindinfo's, which no longer depends on the planner). The executor implements `PlanningHooks` (`EvalSubqueryFirstRow`, column defaults, plan preparation). `genBriefPlanWithSCtx` and `calculatePlanDigestFunc` render the plan and go with the renderer (T22).
- [x] (2026-11-24) T07: Go's plan cache is the planner's: `plan_cache.rs` (one `PlanCacheStmt`, `GetPlanFromPlanCache`, `CachedPlan`, `PlanCacheExecution`, session plan cache, parameter binding), `plan_cache_utils.rs` (`GeneratePlanCacheStmtWithAST`, `NewPlanCacheKey`'s session facts over `PlanCacheSessionContext`), `plan_cache_rebuild.rs`, `plan_cacheable_checker.rs` (`IsASTCacheable`), and `optimize.rs` (`getPlanFromNonPreparedPlanCache`, the EXECUTE lookup over `PlanCacheContext`). The executor's SELECT/DML duplicates of each are gone; its compile traits delegate.
- [x] (2026-11-24) T08: `expressionRewriter`'s AST half is `tidb-planner/src/expression_rewriter/leave.rs`; lower crates reach it through `tidb_expr::column_resolver::rewrite_expr_resolved`, the `expression.BuildSimpleExpr` function variable that `tidb_planner::init` sets (server `main`, every `Session`, and test binaries through `tidb_planner_init::link!()`).
- [x] (2026-11-24) T22: Go's plan renderer is `tidb-planner/src/flat_plan.rs`; `genBriefPlanWithSCtx` / `calculatePlanDigestFunc` / `recordRelevantOptVarsAndFixes` are `optimize.rs`'s. `tidb-executor/src/explain.rs` keeps `ExplainExec`'s run and the process-info publishing.
- [x] (2026-11-25) T10: `pkg/executor` code is the executor's. `executorBuilder.build` over the common plans (`tidb-executor/src/builder.rs`, `ExecutorBuild` over `BuildSession`, with `SetConfigExec`, `CompactTableTiFlashExec`, SHOW NEXT_ROW_ID); `adapter.rs`: `isSelectForUpdate`, `handleNoDelay`'s refusals, `handlePessimisticDML`'s loop and `handlePessimisticLockError` (`HandlePessimisticDml` over `PessimisticDmlSession`, the session's lifecycle supplying the primitives); `exec_stmt_observation.rs`: `FinishExecuteStmt` (Top profiling finish, RU v2 sync, `checkPlanReplayerCapture`, `LogSlowQuery`, `recordLastQueryInfo`, `recordAffectedRows2Metrics`, `SummaryStmt`); `stmt_ctx_build.rs`: `ResetContextOfStmt`'s INSERT arm, `ResetUpdateStmtCtx`, `ResetDeleteStmtCtx`; `trace.rs`: `TraceExec.nextRowJSON`'s run over `TraceExecSession`; `coprocessor.rs`: `CoprocessorDAGHandler`. The preprocess walk runs once per statement (the compile stores the walked tree). Go's `session.go` code (`logStmt`, `ExecuteStmt`'s setup) stays in the session; the funnel that interleaves `ExecuteStmt`, `Compile` and `ExecStmt.Exec` is T23.
- [x] (2026-11-25) T23: Go's `Compile` -> `ExecStmt` -> `Exec` split in the session's statement funnel (`dispatch.rs`).
  - [x] (2026-11-25) M1: a query compiles (`driver::compile_query_plan`) before it opens (`open_compiled_query`), each under its own retrieval loop (ffbc81b0afe).
  - [x] (2026-11-25) M2: INSERT / UPDATE / DELETE compile (`compile_insert_plan` -> `CompiledInsert`, `compile_update_plan`, `compile_delete_plan`) before they execute; the build's privilege check is the compile's (15986c152b8).
- [x] (2026-11-25) T24: a compiled INSERT / UPDATE / DELETE or locking SELECT (`dispatch.rs` `ExecStmt`: `CompiledDml`, `CompiledQuery`, Go's `ExecStmt`) outlives its execution; the lifecycle's pessimistic retry executes it again under a fresh context with the statement's warnings truncated (`exec_compiled_dml_retry`, `open_compiled_query_retry`: Go `handlePessimisticLockError` + `StmtCtx.ResetForRetry`), the retry's statement boundary being the session's `reset_for_retry`; a `session.retry` replay recompiles (Go `RebuildPlan`). `observeStmtBeginForTopProfiling` is `tidb_executor::exec_stmt_observation`'s (one copy; the session had a second, partial one for administrative statements); `SetProcessInfo` is a session method in Go too. Statements that execute while compiling (a cached prepared plan, EXECUTE) still re-run the funnel on a retry.
- [x] (2026-11-24) T21: Go's preprocessor checks are the walk's (`tidb-planner/src/preprocess.rs`): checkSelectNoopFuncs, checkCreateViewWithSelect, resolveShowStmt, DDL tableByName, checkCreateTableGrammar's LIKE source, and `Preprocess(..., InPrepare)` with handleTableName's pinning; ddl-build, the executor and the session lost their copies. That the walk runs once per compile route rather than once in `Compile` is T10's.
- [x] (2026-11-24) T14: one writable implicit transaction per autocommit statement (`TxnManager::enter_implicit`), whose activation binds the SQL buffer to the store transaction's MemDB and whose commit is the statement's; the session-level publication (`commit_autocommit_buffer`, `commit_with_schema_lease`) and the activation's second statement staging are gone.
- [x] (2026-11-24) T01: pipelined DML. The session's `usePipelinedDmlOrWarn` (`PipelinedDmlDecision`, its warnings, BEGIN's included) runs where Go's `PrepareTSFuture` does: at the DML's activation (`update_for_update_ts`), the warm-up being skipped under `tidb_dml_type = bulk` as Go's `txnManager.AdviseWarmup` skips it; the implicit transaction then begins pipelined (`prepare_autocommit_txn`'s `pipelined`, Go `tikv.WithPipelinedTxn`) with `tidb_pipelined_dml_resource_policy`'s `SetSession` config; the SQL buffer binds the pipelined MemDB without a statement staging (Go `initStmtBuf`), write executors call `MayFlush` per batch, `tidb_last_txn_info` reports `pipelined`, the autocommit retry and the mutation checker skip it, and `SetOptionsBeforeCommit`'s invariants are checked. Unistore serves `KvFlush` (Go `MVCCStore.Flush`); the vendored client gained client-go's pipelined failpoints, a union read (buffer tiers then snapshot, Go `KVUnionStore.Get` / `BufferBatchGetter`) and whole client errors from a flush.
- [x] (2026-11-24) T16-T20 (duplicates and executor shapes found after the audit): the unused warning-handler port, the second row decoder, index merge's partial workers, the `kv.TiDB` table reader, IMPORT INTO's URI handling.
- [x] (2026-11-25) Re-verified on a fresh 3-store playground: realtikv txntest 71/71 (the stale-read group, `test_replica_read_effect_scope`, `test_stale_read_no_backoff`, `test_stale_select`) and pessimistictest 70/70 (`test_statements_interrupted_on_disconnect`; `test_plan_cache_schema_change` after the EXECUTE classification fix).

- [x] (2026-11-28) T25: unistore serves requests concurrently, as Go's `Server` does. `KvHandler` and `MvccStore` take `&self` and the `Mutex<KvHandler>` (RPC client, `tidb-ddl`'s embedded store, the session's unistore node) is gone. The store takes Go's `latches` (`region_manager::Latches`) on every write path at Go's point, and fills Go's `writeBatch` (`write.rs`) for `dbWriter.Write`: a `writeDBWorker` and a `writeLockWorker` thread drain every queued batch and apply the group in one pass. The lock store is Go's single-writer, lock-free-reader skiplist: atomic-word arena blocks, acquire/release links, the arena list published whole on growth (`arc-swap`), and a drained block rewritten only after Go's delay and a reader grace period (`arena::Grace`). The vendored client finds the secondaries without a per-key scan of the primary batch, and the client store's runtime has one worker per `GOMAXPROCS`. Go's `TestIndexScanSeekCost` runs without the ANALYZE retry the port had added.

## Surprises & Discoveries

- Untested by the scoped gate and found while moving T08 (2026-11-24): `difftest-result-tests` `expr_diff` fails on `name_const('test_float', cast(3.14159 as double))` since `33bb8315173` (Go checks NAME_CONST's arguments in `preprocess.go`, not in the rewriter); `table_diff` once returned `1;3` for `select 1 union all select 2 union all select 3 limit 2`; `tidb-mvservice` `test_task_executor_backpressure` hung on its mock clock under load; `tidb-exprstatic`'s tests did not compile after T09. `difftest-result-tests` `query_diff` does not finish in 25 min, at the pushed tip as with T08: each topic's mock-store shutdown spends about a minute in `catalog_watch::Syncer::shutdown` joining a PD-client thread (`init_global_var_from_system_db` dropping the DDL session pool).

- The nextest LEAKs seen in several gates are sibling test processes inheriting another test's stderr pipe (macOS `pipe()` + `FD_CLOEXEC` race in parallel spawns), captured with `lsof` during gate 66; not product code.
- Gate 64's panic-recovery retry was a real race: a connection that had already failed could still be cancelled by a starting drain, because it left the drained-clients registry after closing its transport. Go deletes it first (`clientConn.Close`); fixed in `18776e43537`.
- Go's `planner/util.ByItems` had three Rust copies; one remains (`tidb_expression_aggregation::ByItems`).

- Found while pinning T24 (2026-11-25): Go master `ae5be6915c7` (#72838) matches a hint table name without a database to the table in any database of its query block (`QBHintHandler.resolveHintDBName`, the point-get `indexIsAvailableByHints`); the Rust hint code still resolved it to the current database. Ported in `3a2e1c51272` with Go's `TestHintTableNameWithoutDBName`.
- Go's warnings after a pessimistic retry are only what the execution raised: `StmtCtx.ResetForRetry` truncates them and nothing compiles again, so a compile-time hint warning is gone (Go testkit probe, point UPDATE and SELECT FOR UPDATE). That is what pins T24.

- Found while closing T25 (2026-11-28): Go's lock store reuses a drained arena block after `reuseSafeDuration` (100 ms) on the assumption that no reader takes longer. Under the gate's CPU oversubscription a reader stalled past it and read an overwritten node (wrong values, garbage addresses: 4 of 64 stressed runs). The port waits for a grace period instead (`arena::Grace`, two striped epoch counts); without it 49 of 64 stressed runs fail.

## Decision Log

- Decision: Go runs `planner/core`'s `init` in every binary that links it; Rust test binaries that reach `BuildSimpleExpr` from a lower crate name `tidb_planner_init::link!()` once at their root (Go's blank import), and `tidb-planner-init` alone allows `unsafe_code` for the init-section static. `tidb-expr`'s own unit tests compile `leave.rs` in place (`extern crate self as tidb_expr`) and install it on first use, as Go's `pkg/expression` test binary links the planner through its external test package. Date: 2026-11-24.
- Decision: `ScalarSubQueryExpr`'s struct stays in `tidb-expr`: Go's `Expression` is an interface the planner implements, Rust's is a closed enum whose variant payloads live with it. Date: 2026-11-24.

- Decision: skip-init variables (`tidb_ddl_reorg_priority`, the metric-schema step and range) keep `NewSessionVars`' default in their typed field until SET, as Go's fields do. Date: 2026-11-23.
- Decision: the executor keeps thin re-exports (`table_privilege`, `access_cost`, `stats_stmt_ctx` types) while its callers move; no logic stays behind them. Date: 2026-11-23.

## Outcomes & Retrospective

(2026-11-25) Every register row is closed: each Go owner the audit named has one Rust owner in the matching layer. T10's split showed the next mismatch each time it closed (the funnel's compile/execute interleaving, T23; the retry loop around the whole statement, T24); recording each as its own row kept every gate to one row. Left: the realtikv re-verification below needs a live cluster.

## Context and Orientation

T22: `tidb-executor/src/explain.rs` renders plans (`normalize_flat_plan`, `physical_explain_roots`) beside `ExplainExec`'s run. Go reaches the executor from the planner only through function variables the executor sets (`EvalSubqueryFirstRow`, `BuildSimpleExpr`).

T08: `tidb-expr/src/rewriter.rs` (AST to expression) is used by `tidb-expr`'s own builtins and tests and by ddl, table, ttl and ranger code; Go hooks `expression.BuildSimpleExpr` to the planner's `buildSimpleExpr` in `core_init.go`.


T01: `tidb-session/src/txn_manager.rs` (`enter_implicit`, `flush_if_autocommit`), `tidb-session/src/lazy_transaction.rs`, `tidb-domain-core/src/cluster_transactions.rs` (`prepare_autocommit_txn`), the client's pipelined transaction (`tikv-client-rs`, `is_pipelined`).

T14 (2026-11-24). Go enters one `LazyTxn` per autocommit statement (`PrepareTxnCtx` -> `EnterNewTxnBeforeStmt`), writable whatever the statement, pessimistic only when `decideTxnMode` says so, its timestamp future from `newOracleFuture` (low-resolution under `tidb_low_resolution_tso`, whose writes `handleNoDelay` refuses). `finishStmt` commits it; a transaction that staged nothing sends nothing. The Rust node entered two kinds: an INSERT/UPDATE/DELETE classified as an autocommit write got a writable implicit transaction, every other statement a read-only one, and whatever such a statement staged (LOAD DATA, INSERT on routes whose shape reads as `Unknown`, materialized-view refresh, prepared point writes, ...) was published by `commit_autocommit_buffer` through `ClusterTransactions::commit_with_schema_lease` at the read timestamp. Now `TxnManager::enter_implicit` enters Go's one implicit transaction (`ClusterTransactions::prepare_autocommit_txn`), its activation always binds the SQL buffer to the store transaction's MemDB (what was staged before moves in, the statement checkpoint with it: Go's one `stagingHandle`, so the activation takes no second staging), and `flush_if_autocommit` commits it. A survey (a panic on a non-empty buffer in the old publication, over the whole session suite) found no statement left there, so the publication and `commit_with_schema_lease` are gone.

## Plan of Work

2. T08: move the rewriter into `tidb-planner` behind a `BuildSimpleExpr` hook in `tidb-expr`, installed at server start and by each lower crate's tests.
4. T01 over T14's implicit transaction.

## Concrete Steps

Per slice in `rust/`: `cargo check -p <crates> --tests` (exit status, not a grep), the touched tests under nextest with a mutation check, a local commit, `zsh scripts/gate-scoped.sh <worktree> tidb-server tidb-session` from `tidb-wt`, and a push to both remotes.

## Validation and Acceptance

Each row closes when its Go owner's code has one Rust owner in the matching layer and the row's tests (and a mutation of the moved code) pass in a green scoped gate.

## Idempotence and Recovery

Each slice is its own commit; a failing gate is fixed or the commit reverted before the next slice.
