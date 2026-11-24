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
- [ ] T07 (partly): done (2026-11-24): `PlanCacheStmt` (one for every statement), `GeneratePlanCacheStmtWithAST`, `GetPlanFromPlanCache`, `CachedPlan`, `plan_cache_rebuild.rs`, the session plan cache, parameter binding and types, all in `tidb-planner/src/plan_cache.rs` / `plan_cache_utils.rs`. Remaining: `getPlanFromNonPreparedPlanCache` and the plan-cache session facts in the executor's `compiler/` traits.
- [x] (2026-11-24) T08: `expressionRewriter`'s AST half is `tidb-planner/src/expression_rewriter/leave.rs`; lower crates reach it through `tidb_expr::column_resolver::rewrite_expr_resolved`, the `expression.BuildSimpleExpr` function variable that `tidb_planner::init` sets (server `main`, every `Session`, and test binaries through `tidb_planner_init::link!()`).
- [ ] T22: Go's plan renderer (`explain.go`, `flat_plan.go`, `encode.go`) out of `tidb-executor/src/explain.rs` into the planner, then `genBriefPlanWithSCtx` / `calculatePlanDigestFunc`.
- [ ] T10: `pkg/executor` code implemented as `Session` methods in `tidb-session`.
- [ ] T21: Go's `preprocessor` as one Rust owner in `tidb-planner` (its checks now sit in session dispatch, ddl-build, `preprocess.rs` and the rewriter's NAME_CONST check).
- [ ] T14, then T01: a transaction's writes in the store transaction's MemDB (`LazyTxn`), then pipelined DML over it.
- [x] (2026-11-24) T16-T20 (duplicates and executor shapes found after the audit): the unused warning-handler port, the second row decoder, index merge's partial workers, the `kv.TiDB` table reader, IMPORT INTO's URI handling.
- [ ] Re-verify on a live cluster the realtikv transaction tests the retired realtikv-txn plan last left failing (2026-11-16): the stale-read group (`test_stale_*`, `test_staleness_and_history_read`, `test_stmt_ctx_stale_flag`, `test_validate_read_only_in_staleness_transaction`), `test_replica_read_effect_scope`, `test_stale_read_no_backoff`, `test_stale_select`, pessimistictest `test_plan_cache_schema_change` and `test_statements_interrupted_on_disconnect`; register any that still fail.

## Surprises & Discoveries

- Untested by the scoped gate and found while moving T08 (2026-11-24): `difftest-result-tests` `expr_diff` fails on `name_const('test_float', cast(3.14159 as double))` since `33bb8315173` (Go checks NAME_CONST's arguments in `preprocess.go`, not in the rewriter); `table_diff` once returned `1;3` for `select 1 union all select 2 union all select 3 limit 2`; `tidb-mvservice` `test_task_executor_backpressure` hung on its mock clock under load; `tidb-exprstatic`'s tests did not compile after T09. `difftest-result-tests` `query_diff` does not finish in 25 min, at the pushed tip as with T08: each topic's mock-store shutdown spends about a minute in `catalog_watch::Syncer::shutdown` joining a PD-client thread (`init_global_var_from_system_db` dropping the DDL session pool).

- The nextest LEAKs seen in several gates are sibling test processes inheriting another test's stderr pipe (macOS `pipe()` + `FD_CLOEXEC` race in parallel spawns), captured with `lsof` during gate 66; not product code.
- Gate 64's panic-recovery retry was a real race: a connection that had already failed could still be cancelled by a starting drain, because it left the drained-clients registry after closing its transport. Go deletes it first (`clientConn.Close`); fixed in `18776e43537`.
- Go's `planner/util.ByItems` had three Rust copies; one remains (`tidb_expression_aggregation::ByItems`).

## Decision Log

- Decision: Go runs `planner/core`'s `init` in every binary that links it; Rust test binaries that reach `BuildSimpleExpr` from a lower crate name `tidb_planner_init::link!()` once at their root (Go's blank import), and `tidb-planner-init` alone allows `unsafe_code` for the init-section static. `tidb-expr`'s own unit tests compile `leave.rs` in place (`extern crate self as tidb_expr`) and install it on first use, as Go's `pkg/expression` test binary links the planner through its external test package. Date: 2026-11-24.
- Decision: `ScalarSubQueryExpr`'s struct stays in `tidb-expr`: Go's `Expression` is an interface the planner implements, Rust's is a closed enum whose variant payloads live with it. Date: 2026-11-24.

- Decision: skip-init variables (`tidb_ddl_reorg_priority`, the metric-schema step and range) keep `NewSessionVars`' default in their typed field until SET, as Go's fields do. Date: 2026-11-23.
- Decision: the executor keeps thin re-exports (`table_privilege`, `access_cost`, `stats_stmt_ctx` types) while its callers move; no logic stays behind them. Date: 2026-11-23.

## Outcomes & Retrospective

(At completion.)

## Context and Orientation

T07: `tidb-executor/src/driver/planner_bridge.rs` keeps the cached plans (`CachedSelectPlan`) beside the executor's `PlanningHooks`; `access.rs`, `plan_cache.rs` and `dml.rs` hold the plan cache's prepared statements and values. Go reaches the executor from the planner only through function variables the executor sets (`EvalSubqueryFirstRow`, `BuildSimpleExpr`).

T08: `tidb-expr/src/rewriter.rs` (AST to expression) is used by `tidb-expr`'s own builtins and tests and by ddl, table, ttl and ranger code; Go hooks `expression.BuildSimpleExpr` to the planner's `buildSimpleExpr` in `core_init.go`.

T10: `tidb-session/src/{dispatch,show,plan_replayer,ddl_service,import_into,statement_lifecycle,observation}.rs`.

T14/T01: `tidb-session/src/txn_manager.rs` (`MutationBuffer` staging, `commit_autocommit_buffer`), `tidb-domain-core/src/cluster_transactions.rs`.

## Plan of Work

1. T07: move the cached plans (`CachedSelectPlan`, `CachedDmlPlan`, `PreparedSelectPlan`) and `SessionPlanCache` into the planner over the same `PlanningHooks`; T22: split the plan renderer out of `explain.rs`.
2. T08: move the rewriter into `tidb-planner` behind a `BuildSimpleExpr` hook in `tidb-expr`, installed at server start and by each lower crate's tests.
3. T10: move each `pkg/executor` executor out of the session crate onto a `sessionctx` trait surface, one Go file at a time.
4. T14 then T01.

## Concrete Steps

Per slice in `rust/`: `cargo check -p <crates> --tests` (exit status, not a grep), the touched tests under nextest with a mutation check, a local commit, `zsh scripts/gate-scoped.sh <worktree> tidb-server tidb-session` from `tidb-wt`, and a push to both remotes.

## Validation and Acceptance

Each row closes when its Go owner's code has one Rust owner in the matching layer and the row's tests (and a mutation of the moved code) pass in a green scoped gate.

## Idempotence and Recovery

Each slice is its own commit; a failing gate is fixed or the commit reverted before the next slice.
