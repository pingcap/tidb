# Close the 2026-11-23 structural audit rows (T01–T14)

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the repository root; this plan must be maintained according to it.

## Purpose / Big Picture

The symbol-level audit of 2026-11-23 (`rust/docs/parity/current-audit/structural-audit-2026-11-23.md`) scored every symbol of the 451 Go packages `cmd/tidb-server` links and registered fourteen places where the Rust code does not have Go's shape (`structural-findings.md`). After this plan each of those Go owners has one Rust owner in the matching layer: the planner owns `pkg/planner` code, the executor owns `pkg/executor` code, the session variables own `SessionVars`' fields, and a transaction's writes land in the store transaction's MemDB.

## Progress

- [x] (2026-11-23) T02 performance_schema `getRows`, T03 `tidb_auth_token`, T04 `logutil/consistency`, T11 analyze-job lifecycle, T12 `ConnectionVerification`, T13 PointGet / BatchPointGet executors.
- [x] (2026-11-23) T09: `tidb-variable/src/vars_typed.rs` holds 81 `SessionVars` fields, each maintained by its variable's `SetSession` hook; the statement snapshot copies fields instead of parsing variables.
- [x] (2026-11-23) T06: Go's `PartitionProcessor` and pruners live in `tidb-planner-core-rule/src/rule_partition_processor/`; `DataSource.table` is Go's `ds.Table`; the executor's pruning callback is gone.
- [ ] T05 (partly): done: `optimize`'s rounds (`planner_optimize.rs`), `PlanBuilder.Init`'s session reads, `TryFastPlan`'s global-index bypass, `CheckPrivilege` / `CheckTableLock` (`privilege_check.rs`), `stats.GetStatsTable` (`stats/stats_table.rs`). Remaining: the statement entry (`Optimize`, `optimizeNoCache`), `buildLogicalPlan`'s catalog and evaluator wiring (`driver/planner_bridge.rs`), `OptimizeForForeignKeyCascade`, `queryPlanCost`, `genBriefPlanWithSCtx`, `calculatePlanDigestFunc`.
- [ ] T07 (partly): done: `pb_to_plan.rs`, the non-prepared cacheability walk, `RequestLoadStats` / `SyncWaitStatsLoad`, the plan-cache key side (`plan_cache_utils.rs`). Remaining: the prepared statement and cached plan values (`PreparedSelectPlan`, `CachedSelectPlan`, `CachedDmlPlan`, `SessionPlanCache`).
- [ ] T08: the AST expression rewriter in `tidb-expr` (Go's is `pkg/planner/core/expression_rewriter.go`, reached from lower layers through `expression.BuildSimpleExpr`).
- [ ] T10: `pkg/executor` code implemented as `Session` methods in `tidb-session`.
- [ ] T14, then T01: a transaction's writes in the store transaction's MemDB (`LazyTxn`), then pipelined DML over it.
- [ ] T16-T20 (duplicates and executor shapes found after the audit): the unused warning-handler port, the second row decoder, index merge's partial workers, the `kv.TiDB` table reader, IMPORT INTO's URI handling.
- [ ] Re-verify on a live cluster the realtikv transaction tests the retired realtikv-txn plan last left failing (2026-11-16): the stale-read group (`test_stale_*`, `test_staleness_and_history_read`, `test_stmt_ctx_stale_flag`, `test_validate_read_only_in_staleness_transaction`), `test_replica_read_effect_scope`, `test_stale_read_no_backoff`, `test_stale_select`, pessimistictest `test_plan_cache_schema_change` and `test_statements_interrupted_on_disconnect`; register any that still fail.

## Surprises & Discoveries

- The nextest LEAKs seen in several gates are sibling test processes inheriting another test's stderr pipe (macOS `pipe()` + `FD_CLOEXEC` race in parallel spawns), captured with `lsof` during gate 66; not product code.
- Gate 64's panic-recovery retry was a real race: a connection that had already failed could still be cancelled by a starting drain, because it left the drained-clients registry after closing its transport. Go deletes it first (`clientConn.Close`); fixed in `18776e43537`.
- Go's `planner/util.ByItems` had three Rust copies; one remains (`tidb_expression_aggregation::ByItems`).

## Decision Log

- Decision: skip-init variables (`tidb_ddl_reorg_priority`, the metric-schema step and range) keep `NewSessionVars`' default in their typed field until SET, as Go's fields do. Date: 2026-11-23.
- Decision: the executor keeps thin re-exports (`table_privilege`, `access_cost`, `stats_stmt_ctx` types) while its callers move; no logic stays behind them. Date: 2026-11-23.

## Outcomes & Retrospective

(At completion.)

## Context and Orientation

T05/T07: the executor's `driver/` module is the planner's front end: `planner_bridge.rs` configures Go's `PlanBuilder`, evaluates subqueries, runs `optimize` and keeps cached plans; `access.rs`, `plan_cache.rs` and `dml.rs` hold the plan cache's prepared statements and values. Go reaches the executor from the planner only through function variables the executor sets (`EvalSubqueryFirstRow`, `BuildSimpleExpr`).

T08: `tidb-expr/src/rewriter.rs` (AST to expression) is used by `tidb-expr`'s own builtins and tests and by ddl, table, ttl and ranger code; Go hooks `expression.BuildSimpleExpr` to the planner's `buildSimpleExpr` in `core_init.go`.

T10: `tidb-session/src/{dispatch,show,plan_replayer,ddl_service,import_into,statement_lifecycle,observation}.rs`.

T14/T01: `tidb-session/src/txn_manager.rs` (`MutationBuffer` staging, `commit_autocommit_buffer`), `tidb-domain-core/src/cluster_transactions.rs`.

## Plan of Work

1. T05/T07: move `planner_bridge.rs`'s planning core (builder setup, the query build, the physical optimization entry, the cached select plan) into `tidb-planner`, with the executor installing the subquery evaluator, default-value evaluators and dynamic-prune fallback as hooks; then `explain.rs`'s `query_plan_cost` / `gen_brief_plan` / plan digest follow the entry.
2. T08: move the rewriter into `tidb-planner` behind a `BuildSimpleExpr` hook in `tidb-expr`, installed at server start and by each lower crate's tests.
3. T10: move each `pkg/executor` executor out of the session crate onto a `sessionctx` trait surface, one Go file at a time.
4. T14 then T01.

## Concrete Steps

Per slice in `rust/`: `cargo check -p <crates> --tests` (exit status, not a grep), the touched tests under nextest with a mutation check, a local commit, `zsh scripts/gate-scoped.sh <worktree> tidb-server tidb-session` from `tidb-wt`, and a push to both remotes.

## Validation and Acceptance

Each row closes when its Go owner's code has one Rust owner in the matching layer and the row's tests (and a mutation of the moved code) pass in a green scoped gate.

## Idempotence and Recovery

Each slice is its own commit; a failing gate is fixed or the commit reverted before the next slice.
