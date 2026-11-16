# Give the Rust ports Go's shape: register rows S01–S05

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the repository root; this plan must be maintained according to it.

## Purpose / Big Picture

Five places in the Rust rewrite differ from Go's structure in ways that make every later port slower or riskier (register rows S01–S05 in `rust/docs/parity/current-audit/structural-findings.md`). After this plan a builtin that needs session state is a one-file port reading Go's optional eval properties, an index key part always addresses Go's one column list, a DDL error keeps its Go identity to the job, every reorganization runs as Go's `runReorgJob` does, and Go's `pkg/ddl/util` constants exist once. Each row is closed when its refactor lands with Go's own tests passing and the register row removed.

## Progress

- [x] (2026-11-16) Rows S01–S05 recorded in the register, each checked against the Go source.
- [x] (2026-11-16) S05: `tidb-ddl-util` holds Go `util.go`'s constants; `tidb-schemaver`, `tidb-pd-client`, `tidb-ddl-serverstate`, `tidb-domain-serverinfo`, `tidb-session`, `tidb-ddl` and `tidb-server` use it (commit `6393cff6b34`).
- [ ] S01: the live session builds Go's `sessionexpr` expression context; builtins read the optional eval-property providers.
  - [x] (2026-11-16, `6393cff6b34`) Step 1: `Columns::get_optional_prop_provider`; `StmtContext` installs Go's KV-store and privilege-checker providers; `TIDB_MVCC_INFO` reads them through `KVStorePropReader` / `PrivilegeCheckerPropReader`, and the `has_super_privilege` / `get_mvcc_by_encoded_key` hooks are gone. `expropt`'s readers take any `EvalPropContext`, sized or not, so the live `dyn Columns` is one.
  - [x] (2026-11-16, `b866895054f`) Step 2a: `TIDB_DECODE_SQL_DIGESTS`' PROCESS check (build and evaluation) and the extension functions' dynamic-privilege check read `PrivilegeCheckerPropReader`; the `has_process_privilege` / `has_privilege_checker` / `request_dynamic_verification` hooks and the eager per-statement PROCESS lookup are gone.
  - [ ] Step 2b: `TIDB_DECODE_KEY` / `TIDB_ENCODE_*_KEY` over `InfoSchemaPropReader`. Open question: Go's `DecodeKeyFromString` / `EncodeRecordKeyFromRow` are `expression` package variables that `planner/core`'s `init` sets; Rust has no `init`, and today the statement context carries the function. Decide whether an explicit install at domain start (like Go's `init`) or the per-statement function stays.
  - [ ] Step 2c: `active_role_identities` and `retrieve_sql_digest_texts` over `SessionVarsPropReader` / `SQLExecutorPropReader`; expropt's `SessionVars` is a marker today, so this needs the session-variables provider of step 3.
- [ ] S02: `TableCommon` holds Go's one `Columns` list in metadata order; `Cols` / `VisibleCols` / `WritableCols` / `DeletableCols` are views; index offsets are `IndexColumn.Offset`.
- [ ] S04: the local backfill runs under the job's `reorgCtx` like the DXF path (`DistReorgSlot` generalised to Go's `reorgCtx`).
- [ ] S03: one Go-faithful DDL/reorg error type (plain error vs `*terror.Error`) through backfill, DXF and `is_retryable_job_error`.

## Surprises & Discoveries

- Observation: the eager per-statement SUPER check (a symptom of S01) broke `tests_extension_auth::test_plugin_user_modification` (12 plugin privilege calls instead of 3).
  Evidence: gate 593, `rust/crates/tidb-session/src/tests_extension_auth.rs:378`.
- Observation: S02's two offset bases caused a mutation-checker panic on INSERT during MODIFY COLUMN over an indexed column.
  Evidence: `realtikv_addindextest3::test_merge_temp_index_basic`, `mutation_checker.rs:259`.

## Decision Log

- Decision: order S05, S01, S02, S04, S03.
  Rationale: S05 is trivial. S01 has the most leverage: the remaining realtikv ports (txntest, pessimistictest and the others) keep hitting builtins that need session state, and every one costs 6–8 crates today. S02 touches every DML path that indexes rows, so it goes after S01 and needs the full gate plus the realtikv suites. S04 builds on the DXF `DistReorgSlot` that just landed. S03 touches the same DDL error paths as the codex `realtikv-txn` branch, so it waits for that branch to merge.
  Date/Author: 2026-11-16, Claude.
- Decision: global sort (D02's remainder) is not in this plan; another agent owns it.
  Date/Author: 2026-11-16, user.

- Decision: S01's first step moves `DynOptionalEvalPropProvider` (the `Any`-capable form of Go's `exprctx.OptionalEvalPropProvider`) from `tidb-expropt` into `tidb-exprctx`, and gives `tidb_exprctx::Columns` Go's `GetOptionalPropProvider(key)`; `OptionalEvalPropProviders` stays in `tidb-expropt`, as Go's does (`expropt/optional.go`).
  Rationale: Go's `GetOptionalPropProvider` is a method of `pkg/expression/exprctx`'s `EvalContext` (`exprctx/context.go:94`) returning `exprctx.OptionalEvalPropProvider`; `tidb-expropt` depends on `tidb-exprctx`, so the method cannot name an `expropt` type without a cycle. `expropt`'s readers then take any `Columns`.
  Date/Author: 2026-11-16, Claude.

## Outcomes & Retrospective

(none yet)

## Context and Orientation

Go is the source of truth (`pkg/` in this repository). Owners per row:
S01 `pkg/expression/sessionexpr/sessionctx.go` (`NewExprContext`, `setOptionalProp`), `pkg/expression/expropt`, `pkg/session/session.go`; Rust `rust/crates/tidb-sessionexpr`, `rust/crates/tidb-expropt`, the hand-built hooks on `tidb_exprctx::Columns` (`tidb_decode_key`, `tidb_encode_key`, `get_mvcc_by_encoded_key`, `has_process_privilege`, `has_super_privilege`) and their `StmtContext` / `stmt_ctx_build.rs` / `stmt_ctx_session.rs` plumbing.
S02 `pkg/table/tables/tables.go` (`TableCommon`, `newTableCommon`, `initTableCommon`); Rust `rust/crates/tidb-table/src/tables.rs`, `tables/nonpublic_columns.rs`, `table_from_meta.rs` (`kv_index`'s offsets).
S03 `pkg/ddl/job_worker.go`, `pkg/ddl/index.go` (`isRetryableJobError`, `runReorgJobAndHandleErr`); Rust `rust/crates/tidb-ddl/src/real_tikv_ddl.rs` (`ClusterDdlError`, `is_retryable_job_error`), `rust/crates/tidb-ddl-build/src/plan_error.rs`.
S04 `pkg/ddl/reorg.go` (`runReorgJob`, `reorgCtx`); Rust `rust/crates/tidb-ddl/src/real_tikv_ddl/add_index_reorg.rs` (`DistReorgSlot`), `backfilling.rs` (`BackfillExecutor::deadline`).
S05 `pkg/ddl/util/util.go`.

## Plan of Work

Each row is its own commit (or short series), gated and pushed before the next starts. For S01, incrementally, since `tidb_exprctx::Columns` (the live eval context) has 87 methods and 75 implementations: (1) give `Columns` Go's `GetOptionalPropProvider` (one method answering the statement's `OptionalEvalPropProviders`, `None` by default) and have `StmtContext` install Go's providers, built from what the session already hands it (KV store with the region store as Go's `helper.Storage`, latest infoschema snapshot, privilege checker over `session_privilege`, session vars, SQL executor); (2) move one hand-built hook at a time onto its Go reader (`KVStorePropReader` / `PrivilegeCheckerPropReader` for `TIDB_MVCC_INFO`; `InfoSchemaPropReader` / `PrivilegeCheckerPropReader` / `SessionVarsPropReader` for `TIDB_ENCODE_*_KEY` and `TIDB_DECODE_KEY`; `PrivilegeCheckerPropReader` for `TIDB_DECODE_SQL_DIGESTS`' PROCESS check), deleting that hook and its `StmtContext` / `stmt_ctx_build.rs` / `stmt_ctx_session.rs` plumbing in the same commit; (3) when no hook is left, `tidb-sessionexpr`'s `EvalContext` and `StmtContext`'s provider set are one thing, as Go's `sessionexpr.NewExprContext` is. For S02: give `TableCommon` Go's `Columns` and derived views, address rows and index key parts by `col.Offset`, and delete `nonpublic_columns`' separate list and `write_column`. For S04: generalise `DistReorgSlot` into Go's `reorgCtx` and run the local `reorgFn` under it, deleting the cooperative deadline. For S03: one error enum with Go's plain-error and terror variants carried from backfill and DXF to `run_reorg_job_error`.

## Concrete Steps

Per row, in its worktree (`rust/`): `cargo check --offline --tests -p <changed crates>`; the changed crates' suites with `cargo nextest run --offline -p <crate>`; then the gate (`../scripts/gate-alone.sh N`: build, suites, integration replay). S02 and S04 also run the realtikv add-index packages, one package per fresh playground: `/Users/qiliu/projects/tidb-wt/scripts/realtikv-run.sh <log> 'test(realtikv_addindextest3::)'` (and `addindextest`, `addindex_source`, `addindextest2`, `addindextest4`).

## Validation and Acceptance

S01: Go's `TestTiDBEncodeKey`, `TestTiDBDecodeKeyFunc`, `TestTiDBMVCCInfo`-style tests and `tests_extension_auth` pass, and a builtin's privilege is asked only when it is built or evaluated (the plugin-call counts in `test_plugin_user_modification` stay at Go's). S02: the realtikv add-index packages pass, including `test_merge_temp_index_basic`'s MODIFY COLUMN case, and the DML suites pass. S03: `test_issue55808` and `test_add_index_resumes_from_checkpoint_after_partial_scan` keep Go's outcomes. S04: `test_cancel_after_reorg_timeout` and the local-ingest realtikv tests pass with the cooperative deadline removed. S05: `grep -rn '"/tidb/ddl/fg/owner"' rust/crates` finds one definition.

## Idempotence and Recovery

Every step is a source refactor in its own commit; a failing gate is fixed or the commit is reverted before the next row starts. After stopping a gate, resume frozen builds (`kill -CONT` on processes in state `T`). A killed realtikv test leaves cluster state behind: restart the playground with fresh data.
