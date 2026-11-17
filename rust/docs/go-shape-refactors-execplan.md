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
  - [x] (2026-11-17) Step 2b: `TIDB_DECODE_KEY` / `TIDB_ENCODE_*_KEY` read `InfoSchemaPropReader.GetLatestInfoSchema` (and `PrivilegeCheckerPropReader` for the encoders) and call `tidb-expr`'s Go package variables `DecodeKeyFromString` / `EncodeRecordKeyFromRow` / `EncodeIndexKeyFromRow`, which the executor's port of `planner/core`'s `init` sets once before the first session statement context. `StmtContext::with_latest_info_schema` installs Go's infoschema provider; the statement's latest infoschema (`LatestIndexSchema`) carries the codec tables, so the per-session `TidbDecodeKeySnapshot` cache, the `decode_key_from_string` / `encode_key_from_row` hooks and their `tidb_exprctx::Columns` methods are gone. `MetaOnlyInfoSchema: Any` is Go's `isVer.(infoschema.InfoSchema)`.
  - [x] (2026-11-17) Step 2c: `expropt::SessionVars` carries the fields builtins read (`ActiveRoles` so far); `StmtContext` installs Go's session-vars provider (`with_active_roles`) and SQL-executor provider (the session's restricted SQL executor, `StmtContextSessionState::sql_executor`). Extension functions read `SessionVarsPropReader` for `ActiveRoles`; `TIDB_DECODE_SQL_DIGESTS` runs Go's `SQLDigestTextRetriever`, now in `tidb-expr` (`expr_util::sql_digest_text_retriever`, Go `expression/util.go`), through `SQLExecutorPropReader`, and the memory tables use the same retriever. The `active_role_identities` / `retrieve_sql_digest_texts` hooks, `SqlDigestTextRetrieval` and the executor's copy of the retriever are gone.
  - [x] (2026-11-17) Step 3a: `SessionVars.User` and Go's current-user provider. `StmtContext` installs the session-vars and current-user providers over the statement's user, login, authentication plugin and active roles (reinstalled by `with_user`, `with_connection_info` and `with_active_roles`). `CURRENT_USER()`, `USER()` and `CURRENT_ROLE()` read `CurrentUserPropReader` (no user is Go's "Missing session variable when eval builtin" error, no roles is `NONE`), `VALIDATE_PASSWORD_STRENGTH` takes its user from it, the extension context's `User` is `SessionVars.User`, and `TIDB_ENCODE_*_KEY`'s denial names `SessionVars.User`'s matched account as Go's does (it named the login before). The `current_user` / `login_user` / `current_role` / `auth_plugin` hooks are gone.
  - [x] (2026-11-17) Step 3b: `StmtContext` answers Go's session-vars and current-user providers from its fields when a builtin reads them (Go's providers call `sctx.GetSessionVars()` then), replacing 3a's reinstall in every builder. `expropt::SessionVars` carries `ConnectionID`, `LastFoundRows`, `StmtCtx.PrevAffectedRows`, `StmtCtx.PrevLastInsertID`, `SetLastInsertID` and `getHintResourceGroupName`; `CONNECTION_ID()`, `FOUND_ROWS()`, `ROW_COUNT()`, `LAST_INSERT_ID()` (both forms, row and vectorized) and `CURRENT_RESOURCE_GROUP()` read them through `SessionVarsPropReader`, with Go's integer answers (0, not NULL, when nothing was published). The `found_rows` / `row_count` / `last_insert_id` / `set_last_insert_id` / `current_resource_group` hooks are gone; `Columns::connection_id` stays as Go's `BuildContext.ConnectionID`.
  - [x] (2026-11-17) Step 3c: `SET @v := expr` (row and vectorized) writes through `SessionVars.SetUserVarVal` (the statement's user-variable handle); the `set_uservar` hook is gone (reading stays on `Columns::get_uservar`, Go's `EvalContext.GetUserVarsReader`). `expropt::SessionVars`' methods default to a zero-valued Go `SessionVars` that keeps nothing, so fixtures implement only what they test.
  - [x] (2026-11-17) Step 3d: `SLEEP` is Go's `doSleep` over `SessionVars.SQLKiller`, `StmtCtx` table ids and `InInsertStmt || InUpdateStmt || InDeleteStmt`; the `StatementMemory` sleep helpers are gone.
  - [x] (2026-11-17) Step 3e: `TIDB_CURRENT_TSO()` reads `SessionVars` `tidb_current_ts`; `VALUES(col)` reads `SessionVars.CurrInsertValues`.
  - [x] (2026-11-17) Step 3f: `GET_LOCK`, `RELEASE_LOCK`, `IS_FREE_LOCK`, `IS_USED_LOCK` and `RELEASE_ALL_LOCKS` read `AdvisoryLockPropReader` (`GetAdvisoryLockError` keeps Go's lock-wait-timeout and deadlock cases apart); `TIDB_IS_DDL_OWNER()` reads `DDLOwnerPropReader`, which the session installs over its DDL owner manager. Fixed: `TIDB_IS_DDL_OWNER()` failed in every session, which had no DDL-owner provider.
  - [x] (2026-11-17) Step 3g: `NEXTVAL`, `LASTVAL` and `SETVAL` follow Go's `evalInt`: `GetSequenceOperator(db, seq)` (a bare name in `CurrentDB`), `GetSessionVars`, `PrivilegeChecker.RequestVerification` (INSERT for `NEXTVAL`/`SETVAL`, SELECT for `LASTVAL`), and `SessionVars.SequenceState`. `ExprOptError` carries the producer's `EvalError`, so sequence errors keep their codes. The `Columns` sequence hooks and the separate sequence privilege closure are gone.
  - [x] (2026-11-17) Step 3h: `TIDB_BOUNDED_STALENESS` reads `KVStorePropReader` and `SessionVarsPropReader` before its arguments and computes Go's `GetStmtMinSafeTime` (`store.GetMinSafeTS(txnScope)`, `injectSafeTS`, the statement's SafeTS cache); `FTS_MATCH_WORD` sets `StmtCtx.FTSFunctionIsUsed` through `GetSessionVars` at build time. The `get_stmt_min_safe_ts` / `set_fts_function_is_used` hooks are gone (the planner still reads `fts_function_is_used`, as Go reads `StmtCtx`).
  - [ ] Step 4: `tidb-sessionexpr`'s `EvalContext` and `StmtContext`'s provider set become one thing; `InfoSchemaPropReader` for `TIDB_ENCODE_*_KEY` / `TIDB_DECODE_KEY`; `VALIDATE_PASSWORD_STRENGTH`'s globals.
- [ ] S02: `TableCommon` holds Go's one `Columns` list in metadata order; `Cols` / `VisibleCols` / `WritableCols` / `DeletableCols` are views; index offsets are `IndexColumn.Offset`.
- [x] (2026-11-17) S04: `ReorgSlot` is Go's `reorgCtx` (`doneCh`, `rowCount`, warnings) for every reorganization: `run_reorg` starts the local backfill (`run_local_reorg`) on its own thread as it does the DXF one, each step waits one `updateProgressInterval` and answers `ErrWaitReorgTimeout` with the rows so far; the local backfill checks ownership through `IndexBackfiller::check_owner` (Go `isReorgRunnable`), and the cooperative deadline (`BackfillExecutor::deadline`, `TableRecordOutcome::Progress`) is gone.
- [x] (2026-11-17) S03: `ClusterDdlError::Backfill` carries a `DdlPlanError` built from the error's Go cause (`DdlPlanError::from_cause`, Go `errors.Cause`); storage and SQL failures keep their identity; `reorg_fn_error` keeps it for commit and index-backfill failures.
- [x] (2026-11-17) S06: `Domain::close` (Go `Domain.Close`, DDL stops first) and `ClusterDdl::stop` (Go `DDL.Stop`); server shutdown and the realtikv harness close the domain (commit `80beac1d46f`).

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

- Decision: S01 step 2b keeps Go's package variables (`expression.DecodeKeyFromString` and the two encoders) as `OnceLock`s in `tidb-expr`, set by `tidb_executor::expression_codec_fn::init` (Go `core_init.go`), which `session_statement_context` runs once through a `Once`.
  Rationale: Go sets them in `planner/core`'s `init`, which runs before any statement; the executor crate is where `planner/core`'s codec helper lives in Rust, and every session statement context is built there. The per-statement closures the statement context carried were a second way to reach the same function.
  Date/Author: 2026-11-17, Claude.
- Decision: `StmtContext` builds Go's session-vars and current-user providers when a builtin asks for them (`get_optional_prop_provider`), from its fields as they are then, with the shared cells a builtin writes (the last-insert-id cell; the user variables next).
  Rationale: Go's providers call `sctx.GetSessionVars()` at read time. A provider installed while the context is built goes stale when a later builder or the planner changes a field (the resource group, the last-insert-id channel), and a weak back-reference from an installed provider does not survive the builders' `Arc::make_mut`, which detaches weak pointers.
  Date/Author: 2026-11-17, Claude.
- Decision: the infoschema provider answers the statement's one latest infoschema for both `p(true)` and `p(false)`.
  Rationale: a Rust statement holds one infoschema snapshot (the domain's as it starts), and every Rust reader asks for the latest one (`GetLatestInfoSchema`); the session's transaction schema is added when a builtin that reads `GetSessionInfoSchema` is ported.
  Date/Author: 2026-11-17, Claude.

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
