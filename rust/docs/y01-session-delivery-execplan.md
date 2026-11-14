# Follow Go's DDL delivery and pooled system-table execution

This living ExecPlan follows root `PLANS.md`. All Go files are read-only. Work stays on the current branch, commits only Rust changes, and never pushes or deletes build artifacts.

## Purpose / Big Picture


DDL workers must observe cancellation, pause, and another owner's completion at the same boundaries as Go. The scheduler must retain the delivered `model.JobW` (a decoded job and its exact stored bytes), refresh it between steps through `systable.Manager.GetJobByID`, retry transient refresh failures, and stop on `ErrNotFound`. Bootstrap and store tests must execute DDL system-table SQL through sessions, as ordinary nodes do.

## Progress


- [x] Read repository guidance, current Go delivery/worker/manager code, and locate the Rust runner and bootstrap.
- [x] Port delivery, refresh failpoints, and the transaction byte comparison with Go-pinned regression tests.
- [x] Replace unbound-pool snapshot and raw-row execution with SQL sessions, including bootstrap and store harnesses.
- [ ] Run targeted checks and tests, then changed crates' full suites; update both structural registers and commit coherent changes.

## Surprises & Discoveries


Go `worker.transitOneJobStep` still reads job bytes in its transaction: it compares them with `JobW.Bytes` and returns `job meta changed by others` on mismatch. It does not execute a freshly decoded transaction row. Rust bootstrap currently plans all jobs over a borrowed snapshot overlay and publishes one net transaction; this needs session wiring without inventing another SQL interpreter.

## Decision Log


- Decision: Retain Go's byte comparison while moving decoded-job ownership to delivery. Rationale: merely removing the transaction read would lose cancellation/owner-change conflict protection. Date: 2026-11-14.
- Decision: Use the existing SQL session implementation for pool-less paths. Rationale: a system-table-specific SQL emulator would preserve the structural gap. Date: 2026-11-14.

## Context and Orientation


Go owners are `pkg/ddl/job_scheduler.go`, `job_worker.go`, `systable/manager.go`, and `pkg/session/bootstrap.go`. Rust runs jobs in `rust/crates/tidb-ddl/src/real_tikv_ddl.rs`; step planning is in `cluster_ddl.rs` and scheduling in `worker.rs`. `tidb-ddl-systable` owns pooled reads and the snapshot twin. `tidb-session/src/ddl_session_pool.rs` implements real SQL sessions. `tidb-session/src/mysql_bootstrap/jobs.rs` currently stages jobs over a snapshot overlay. `tidb-ddl-build/src/session_pool.rs` is the dependency boundary between DDL and SQL execution.

## Plan of Work


First retain `JobW` from `loadAndDeliverJobs`, pass it into worker steps, and refresh only after a step and schema synchronization. Port `beforeRefreshJob` and `mockGetJobByIDFail` through `tidb-failpoint`, using the Go tests `TestCreateTables`, `TestUpgradingRelatedJobState`, and the admin pause/cancel tests as behavioral anchors. Preserve Go's transaction byte check and failure checkpoint behavior.

Then give store harness and bootstrap actual SQL sessions, remove `SnapshotSystemTables` and unbound-pool raw staging, and migrate the existing tests to those sessions. Inspect dependencies before moving interfaces; preserve unrelated behavior and generated code. Update Y01 in both audit registers only to the extent verified.

## Concrete Steps


Run all Cargo commands from `rust/` with this environment prefix and six build workers:

    CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo check -j6 -p <crate> --tests
    CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo nextest run -j6 --build-jobs 6 -p <crate> -E 'test(<name>)'
    CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo nextest run -j6 --build-jobs 6 -p <crate>

Run checks in the background and inspect their logs. Before committing, inspect `git diff --check`, status, and the staged diff. Commit with `git add rust/ && git commit` only after verifying no unrelated Rust changes appeared. Never commit probes or push.

## Validation and Acceptance


The new Go-pinned tests must fail before the implementation and pass afterward. Assert transient refresh retry, exact failpoint boundaries, cancellation/pause visibility, missing-job termination, and stale delivered-byte rejection. Existing crate suites must pass, including bootstrap artifacts. A structural search must show that production job execution cannot fall back to snapshot system tables or raw job-row staging. Record exact commands and counts here. These are local tests; no live TiKV deployment is implied.

## Idempotence and Recovery


Tests use isolated stores. Re-run failed commands after a targeted fix; do not delete caches. Keep production changes and tests together. Preserve any concurrent changes discovered by status checks. If a test fails on unchanged code, confirm it once and record it as baseline evidence.

## Interfaces and Dependencies


Use `tidb_model::JobW`, `tidb_ddl_systable::Manager`, `tidb_ddl_session::DynPool`, and `tidb_ddl_build::session_pool::DdlSessionPool`. Failpoints use full `github.com/pingcap/tidb/pkg/ddl/...` paths and Go terms. The SQL executor remains in the session layer, outside the DDL dependency graph.

## Outcomes & Retrospective


Delivery baseline: `cargo nextest run -j6 -p tidb-session -E 'test(ddl_delivery::)'` failed both initial regressions before implementation (`/tmp/y01-before.log`): cancellation before transit was lost and beforeRefreshJob was never called. The expanded four-test suite passed after implementation. Store harness plus delivery: 15/16 passed (`/tmp/y01-pool-tests2.log`); the remaining test expected the old notification-failure wait count and is corrected to include Go delivery's recovery wait.

Production session migration now removes `SnapshotSystemTables`, snapshot scheduler/min-ID loaders, raw reorg-handle reads/writes, and raw submission/step/MDL fallbacks. Store-only tests open real SQL sessions backed by the same KV store and coprocessor. Bootstrap uses the common statement sequence with a real DDL executor; deterministic artifact fixtures retain their explicitly separate snapshot planner. Bootstrap and full suites remain to validate.


The full changed-crate run exposed a bootstrap-owner lifetime defect: a running scheduler session retained the domain after the temporary bootstrap pool was dropped, so it consumed jobs subsequently seeded by worker tests. The store-owned pool now cancels its owner before releasing the domain. Four of five affected worker tests passed on the focused rerun; the fifth exposed the delivery result treating Go's final PAUSED state as finished. Delivery now releases the worker with the existing Paused outcome and retains the active row. The full suite is running again.

Source tests ported: `TestCreateTables`, `TestCancelJobBeforeRun`, `TestUpgradingRelatedJobState`, the four `TestPauseCancelAndRerun*Stmt` tests (all 85 case rows), `systable.TestManager`, `session.TestBootstrap`, and `session.TestBootstrapWithError`. The latter discards doDMLWorks after the live DDL jobs have committed and verifies that a subsequent bootstrap resumes, matching the Go killed-session scenario. The store-level tests exercise SQL over the embedded store; this is not real TiKV deployment validation.

All current production paths require a bound session pool. Raw bootstrap job planning remains accessible solely to deterministic artifact fixtures; the publishing bootstrap no longer calls it. Go's InitDDLTables still directly creates the reserved DDL tables in meta because those tables must exist before any DDL can execute, and this behavior is preserved.


Final five-crate `cargo check -j6 -p tidb-ddl -p tidb-ddl-build -p tidb-ddl-systable -p tidb-domain-core -p tidb-session --tests` passed (`/tmp/y01-check-final.log`). The second full suite passed the live bootstrap, interrupted-bootstrap recovery, both node bootstrap layouts, and the SQL systable manager test. A later worker-test fixture returned cancellation from its retry wait without retaining cancellation for the next refresh; the fixture now retains it as Go's scheduler context does. Its full barrier/pause/error-checkpoint/validation-cancellation scenario passed on the focused rerun (`/tmp/y01-context-retry-tests.log`, 255.6 s). The cached-table `TestStateRemote` port failed its 200 ms lease renewal in both loaded runs and remains under investigation; its assertions are unchanged. No TSO drift was found: the embedded oracle follows Go's wall-clock physical component.


Focused Go admin-pause validation: all ten partition cases and all 14 schema/table/placement cases passed (`/tmp/y01-adminpause-tests.log`). The index group failed on its second case: the first successful rerun stored `ADD PRIMARY KEY idx_id` as `idx_id`, so Go's cleanup `DROP PRIMARY KEY` did not remove it. Go `executor.CreatePrimaryKey` checks the supplied name and then replaces it with `mysql.PrimaryKeyName`; `build_add_index_job` now does the same. `cargo check -j6 -p tidb-ddl --tests` passed after that correction (`/tmp/y01-check-primary-name.log`). A throwaway `zzprobe/y01_primary_name` Go test captures the canonical name and overlong-name diagnostic; it is not staged.

The full run also exposed `analyzetest::test_adjust_sample_rate_note`: after updating `mysql.stats_meta.count` to 220000, `SHOW STATS_META` still returned 0. Its relationship to Y01 has not been established; do not call it a baseline failure without verification. The source assertions remain unchanged.


Go capture completed successfully:

    go test --tags=intest -p 6 ./zzprobe/y01_primary_name -run TestPrimaryName -count=1 -v

Output pinned in the index group's setup:

    INDEX_NAMES=[[PRIMARY]]
    LONG_PRIMARY=[ddl:1059]Identifier name 'PRIMARY' is too long

The probe is outside `pkg/`, injects no failpoints, and changes no owning Go source. The package-only failpoint runner workflow therefore does not require source rewriting. The initial `-p6` spelling was rejected before executing the probe; the successful command above uses Go's `-p 6`. `cargo check -j6 -p tidb-session --tests` passed with the new captured assertions (`/tmp/y01-check-primary-test.log`). The 39-case index group is rerunning in `/tmp/y01-primary-index-tests.log`.

The ongoing full run also failed `auto_analyze_priority_queue_uses_shared_stats_ddl_and_ordinary_analyze_path` (queue length 0, expected 2); baseline status remains unverified. The schema/table/placement and partition pause groups completed in 249.1 s and 201.9 s, respectively. The earlier column group covered all 22 cases in 520.6 s.


The corrected index group passed all 39 cases and both Go-captured primary-name assertions (607.9 s; `/tmp/y01-primary-index-tests.log`). Together with the 22 column, ten partition, and 14 schema/table/placement cases, all 85 Go adminpause rows have passed. The ongoing full run also passed `TestCreateTables`' transient GetJobByID retry, `TestCancelJobBeforeRun`, `TestUpgradingRelatedJobState`, and the step-session history/MDL/delete-range SQL tests. A full `tidb-ddl` suite is running after the primary-key correction (`/tmp/y01-ddl-final-suite.log`).


The final `tidb-ddl` suite passed all 197 tests (257.4 s; `/tmp/y01-ddl-final-suite.log`), including the retained cancellation fixture and primary-key-name correction. The five-crate full run is still progressing through the SQL-backed session tests. The two failures already corrected have passing reruns; three lease/statistics failures need unchanged-tip comparison before attribution. For that comparison, preserve the staged implementation, temporarily read tracked files from HEAD in this same worktree, compile the session test with a separate `-Cextra-filename=-y01-baseline`, run the three original tests, and restore the staged source. The separately named executable leaves the running suite's executable intact. No other worktree or build-artifact deletion is involved.
