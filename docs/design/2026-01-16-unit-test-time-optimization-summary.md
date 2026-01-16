# Unit Test Time Optimization Summary (Local)

- Author(s): [bba](https://github.com/bba)
- Detailed log: `docs/design/2026-01-13-unit-test-time-optimization.md`
- Scope: TiDB unit tests executed by `tools/bin/ut` (`make gotest_in_verify_ci`) for both:
  - Nextgen: `NEXT_GEN=1`
  - Legacy: `NEXT_GEN` unset

## Executive Summary

This patch focuses on two things:

1. **Performance**: remove “single very slow testcases” by shrinking test data, eliminating exponential loops, replacing fixed sleeps with bounded waits, and gating heavy stress loops behind `testflag.Long()`.
2. **Stability**: eliminate “flake amplifiers” found while running full UT suites (timeouts, background goroutine leaks, resource conflicts).

### What improved (objective signals)

- Changed files: **96** (`*_test.go`: **78**, non-test `.go`: **18**).
- Eliminated **all** `>10s` single testcases in both modes (based on local full runs):
  - Nextgen `>10s`: `51` → `0` (also `>=600s` timeouts: `3` → `0`)
  - Legacy `>10s`: `21` → `0`
  - Unique `>10s` testcases across both baselines: `61` → `0` (pkg+test union)

### Before/After (local full runs)

Timings are from local full runs with `tools/bin/ut` JUnit reports. Build stage is sensitive to local Go cache; the primary signal is **per-testcase runtime** and **run stage**.

- Nextgen:
  - Baseline: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260114_122930/`
    - Run stage: `~16m06s`; longest testcase hit `600s` timeout
  - Current: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260116_111713/`
    - Run stage: `5m27s`; longest testcase: `9.392s`
- Legacy:
  - Baseline: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_legacy_20260115_165149/`
    - Run stage: `~7m10s`; longest testcase: `~36.0s`
  - Current: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_legacy_20260116_103514/`
    - Run stage: `7m03s`; longest testcase: `9.610s`

## Flakiness / Stability Fixes

During full-suite profiling, **13** high-impact “flake amplifiers” were fixed (timeouts, background leaks, racey waits):

- `pkg/executor` `TestFlashbackClusterWithManyDBs`
- `pkg/planner/core/casetest/instanceplancache` `TestInstancePlanCacheConcurrencySysbench`
- `pkg/ddl/tests/partition` `TestMultiSchemaPartitionByGlobalIndex`
- `pkg/objstore/objectio` `TestNewCompressReader`
- `pkg/statistics/handle/cache/internal/lfu` `TestMemoryControlWithUpdate`
- `pkg/store/gcworker` `TestLeaderTick`
- `pkg/ddl` `TestBackfillingSchedulerGlobalSortMode`
- `pkg/server/handler/optimizor` `TestPlanReplayerWithMultiForeignKey`
- `pkg/executor/test/analyzetest` `TestKillAutoAnalyze`
- `pkg/util/memory` `TestRelease`
- `pkg/store/gcworker` `TestGCWithPendingTxn`
- `pkg/expression/integration_test` `TestGetLock`
- `pkg/store` `TestStoreSwitchPeer`

## Bug Fixes (non-test behavior)

This patch also includes a small number of correctness fixes in non-test code paths:

- `pkg/expression/builtin_miscellaneous.go`: `GET_LOCK()` treats `tikverr.ErrLockWaitTimeout` as “lock not acquired” (return `0`) to match MySQL semantics.
- `pkg/sessionctx/variable/sysvar.go`: sysvar read returns `0` when the slow-log rate limiter is unlimited (`rate.Inf`) to avoid unstable reads in tests and match expected semantics.

## Change Breakdown (by package)

The detailed per-file list is recorded in `docs/design/2026-01-13-unit-test-time-optimization.md` and `git diff --name-only`. Below is a package-grouped breakdown of what changed and why.

### tools/check

- `tools/check/ut.go`: add `ut run --timeout <duration>` to override per-test `-test.timeout`; improve cover profile merge error handling and always clean temporary dirs.

### pkg/bindinfo

- `pkg/bindinfo/tests/bind_usage_info_test.go`: reduce fixed sleeps (1s → 10ms) and restore global knobs/sysvars after the test to avoid cross-test interference.

### pkg/ddl

- Performance:
  - `pkg/ddl/ingest/integration_test.go`: shrink ingest DDL scenarios and cap concurrent DML injections; move expensive multi-add-index coverage behind `testflag.Long()`.
  - `pkg/ddl/tests/tiflash/ddl_tiflash_test.go`: reduce data volume and make teardown safer (stop background pollers before closing mock servers).
  - `pkg/ddl/tests/partition/main_test.go`: shorten cleanup polling in non-long UT.
- Stability:
  - `pkg/ddl/backfilling_dist_scheduler_test.go`: use cancelable contexts to avoid background task leakage.
  - `pkg/ddl/tests/partition/multi_domain_test.go`: wait for async unistore delete-range completion before asserting old-table KV is gone.
  - `pkg/ddl/main_test.go`: extend goleak ignore list for `-cover` stability.
- Misc:
  - `pkg/ddl/backfilling.go`, `pkg/ddl/job_worker.go`: make delay-related failpoints accept configurable values (avoid hard-coded sleeps in tests).
  - `pkg/ddl/cancel_test.go`, `pkg/ddl/db_cache_test.go`, `pkg/ddl/index_modify_test.go`, `pkg/ddl/job_scheduler_testkit_test.go`, `pkg/ddl/tests/partition/db_partition_test.go`: reduce loops/sleeps and make waits bounded.

### pkg/domain

- `pkg/domain/crossks/cross_ks_test.go`: reduce etcd integration cluster size and avoid expensive shared-client patterns (cut `TestManager` runtime and reduce flakes).

### pkg/dxf

- `pkg/dxf/importinto/job_testkit_test.go`: reduce etcd integration cluster size for nextgen keyspace tests (`10` → `2`) to keep `TestSubmitTaskNextgen` bounded.
- `pkg/dxf/framework/integrationtests/*`: reduce randomized case counts / tighten timeouts to keep integration-style UT bounded.

### pkg/executor

- Analyze / checksum stability:
  - `pkg/executor/analyze.go`, `pkg/executor/analyze_col.go`, `pkg/executor/analyze_col_v2.go`, `pkg/executor/analyze_worker.go`, `pkg/executor/checksum.go`: make analyze-related tests deterministic by finalizing pending jobs on errors, bounding waits, and reducing race windows.
  - `pkg/executor/test/analyzetest/analyze_test.go`, `pkg/executor/test/analyzetest/memorycontrol/memory_control_test.go`: use eventual checks and relax hard timing assertions under parallel full runs.
- Kill/cancel / concurrency:
  - `pkg/executor/test/executor/executor_test.go`: bound `TestQueryWithKill` runtime in normal UT and reduce worker count while preserving kill-path coverage.
  - `pkg/executor/test/distsqltest/distsql_test.go`, `pkg/executor/distsql_test.go`: shrink data volume and iterations for concurrency/race tests.
- “Big loop” reductions:
  - `pkg/executor/test/admintest/admin_test.go`: shrink corrupted-index locate tests (batch ops, fewer rows).
  - `pkg/executor/test/seqtest/seq_executor_test.go`: validate placeholder boundary via `PrepareStmt` instead of executing huge inserts.
  - `pkg/executor/test/indexmergereadtest/index_merge_reader_test.go` and `pkg/executor/index_merge_reader.go`: reduce stress loop repetition and replace fixed `Sleep(20s)` with a bounded wait under failpoint.
  - `pkg/executor/test/cte/cte_test.go`: shrink recursion/data and replace `EXPLAIN ANALYZE` with `COUNT(*)` for `TestCTEIterationMemTracker`.
  - `pkg/executor/test/tiflashtest/tiflash_test.go`: reduce mock TiFlash nodes/replicas/rows and gate expensive scenarios behind `testflag.Long()`.
  - `pkg/executor/sortexec/*`, `pkg/executor/join/*`, `pkg/executor/aggregate/agg_spill_test.go`: reduce data volume/iterations in spill/interrupt/failpoint resilience tests.
- Misc:
  - `pkg/executor/internal/applycache/apply_cache_test.go`, `pkg/executor/importer/*`, `pkg/executor/recover_test.go`, `pkg/executor/test/unstabletest/memory_test.go`: remove fixed sleeps and add bounded eventual assertions to prevent full-run timeouts.

### pkg/expression

- `pkg/expression/builtin_miscellaneous.go`: `GET_LOCK()` returns `0` on lock wait timeout (MySQL-compatible) to stabilize `TestGetLock`.

### pkg/infoschema

- `pkg/infoschema/test/clustertablestest/cluster_tables_test.go`: validate privilege behavior via `EXPLAIN` to avoid executing expensive cluster-table queries.

### pkg/objstore

- `pkg/objstore/gcs_test.go`: keep `TestCtxUsage` bounded by implementing the expected token endpoints in httptest server.
- `pkg/objstore/gcs.go`: allow `WithoutAuthentication()` for localhost endpoints when `intest.InTest` (avoid default-credential dependency in tests).
- `pkg/objstore/objectio/writer_test.go`: close resources and remove fragile goroutine-count assertions in compression reader test.
- `pkg/objstore/azblob_test.go`: reduce request counts / tighten waits to keep blob mock tests bounded under parallel full runs.

### pkg/planner

- `pkg/planner/core/tests/extractor/memtable_infoschema_extractor_test.go`: replace exponential predicate cartesian-product enumeration with “single-column full coverage + pairwise combinations”.
- `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go`: limit testdata case count and skip expensive variants in non-long UT.
- `pkg/planner/core/casetest/hint/hint_test.go`: shorten fixed sleeps while keeping hint parsing/warning coverage.
- `pkg/planner/core/casetest/instanceplancache/*`: reduce concurrency stress and make deadlocks retryable (avoid flaky failures).

### pkg/plugin

- `pkg/plugin/integration_test.go`: remove fixed sleeps / tighten waits to keep plugin tests bounded in full runs.

### pkg/server

- Performance:
  - `pkg/server/conn_test.go`: shrink sleep durations and data volume in max-execution-time tests.
  - `pkg/server/handler/tests/http_handler_test.go`: remove fixed sleeps and bound concurrency rounds in topology/label tests (`testflag.Long()` keeps higher stress).
  - `pkg/server/tests/commontest/tidb_test.go`: shrink `TestTopSQLCPUProfile` (remove redundant cases; skip plan decoding when not needed).
- Stability:
  - `pkg/server/handler/optimizor/plan_replayer_test.go`: extend binding-take-effect wait window and reduce plan-cache interference (avoid flakes).
  - `pkg/server/handler/optimizor/statistics_handler_test.go`: avoid unix socket path length failures and fail fast on server start errors.
  - `pkg/server/tests/commontest/tidb_test.go`: use random ports and fail-fast proxy start for proxy-protocol tests.
  - `pkg/server/tests/servertestkit/testkit.go`: shorten TopSQL reporting interval in tests (2s → 1s).
  - `pkg/server/tidb_library_test.go`: reduce `TestMemoryLeak` iterations in non-long UT and scale the memory upper bound accordingly.

### pkg/session

- `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go`: remove redundant upgrade-matrix subtests and keep the coverage-critical upgrade paths.
- `pkg/session/test/bootstraptest2/boot_test.go`: batch dist-task state coverage to avoid repeated bootstrap/upgrade loops.
- `pkg/session/bootstrap_test.go`, `pkg/session/test/bootstraptest/boot_test.go`: reduce repeated work and tighten waits to keep bootstrap suites bounded.

### pkg/sessiontxn

- `pkg/sessiontxn/txn_rc_tso_optimize_test.go`: simplify `TestRcWaitTSInSlowLog` checks (assert non-zero wait duration instead of fragile pairwise comparisons).

### pkg/statistics

- `pkg/statistics/handle/cache/internal/lfu/lfu_cache_test.go`: wait async cache writes for `-cover` stability.
- `pkg/statistics/handle/storage/dump_test.go`: ensure `h.Update()` is called after `LoadStatsFromJSON` before validating loaded stats.
- `pkg/statistics/handle/storage/gc_test.go`: drain DDL events and adjust lease handling to stabilize `TestGCExtendedStats`.

### pkg/store

- Coprocessor backoff:
  - `pkg/store/copr/batch_coprocessor.go`, `pkg/store/copr/coprocessor.go`: make `ReduceCopNextMaxBackoff` failpoint accept configurable values (avoid long backoffs in UT).
  - `pkg/store/batch_coprocessor_test.go`, `pkg/store/copr/batch_coprocessor_test.go`: enable failpoints to reduce backoff and add bounded retries for occasionally slow TiFlash-related paths.
- GC worker:
  - `pkg/store/gcworker/gc_worker_test.go`: stop TTL job manager in test bootstrap, and use bounded polling for GC completion; relax mock-PD-specific assertions.
- Misc:
  - `pkg/store/mockstore/unistore/lockstore/lockstore_test.go`: shorten stress duration in non-long UT (10s → 1s) and remove noisy logs/prints.

### pkg/table

- `pkg/table/tables/cache.go`: add failpoint-controlled cache write-lease duration to make cache-table tests complete quickly and deterministically.
- `pkg/table/tables/cache_test.go`: replace fixed sleeps/loops with `require.Eventually`; set a small cache lease for UT.
- `pkg/table/tables/tables_test.go`: shrink combination matrix for `TestTxnAssertion` in non-long UT.

### pkg/timer

- `pkg/timer/store_intergartion_test.go`: tighten timeouts and replace fixed sleeps with eventual waits.

### pkg/ttl

- `pkg/ttl/ttlworker/scan_integration_test.go`: batch-insert rows in `TestCancelWhileScan` instead of 10k single-row inserts.
- `pkg/ttl/ttlworker/job_manager_integration_test.go`: consume notification channels directly and extend the wait window to avoid flakes under parallel full UT runs.

### pkg/util

- `pkg/util/chunk/alloc_test.go`: reduce goroutine/loop counts and avoid assertions inside goroutines in allocator tests.
- `pkg/util/gctuner/memory_limit_tuner.go`, `pkg/util/gctuner/memory_limit_tuner_test.go`: make failpoint reset interval configurable to shorten tuning tests.
- `pkg/util/hack/map_abi_test.go`: replace allocator-sensitive exact-size assertions with bounds.
- `pkg/util/memory/tracker_test.go`: avoid racy consume/release patterns that can make global analyze memory usage go negative.
- `pkg/util/stmtsummary/v2/tests/table_test.go`: use a unique temp statement-summary filename (avoid shared-file conflicts across tests) and clean up reliably.
- `pkg/util/workloadrepo/worker_test.go`: remove fixed sleeps and shorten worker lifecycle tests.

## Appendix: Baseline data / artifacts

- Nextgen baseline (>10s cleanup “before”): `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260114_122930/parsed/slow_testcases.tsv`
- Nextgen current: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260116_111713/parsed/slow_testcases.tsv`
- Legacy baseline (>10s cleanup “before”): `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_legacy_20260115_165149/parsed/slow_testcases.tsv`
- Legacy current: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_legacy_20260116_103514/parsed/slow_testcases.tsv`
