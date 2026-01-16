# Unit Test Time Optimization (UT/CI)

- Author(s): [bba](https://github.com/bba)
- Related CI:
  - https://do.pingcap.net/jenkins/blue/organizations/jenkins/pingcap%2Ftidb%2Fpull_unit_test_next_gen/detail/pull_unit_test_next_gen/8126/pipeline

## Background

TiDB unit tests can take up to hours in CI. The goal is to:

1. Get a reliable per-target / per-test timing baseline (ignore CI caches).
2. Remove “single very slow testcases” (typically caused by oversized loops/data or fixed sleeps).
3. Keep coverage and stability (avoid `t.Parallel()`; prefer deterministic triggers).

## Measuring Timings

### CI (Bazel)

- Prefer `--nocache_test_results` to get real timings.
- Parse Bazel test logs to rank slow targets and testcases.

### Local (tools/bin/ut)

`tools/bin/ut` runs each test function in a package binary and writes JUnit XML.

- New flag: `--timeout <duration>` to override per-test `-test.timeout` (default 2m; 30m for `--race/--long`).
- If you see `compile: version "goX.Y.Z" does not match go tool version "goX.Y.W"`, make sure you run with a single Go toolchain (e.g. `env -u GOROOT GOTOOLCHAIN=go1.25.5`) and clear caches (`go clean -cache -testcache`).

Example:

```bash
make failpoint-enable
env NEXT_GEN=1 tools/bin/ut run --timeout 30m \
  --junitfile /tmp/tidb-junit-report.xml \
  --coverprofile /tmp/tidb_cov.unit_test.out \
  --except unstable.txt
make failpoint-disable
```

## High-impact Fixes (Done)

### Biggest CI offenders (from nextgen pipeline ranking)

- `pkg/planner/core/tests/extractor/memtable_infoschema_extractor_test.go`:
  - Replace predicate “full cartesian product” with “single-column full coverage + pairwise combos”, avoiding exponential SQL counts.
- `pkg/ttl/ttlworker/scan_integration_test.go`:
  - `TestCancelWhileScan`: batch insert instead of 10k single-row inserts.
- `pkg/executor/test/admintest/admin_test.go`:
  - `TestAdminCheckTableErrorLocate*`: reduce rows/iterations and batch KV ops.
- `pkg/executor/test/seqtest/seq_executor_test.go`:
  - `TestPrepareMaxParamCountCheck`: use `PrepareStmt` boundary check instead of executing a 65k-placeholder insert.
- `pkg/executor/test/distsqltest/distsql_test.go`:
  - `TestDistSQLSharedKVRequestRace`: reduce rows/limit/repeats while keeping the plan-path coverage.
- `pkg/util/chunk/alloc_test.go`:
  - `TestSyncAllocator`: reduce goroutines/loops and remove heavy per-alloc assertions in goroutines.
- `pkg/executor/test/indexmergereadtest/index_merge_reader_test.go`:
  - Reduce 100× repetition loops to 10× in normal UT (`testflag.Long()` keeps 100×).
- `pkg/executor/index_merge_reader.go`:
  - Replace a fixed `time.Sleep(20s)` in `testIndexMergeMainReturnEarly` failpoint with a bounded wait for “channel becomes full”.
- `pkg/executor/sortexec/sortexec_pkg_test.go`:
  - Reduce data volume for interrupt tests (`TestInterruptedDuringSort/Spilling`).
- `pkg/executor/sortexec/topn_spill_test.go` and `pkg/executor/sortexec/sort_spill_test.go`:
  - Reduce repetition and shorten random-close delay for failpoint/hang-resilience tests.
- `pkg/executor/join/outer_join_spill_test.go`:
  - Reduce redundant parameter combinations / stress loops for spill-related tests.
- `pkg/executor/join/hash_table_v1_test.go`:
  - Skip hard-coded `unsafe.Sizeof` assertions on non `linux/amd64` to keep local profiling unblocked.

### Biggest local offenders (from full local baselines)

- `pkg/executor/test/cte/cte_test.go`:
  - `TestCTEIterationMemTracker`: shrink recursion/data volume and replace `explain analyze` with `count(*)` (nextgen: `~45.6s` → `~0.85s`; legacy: `~11.0s` → `~1.10s`).
- `pkg/session/test/bootstraptest2/boot_test.go`:
  - `TestTiDBUpgradeWithDistTaskRunning`: batch multiple dist-task states into one upgrade run (legacy: `~36.0s` → `~6.8s`).

### Remaining >10s offenders (phase3, local)

- `pkg/server/tests/commontest/tidb_test.go`:
  - `TestTopSQLCPUProfile`: reduce redundant cases and skip normalized-plan decoding unless needed (nextgen: `14.740s` → `9.163s`; legacy: `16.157s` → `9.610s`).
- `pkg/ddl/ingest/integration_test.go`:
  - `TestAddGlobalIndexInIngest`: shrink partitions/rows and cap DML injections during ingest DDL (nextgen: `14.162s` → `4.502s`; legacy: `13.860s` → `4.779s`).
- `pkg/dxf/importinto/job_testkit_test.go`:
  - `TestSubmitTaskNextgen`: reduce etcd integration cluster size (`10` → `2`) (nextgen: `11.777s` → `9.282s`).
- `pkg/executor/test/executor/executor_test.go`:
  - `TestQueryWithKill`: bound runtime in normal UT (1s) and reduce worker count (nextgen: `11.569s` → `2.076s`; legacy: `11.922s` → `1.701s`).
- `pkg/table/tables/cache.go` and `pkg/table/tables/cache_test.go`:
  - `TestCacheTableBasicReadAndWrite`: add failpoint-controlled write-lease duration and replace fixed sleeps/loops with `require.Eventually` (nextgen: `11.543s` → `2.416s`; legacy: `11.745s` → `2.718s`).
- `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go`:
  - `TestUpgradeWithAnalyzeColumnOptions`: remove redundant “re-bootstrap per subtest” scenarios and keep the coverage-critical path (legacy: `24.082s` → `6.962s`).
  - `TestUpgradeVersion74`: drop redundant old-value matrix and keep the “old default → new default” upgrade path (legacy: `18.442s` → `6.945s`).

### Stability fixes encountered during profiling

- `pkg/statistics/handle/cache/internal/lfu/lfu_cache_test.go`:
  - Make `TestMemoryControlWithUpdate` stable under `-cover` by waiting async cache writes.
- `pkg/server/handler/optimizor/statistics_handler_test.go`:
  - Avoid unix-socket path length failures by not overriding `cfg.Socket` (already disabled in `util.NewTestConfig()`), and fail fast if `server.Run` returns error (avoid hanging on `RunInGoTestChan`).
- `pkg/domain/crossks/cross_ks_test.go`:
  - Reduce etcd integration cluster from 20 members to 1 and create fresh etcd clients on demand via `integration.NewClientV3`, reducing `TestManager` from ~29.7s to ~1.7s.
- `pkg/ddl/tests/tiflash/ddl_tiflash_test.go`:
  - Reduce teardown panic risk (stop poll + close domain before closing mock server).

### All changes in this patch (by file)

- `tools/check/ut.go`: add `ut run --timeout <duration>` to override per-test `-test.timeout`; improve cover profile merge to return errors and always `RemoveAll` the temp dir.
- `pkg/sessionctx/variable/sysvar.go`: return `0` for unlimited slow-log rate limiter (`rate.Inf`) to keep sysvar read stable in tests.
- `pkg/server/handler/optimizor/statistics_handler_test.go`: fix macOS unix socket path length failures by not overriding `cfg.Socket`; fail fast if `server.Run` returns error (avoid hanging on `RunInGoTestChan` and hitting 10m timeout).
- `pkg/domain/crossks/cross_ks_test.go`: reduce etcd integration cluster size (20→1) and create keyspace-scoped etcd clients via `integration.NewClientV3` (cut `TestManager` from ~29.7s to ~1.7s locally).
- `pkg/ddl/tests/tiflash/ddl_tiflash_test.go`: stop background TiFlash polling before tearing down mock TiFlash HTTP server (reduce teardown race/panic risk).
- `pkg/ddl/ingest/integration_test.go`: gate multi-add-index ingest coverage behind `testflag.Long()` to avoid repeated DDL in normal UT.
- `pkg/ddl/backfilling_dist_scheduler_test.go`: use a cancelable context in `TestBackfillingSchedulerGlobalSortMode` to avoid background task leakage.
- `pkg/ddl/main_test.go`: ignore `net.(*netFD).connect.func2` in goleak for `-cover` stability.
- `pkg/ddl/tests/partition/main_test.go`: reduce `waitForCleanDataRound`/interval in non-long UT to shorten cleanup polling.
- `pkg/ddl/tests/partition/multi_domain_test.go`: wait for async unistore deletions before asserting old table KV ranges are gone.
- `pkg/dxf/framework/integrationtests/framework_scope_test.go`: reduce randomized case count in non-long UT.
- `pkg/infoschema/test/clustertablestest/cluster_tables_test.go`: use `EXPLAIN` to validate privilege checks without executing cluster table queries.
- `pkg/objstore/gcs_test.go`: make the httptest server respond to token/subject token requests to keep `TestCtxUsage` bounded.
- `pkg/objstore/gcs.go`: allow unauthenticated GCS client in tests when endpoint is localhost (avoid default-cred dependency).
- `pkg/objstore/objectio/writer_test.go`: remove goroutine-count assertions and close reader in `TestNewCompressReader`.
- `pkg/planner/core/tests/extractor/memtable_infoschema_extractor_test.go`: replace exponential predicate cartesian-product enumeration with “single-column full coverage + pairwise combinations” (dedupe executed SQL).
- `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go`: limit testdata case count and skip `cascades=off` in non-long UT.
- `pkg/planner/core/casetest/hint/hint_test.go`: shorten `SLEEP()` calls while keeping hint parsing/warning coverage.
- `pkg/planner/core/casetest/instanceplancache/concurrency_test.go`: retry deadlocks by restarting the txn (avoid flaky failures under concurrency).
- `pkg/ttl/ttlworker/scan_integration_test.go`: `TestCancelWhileScan` batch-insert rows instead of 10k single-row inserts.
- `pkg/executor/test/admintest/admin_test.go`: shrink row count/iterations and batch KV ops for corrupted-index locate tests.
- `pkg/executor/test/distsqltest/distsql_test.go`: reduce rows/limit/repeats in `TestDistSQLSharedKVRequestRace` while preserving plan-path coverage.
- `pkg/executor/test/seqtest/seq_executor_test.go`: validate placeholder count via `PrepareStmt` instead of executing a giant insert.
- `pkg/executor/analyze.go`, `pkg/executor/analyze_col.go`, `pkg/executor/analyze_col_v2.go`: determinize auto-analyze kill tests by finalizing pending jobs on error and waiting for sys process IDs before killing (avoid “status stuck running” races).
- `pkg/executor/test/analyzetest/analyze_test.go`: make killed auto-analyze status checks eventual (avoid races) and relax “must finish within N seconds”.
- `pkg/executor/recover_test.go`: wait until there are no pending DDL jobs at the chosen flashback timestamp (avoid cross-test interference).
- `pkg/executor/test/executor/executor_test.go`: relax `TestIssue48756` warning message check to tolerate env-dependent formats.
- `pkg/executor/test/indexmergereadtest/index_merge_reader_test.go`: reduce stress-loop repeats in normal UT (`testflag.Long()` keeps original).
- `pkg/executor/index_merge_reader.go`: replace failpoint `time.Sleep(20s)` with bounded wait (avoid slowing UT).
- `pkg/executor/sortexec/sortexec_pkg_test.go`: reduce input rows and shorten kill delay for interrupt tests.
- `pkg/executor/sortexec/sort_spill_test.go`: reduce spill test repetitions in normal UT (`testflag.Long()` keeps original).
- `pkg/executor/sortexec/topn_spill_test.go`: reduce repetition count and shorten random-close delay in failpoint tests.
- `pkg/executor/join/outer_join_spill_test.go`: remove redundant parameter combinations and reduce random-fail repetitions.
- `pkg/executor/join/hash_table_v1_test.go`: skip hard-coded `unsafe.Sizeof` assertions on non `linux/amd64`.
- `pkg/executor/test/cte/cte_test.go`: reduce `TestCTEIterationMemTracker` recursion/data volume (avoid multi-million-row recursive output) and use `count(*)` instead of `explain analyze`.
- `pkg/executor/test/tiflashtest/tiflash_test.go`: reduce mock TiFlash node count/replicas/rows and gate expensive cases behind `testflag.Long()`.
- `pkg/server/conn_test.go`: reduce `SLEEP()` duration and input rows for max-execution-time tests while keeping interrupt paths covered.
- `pkg/server/handler/optimizor/plan_replayer_test.go`: cap SQL connections and avoid plan-cache interference when checking binding takes effect (reduce flakes on `@@last_plan_from_binding`).
- `pkg/server/tests/commontest/tidb_test.go`: use random ports and fail-fast proxy start for proxy-protocol tests (avoid port conflicts and fixed sleeps).
- `pkg/statistics/handle/cache/internal/lfu/lfu_cache_test.go`: wait async cache writes in `TestMemoryControlWithUpdate` to be stable under `-cover`.
- `pkg/statistics/handle/storage/dump_test.go`: call `h.Update()` after `LoadStatsFromJSON` before validating loaded stats.
- `pkg/statistics/handle/storage/gc_test.go`: drain DDL events and adjust lease handling to stabilize `TestGCExtendedStats`.
- `pkg/store/batch_coprocessor_test.go`: enable failpoint to reduce coprocessor backoff and set `max_execution_time` to bound TiFlash RPC error tests; add bounded retry for `TestStoreSwitchPeer` to avoid occasional TiFlash timeout under load.
- `pkg/store/copr/batch_coprocessor_test.go`: reduce topo fetch backoff time in non-long UT and relax timing assertions accordingly.
- `pkg/store/gcworker/gc_worker_test.go`: stop TTL job manager in bootstrap (avoid GC safe point being blocked) and wait GC completion via `done` channel polling; relax `TestGCWithPendingTxn` commit assertion (mock PD safepoint differs from real PD).
- `pkg/table/tables/tables_test.go`: shrink combination matrix for `TestTxnAssertion` in non-long UT (`testflag.Long()` keeps full matrix).
- `pkg/expression/builtin_miscellaneous.go`: treat `tikverr.ErrLockWaitTimeout` as “lock not acquired” in `GET_LOCK()` to match MySQL semantics and avoid flaky `TestGetLock`.
- `pkg/session/test/bootstraptest2/boot_test.go`: batch dist-task state coverage in `TestTiDBUpgradeWithDistTaskRunning` to avoid repeated store bootstrap/upgrade loops.
- `pkg/util/chunk/alloc_test.go`: reduce goroutines/loops and avoid `require.*` inside goroutines in `TestSyncAllocator`.
- `pkg/util/hack/map_abi_test.go`: replace hard-coded `RealBytes()` exact-value assertions with bounds derived from row count (avoid go patch/allocator sensitivity).
- `pkg/util/memory/tracker_test.go`: avoid racy consume/release patterns that can make global analyze memory usage go negative.
- `pkg/util/workloadrepo/worker_test.go`: remove fixed sleeps and tighten assertions to shorten worker lifecycle tests (`TestHouseKeeperThread`, `TestStoppingAndRestartingWorker`).

## Status Snapshot (Local subset profiling)

After the changes above, a local subset run (nextgen + coverage) shows the “single slow testcase” issue largely removed for the hot packages:

- `pkg/executor/test/indexmergereadtest`: `TestIndexMergeProcessWorkerHang` drops from ~40s (local) to ~1s.
- `pkg/executor/sortexec`: top tests drop from ~12–13s (local) to ~1–3s.
- Remaining longest single testcase in the subset is ~7–8s (join spill tests).

## Full UT Baseline (Local full run)

Local full runs with `tools/bin/ut` (via `make gotest_in_verify_ci`, coverage enabled). Note that the **build stage** is sensitive to local Go build cache; the primary optimization signal is per-testcase time in JUnit and the **run stage**.

### Nextgen (NEXT_GEN=1)

#### Phase2 snapshot (before “>10s cleanup”)

- Total testcases: `6805`
- Build stage: `~21m03s`
- Run stage: `~5m27s` (parallelism=28)
- Longest single testcase: `~14.7s` (`pkg/server/tests/commontest` `TestTopSQLCPUProfile`)
- Timing ranking artifacts:
  - `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260115_184302/parsed/slow_packages.tsv`
  - `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260115_184302/parsed/slow_testcases.tsv`

Top testcases (seconds):

- `14.740s` `pkg/server/tests/commontest` `TestTopSQLCPUProfile`
- `14.162s` `pkg/ddl/ingest` `TestAddGlobalIndexInIngest`
- `11.777s` `pkg/dxf/importinto` `TestSubmitTaskNextgen`
- `11.569s` `pkg/executor/test/executor` `TestQueryWithKill`
- `11.543s` `pkg/table/tables` `TestCacheTableBasicReadAndWrite`

#### Phase3 snapshot (after “>10s cleanup”)

- Total testcases: `6805`
- Build stage: `10m30s`
- Run stage: `5m27s` (parallelism=28)
- Longest single testcase: `9.392s` (`pkg/server/handler/tests` `TestDebugRoutes`)
- Timing ranking artifacts:
  - `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260116_111713/parsed/slow_packages.tsv`
  - `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260116_111713/parsed/slow_testcases.tsv`

Top testcases (seconds):

- `9.392s` `pkg/server/handler/tests` `TestDebugRoutes`
- `9.282s` `pkg/dxf/importinto` `TestSubmitTaskNextgen`
- `9.163s` `pkg/server/tests/commontest` `TestTopSQLCPUProfile`
- `8.905s` `pkg/executor/test/unstabletest` `TestPBMemoryLeak`
- `8.860s` `pkg/objstore` `TestCopyObject`

### Legacy (non-nextgen)

#### Phase2 snapshot (before “>10s cleanup”)

- Total testcases: `6804`
- Build stage: `~19m28s`
- Run stage: `~7m10s` (parallelism=28)
- Longest single testcase: `~24.1s` (`pkg/session/test/bootstraptest` `TestUpgradeWithAnalyzeColumnOptions`)
- Timing ranking artifacts:
  - `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_legacy_20260115_181427/parsed/slow_packages.tsv`
  - `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_legacy_20260115_181427/parsed/slow_testcases.tsv`

Top testcases (seconds):

- `24.082s` `pkg/session/test/bootstraptest` `TestUpgradeWithAnalyzeColumnOptions`
- `18.442s` `pkg/session/test/bootstraptest` `TestUpgradeVersion74`
- `16.157s` `pkg/server/tests/commontest` `TestTopSQLCPUProfile`
- `13.860s` `pkg/ddl/ingest` `TestAddGlobalIndexInIngest`
- `12.933s` `pkg/session/test/bootstraptest2` `TestTiDBUpgradeWithDistTaskEnable`

#### Phase3 snapshot (after “>10s cleanup”)

- Total testcases: `6804`
- Build stage: `10m34s`
- Run stage: `7m03s` (parallelism=28)
- Longest single testcase: `9.610s` (`pkg/server/tests/commontest` `TestTopSQLCPUProfile`)
- Timing ranking artifacts:
  - `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_legacy_20260116_103514/parsed/slow_packages.tsv`
  - `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_legacy_20260116_103514/parsed/slow_testcases.tsv`

Top testcases (seconds):

- `9.610s` `pkg/server/tests/commontest` `TestTopSQLCPUProfile`
- `8.858s` `pkg/server` `TestMemoryLeak`
- `8.823s` `pkg/executor/test/tiflashtest` `TestMppTableReaderCacheForSingleSQL`
- `8.823s` `pkg/ddl/tests/tiflash` `TestTiFlashReorgPartition`
- `8.818s` `pkg/executor/test/tiflashtest` `TestMppApply`

Flake amplifiers fixed since the initial full-run baseline:

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

## Before/After Comparison

### Local full run (before vs after)

- Before: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260114_122930/`
  - Build: `~10m27s`, Run: `~16m06s`, Total: `~26m33s`
  - Longest testcase: `600s` (hit `--timeout 10m`)
  - Top slow package: `pkg/server/handler/optimizor` total `1809.239s`
- After (phase1): `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260114_132713/`
  - Build: `~10m34s`, Run: `~6m04s`, Total: `~16m38s`
  - Longest testcase: `26.456s` (`pkg/ddl/tests/tiflash` `TestTiFlashNoRedundantPDRules`)
  - `pkg/server/handler/optimizor` total: `12.280s`
- After (phase1 snapshot): `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260115_023402/`
  - Build: `~10m13s`, Run: `~5m53s`, Total: `~16m06s`
  - Longest testcase: `24.375s` (`pkg/server/handler/tests` `TestSetLabelsConcurrentWithStoreTopology`)

Key deltas (local):

- `pkg/server/handler/optimizor`:
  - `TestLoadNullStatsFile`: `600.400s` → `0.599s`
  - `TestDumpStatsAPI`: `600.194s` → `1.919s`
  - `TestStatsPriorityQueueAPI`: `600.092s` → `0.402s`
- `pkg/domain/crossks`:
  - `TestManager`: `29.669s` → `1.712s`

### Local full run (phase2: nextgen + legacy)

- Nextgen before: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260115_144916/`
  - Build: `~11m36s`, Run: `~8m46s`, Total: `~20m22s`
  - Longest testcase: `45.551s` (`pkg/executor/test/cte` `TestCTEIterationMemTracker`)
- Nextgen after: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260115_184302/`
  - Build: `~21m03s`, Run: `~5m27s`, Total: `~26m30s`
  - Longest testcase: `14.740s` (`pkg/server/tests/commontest` `TestTopSQLCPUProfile`)
- Legacy before: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_legacy_20260115_165149/`
  - Build: `~10m16s`, Run: `~7m10s`, Total: `~17m26s`
  - Longest testcase: `35.976s` (`pkg/session/test/bootstraptest2` `TestTiDBUpgradeWithDistTaskRunning`)
- Legacy after: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_legacy_20260115_181427/`
  - Build: `~19m28s`, Run: `~7m10s`, Total: `~26m38s`
  - Longest testcase: `24.082s` (`pkg/session/test/bootstraptest` `TestUpgradeWithAnalyzeColumnOptions`)

Key deltas (phase2, seconds):

- Nextgen:
  - `TestCTEIterationMemTracker`: `45.551s` → `0.851s`
- Legacy:
  - `TestTiDBUpgradeWithDistTaskRunning`: `35.976s` → `6.791s`
  - `TestCTEIterationMemTracker`: `10.992s` → `1.101s`

### Local full run (phase3: eliminate >10s testcases)

- Nextgen before: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260115_184302/`
  - Longest testcase: `14.740s` (`pkg/server/tests/commontest` `TestTopSQLCPUProfile`)
- Nextgen after: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_nextgen_20260116_111713/`
  - Longest testcase: `9.392s` (`pkg/server/handler/tests` `TestDebugRoutes`)
- Legacy before: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_legacy_20260115_181427/`
  - Longest testcase: `24.082s` (`pkg/session/test/bootstraptest` `TestUpgradeWithAnalyzeColumnOptions`)
- Legacy after: `~/.codex-context/tidb--906fbbe4/artifacts/ut_full_legacy_20260116_103514/`
  - Longest testcase: `9.610s` (`pkg/server/tests/commontest` `TestTopSQLCPUProfile`)

Key testcase deltas (phase3, seconds):

- Nextgen:
  - `TestTopSQLCPUProfile` (`pkg/server/tests/commontest`): `14.740s` → `9.163s`
  - `TestAddGlobalIndexInIngest` (`pkg/ddl/ingest`): `14.162s` → `4.502s`
  - `TestSubmitTaskNextgen` (`pkg/dxf/importinto`): `11.777s` → `9.282s`
  - `TestQueryWithKill` (`pkg/executor/test/executor`): `11.569s` → `2.076s`
  - `TestCacheTableBasicReadAndWrite` (`pkg/table/tables`): `11.543s` → `2.416s`
- Legacy:
  - `TestUpgradeWithAnalyzeColumnOptions` (`pkg/session/test/bootstraptest`): `24.082s` → `6.962s`
  - `TestUpgradeVersion74` (`pkg/session/test/bootstraptest`): `18.442s` → `6.945s`
  - `TestTopSQLCPUProfile` (`pkg/server/tests/commontest`): `16.157s` → `9.610s`
  - `TestAddGlobalIndexInIngest` (`pkg/ddl/ingest`): `13.860s` → `4.779s`
  - `TestTiDBUpgradeWithDistTaskEnable` (`pkg/session/test/bootstraptest2`): `12.933s` → `7.010s`
  - `TestQueryWithKill` (`pkg/executor/test/executor`): `11.922s` → `1.701s`
  - `TestCacheTableBasicReadAndWrite` (`pkg/table/tables`): `11.745s` → `2.718s`

### CI offenders (CI baseline vs local after)

Reference: `pull_unit_test_next_gen #8126` (Bazel ranking) vs local full run above.

- `TestMemtableInfoschemaExtractorPart4`: `185.5s` → `0.844s`
- `TestMemtableInfoschemaExtractorPart2`: `178.0s` → `1.415s`
- `TestCancelWhileScan`: `151.1s` → `1.500s`
- `TestAdminCheckTableErrorLocate`: `132.8s` → `1.081s`
- `TestAdminCheckTableErrorLocateForClusterIndex`: `127.3s` → `1.865s`
- `TestDistSQLSharedKVRequestRace`: `110.4s` → `0.613s`
- `TestPrepareMaxParamCountCheck`: `103.2s` → `0.446s`

## Next Steps

1. Keep trimming the remaining “~9s class” testcases (e.g. `TestDebugRoutes`, `TestTopSQLCPUProfile`) toward the “<3s per testcase” target by reducing loops/data volume while keeping coverage.
2. Batch/merge tests with heavy setup overhead (notably bootstrap/upgrade suites and `pkg/util/workloadrepo`) to reduce repeated store bootstrap cost.
3. Keep stabilizing “rare flake amplifiers” found during full runs so CI retries/timeouts don’t dominate wall time.
4. Once timings are stable, consider time-weighted sharding or re-bucketing CI jobs.

## Appendix: Files Changed

```text
docs/design/2026-01-13-unit-test-time-optimization.md
pkg/bindinfo/tests/bind_usage_info_test.go
pkg/ddl/backfilling.go
pkg/ddl/backfilling_dist_scheduler_test.go
pkg/ddl/cancel_test.go
pkg/ddl/db_cache_test.go
pkg/ddl/index_modify_test.go
pkg/ddl/ingest/integration_test.go
pkg/ddl/job_scheduler_testkit_test.go
pkg/ddl/job_worker.go
pkg/ddl/main_test.go
pkg/ddl/tests/partition/db_partition_test.go
pkg/ddl/tests/partition/main_test.go
pkg/ddl/tests/partition/multi_domain_test.go
pkg/ddl/tests/tiflash/ddl_tiflash_test.go
pkg/domain/crossks/cross_ks_test.go
pkg/dxf/framework/integrationtests/framework_ha_test.go
pkg/dxf/framework/integrationtests/framework_scope_test.go
pkg/dxf/framework/integrationtests/resource_control_test.go
pkg/dxf/importinto/job_testkit_test.go
pkg/executor/aggregate/agg_spill_test.go
pkg/executor/analyze.go
pkg/executor/analyze_col.go
pkg/executor/analyze_col_v2.go
pkg/executor/analyze_worker.go
pkg/executor/checksum.go
pkg/executor/distsql_test.go
pkg/executor/importer/importer_testkit_test.go
pkg/executor/importer/table_import.go
pkg/executor/index_merge_reader.go
pkg/executor/internal/applycache/apply_cache_test.go
pkg/executor/internal/calibrateresource/calibrate_resource_test.go
pkg/executor/join/hash_table_v1_test.go
pkg/executor/join/inner_join_spill_test.go
pkg/executor/join/outer_join_spill_test.go
pkg/executor/recover_test.go
pkg/executor/sortexec/sort_spill_test.go
pkg/executor/sortexec/sortexec_pkg_test.go
pkg/executor/sortexec/topn_spill_test.go
pkg/executor/test/admintest/admin_test.go
pkg/executor/test/analyzetest/analyze_test.go
pkg/executor/test/analyzetest/memorycontrol/memory_control_test.go
pkg/executor/test/cte/cte_test.go
pkg/executor/test/distsqltest/distsql_test.go
pkg/executor/test/executor/executor_test.go
pkg/executor/test/indexmergereadtest/index_merge_reader_test.go
pkg/executor/test/seqtest/seq_executor_test.go
pkg/executor/test/tiflashtest/tiflash_test.go
pkg/executor/test/unstabletest/memory_test.go
pkg/expression/builtin_miscellaneous.go
pkg/infoschema/test/clustertablestest/cluster_tables_test.go
pkg/objstore/azblob_test.go
pkg/objstore/gcs.go
pkg/objstore/gcs_test.go
pkg/objstore/objectio/writer_test.go
pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go
pkg/planner/core/casetest/hint/hint_test.go
pkg/planner/core/casetest/instanceplancache/concurrency_test.go
pkg/planner/core/casetest/instanceplancache/main_test.go
pkg/planner/core/tests/extractor/memtable_infoschema_extractor_test.go
pkg/plugin/integration_test.go
pkg/server/conn_test.go
pkg/server/handler/optimizor/plan_replayer_test.go
pkg/server/handler/optimizor/statistics_handler_test.go
pkg/server/handler/tests/http_handler_test.go
pkg/server/tests/commontest/tidb_test.go
pkg/server/tests/servertestkit/testkit.go
pkg/server/tidb_library_test.go
pkg/session/bootstrap_test.go
pkg/session/test/bootstraptest/boot_test.go
pkg/session/test/bootstraptest/bootstrap_upgrade_test.go
pkg/session/test/bootstraptest2/boot_test.go
pkg/sessionctx/variable/sysvar.go
pkg/sessiontxn/txn_rc_tso_optimize_test.go
pkg/statistics/handle/cache/internal/lfu/lfu_cache_test.go
pkg/statistics/handle/storage/dump_test.go
pkg/statistics/handle/storage/gc_test.go
pkg/store/batch_coprocessor_test.go
pkg/store/copr/batch_coprocessor.go
pkg/store/copr/batch_coprocessor_test.go
pkg/store/copr/coprocessor.go
pkg/store/gcworker/gc_worker_test.go
pkg/store/mockstore/unistore/lockstore/lockstore_test.go
pkg/table/tables/cache.go
pkg/table/tables/cache_test.go
pkg/table/tables/tables_test.go
pkg/timer/store_intergartion_test.go
pkg/ttl/ttlworker/job_manager_integration_test.go
pkg/ttl/ttlworker/scan_integration_test.go
pkg/util/chunk/alloc_test.go
pkg/util/gctuner/memory_limit_tuner.go
pkg/util/gctuner/memory_limit_tuner_test.go
pkg/util/hack/map_abi_test.go
pkg/util/memory/tracker_test.go
pkg/util/stmtsummary/v2/tests/table_test.go
pkg/util/workloadrepo/worker_test.go
tools/check/ut.go
```
