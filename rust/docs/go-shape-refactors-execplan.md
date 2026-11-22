# Close the 2026-11-22 re-audit rows: R01–R04 and X02

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the repository root; this plan must be maintained according to it.

## Purpose / Big Picture

The file-level re-audit of 2026-11-22 (`rust/docs/parity/current-audit/structural-audit-2026-11-22.md`) left five rows in the register (`structural-findings.md`). After this plan a session that sets `tidb_cost_model_version = 1` gets Go's version-1 costs and plans (R01), `EXPLAIN EXPLORE <sql>` answers Go's binding exploration (R02), ANALYZE runs through Go's executor shape with the planner and global-stats parts in their own owners (R03), sorting runs Go's parallel workers and multi-way merge (R04), and the DDL code has one port of Go's `sess.Session` (X02).

## Progress

- [x] (2026-11-22) Re-audit: 11 behavior gaps found and fixed (pushed `ac1e5b40a18`); X01's duplicate implementations collapsed (`55f9417f16b`, `346f2fff28a`).
- [x] (2026-11-23) R01: cost model version 1 -- `coster/ver1.rs` holds Go's `getPlanCostVer14<Operator>` bodies, `CostEnv::ver1` carries the version-1 factors when `tidb_cost_model_version = 1`, and the pushed-down / point-plan Selections no longer claim Go's `FromDataSource`; 79 probe statements (pseudo and analyzed statistics, both versions) match a Go server.
- [x] (2026-11-23) R04: `SortExec` runs Go's parallel mode (`parallel_sort.rs`: `ExecutorConcurrency` workers over a bounded chunk channel, batch sorts of `maxChunkSize*30` rows merged per worker, `parallelSortSpillAction` / `parallelSortSpillHelper` spilling the merged worker runs, the in-memory result spilling when the action fires mid-merge); the unparallel mode stays Go's test hook. TopN's spill workers were already Go's `topn_worker.go` (`topn.rs`); the crate docs that said otherwise are fixed and `TestKillSignalInTopN` carries its Go name. Go's `TestParallelSort`, `TestFailpoint`, `TestParallelSortSpillDisk`, `TestParallelSortSpillDiskFailpoint`, `TestIssue59655`, `TestIssue63216`, `TestIssue55344` are ported (with `util/chunk`'s `ChunkInDiskError` failpoint and Go's package-level `rand.Int31n`).
- [x] (2026-11-23) R02: EXPLAIN EXPLORE. `tidb-bindinfo` ports `binding_auto.go` (historical candidates from `mysql.bind_info` and the statement stats, `getBindingPlanDigest`, `runToGetExecInfo`, `fillRecommendation`), `binding_plan_generation.go` (start state, `adjustVar`/`adjustFix`, the breadth-first search, leading-2 / index / `NO_DECORRELATE` states, the AST extractors) and `binding_plan_evolution.go` (rule-based and LLM predictors); the planner records Go's 39 `RecordRelevantOptVar`/`Fix` sites through `tidb_variable::relevant_opt`; the executor answers `GenBriefPlanWithSCtx`, `CalculatePlanDigest` and `RecordRelevantOptVarsAndFixes`; the binding handle's sessions run the exploration; `EXPLAIN EXPLORE REPLAYER` loads the replayer archive first. The five `TestPlanGeneration` queries match a Go server row for row (plans, digests, hints). Found on the way: Go's table scans without ranges are not full ranges, a limit prices its children without `isChildOfINL`, and an index-lookup cop task's table side is priced without it (all three now as Go); R05 records the remaining cop-task comparison gap.
- [x] (2026-11-23) R05: Go's per-operator cost cache. `BasePhysicalPlan::plan_cost_ver2` is a `PlanCostVer2Cache` (`PlanCostInit` + `PlanCostVer2`) that every pricing writes and every non-recalculating pricing reads, so a cop task priced by `getTaskPlanCost` keeps its cost inside the reader built from it; the post-optimize recording pass is gone (`physicalOptimize`'s `getPlanCost` fills the rest).
- [ ] R03: ANALYZE in Go's shape.
- [x] (2026-11-23) X02: one port of Go's `sess.Session`. The `StepSqlSession` trait is gone: the DDL pools hand out `tidb_ddl_session::DdlSession` over the node's `ClusterStatsSessionContext` (a step session is `new_step`, whose drop commits into and releases the step's buffer), `getJobsBySQL` and `updateDDLJob2Table` have one body each (`ddl_job_scheduler.rs`), `initDDLReorgHandle` / `cleanDDLReorgHandles` / the ingest checkpoint use `Session.RunInTxn`, every `Execute` carries Go's metric label, `tidb_ddl_session::Error` keeps the error it wraps (Go's `errors.Trace`), and the session context's `ExecuteInternal` is the session's own (request source, cancellation, goroutine labels restored).

## Surprises & Discoveries

- R05 (first half, 2026-11-23): Go's `compareTaskCost` prices both tasks before checking validity, so the first candidate of every data-source search is priced (and records its relevant variables); this planner short-circuited on an invalid best task, so a single cop candidate was never priced. Fixed; the second half (the per-operator cost cache) closed R05.
- R02: Go's injected LEADING hint carries `Tables` but no `LeadingList`, so the join reorder never applies it (only `LeadingJoinOrder` is set); the Rust port injects the same table-list shape. The `binding_hint` column is `ParseHintsSet`'s normalized set (query-block names added), and explore rows render in plan order (`FlattenPhysicalPlan(p, false)`), not build side first.
- X02: the node's DDL-pool session context answered `ExecuteInternal` through the plain `Execute` path, so internal SQL did not restore the caller's goroutine labels (Go's `ExecuteInternal` defers `SetGoroutineLabels`); two label tests caught it once the DDL sessions moved onto it.
- R04: the fetcher runs on the thread that calls `Next` (Go's fetcher goroutine runs while `Next` blocks on `resultChannel`), and a failed worker keeps draining the chunk channel: a worker that stopped draining left the fetcher waiting on its in-flight chunk forever (the mutant hangs `TestFailpoint`).
- R01: the planner marked every pushed-down and point-plan Selection `FromDataSource`; Go marks only the root Selection over `RootTaskConds`. Version 2 never reads the flag, so it was invisible until version 1 priced the Selection.

- The 11-18 audit matched Go packages to Rust crates by name, so whole Go files inside a matched package could be missing (`select_into.go` was: `SELECT ... INTO OUTFILE` answered rows and wrote no file).
- A per-file symbol-coverage score over-reports: most low-scoring files are ported under other names or decompositions. Every candidate was read before registering.
- Two gate retry-passes were Go-inherited races widened by load (the submitter's DDL ticker reading the history row before `cleanupDDLReorgHandles`; the schema validator's 8027 when a pool's reload misses a lease); the tests now wait for, or retry, what Go does not guarantee.

## Decision Log

- Decision: `ALTER TABLE ... PARTITION BY SYSTEM_TIME` stays refused. Rationale: Go keeps the old partitioning and its reorganization strands a `PARTITION BY NONE` intermediate partition, a Go bug. Date: 2026-11-22.
- Decision: the rows are named R01–R04 so they do not collide with the earlier, closed S01–S05. Date: 2026-11-22.

## Outcomes & Retrospective

(At completion.)

## Context and Orientation

R01: Go `pkg/planner/core/plan_cost_ver2.go` (`getPlanCost` picks version 2 or `GetPlanCostVer1`), `plan_cost_ver1.go` (one `getPlanCostVer14<Operator>` per operator and the `getCost4<Operator>` helpers), the version-1 factors (`tidb_opt_cpu_factor`, `copcpu`, `network`, `scan`, `desc_scan`, `seek`, `memory`, `disk`, `concurrency`). Rust prices every task with `tidb-planner/src/find_best_task/coster.rs` (`Ver2Coster`, a `TaskCoster`) over the formulas in `plan_cost_ver2.rs`; `CostEnv` carries only the version-2 factors.

R04: Go `pkg/executor/sortexec/sort.go` (`Open` sets `IsUnparallel = false`), `parallel_sort_worker.go`, `parallel_sort_spill_helper.go`, `multi_way_merge.go`, `topn_worker.go`. Rust `tidb-executor-sortexec/src/lib.rs` is Go's unparallel mode; the parallel HashAgg in `tidb-executor-aggregate/src/agg_hash_executor.rs` shows the thread and channel pattern to follow.

R02: Go `pkg/bindinfo/binding_auto.go`, `binding_plan_generation.go`, and the EXPLAIN EXPLORE executor.

R03: Go `pkg/executor/analyze*.go`, `pkg/planner/core/planbuilder.go` (`buildAnalyze*`), `pkg/statistics/handle/globalstats`. Rust `tidb-executor/src/real_tikv_analyze.rs` (3,055 lines mixing the three), `analyze.rs`, `cluster_analyze.rs`, `cluster_analyze/sampling.rs`.

X02: Go `pkg/ddl/session` (`Session`). Rust `tidb-ddl-session` (`Session`) and `tidb-ddl-build/src/session_pool.rs` (`StepSqlSession`).

## Plan of Work

R01: plumb the version-1 factors into `CostEnv`; add a version-1 coster next to `Ver2Coster` that walks the same enum with Go's per-operator version-1 bodies; select it where Go's `getPlanCost` does. R04: add Go's parallel mode to `SortExec` (workers over a chunk channel, local sort and merge, the spill helper, the final multi-way merge), then TopN's workers. R02, R03 and X02 follow, each as its own series.

R03 (ANALYZE), in Go's three layers, each step gated on its own:

1. Planner. `Plan::Analyze` carries Go's `core.Analyze` (`ColTasks`, `IdxTasks`, `Opts`, `OptionsMap`, with `AnalyzeInfo` / `V2AnalyzeOptions`), built by `plan_builder/analyze.rs` from `planbuilder.go`'s analyze section (`buildAnalyze`, `buildAnalyzeTable`, `buildAnalyzeFullSamplingTask`, `genV2AnalyzeOptions`, `getSavedAnalyzeOpts`, `getFullAnalyzeColumnsInfo`, `getMustAnalyzedColumns`, `filterSkipColumnTypes`, `getModifiedIndexesInfoForAnalyze`, `BuildHandleColsForAnalyze`, `generateIndexTasks`, `handleAnalyzeOptions`). It runs over the session's InfoSchema at compile time. `SessionPlanContext` gives it what Go reads through `b.ctx`: the restricted SQL executor (`mysql.analyze_options`, predicate columns) and the stats handle's version checks. This replaces `resolve_cluster_analyze_statement`, which plans at execution time from a fresh TiKV snapshot.
2. Executor builder. `buildAnalyze` -> `AnalyzeExec { tasks }`: `buildAnalyzeSamplingPushdown` (`StatsMetaCountAndModifyCount`, `getAdjustedSampleRate` and its NOTE, the `AnalyzeReq`, job info) and `buildAnalyzeIndexPushdown`.
3. Executor. `analyze.rs` gets `AnalyzeExec.Next`: `filterAndCollectTasks`, `buildStatsConcurrency` workers (`analyzeWorker`), `handleResultsError` with `analyzeSaveStatsWorker`s at `AnalyzePartitionConcurrency` (a failed save keeps the others saving; errors join), `handleGlobalStats`, `saveAnalyzeOptions`, `statsHandle.Update`. `analyze_col.rs` / `analyze_col_sampling.rs` get `AnalyzeColumnsExec` (`readDataAndSendTask`, `subMergeWorker`, `subBuildWorker`, `handleNDVForSpecialIndexes`), `analyze_idx.rs` `AnalyzeIndexExec`, plus `analyze_global_stats.rs`, `analyze_utils.rs` and `analyze_worker.rs`. The coprocessor transport, `RowSampleCollector`, the histogram builders and `save_analyze_result_to_storage` are reused.
4. Delete `real_tikv_analyze.rs`, `cluster_analyze*.rs`, `analyze/kv.rs` and the `AnalyzeRun` row path. Port Go's analyze tests under their names.

## Concrete Steps

Per row in `rust/`: `cargo check -p <crates> --tests`, the crates' suites under nextest, then `zsh scripts/gate-scoped.sh <worktree> tidb-server` from `tidb-wt` after a local commit, and a push to both remotes.

## Validation and Acceptance

R01: with `tidb_cost_model_version = 1`, `EXPLAIN FORMAT='verbose'` costs and plan shapes match a Go server on the same statements (probe set in the audit document). R04: Go's `sortexec` tests (parallel sort, its spill and its random-failure tests) pass. R02: Go's EXPLAIN EXPLORE tests. R03: Go's analyze tests keep passing with the code in Go's files. X02: `getJobsBySQL` has one body.

## Idempotence and Recovery

Each step is its own commit; a failing gate is fixed or the commit reverted before the next step.
