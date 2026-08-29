# Complete Go-to-Rust statistics package parity

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. The pinned source of truth is Go commit `e2788410d8d696605e8cb002585877a063ccc909`. Repository policy requires one complete upstream Go package, including production sources, variants, tests, fixtures, and validation gates, as the minimum completion unit.

## Purpose / Big Picture

Rust must make the same statistics-loading and planning decisions as the pinned Go implementation. A query should collect the same predicate-column demand at the same logical-rule position, request the same column and index items, apply the same synchronous timeout policy, publish the same loaded state, and expose the same statistics through planning and `SHOW` statements. Rust-only thresholds, narrow cache paths, and behavioral approximations are removed rather than retained as alternatives.

## Progress

- [x] (2026-08-29) Persisted analyze snapshot metadata and matched outdated-statistics pseudo policy.
- [x] (2026-08-29) Added exact secondary-index reads and per-item histogram, bucket, TopN, and CMS loading.
- [x] (2026-08-29) Added logical predicate-column demand collection, including lineage, physical-table visitation, and operator counting.
- [x] (2026-08-29) Wired collection and sync/async requests at Go's logical-rule position, including partition expansion, statement timeout/cache state, interesting-column collection, first-phase access-path pruning, and retained-index filtering.
- [x] (2026-08-29) Split request dispatch from synchronous waiting and added Go's later `SyncWaitStatsLoadPoint` at its pinned logical-rule position.
- [x] (2026-08-29) Replaced eager startup and refresh reads with Go's lite load; preserved resident unchanged payload and fully reloaded only changed resident items.
- [x] (2026-08-29) Corrected wired `SHOW STATS_META` traversal to honor the session partition-prune mode and suppress pseudo cache entries like `GetNonPseudoPhysicalTableStats`.
- [x] (2026-08-29) Wired `SHOW STATS_HEALTHY` to production catalog statistics and made analyzed-row selection ignore metadata-only cache items like Go's `GetAnalyzeRowCount`.
- [x] (2026-08-29) Wired `SHOW STATS_TOPN` through production table/index metadata and the session-aware `ValueToString` equivalent, including hidden-column-capable index type lookup.
- [x] (2026-08-29) Wired `SHOW STATS_BUCKETS` through the same production traversal and decoder, preserving cumulative count, repeat, bounds, and bucket NDV.
- [x] (2026-08-29) Wired `SHOW STATS_HISTOGRAMS` through the shared statistics cache, including initialized-state filtering, average column size, load status, and live histogram/TopN/CMS memory usage.
- [x] (2026-08-29) Wired `SHOW HISTOGRAMS_IN_FLIGHT` to the shared needed-item set and Go's cache-state cleanup semantics.
- [x] (2026-08-29) Completed pinned `pkg/statistics/handle/lockstats` and `pkg/executor/lockstats` behavior across the shared policy, in-process catalog, TiKV internal transaction, session routing, privileges, warnings, errors, and `SHOW STATS_LOCKED`.
- [x] (2026-08-29) Completed pinned `pkg/statistics/asyncload` as the process-wide 128-shard needed-item set, including monotonic full-load upgrades and removal after successful, stale-metadata, or corrupted loads.
- [x] (2026-08-29) Completed pinned `pkg/statistics/handle/syncload`, including per-item global singleflight, configured bounded priority queues and workers, timeout demotion, one retry with lease-derived backoff, panic recovery, live skip-type policy, stale-DDL guards, and the split request/wait contract.
- [x] (2026-08-29) Completed pinned `pkg/statistics/util` and removed the executor's duplicate partial JSON statistics model; dump/load now shares the complete Go object shape, ordering, global marker, and memory accounting from `tidb-stats`.
- [x] (2026-08-29) Completed pinned `pkg/statistics/handle/metrics` as its own Rust crate, preserving every health-bucket index, exclusive bound, compatibility label, shared gauge child, and historical-dump result counter.
- [x] (2026-08-29) Audited and completed pinned `pkg/statistics/handle/logutil`; the existing crate already matched all four logger routes, category fields, shared sampler state, and 5/10-minute sampling policies.
- [ ] Wire all pinned Go `SHOW STATS_*` surfaces to the shared cache and storage semantics.
- [ ] Inventory every production file, platform/generated variant, original test/support artifact, fixture, and validation gate in pinned `pkg/statistics`; close or explicitly retain seed-only gaps until the whole package is complete.
- [ ] Run the Ready validation profile, including `make lint`, only after the complete package inventory is closed.

## Surprises & Discoveries

- Observation: the pinned `CollectPredicateColumnsPoint` is not only a collector. It prunes each `DataSource`'s access paths and uses the retained index IDs to avoid loading statistics for indexes the planner just removed.
  Evidence: `pkg/planner/core/rule/rule_collect_plan_stats.go` calls `pruneIndexesForAllDataSources` before `collectSyncIndices`.

- Observation: synchronous load timeout is statement state, not merely a warning. Go marks sync loading failed, prevents plan-cache admission, appends the original load error when pseudo fallback is enabled, and caps the wait by `max_execution_time`.
  Evidence: pinned `RequestLoadStats` in `pkg/planner/core/rule/rule_collect_plan_stats.go`.

- Observation: Go expands requested table items to physical partition IDs only after column and index demand is combined.
  Evidence: pinned `expandStatsNeededColumnsForStaticPruning` appends one item per `tid2pids` entry.

- Observation: Rust's `PossiblePath` is the exact stage used by this pinned pruning rule: like Go before `fillIndexPath`, it has index metadata but no `FullIdxCols` or `IsSingleScan`. Consequently the pinned fallback scoring branch applies and consecutive-prefix/single-scan bonuses are unreachable at this point.
  Evidence: pinned `scoreIndexPath` branches on nil `FullIdxCols`; Rust `access_path::PossiblePath` is documented and populated as the newborn access path.

- Observation: pinned Go dispatches statistics work at `CollectPredicateColumnsPoint` but waits only at the later `SyncWaitStatsLoadPoint`; Rust had combined those phases and therefore blocked intervening logical rules.
  Evidence: pinned `SendLoadRequests` stores statement wait state and returns, while `SyncWaitStatsLoad` consumes that state in the later rule.

- Observation: Go's periodic cache update is not equivalent to replacing the cache with a fresh lite image. A row-count-only `stats_meta.version` change preserves resident histogram payload, while a newer histogram is reloaded in full only when the old item was resident.
  Evidence: pinned `StatsCacheImpl.Update` reuses `MetaOnly` when `LastStatsHistVersion` has not moved, and `TableStatsFromStorage(..., loadAll=false)` branches on each old item's load state and histogram version.

- Observation: the pre-existing Rust `SHOW STATS_META` path hard-coded dynamic pruning and exposed pseudo cache entries, although pinned Go branches on `IsDynamicPartitionPruneEnabled` and calls `GetNonPseudoPhysicalTableStats`.
  Evidence: the regression first returned the empty analyzed table plus the partitioned table's global row under static mode; pinned behavior returns only the two physical partitions.

- Observation: Rust had the `SHOW STATS_HEALTHY` parser and an isolated helper, but no session dispatch, and its reduced `analyze_row_count` selected the first item without checking full-load status.
  Evidence: the end-to-end regression initially failed as unsupported; pinned `GetAnalyzeRowCount` sorts items and returns only the first fully loaded column or index.

- Observation: Rust's TopN row helper retained Go's untyped byte type-code seam even though the production decoder accepts `FieldTypeCode`; no execution path called the helper.
  Evidence: wiring `SHOW STATS_TOPN` required replacing that isolated byte slice with the decoder's native type slice and resolving index offsets against all table columns, including hidden expression-index columns.

- Observation: a one-value Analyze v2 result has no bucket under the default TopN size because the value lives entirely in TopN; pinned bucket tests explicitly use `WITH 0 TOPN` when they require histogram rows.
  Evidence: the initial end-to-end bucket assertion returned no rows; with the pinned setup it produced `count=1`, `repeat=1`, equal bounds, and `ndv=0`.

- Observation: Rust already aggregated column/index memory like Go, but histogram bytes were a caller-injected field that every production construction boundary discarded.
  Evidence: the full `Column`/`Index` types had `MemoryUsage`, while cluster-loaded and in-process planner statistics retained only the histogram, TopN, and CMS payloads. Moving measurement onto `Histogram` makes all ordinary construction paths report the resident payload without a SHOW-specific cache.

- Observation: `SHOW HISTOGRAMS_IN_FLIGHT` is not a thread counter. Go sweeps its global needed-item set against current cache load state and counts only entries that still require loading.
  Evidence: pinned `CleanFakeItemsForShowHistInFlights` deletes missing tables and already-satisfied column/index items before returning the count.

- Observation: statistics locking is an internal statistics-handle transaction, not part of the caller's user transaction. Its executor first checks INSERT and SELECT on every target, and warnings are statement-local.
  Evidence: pinned `LockExec`/`UnlockExec` call the domain statistics handle, while `statsLockImpl` uses `util.CallWithSCtx(..., util.FlagWrapTxn)`; pinned plan building records INSERT then SELECT visit-info for each target.

- Observation: pinned table-level unlock deliberately skips a target whose logical table row is unlocked, even if a partition row remains locked; partition unlock also refuses every partition while the logical table is locked.
  Evidence: pinned `RemoveLockedTables` continues before visiting partitions when the logical ID is absent, and `RemoveLockedPartitions` returns the whole-table warning before its partition loop.

- Observation: pinned asynchronous-load demand is one process-wide, 128-shard map, not a session or catalog-local queue. Re-inserting an item may upgrade metadata-only demand to full load but never downgrade it.
  Evidence: the complete pinned `pkg/statistics/asyncload/async_load.go` owns the exported singleton; its shard key is the absolute column/index ID modulo 128 and each shard has its own RW mutex.

- Observation: pinned synchronous loading has two independent time boundaries: each singleflight operation times queue admission plus worker response, while the later planner wait starts one common statement timer over all returned result channels. A loader error is an item result and is logged, whereas only the outer common wait error triggers pseudo fallback.
  Evidence: pinned `SendLoadRequests` creates its timer inside `singleflight.DoChan`; pinned `SyncWaitStatsLoad` creates a separate timer and distinguishes `singleflight.Result.Err` from `stmtctx.StatsLoadResult.Error`.

- Observation: zero-wait statistics demand is not processed by the synchronous worker pool. It remains in the process-global asynchronous map until the domain refresh tick invokes `storage.LoadNeededHistograms`.
  Evidence: pinned `CollectPredicateColumnsPoint` inserts directly into `asyncload.AsyncLoadHistogramNeededItems` when `syncWait == 0`; the domain `loadStatsWorker` drains it separately.

- Observation: pinned `pkg/statistics/util` owns the full statistics JSON object model, while Rust had split it into a predicate-only `tidb-stats` type and a separate executor-only load type that omitted FM sketch, usage timestamps, and historical state.
  Evidence: pinned `json_objects.go` defines `JSONTable`, `JSONColumn`, and `JSONPredicateColumn` together; the executor consumes that package rather than declaring a second representation.

- Observation: pinned handle metrics do not define a second health calculation. They only fix bucket identities/configuration and bind child handles to the shared `tidb_statistics_stats_healthy` and `tidb_statistics_historical_stats` metric families.
  Evidence: pinned `pkg/statistics/handle/metrics/metrics.go` contains only constants, `HealthyBucketConfigs`, three metric variables, and `InitMetricsVars`; bucket population remains in `pkg/statistics/handle/cache`.

## Decision Log

- Decision: reconstruct and test each pinned Go branch before editing Rust; do not preserve Rust-only fallback paths.
  Rationale: the requested acceptance condition is behavioral parity, and local convenience policies are explicitly out of scope.
  Date/Author: 2026-08-29 / Codex

- Decision: keep statistics storage shared across sessions while making request/wait outcome statement-local.
  Rationale: this matches Go's domain statistics handle plus statement context split.
  Date/Author: 2026-08-29 / Codex

- Decision: do not claim `pkg/statistics` complete from individual files or functions.
  Rationale: repository policy makes the whole pinned Go package the atomic completion unit.
  Date/Author: 2026-08-29 / Codex

- Decision: make histogram memory an owned histogram operation rather than carrying a manually supplied measurement beside it.
  Rationale: Go computes `Histogram.MemoryUsage` from the live object. Rust's bounds representation differs, so its method measures the equivalent live ownership—histogram value, reserved bucket storage, and variable bound payloads—while preserving Go's empty-histogram zero and component aggregation behavior.
  Date/Author: 2026-08-29 / Codex

- Decision: implement lockstats policy once over a narrow transaction interface, with separate catalog and TiKV adapters.
  Rationale: pinned Go centralizes branching, warnings, and delta merging in `pkg/statistics/handle/lockstats` while its restricted SQL session is only the storage boundary. Sharing the policy prevents the two Rust runtime modes from diverging without inventing a Go-visible path.
  Date/Author: 2026-08-29 / Codex

## Outcomes & Retrospective

The exact storage item reader, lite bootstrap/refresh lifecycle, logical demand collector, split request/wait rule positions, partition expansion, newborn access-path pruning, and synchronous worker concurrency/retry behavior are integrated. The current milestone is not complete: remaining `SHOW STATS_*` surfaces and the whole-package inventory remain.

## Context and Orientation

`rust/crates/tidb-planner/src/logical/rule_collect_plan_stats.rs` corresponds to Go's logical statistics-usage collector and rule. `rust/crates/tidb-executor/src/driver/catalog.rs` owns the session-visible catalog and planner statistics cache. `rust/crates/tidb-exec/src/cluster_stats_load.rs` reads individual statistics objects from TiDB system tables. `rust/crates/tidb-server/src/cluster_session_node/mod.rs` connects production cluster snapshots to the executor catalog. `rust/crates/tidb-session/src/stmt_ctx.rs` snapshots system variables into `tidb_executor::StmtContext`.

A full load includes histogram payload needed for estimation; a metadata load preserves existence/load-state information without fetching all buckets and TopN data. Static partition pruning plans against physical partition table IDs, so a logical table request must be copied to those IDs. Plan-cache admission must be denied after sync-load fallback because a pseudo-derived plan must not be reused as though loaded statistics produced it.

## Plan of Work

First, finish the logical rule as one behavior: collect usage, mark a determinate full item per analyzed table, prune access paths, collect retained index demand, expand to physical partitions, request async or sync loading, and record Go's statement outcome. Keep planner-only traversal in `tidb-planner`; pass only the resulting item demand across the executor boundary.

Second, change startup and periodic refresh to load Go's lite table state. Prove a later item request adds exactly the requested payload and atomically republishes the planner view.

Third, map every pinned Go statistics `SHOW` producer and filter to Rust. Remove placeholder documentation and ignored/stub tests only after their behavior is either implemented or shown absent in the pinned package.

Finally, build a complete pinned-package inventory and run the required validation gates. A file/function match is evidence, not a package completion claim.

## Concrete Steps

Run commands from `rust/` unless stated otherwise:

    cargo check --locked -p tidb-planner -p tidb-executor -p tidb-session -p tidb-server
    cargo test --locked -p tidb-planner rule_collect_plan_stats::tests --lib
    cargo test --locked -p tidb-executor <focused_statistics_test>
    cargo fmt -p tidb-planner -p tidb-executor -p tidb-session -p tidb-server -- --check

From repository root, use `git diff --check` after every slice. Before a completion claim, follow `.agents/skills/tidb-verify-profile/SKILL.md` Ready profile and run `make lint` because code changed.

## Validation and Acceptance

Focused tests must prove metadata versus full demand, determinate first-column selection, exclusion of already-full and pruned indexes, virtual generated-column dependency discovery, static partition expansion, async publication, sync success, sync error, timeout fallback, wait capping, and plan-cache refusal. Production cluster tests must prove a fresh snapshot is used per requested item and the shared cache publishes the loaded object.

Package acceptance additionally requires a complete inventory of pinned `pkg/statistics` and its required tests/fixtures. No package-complete statement is permitted while an inventory row is absent or marked incomplete.

## Idempotence and Recovery

Checks and focused tests are safe to rerun. Statistics workers publish immutable table snapshots into shared locks, so a retry may replace a table with an equivalent or newer view. Do not reset or discard unrelated working-tree changes. If a test reveals a mismatch, return to the pinned Go function and update this plan's discovery or decision log before changing behavior.

## Artifacts and Notes

Pushed evidence includes commits `e0d5b4c1f0` (individual item loading), `00179562d2` (logical demand collection), `cb732f4908` (logical request integration), and `9cd431bd02` (newborn access-path pruning). The current wait-point slice compiles and has focused tests proving request dispatch and waiting occur at their separate pinned-Go rule positions, including timeout fallback only at the wait point.

    cargo check --locked -p tidb-session -p tidb-server

## Interfaces and Dependencies

`tidb_planner::logical::rule_collect_plan_stats::StatisticsLoadRequester` is the rule-to-runtime request interface. `tidb_executor::driver::StatisticsItemLoader` is the runtime-to-storage interface. `Catalog::request_statistics_load` owns session cache publication and wait semantics. `ClusterStatisticsItemLoader` owns production snapshot acquisition and `SharedStats` publication. These interfaces must remain narrow enough that the planner does not depend on cluster storage types, while their observable behavior remains identical to pinned Go.

Revision note (2026-08-29): created the living plan after completing exact item loading and logical usage collection; recorded the three parity gaps found while reviewing the first request-seam implementation.

Revision note (2026-08-29): completed the pinned first-phase interesting-column/access-path pruning behavior over Rust's newborn path representation and recorded why Go's later path-growth scoring fields are unreachable at this phase.

Revision note (2026-08-29): moved synchronous waiting out of initial request dispatch and registered `SyncWaitStatsLoadPoint` at the pinned later logical-rule position.

Revision note (2026-08-29): made production bootstrap and refresh consume the existing lite table loader, removed the unused snapshot wrapper, and added Go's resident-payload-preserving update behavior.

Revision note (2026-08-29): fixed the existing `SHOW STATS_META` production path before expanding the SHOW family; the new regression was observed failing with pseudo/global rows before the fix and passing afterward.

Revision note (2026-08-29): wired `SHOW STATS_HEALTHY` through the production cache, reused the session prune-mode traversal, and aligned analyzed-row selection with full-load status.

Revision note (2026-08-29): wired `SHOW STATS_TOPN` and verified the pinned integration result for a repeated column and its secondary index; the unique column/index correctly contributes no TopN rows.

Revision note (2026-08-29): wired `SHOW STATS_BUCKETS` and verified the pinned single-value row under `WITH 0 TOPN`.

Revision note (2026-08-29): wired `SHOW STATS_HISTOGRAMS`, removed caller-injected histogram-memory fields, and verified initialized column/index rows plus memory-component totals through the SQL path.

Revision note (2026-08-29): added the shared needed-item lifecycle and wired `SHOW HISTOGRAMS_IN_FLIGHT`; delayed-load coverage observes one live item and then zero after worker cleanup.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/handle/lockstats` (`lock_stats.go`, `query_lock.go`, `unlock_stats.go`; all four original test/support files; `BUILD.bazel`) and `pkg/executor/lockstats` (both executors, executor tests, `BUILD.bazel`). The Rust mapping is `tidb-stats::lock_stats` for policy/query/delta behavior, `tidb-executor::stats_lock` plus `tidb-session::stats_lock_arm` for the in-process restricted-session equivalent, and `tidb-exec::{cluster_stats_lock,real_tikv_stats_lock}` plus the server seam for one independent TiKV transaction. Focused tests cover stable skip messages, table/partition gates, delta propagation and clamping, real persisted system-row mutations, duplicate no-write warnings, INSERT-before-SELECT privilege admission, statement warning publication, and preservation of the caller's transaction.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/asyncload` (`async_load.go`, `async_load_test.go`, `BUILD.bazel`). `tidb-stats::async_load` now owns the exact process-global 128-shard map and all four map operations; the executor's request, completion, and `SHOW HISTOGRAMS_IN_FLIGHT` cleanup paths use that singleton. The five original integration cases collapse onto two Rust integration boundaries: missing table/column/index metadata makes the cluster item loader skip publication and the worker always deletes the item, while a corrupted payload returns an error and the same unconditional deletion applies. Focused tests cover concurrent shards, full-load upgrade/no-downgrade, successful completion cleanup, and corrupted-load cleanup.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/handle/syncload` (`stats_syncload.go`, `stats_syncload_test.go`, `BUILD.bazel`). The Rust mapping is `tidb-executor::driver::catalog::sync_load` for singleflight, queues, workers, retry, panic recovery, and result transport; `Catalog::{request_statistics_load,wait_statistics_load,load_needed_histograms}` for the planner/domain lifecycle; and `ClusterStatisticsItemLoader` for fresh per-item storage snapshots, live `tidb_analyze_skip_column_types`, stale metadata checks, and shared-cache publication. Focused tests cover deduplication, both retry causes, terminal item errors, bounded admission timeout, urgent-task precedence, split synchronous request/wait semantics, explicit asynchronous draining, corrupted-item cleanup, and stale-column cleanup.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/util` (`json_objects.go`, `BUILD.bazel`; no package-local Go tests or fixtures). `tidb-stats::json_metadata` now owns the complete table, column/index, histogram, CMS, FM, and predicate-usage JSON shapes, `TiDBGlobalStats`, predicate ordering, and `TotalMemoryUsage`; `tidb-executor::load_stats` imports that shared model instead of retaining a second partial definition. Focused tests cover stable sorting/global identity, component protobuf-size summation, and the existing load-stat JSON conversion path.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/handle/metrics` (`metrics.go`, `BUILD.bazel`; no package-local Go tests or fixtures). New crate `tidb-stats-handle-metrics` owns the exact ten bucket indices/configs and binds ordered gauge children plus the `dump/success` and `dump/fail` counters to the correctly named and labeled shared Prometheus families. Focused tests cover the complete config sequence, compatibility labels, child count/order, rebinding, gauge writes, and distinct historical-result counters. Health-bucket population remains assigned to the separate pinned cache package, matching Go ownership.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/handle/logutil` (`logutil.go`, `BUILD.bazel`; no package-local Go tests or fixtures). Existing crate `tidb-stats-handle-logutil` already mapped `StatsLogger`, `StatsErrVerboseLogger`, `StatsSampleLogger`, and `StatsErrVerboseSampleLogger` onto the pinned background/error-verbose bases, `category=stats`, `sampled=""`, one shared sampler per factory, five-minute ordinary sampling, and ten-minute verbose-error sampling. A focused composition test now proves all four routes and first-only suppression through the emitted log contract.
