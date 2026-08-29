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
- [x] (2026-08-29) Completed pinned `pkg/statistics/handle/usage/collector` and `pkg/statistics/handle/usage/indexusage`, including close-aware synchronous delivery and the complete node/session/statement aggregation behavior.
- [x] (2026-08-29) Removed obsolete ignored predicate-collection gap carriers after wiring the real logical rule, retaining the pinned system-schema exclusion as an executable production-unit regression.
- [x] (2026-08-29) Completed pinned `pkg/statistics/handle/cache/metrics`, preserving all six operation labels and both gauge labels on the shared TiDB statistics metric families.
- [x] (2026-08-29) Completed pinned `pkg/statistics/handle/cache/internal/lfu` with buffered TinyLFU admission, cost eviction, the 256-shard secondary table set, metadata-only eviction, dynamic capacity, and wait barriers.
- [x] (2026-08-29) Audited and completed pinned `pkg/statistics/handle/cache/internal/mapcache`, including replacement cost accounting, shared table values, shallow table-copy semantics, and no-op capacity/lifecycle controls.
- [x] (2026-08-29) Audited and completed pinned `pkg/statistics/handle/cache/internal/testutil`, including every table constructor and append helper used by the parent cache and LFU test inventories.
- [x] (2026-08-29) Completed pinned `pkg/statistics/handle/initstats`, preserving concurrency selection, atomic progress, sampled logging, bounded task admission, and multi-worker range processing.
- [x] (2026-08-29) Audited pinned `pkg/statistics/handle/internal`; its sole test-support assertion helper already preserves the complete Go equality contract.
- [x] (2026-08-29) Audited pinned `pkg/statistics/handle/util/test`; its context matcher preserves the exact internal stats foreground request-source contract, panic boundary, and diagnostic text.
- [x] (2026-08-29) Completed pinned `pkg/statistics/handle/util`, including restricted-session variable synchronization, transaction/error/panic lifecycle, request source, worker/session pools, table metadata lookup, lease ownership, auto-analyze process tracking, and all original tests.
- [x] (2026-08-29) Ported the pinned parent cache's full-table core: global LFU/map selection, runtime quota backing, hit/miss/update/delete accounting, lifecycle max version, copy-on-write update, in-place update, capacity, eviction, wait, and close behavior.
- [x] (2026-08-29) Ported the parent cache's atomic wrapper, ordered metadata refresh, batched publication, cancellation, targeted-update version policy, health gauges, and histogram-version-aware reuse; made the cluster storage image construct the canonical full `statistics.Table` before deriving the planner view.
- [x] (2026-08-29) Ported `StatsTableRowCache`: atomic two-query map updates, negative column-size clamping, fixed/variable column accounting, local/global partition-index rules, sequence row-count policy, and exact ID predicate construction.
- [x] (2026-08-29) Wired the row cache to the cluster statistics snapshot and removed the hard-coded-zero `information_schema.tables` divergence for analyzed tables.
- [x] (2026-08-29) Removed the live cache's reduced `ClusterTableStats` authority: bootstrap, refresh, sync load, planner derivation, and row-cache reads now share canonical full `statistics.Table` objects like Go.
- [x] (2026-08-29) Wired cluster `LOAD STATS` through Go's text-protocol client-local-file transfer, independent restricted TiKV transactions, optional history writes, final metadata publication, and the common shared-cache refresh path.
- [x] (2026-08-29) Wired `SHOW COLUMN_STATS_USAGE` to a fresh shared-storage snapshot, including session-location timestamps and logical/global plus all-partition traversal in both prune modes.
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

- Observation: pinned `pkg/statistics/handle/types` is an interface-only closure package whose composite `StatsHandle` embeds concrete cache, usage, history, analyze, storage, global-stats, lock, and DDL owners. A standalone Rust umbrella crate before those owners are complete would duplicate the existing narrow integration seams without adding Go behavior.
  Evidence: the complete pinned `interfaces.go` contains declarations only; `StatsHandle` embeds the owner interfaces at the end of the file.

- Observation: Go's high-priority collector send observes `closeCh` and returns `false` after close. Rust previously enqueued after close, so index-usage `Flush` could report success to a worker that no longer existed or block on a full abandoned queue.
  Evidence: pinned `sessionCollector.SendDeltaSync` selects between `highPriorityDataCh` and `closeCh`; the regression `synchronous_send_stops_after_close` failed before the close-state check and passes after it.

- Observation: two ignored planner integration files still described `CollectColumnStatsUsage` and its logical-rule carrier as unported after both had been wired. Keeping those empty tests made the test inventory contradict production behavior.
  Evidence: both files contained only ignored empty functions; `logical::rule_collect_plan_stats` now owns the collector, rule positions, partition expansion, and request/wait tests.

- Observation: the Rust `StatsCacheInner` and map backend were not shareable across threads, although Go publishes one cache to every session. The LFU backend was thread-safe, but the common trait could not form the parent cache's process-wide ownership boundary.
  Evidence: `StatsCacheInner` lacked `Send + Sync` and `MapCache` stored its map in `RefCell`; a compile-time shared-cache boundary rejected both before the parent package could be wired.

- Observation: Rust registered `tidb_stats_cache_mem_quota` but did not publish its validated global value to a process-wide backing store, so Go's cache constructor could not observe runtime quota changes.

- Observation: the cluster loader constructed the reduced planner statistics directly from storage rows, bypassing Go's full cached `statistics.Table` ownership boundary. That lost the existence map, load-state payload, eviction cost, schema update timestamp, and histogram-refresh version as one coherent object.
  Evidence: `ClusterTableStats` fed `TableStatistics` directly; it now first produces a canonical `tidb_stats::Table`, and the planner view is derived from that table.

- Observation: Rust's `information_schema.tables` documentation explicitly fixed statistics sizes at the never-analyzed zero fallback, while pinned Go refreshes a separate process-wide `StatsTableRowCache` only when a requested projection needs its four size columns.
  Evidence: pinned `infoschema_reader.go` calls `UpdateByID` for table and partition IDs, including the logical table for global-index size, before calling `EstimateDataLength`.
  Evidence: the session global publisher updated the two ANALYZE atomics only; the new `vardef::STATS_CACHE_MEM_QUOTA` is populated from the same resolved global image before `StatsCache::new` reads it.

- Observation: the live Rust cache still published decoded `ClusterTableStats` storage rows after the parent cache crate had been taught to own full tables. That made sync loading replace storage DTOs and forced every planner/catalog consumer to reconstruct another full table.
  Evidence: pinned `StatsCacheImpl.Update` and `statsSyncLoad.updateCachedItem` both publish `*statistics.Table`; `TableStatsState::Loaded` now carries that same canonical Rust object and `ClusterTableStats` remains only at storage decode/write boundaries.

- Observation: Rust collapsed `LastAnalyzeVersion` and `LastStatsHistVersion` into the one `stats_meta.last_stats_histograms_version` value. Go initializes the former from the analyze snapshot and analyzed item versions, but uses the latter only as the independent table-level histogram refresh marker.
  Evidence: pinned bootstrap initializes both from `snapshot` and then advances them independently; pinned cache refresh assigns `LastStatsHistVersion` from the metadata row while changing `LastAnalyzeVersion` only from analyzed histograms or the zero-value snapshot fallback.

- Observation: the initial Rust `initstats` worker wrapped a single-consumer standard channel receiver in a mutex. Go's buffered channel permits every worker goroutine to wait independently, while the wrapper admitted only one waiting Rust receiver at a time.
  Evidence: pinned `RangeWorker.LoadStats` starts `concurrency` goroutines ranging directly over one channel; Rust now uses a bounded multi-consumer channel with the same capacity of one.

- Observation: Rust's load-stat JSON path had no equivalent of storage's gzip block framing, so `pkg/statistics/handle/history` could not persist or reconstruct the same `mysql.stats_history.stats_data` rows.
  Evidence: pinned `JSONTableToBlocks` marshals once, gzip-compresses once, and slices the compressed byte stream; `BlocksToJSONTable` concatenates ordered blocks before one gzip decode and JSON unmarshal.

- Observation: Rust's LOAD STATS converter returned only the planner's reduced statistics map and walked only visible columns. Pinned Go returns a canonical full `statistics.Table`, walks every `TableInfo.Columns` entry (including hidden expression-index columns), retains FM sketches and full-load state, and populates the column/index existence map.
  Evidence: pinned `TableStatsFromJSON` constructs `statistics.Column`/`statistics.Index` objects in a new `HistColl`; Rust now constructs the same full table first, and both in-process LOAD STATS and cluster-loaded statistics derive their planner view through one shared converter.

- Observation: pinned LOAD STATS does not use the ANALYZE replacement policy. It commits each named column/index independently, leaves unmentioned histograms intact, clears only that item's prior TopN/buckets/FM rows, persists v1 CMSketch bytes, then performs a final meta upsert that preserves columns it does not name.
  Evidence: `loadStatsFromJSON` calls the restricted-session `SaveColOrIdxStatsToStorage` wrapper once per object and `SaveMetaToStorage` last; the Rust cluster writer now has separate item and final-meta plans, with regressions proving unrelated payload retention, stale-tail removal, CMS round-trip, per-item snapshot reset, and final-meta snapshot preservation.

- Observation: LOAD STATS data belongs to the MySQL connection, not the server filesystem or a cache-only executor. Go's text `handleStmt` parks a request, sends `0xfb + path`, concatenates client packets through the empty terminator, and resumes the same statement. The pinned prepared path does not call `handleFileTransInConn`, so adding a prepared-only transfer would exceed Go behavior.
  Evidence: pinned `clientConn.getDataFromPath`, `handleStmt`, and `handleStmtExecute`; the Rust wire regression proves the text request/data/terminator/OK sequence.

- Observation: the Rust statistics worker pool exposed an `is_closed` observer solely for its own tests. Pinned `handle/util` returns the external `gp.Pool`, whose API has `Go` and `Close` but no closed-state accessor.
  Evidence: pinned `pool.go` and pinned external module `github.com/tiancaiamao/gp` at `4025bc8a4d4a`; tests now verify post-close submission behavior directly.

- Observation: the existing Rust `SHOW COLUMN_STATS_USAGE` code was only two disconnected row helpers. Pinned Go reads the complete persisted usage map through the statistics handle on every SHOW, then visits the logical table ID and every partition ID regardless of prune mode.
  Evidence: pinned `pkg/executor/show_stats.go::fetchShowColumnStatsUsage`; the production cluster session now installs a storage-backed provider and the SQL regression exercises the global and partition rows under static pruning.

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

- Decision: make `tidb-stats-handle-cache` own full `tidb_stats::Table` objects and keep reduced planner statistics as a derived consumer view.
  Rationale: pinned Go caches `statistics.Table`; wrapping the executor's reduced planner view would lose eviction payload, existence-map, histogram-version, and refresh semantics and create a second source of truth.
  Date/Author: 2026-08-29 / Codex

- Decision: keep LOAD STATS on the ordinary text/prepared connection path and use a dedicated restricted statistics writer underneath it.
  Rationale: reading the path with `std::fs` would replace Go's client-local-file behavior, while reusing ANALYZE's whole-table transaction would change item independence, history, and preservation of unmentioned histograms.
  Date/Author: 2026-08-29 / Codex

## Outcomes & Retrospective

The exact storage item reader, lite bootstrap/refresh lifecycle, logical demand collector, split request/wait rule positions, partition expansion, newborn access-path pruning, synchronous worker concurrency/retry behavior, several SHOW surfaces, and cluster LOAD STATS path are integrated. The current milestone is not complete: remaining SHOW and whole-package inventory work stays open.

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

Revision note (2026-08-29): added the complete predicate-column storage primitives (load all/table, replacement save, cleanup-and-get planning), reused them from LOAD STATS, and wired the SHOW consumer through a fresh cluster snapshot. The parent usage package's ANALYZE PREDICATE COLUMNS consumer remains an explicit integration gap, so no package-complete claim is made yet.

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

Revision note (2026-08-29): inventoried all 537 lines of pinned `pkg/statistics/handle/types/interfaces.go` plus `BUILD.bazel`. This remains a closure receipt rather than a new Rust crate: its declarations compose concrete packages that must be completed first, and equivalent Rust behavior is intentionally integrated through narrow owner-specific seams. Completed the concrete dependency packages `pkg/statistics/handle/usage/collector` (`collector.go`, `collector_test.go`, `BUILD.bazel`) and `pkg/statistics/handle/usage/indexusage` (`collector.go`, `collector_test.go`, `BUILD.bazel`). The Rust crates preserve the two ten-entry channels, five-minute priority escalation, close/drain lifecycle, node/session/statement aggregation, exact percentage buckets, modulo counters, latest-use timestamp, metadata GC, duplicate-query suppression, and benchmark support. A new regression proves the previously missing close branch of synchronous delivery.

Revision note (2026-08-29): removed the ignored empty `rule_collect_column_stats_usage_source` and `collect_column_stats_usage_skip_system_tables_source` legacy gap carriers. The real collector is already exercised in its production module; a new executable test pins Go's `mysql.*` exclusion directly on that implementation instead of retaining documentation that says the feature is unported.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/handle/cache/metrics` (`metrics.go`, `BUILD.bazel`; no package-local Go tests or fixtures). New crate `tidb-stats-handle-cache-metrics` binds `miss`, `hit`, `update`, `del`, `evict`, and `reject` counter children plus the `track` and `capacity` gauges to the exact `tidb_statistics_stats_cache_op` and `tidb_statistics_stats_cache_val` families. Focused tests cover independent children, gauge identity, and `InitMetricsVars` rebinding.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/handle/cache/internal/lfu` (`key_set.go`, `key_set_shard.go`, `lfu_cache.go`, `lfu_cache_test.go`, `BUILD.bazel`). New crate `tidb-stats-handle-cache-internal-lfu` uses the Ristretto-compatible Stretto TinyLFU implementation with transparent integer keys, Go's counter/capacity/buffer settings, production internal-cost accounting, all three callback routes, 256 secondary shards, full-to-metadata eviction copies, cost tracking, dynamic capacity, close-once semantics, shared `Copy`, and buffered-write barriers. Focused tests consolidate all ten original Go cases across put/get/delete, replacement accounting, oversized rejection, length/value retention, concurrent access, metadata eviction, capacity reduction, and shared-copy behavior. Live selection remains assigned to the parent cache package and is the next integration slice.

Revision note (2026-08-29): completed the atomic inventories for pinned `pkg/statistics/handle/cache/internal/mapcache` (`map_cache.go`, `BUILD.bazel`; no package-local Go tests or fixtures) and `pkg/statistics/handle/cache/internal/testutil` (`testutil.go`, `BUILD.bazel`; support library only). Existing crates `tidb-stats-handle-cache-internal-mapcache` and `tidb-stats-handle-cache-internal-testutil` cover every production/support symbol. Map replacement, deletion, memory cost, unordered keys/values, shared-value copy, and no-op controls match Go; the lock is solely the Rust ownership mechanism required by the common shared cache. Test tables preserve full-load construction and optional CMS/TopN/histogram components, while append helpers add the next numeric ID with only the Go CMS payload. Focused Rust tests cover the map's production behaviors; the support package has no independent upstream test gate.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/handle/util/test` (`ctx_matcher.go`, `BUILD.bazel`; test-support package with no package-local tests or fixtures). Existing crate `tidb-stats-handle-util-test` checks the actual TiKV trace request source against `internal_{stats_foreground}`, panics on a non-context input like Go's direct type assertion, and exposes the exact matcher description through `Display`. No production integration is required because pinned visibility and use are test-only.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/handle/util` (`auto_analyze_proc_id_generator.go`, `lease_getter.go`, `pool.go`, `table_info.go`, `util.go`, `util_test.go`, and `BUILD.bazel`; no fixtures, generated inputs, or platform variants). Existing crate `tidb-stats-handle-util` preserves every exported behavior and the pinned external `gp.Pool` size/recycle/close contract. The audit removed the Rust-only public closed-state observer and converted its tests to Go-observable post-close submission. Focused tests cover generator/tracker ordering, lease atomics, process-set semantics, worker and session pool ownership, V1/V2 partition lookup/cache invalidation, source-order partial session-variable updates, timezone synchronization, restricted SQL context/options, transaction commit/rollback/panic behavior, request timestamp composition, and special global indexes, including all four original Go integration cases through native seams.

Revision note (2026-08-29): wired cluster LOAD STATS end to end. The connection now advertises and enforces `CLIENT_LOCAL_FILES`, transfers text-protocol payloads with Go's packet sequencing, parses empty/null inputs as no-ops, executes one restricted TiKV transaction per named item plus usage and final metadata transactions, emits best-effort history under the pinned gates, and refreshes the same process-wide full-table cache used by ANALYZE while propagating LOAD STATS refresh failures. A first prepared-transfer draft was removed after direct inspection showed that pinned `handleStmtExecute` does not enter `handleFileTransInConn`.

Revision note (2026-08-29): corrected the common `cache/internal` ownership boundary before parent-cache integration. `StatsCacheInner` is now `Send + Sync`, and `mapcache` uses a poisoned-lock-tolerant `RwLock` instead of `RefCell`, preserving Go's shared-read behavior and COW copy independence. Focused tests cover cost replacement, deletion, copy isolation, and concurrent reads; the LFU suite proves the stronger trait does not change its behavior.

Revision note (2026-08-29): ported the pinned parent cache's `StatsCacheImpl`, `Update`, batch-update, healthy-distribution, and version-offset behavior. Refresh now preserves resident payload only when the latest histogram version permits it, deletes removed physical IDs, skips per-table load failures, observes cancellation before each row, publishes ten-row batches, and prevents targeted refreshes from advancing the lifecycle maximum. The production cluster storage image now converts to the full shared `tidb_stats::Table`, and the reduced planner statistics are derived from that object. Live `StatsSnapshot` ownership still needs consolidation onto this cache, and `stats_table_row_cache.go` remains outstanding, so the parent Go package is not yet claimed complete.

Revision note (2026-08-29): ported the complete pinned `stats_table_row_cache.go` behavior into the parent cache crate. Focused tests cover all-or-nothing two-read updates, negative-size clamping, missing-key zeros, exact ID SQL predicate text, fixed and variable columns, partition row aggregation, and the split between table-level global indexes and partition-level local indexes. The cache's executor-facing restricted-read adapter and `information_schema` consumption remain integration work, so this source completion does not yet close the parent package claim.

Revision note (2026-08-29): replaced LOAD STATS' reduced JSON conversion with pinned `storage.TableStatsFromJSON`'s canonical full-table shape, including hidden columns, FM sketches, physical IDs, full-load state, stats-version propagation, handle metadata, and the existence map. Removed the server's duplicate full-table-to-planner conversion and routed both Go `TableInfo` and executor `KvTable` metadata through one shared derived planner view. Cluster persistence, history rows, predicate-column usage, and the remaining storage package files are still outstanding, so this is explicit seed evidence rather than a package completion claim.

Revision note (2026-08-29): added the cluster mutation planners for pinned LOAD STATS' per-object `SaveColOrIdxStatsToStorage` and final `SaveMetaToStorage` policies instead of reusing ANALYZE's whole-table replacement. The production statement route, predicate-column usage writes, historical-meta recording, and cache refresh remain to be connected; this writer slice is not a storage-package completion claim.

Revision note (2026-08-29): completed the row-cache consumer path. A cluster catalog image now refreshes the process-wide cache from the same loaded `stats_meta` and column-histogram image, includes logical and physical partition IDs, computes the four Go size values from the original `TableInfo`, and carries them into `information_schema.tables`. The production regression observes `TABLE_ROWS=3000065`, `AVG_ROW_LENGTH=24`, `DATA_LENGTH=72001560`, and `INDEX_LENGTH=0` instead of the previous documented zeros. Parent-package closure still depends on replacing the parallel `StatsSnapshot` full-table ownership path with `StatsCacheImpl`.

Revision note (2026-08-29): replaced `StatsSnapshot`'s decoded storage-row payload with canonical `tidb_stats::Table` objects. Lite bootstrap converts once at the storage boundary; periodic refresh copies the canonical column/index maps and preserves or reloads resident payload according to pinned `TableStatsFromStorage`; sync load uses `ColumnMapWritable`/`IndexMapWritable`; planner and `information_schema` consumers read the same table object. The remaining parent-cache closure is to make the live owner itself `StatsCacheImpl` rather than the snapshot publisher.

Revision note (2026-08-29): made `StatsCacheImpl` the live table authority. Bootstrap, periodic refresh, deletion, and sync load now publish through the parent cache; the statement-facing snapshot indexes the exact cache-owned `Arc<Table>` objects and no longer mutates an independent table image. Cache construction errors propagate through node startup like pinned `NewHandle`, and focused tests prove pointer identity, deletion, held-reader immutability, and sync-load replacement. The parent-cache source inventory still requires final integration review before a package-complete claim.

Revision note (2026-08-29): continued the parent-cache integration audit and restored the pinned `StatsCacheGetNil` failpoint through the server feature graph. Corrected the storage boundary to carry `LastAnalyzeVersion` and `LastStatsHistVersion` separately; bootstrap, refresh of an existing table, refresh of a missing table, and analyze-produced tables now follow their distinct pinned update rules. Regressions use deliberately different snapshot, analyzed-histogram, and histogram-refresh versions so a future collapse cannot pass accidentally.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/handle/cache` (`statscache.go`, `statscacheinner.go`, `stats_table_row_cache.go`, `statscache_test.go`, `bench_test.go`, and `BUILD.bazel`). The parent cache is the live Rust statistics authority, all production readers share its immutable table objects, the failpoint is reachable through the server feature graph, and the six original benchmark shapes are represented by a compiling benchmark target. Focused tests cover cache selection and updates, refresh and deletion, row-cache SQL and aggregation, live-owner identity, version-field separation, and feature-gated failpoint behavior. This closes only the parent cache package; the enclosing pinned `pkg/statistics` inventory remains incomplete.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/handle/initstats` (`load_stats.go`, `load_stats_page.go`, and `BUILD.bazel`; no package-local Go tests, fixtures, variants, or generated inputs). `tidb-stats-handle-initstats` owns Go's force/default CPU formulas and `[2,16]` clamp, process-global atomic percentage, one-minute/one-message statistics sampler, one-slot task channel, worker lifecycle, error logging, and completion progress formula. Replacing the mutex-wrapped standard receiver with a bounded multi-consumer channel restores Go's independent receiver waiters without adding another execution path. The caller-side four-phase bootstrap remains part of the parent `pkg/statistics/handle` inventory rather than this leaf package.

Revision note (2026-08-29): completed the atomic inventory for pinned `pkg/statistics/handle/internal` (`testutil.go` and `BUILD.bazel`; no package-local Go tests, fixtures, variants, or generated inputs). Existing `tidb-stats-handle-internal::assert_table_equal` preserves realtime/modify counts, column/index cardinality and presence, Go `HistogramEqual(..., false)` through the same textual form, CMS equality, nil-or-empty TopN equality and ordered entries, and the complete column/index existence maps. No new code or tests were added because the pinned package contains no other behavior or original test artifact.

Revision note (2026-08-29): added pinned `SaveColumnStatsUsageToStorage` seed evidence for LOAD STATS. One planned transaction REPLACEs each `(table_id,column_id)` row, parses dump timestamps as UTC TIMESTAMP(6), converts them to the cluster table's declared precision, and preserves explicit NULL replacement. System-row reconciliation now supplies a timezone when a full system-table row contains TIMESTAMP columns. `TableStatsFromJSON` also consumes one schema-only metadata contract, allowing the forthcoming cluster route to use canonical `model.TableInfo` without constructing an executor table or duplicating JSON conversion. The production statement route, historical-meta recording, transaction execution, and cache refresh remain outstanding, so the storage package is not claimed complete.

Revision note (2026-08-29): lowered LOAD STATS against the cluster's canonical `model.TableInfo`, including the pinned nil-versus-present partition rule, selective partition-name matching, logical `global` target, all schema columns/indexes, signed counts, and stable column-before-index item order. Corrected two edge mismatches found by reading the pinned code: negative per-item counts now update only existing version markers instead of REPLACE-resetting meta, and nullable `[]*JSONPredicateColumn` entries are represented and skipped during persistence. Transaction execution, historical-meta recording, cache refresh, and server routing remain outstanding; this is still storage-package seed evidence.

Revision note (2026-08-29): added the real-TiKV LOAD STATS storage coordinator with pinned per-column, per-index, predicate-usage, and final-meta transaction boundaries. Historical meta uses a later best-effort transaction, requires the exact current stats version, preserves Go's `enforce=false` initialized-cache gate for items and `enforce=true` behavior for final meta, and writes source `load stats`. Shared-cache refresh and server/file-transfer routing remain outstanding, so this remains explicit storage-package seed evidence rather than a package-complete claim.
