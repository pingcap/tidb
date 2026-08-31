# `pkg/statistics/handle/syncload` — complete package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 55 | `0aa0a175d3573ce6e6c1e6d562ceb9d980594f3a` | `940ec9c6fa14151361ac498c978eee720965e063e11dd44fccf482e480c84b05` |
| `stats_syncload.go` | 619 | `7e33bce5a591c0aa57ed5379c9551c49fbf23fe2` | `303ad7896847f79c187fbdf35a09437021e442e2ead5a625095883fad54f8106` |
| `stats_syncload_test.go` | 446 | `8b703eb4951d10fea67c81201ca49608e49987d1` | `df7359f9a2f2293adcab236b77d3c40d42671a003e8a718e721de54c59c4da07` |

All 1,120 lines were read. The package has no `doc.go`, generated input or
output, platform/build-tag variant, fixture, benchmark, fuzz target, example,
or package-local support artifact.

## Production behavior and Rust mapping

- `tidb-executor::driver::catalog::sync_load` owns Go's process-global
  per-item singleflight group, configured bounded urgent and expired queues,
  CPU-derived concurrency, one-retry worker loop, lease-derived backoff with
  jitter, panic conversion, queue admission timer, expired-task demotion, and
  item/transport result distinction.
- Each shared `StatisticsCache` owns exactly one `SyncLoadService` and its
  worker pool. Distinct statistics handles now have distinct queues and worker
  groups, as Go's `NewStatsSyncLoad(statsHandle)` requires; only singleflight
  remains process-global.
- Singleflight removes a completed key and broadcasts all buffered results
  under the same group lock, matching `x/sync/singleflight.doCall` ordering.
- Dropping a statistics handle signals and joins all active workers, matching
  domain close plus the worker wait group. An in-progress item finishes before
  shutdown returns.
- `Catalog::{statistics_load_items,request_statistics_load,
  wait_statistics_load}` performs cache filtering, split planner request/wait,
  one shared wait deadline, pseudo-timeout policy, and later asynchronous
  demand for items still absent after a diagnostic load error.
- `ClusterStatisticsItemLoader` reads the live skip-column-type setting,
  validates current schema, creates Go's empty unanalyzed column, opens a
  high-priority fresh snapshot per item, reads metadata and optional full
  histogram/CMS/TopN payload, and publishes through
  `SharedStats::update_item`. Publication preserves held snapshots and refuses
  a metadata-only downgrade over a fully loaded item.
- A task dropped from a full expired queue no longer turns into Rust's
  source-absent `channel closed unexpectedly` error. Go's request goroutine
  retains `task.ResultCh`, so Rust now waits for the original singleflight
  deadline and returns exactly `sync load took too long to return`.

## Pinned Go test disposition

| Go test | Rust evidence |
| --- | --- |
| `TestConcurrentLoadHist` | `concurrent_identical_requests_share_one_load`, catalog split request/wait tests, cluster loader and `stats_watch` publication tests |
| `TestConcurrentLoadHistTimeout` | `dropped_expired_task_waits_for_the_singleflight_timer`, `urgent_queue_preempts_an_expired_task`, catalog pseudo/transport timeout tests |
| `TestConcurrentLoadHistWithPanicAndFail` | `worker_retries_one_failure_or_panic_once` and the shared-listener assertions in `concurrent_identical_requests_share_one_load` |
| `TestRetry` | `worker_retries_one_failure_or_panic_once` and `second_failure_is_reported_as_an_item_result` |
| `TestSendLoadRequestsWaitTooLong` | `full_needed_queue_times_out_admission` plus the exact dropped-expired-task regression |
| `TestSyncLoadOnObjectWhichCanNotFoundInStorage` | `stats_watch::sync_load_installs_go_empty_column_for_known_unanalyzed_metadata`, analyzed missing-object insertion tests, and stale table/column/index catalog loader tests |

The two additional lifecycle regressions prove source contracts not isolated by
the Go package tests: `production_services_own_independent_worker_pools` and
`dropping_a_worker_pool_waits_for_an_active_worker`.

## Corrected non-parity behavior

The previous Rust implementation used one process-global worker pool and
detached its threads on drop. That was a Rust-specific Sysbench policy: the
first-created catalog fixed queue size/concurrency for every later domain and
one domain's backlog delayed another. Go owns queues and workers per
statistics handle and waits for them at domain shutdown. The global pool and
its explanatory policy comment were removed.

The original regression results were preserved:

- FAIL before / PASS after:
  `cargo test --locked -p tidb-executor driver::catalog::sync_load::tests::production_services_own_independent_worker_pools -- --exact --nocapture`
- FAIL before / PASS after:
  `cargo test --locked -p tidb-executor driver::catalog::sync_load::tests::dropping_a_worker_pool_waits_for_an_active_worker -- --exact --nocapture`
- FAIL before / PASS after:
  `cargo test --locked -p tidb-executor driver::catalog::sync_load::tests::dropped_expired_task_waits_for_the_singleflight_timer -- --exact --nocapture`

## Atomic claim status

The executable load/cache behavior and all six original test contracts are
wired, but this package remains **unclaimed**. Its production source increments
and observes `metrics.SyncLoadCounter`, `SyncLoadTimeoutCounter`,
`SyncLoadDedupCounter`, `SyncLoadHistogram`, and `ReadStatsHistogram`. Those
collectors are owned and registered by pinned Go's separate 60-artifact
`pkg/metrics` package, for which Rust has no complete atomic owner. Porting five
private collectors here would fragment that package and change registration
identity. The package can be claimed only after complete `pkg/metrics`
transcreation supplies the shared collectors and these call sites are wired.

## Validation

WIP profile:

- PASS: `cargo test --locked -p tidb-executor driver::catalog::sync_load::tests -- --nocapture` (9/9)
- PASS: each of the three exact fail-before/pass-after regressions listed above
- PASS: `cargo fmt --all -- --check`
- PASS: `git diff --check`

No Go or Bazel file changed, so `make bazel_prepare` is not required. The
integrated batch owns the final package test rerun and Ready-profile gate.
