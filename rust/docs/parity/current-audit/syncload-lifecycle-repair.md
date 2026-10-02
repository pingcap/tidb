# Statistics synchronous-load lifecycle repair

Reviewed 2026-10-02 from integration
`2e66b7c28f60ab2083559222735496c0596f1b4a`, freshly fetched Go master
`93a01d31f6da205ae4bf376825293903a6899fdb` and native client/dependency
`19a56ccda1e128218cd33c69709038219aced9bc`. Integration was already current;
the native repository's remote master matches the maintained dependency.

**O19's missing event observations are repaired. There are now 69 unresolved
findings (61 open, eight partial) and 17 repaired, out of 86 tracked.** The other
69 dispositions carry forward the [previous full-register review](mdl-mode-review.md);
this follow-up does not claim a new exhaustive review or full package acceptance.

## Source boundary and design

The [complete upstream package inventory](syncload-review-recheck/package.json)
covers all three artifacts in `pkg/statistics/handle/syncload`: production,
seven original tests and BUILD.bazel. There are no package-local generated,
platform, fixture or documentation variants. Its dependencies and original
Go race gate remain explicit; no Go package acceptance is granted by this repair.
The shared collector definitions are in master `pkg/metrics/stats.go`.

Go master has a seventh regression,
`TestSyncWaitStatsLoadWithFailedResultBeforeTimer`, and a corresponding failure
branch absent from the integration branch's Go working tree. The old Rust test
asserted that outdated success behavior. Master, not that working tree, is the
reference for this repair.

`tidb-executor` now consumes the existing `tidb-stats-handle-metrics` collectors,
which the server already registers. Cargo updated the existing workspace edge
in Cargo.lock; there is no external dependency or native-client revision change.

| Event | Go observation point | Rust owner |
| --- | --- | --- |
| Distinct task admitted | `SendLoadRequests`, after the queue send inside singleflight | `SyncLoadService::run_flight`; no increment for joined callers or rejected admission |
| Result consumed, closed channel or wait timer | `SyncWaitStatsLoad` receive/timer cases | `Catalog::wait_statistics_load`, one total increment per consumed outcome |
| Wait timeout or undelivered identities | timer branch or master's final incomplete-result branch | Same wait owner; timeout counter increments once and common statement policy handles either route |
| All requested identities delivered | successful final result-set check | `PendingStatisticsLoad.started` through final wait; integer milliseconds include the interval before waiting |
| Successful actual storage read | `handleOneItemTaskWithSCtx`, after `readStatsForOneItem` succeeds and before cache update | `ClusterStatisticsItemLoader::load_items`, after `Some` metadata/payload; skipped objects, missing histograms and failed attempts have no read observation |

Worker errors are diagnostic delivered results, as in Go. They remove their item
from the outstanding set and permit a completed-wait observation. Transport
errors do not deliver an identity: after the channels drain, the statement is
marked failed and either returns master's error or uses its configured pseudo
fallback, warning and plan-cache exclusion. The duplicated timer-only fallback
and the obsolete silent-success path are removed. Closed-channel failures use
the same planner policy as other failures. Pending state is consumed once.

## Regression and original-test coverage

Five changed/new regressions failed before production edits and passed after:
failed results buffered before waiting, delivered worker errors, wait timeout /
closed channel observations, shared-task admission observations, and actual
storage-read observations. [Before results](syncload-review-recheck/regressions-before.json)
retain exact commands and failures. Prepared channels establish timer ordering;
the dedup test holds the first task until the second caller joins. No global
metric reset is used. Monotone metric assertions tolerate concurrent unrelated
observations; task/result assertions separately verify lifecycle behavior.

| Original Go test contract | Existing Rust validation surface |
| --- | --- |
| `TestConcurrentLoadHist` | determinate request/wait and storage-backed `ddl_after_loaded_statistics_matches_go`, including payload eviction/reload and peer visibility |
| `TestConcurrentLoadHistTimeout` | statement timeout/error policy and expired-task tests |
| `TestConcurrentLoadHistWithPanicAndFail` | shared-task test plus worker panic/error retry controls |
| `TestRetry` | one retry and second failure delivered as an item |
| `TestSendLoadRequestsWaitTooLong` | full needed queue and dropped expired task keep the request timer |
| `TestSyncWaitStatsLoadWithFailedResultBeforeTimer` | deterministic buffered transport result, both pseudo-timeout policies |
| `TestSyncLoadOnObjectWhichCanNotFoundInStorage` | table/column/index disappearance tests and storage-backed reload after ADD COLUMN |

These are maintenance regression surfaces, not a claim that every original Go
test, imported support artifact and dependency has been atomically accepted.
The broader statistics/metrics package work remains tracked by its ExecPlan.

## Validation and remaining failures

The [validation record](syncload-review-recheck/validation.json) lists exact
commands. Scoped results: **29 passed, three baseline failures**. Worker tests,
the storage-backed server regression and shared metric tests pass. Affected
all-target checking, root `GOTOOLCHAIN=go1.25.14 make lint` and `git diff --check`
pass. No Go/Bazel inputs changed, so `make bazel_prepare` was not required.

The [baseline control](syncload-review-recheck/baseline.json) restored unchanged
production from the starting integration commit and reproduced all three:

- `original_collect_histogram_needed_columns_cases_match`: a correlated ANY
  query marks the outer histogram as a full load where the fixture expects a
  metadata load. The same failure also occurred before production edits.
- `subset_index_cardinality_after_async_statistics_load`: IndexReader estimate
  1.44 rather than fixture 16.00.
- `builtin_in_estimate_survives_statistics_initialization`: pseudo table/selection
  estimates 10000/80 rather than fixture 10/1.

Those expectations were not weakened. The task edits were restored after the
baseline controls and final executor/all-target checks rerun. Complete Go race
tests, live TiKV/mixed-node behavior, benchmark throughput and exhaustive
statistics parity were not validated locally. No performance improvement is
claimed; the new observation overhead matches the source lifecycle.

Publication uses the actual pre-commit hook's locked server build, followed by
a fresh `cd rust && cargo build --locked -p tidb-server` immediately before push.
The task thread records the resulting commit and remote verification.
