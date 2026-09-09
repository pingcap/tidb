# `pkg/statistics/handle/autoanalyze/refresher` package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 53 | `fa1451885086b9849d399bee65be0d9887725f82` |
| `main_test.go` | 34 | `5dda135affd0a7a422495d1fcde9101def90ac0d` |
| `refresher.go` | 278 | `d36b515fc727325bd415dd8024e404c426d65169` |
| `refresher_test.go` | 608 | `f46abbfc21d626c76e3391e07a36cb00f46dd085` |
| `worker.go` | 152 | `3bb89627bdd00b7aaccbea7776e1dc2d9f622b6b` |
| `worker_test.go` | 180 | `40c4c87e28c21961d6c38cce9685c8e31f06b30d` |

All 1,305 lines were read. The package has 12 top-level assertion tests plus
six `TestWorker` subtests, one shared leak-checking `TestMain`, and no
benchmark.

## Go behavior

`Refresher` owns the live priority queue and worker. It registers the real DDL
notifier, reads current session parameters, initializes or rebuilds the queue,
does so even outside the execution window, updates concurrency, skips already
running or invalid jobs, and submits highest-priority jobs. The worker
synchronizes running identities and concurrency, executes concrete jobs with
the statistics handle/process tracker, recovers panics, waits, and stops.

Tests validate enable/disable notifier progress, outside-window initialization,
prune-mode rebuilds, ratio zero, nil/pseudo/tiny filtering, ordered and
concurrent real ANALYZE effects, deleted-table retry behavior, recent-failure
backoff, worker admission/update/snapshots, and panic cleanup.

## Rust mapping and completion decision

The obsolete scalar leaves described by the original audit remain removed.
The package is now implemented by the dedicated
`tidb-stats-handle-autoanalyze-refresher` crate and its production server
integration:

| Go owner | Rust owner |
| --- | --- |
| `Refresher`, remembered ratio/prune mode, time window, queue lifecycle | `tidb_stats_handle_autoanalyze_refresher::Refresher` |
| `worker`, concurrency updates, running-table snapshot, admission, wait, panic cleanup | `tidb_stats_handle_autoanalyze_refresher::Worker` |
| concrete `AnalysisJob.Analyze` | `priorityqueue::RunningAnalysisJob` and the production `ClusterPriorityQueueSource` |
| Domain construction and DDL notifier registration | `ClusterSessionFactory::{auto_analyze_refresher,notify_auto_analyze_priority_queue}` over the same queue instance |
| owner/config tick and shutdown | `AutoAnalyzeWorker`, `handle_auto_analyze_tick`, and `ClusterSessionFactory::drop` |
| latest statistics session variables | `RefreshParameters` plus the shared statistics-session reset used by concrete jobs |

Rust serializes the non-thread-safe refresher with the factory mutex, just as
Go's statistics handle owns one refresher. DDL delivery is synchronous at the
cluster DDL commit boundary instead of passing through Go's durable notifier
table; it calls the same queue event operation and preserves the observable
initialization gate, deletion/recreation, and disabled-queue behavior. This is
the server integration decision, not a second queue implementation.

## Original test mapping

The package-local crate tests cover every `TestWorker` subtest (construction,
concurrency update, capacity rejection, defensive running snapshot, one and
multiple panics) plus `Stop`/wait. Refresher tests cover initialization before
the time-window check, ratio/prune-mode rebuild, invalid window, close, and
empty queue behavior.

The original store-backed cases are consolidated across the owning queue and
server boundaries:

- `source_factory_matches_ratio_index_version_and_partition_rules`,
  `source_queue_static_lock_and_system_view_filters`, and
  `source_validation_reasons_and_retry_flags_match_go` cover ratio zero,
  nil/pseudo/tiny statistics, static/dynamic pruning, deleted tables, and
  recent-failure backoff;
- priority-calculator/heap and worker tests cover ordered and concurrent
  admission;
- `auto_analyze_priority_queue_uses_shared_stats_ddl_and_ordinary_analyze_path`
  executes the production queue, DDL mutation, disable/close/reinitialize
  cycle, concrete ANALYZE, persisted job, and statistics publication;
- `failed_analysis_duration_resets_the_pooled_session_timezone` exercises the
  same persisted failed-analysis query through the reused statistics session.

`main_test.go` contributes only Go's package-level goleak wrapper; Rust has no
equivalent package test hook. `BUILD.bazel` maps to the crate and server Cargo
targets. There are no fixtures, generated inputs, platform variants, or
benchmarks. All six pinned artifacts are accounted for, so this package is
complete at the pinned commit.

## Validation

- `cargo test -p tidb-stats-handle-autoanalyze-refresher`
- `cargo test -p tidb-server auto_analyze_priority_queue_uses_shared_stats_ddl_and_ordinary_analyze_path -- --nocapture`
- `cargo test -p tidb-server failed_analysis_duration_resets_the_pooled_session_timezone -- --nocapture`
- `cargo check -p tidb-server`
- `cargo fmt --all -- --check`
- `git diff --check`

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Follow-up closure — discardable refresher returns (2026-09-06)

The complete six-artifact, 1,305-line Go package was re-read at current
`origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; all production,
test, harness, and BUILD files remain byte-identical to the pinned source. The
single-file Rust owner and its complete test module were reviewed before
editing.

Go permits callers to discard `NewWorker`, `worker.GetRunningJobs`,
`worker.GetMaxConcurrency`, `NewRefresher`, `Refresher.GetRunningJobs`,
`Refresher.IsQueueInitializedForTest`, and `Refresher.Len` results. Rust had
marked all seven direct counterparts `#[must_use]`, imposing Rust-only return
diagnostics. The annotations were removed without changing queue state,
concurrency admission, running-job snapshots, or worker/refresher lifecycle.
`tests::go_refresher_query_returns_can_be_ignored` exercises all seven APIs
under `#[deny(unused_must_use)]`; with the annotations present it failed with
exactly seven diagnostics, and after the edit it passes.

Ready validation for this Rust-only follow-up:

```text
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-autoanalyze-refresher --lib go_refresher_query_returns_can_be_ignored -- --test-threads=1
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-autoanalyze-refresher --lib --test-threads=1
PASS; 6/6 owner tests passed.

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-autoanalyze-refresher --all-targets
PASS.

rustfmt +nightly-2026-08-22 --check --edition 2021 rust/crates/tidb-stats-handle-autoanalyze-refresher/src/lib.rs
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

Only Rust source/tests and parity documentation changed. No Go, Bazel, Cargo
dependency, or module file changed, so `make bazel_prepare` was not required.
