# `pkg/statistics/handle/initstats` → `tidb-stats-handle-initstats`

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Rust owner |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 18 | `c708dbdbed8298b626196ccc7811352b686f501b` | workspace member and crate manifest |
| `load_stats.go` | 38 | `111ff66297c31c0a8a414e147bc92c94e7311f84` | `src/lib.rs::get_concurrency` |
| `load_stats_page.go` | 121 | `2b750323cc91bc389bdb717d833518661a532cb5` | `src/lib.rs::{INIT_STATS_PERCENTAGE, Task, RangeWorker}` |

The package has no generated, platform-specific, test, benchmark, or fixture
artifacts.

## Behavior mapping

- `get_concurrency` reads the live global `force_init_stats` configuration,
  derives the count from Rust's effective process parallelism, applies the
  source half-CPU or two-reserved-CPU policy, and clamps to `[2, 16]`.
- `INIT_STATS_PERCENTAGE` is a process-global sequentially consistent atomic
  floating-point value initialized to zero. The `/status` consumer now reads
  and caps this shared value as Go does instead of returning a Rust-only 100.
- `Task` carries the source start/end table IDs.
- `RangeWorker::new` snapshots the current percentage, creates the source
  one-slot bounded channel, retains one shared task callback, and uses the
  source one-minute/first-one sampled statistics logger.
- `load_stats` starts exactly the configured number of workers, including the
  source zero-worker behavior for nonpositive caller input. Workers drain
  until close, log task errors without stopping, count every completed task,
  publish the same floating-point percentage formula, and emit the source
  progress message.
- `send_task` is blocking and fails after channel close. `wait` closes the
  channel, joins every worker, and preserves worker panics.

The former aggregate-crate modules accepted pre-read CPU/config values or
pre-counted progress inputs and omitted all runtime behavior. Their five tests
do not exist in the pinned package. Both modules and all five tests were
removed.

## Validation

WIP profile: the source package has no tests, so validation uses strict package
compilation/linting, its concrete status consumer, and the affected statistics
owner gate.

- `cargo check --locked -p tidb-stats-handle-initstats -p tidb-server`
- `cargo clippy --locked -p tidb-stats-handle-initstats --no-deps -- -D warnings`
- `cargo test --locked -p tidb-server http_status::tests::status_answers_gos_shape_and_other_paths_answer_404 -- --exact`
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- `rustfmt --edition 2021 --check crates/tidb-stats-handle-initstats/src/lib.rs crates/tidb-server/src/http_status.rs crates/tidb-stats/src/lib.rs`
- `git diff --check`
