# `pkg/statistics/handle/initstats` → `tidb-stats-handle-initstats`

Historical pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.
Current Go source rechecked at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

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

## Follow-up: discardable init-stats returns (2026-09-06)

The complete three-artifact, 159-line Go package was re-read at current
`origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723` and remains
byte-identical to the historical pin. It contains 38-line `load_stats.go`,
121-line `load_stats_page.go`, and 18-line BUILD metadata. There is no
`doc.go`, Go test, fixture, generated input/output, benchmark, fuzz target,
example, or platform/build-tag variant. All Go functions and the complete
single-file Rust owner plus its workspace/lock registration were reviewed.

The Rust owner has four explicit annotations. `AtomicF64::new` is retained as
a Rust-only static-initialization helper. Go permits callers to discard
`InitStatsPercentage.Load`, `GetConcurrency`, and `NewRangeWorker`; Rust's
direct `AtomicF64::load`, `get_concurrency`, and `RangeWorker::new`
counterparts instead emitted three `unused_must_use` diagnostics. Those three
annotations were removed without changing atomic ordering, concurrency
clamping, worker channels, progress accounting, or task error handling. A
focused unit regression invokes all three APIs under
`#[deny(unused_must_use)]`; it failed before the implementation edit with
exactly three diagnostics and passes afterward.

Ready validation for this follow-up (Rust scope, per the request to skip Go
code execution):

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-stats-handle-initstats --lib source_return_values_may_be_ignored_like_go --offline --locked -- --nocapture
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-stats-handle-initstats --offline --locked -- --test-threads=1
PASS; 1 unit test passed, 0 failed; doc tests had 0 tests.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-stats-handle-initstats --all-targets --offline --locked
PASS; pre-existing dependency warnings remain outside this crate.

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

Only Rust source/tests and parity documentation changed. No Go, Bazel, Cargo
metadata, or module dependency changed, so `make bazel_prepare` is not
required. The Go package has no tests; live server consumers remain covered by
their existing owner gates.
