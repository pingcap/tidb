# `pkg/util/breakpoint` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

| Artifact | Bytes | Blob |
| --- | ---: | --- |
| `pkg/util/breakpoint/breakpoint.go` | 1,216 | `60c0e1f82879995330ec051553b42c771375a5bf` |
| `pkg/util/breakpoint/BUILD.bazel` | 363 | `351dbcf3d3186ff7809777683088be53014b4a79` |

There is no `doc.go`, test, support file, fixture, benchmark, generated source,
or platform variant.

## Rust ownership and behavior

`rust/crates/tidb-util/src/breakpoint.rs` owns the exact package surface:

- `NOTIFY_BREAK_POINT_FUNC_KEY` has Go's `breakPointNotifyFunc` value;
- `inject` evaluates the named process failpoint first, then loads the session
  value and invokes it synchronously only when its concrete native type is the
  Rust representation of Go `func(string)`;
- disabled failpoints, missing values, and values of another type are no-ops;
  callback panics propagate as Go callback panics do.

The session now implements the already-transcreated heterogeneous
`ValueStoreContext`. Its values carry `Send + Sync`, a native constraint
because a Rust session moves between connection workers; it does not change
the key lookup, concrete-type check, or callback behavior.

The two pinned production consumers are integrated at their ordinary runtime
seams:

- fresh and cache-rebuilt SELECT/DML physical trees share one executor-build
  path, which injects `beforeExecutorFirstRun` after construction and before
  `open`/drain;
- VALUES and in-memory DML inject only after their fused parent executor state
  is valid, while multi-table DML injects after source/target binding and
  before joined-row execution;
- the cluster point-write pre-lock injects at the same observable boundary,
  because that pre-lock is Go's executor-time `PointGetExecutor` lock moved
  ahead of the fused Rust session runner;
- lock-error retries inject `lockErrorAndThenOnStmtRetryCalled` after advancing
  the retry timestamp and rolling back statement writes, before rebuilding;
- the attempt latch matches Go's two retry layers: inner pessimistic retries
  do not repeat the first-run hook, while a full autocommit retry does;
- PREPARE metadata probes build no executor and suppress the execution hook.

No Rust-only package test or callback registry was retained. The small
failpoint-enabled session probe used during implementation was deleted after
proving ordinary execution, PREPARE suppression, EXECUTE, and per-statement
re-arming.

## Validation

Profile: WIP; this completes one pinned package within the continuing
repository parity audit.

Passed:

- `cargo fmt --all -- --check`
- `cargo check --quiet --offline -p tidb-util -p tidb-executor -p tidb-session -p tidb-server`
- `cargo check --quiet --offline -p tidb-server --features failpoints`
- non-landed probe:
  `cargo test --quiet --offline -p tidb-session --test breakpoint_probe --features tidb-util/failpoints -- --test-threads=1`
- `cargo test --quiet --offline -p tidb-session --lib tests_prepared_statements::prepared_statement_without_markers_executes_bare -- --exact --test-threads=1`
- `cargo test --quiet --offline -p tidb-session --lib rebuilds_and_executes -- --test-threads=1`
- `git diff --check`

`cargo clippy --quiet --offline -p tidb-util --features failpoints --no-deps
-- -D warnings` reached the changed crate but is blocked by fifteen existing
warnings in unrelated modules such as `cpu`, `encrypt`, `membuf`, `mvmap`, and
`watcher`; none is in `breakpoint.rs` or `context/mod.rs`.

A wider prepared-statement session sweep ran 84 tests; 77 passed, one was
ignored, and six unrelated existing tests failed in row-codec bounds, system
schema case, parallel HashAgg shutdown, unordered GROUP BY ordering,
unknown-column marker binding, and a source-inspection assertion. A focused
server lifecycle test could not start because the host's `sysctl hw.memsize`
probe failed. Neither blocked sweep is claimed as a pass.

The Go package uses failpoints, so the repository wrapper was selected. It
enabled and cleaned up failpoints correctly, but the package could not reach
compilation because this checkout's unrelated Go dependency graph currently
fails in `pkg/util/hack` (`checkMapABI`) and gRPC transport
(`http2.TrailerPrefix`):

- `./tools/check/failpoint-go-test.sh pkg/util/breakpoint -count=1`

No Go/Bazel source changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: callback type checks, timing, retry cardinality, and PREPARE
  suppression now match the pinned consumers.
- Compatibility: the existing value-store trait now requires thread-safe
  native values; there were no prior implementations or consumers storing a
  non-thread-safe value.
- Performance: one release statement-boundary store and one atomic swap per
  execution attempt; disabled/missing callbacks allocate nothing.
