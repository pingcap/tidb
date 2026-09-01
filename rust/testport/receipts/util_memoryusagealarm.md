# `pkg/util/memoryusagealarm` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

- `BUILD.bazel`
- `memoryusagealarm.go`
- `memoryusagealarm_test.go` (four tests, helpers, and the goroutine-profile
  benchmark)

## Rust ownership and integration

- `tidb-util::memoryusagealarm` owns the monitor, configuration provider,
  alarm/retention policy, running-SQL record, heap profile, and fixed 64 MiB
  native thread-stack profile.
- `tidb-session::process` supplies the live session-manager/process snapshots;
  server construction installs that manager before the 100ms monitor starts
  and server shutdown closes and joins the monitor.
- `tidb-vardef`, `tidb-util::memory`, cgroup/system memory readers, and the
  production jemalloc allocator provide the same live configuration and memory
  inputs used by Go. The heap file is emitted by jemalloc profiling in the
  production server feature set.
- The native stack profile retains Go's `goroutine` filename and fixed-buffer,
  truncating write behavior, but records real Rust thread/backtrace content;
  it does not fabricate Go runtime labels to satisfy Go-specific text checks.
- All four Go tests are represented. The benchmark has a Cargo bench target,
  including the pinned Go source's `10000` label with an actual count of 1000.

## WIP validation

Commands run from `rust/`:

```text
cargo test --quiet --offline -p tidb-util --lib memoryusagealarm::tests
cargo bench --quiet --offline -p tidb-util --bench memoryusagealarm --features testexport --no-run
cargo check --quiet --offline -p tidb-session -p tidb-server -p tidb-exec -p tidb-stmtsummary
```

Results: all four memory-alarm tests passed; the benchmark compiled; all four
affected integration crates checked successfully. Cargo emitted existing
workspace warnings. The non-jemalloc unit-test build also logged the expected
failed host `sysctl hw.memsize` probe while a test exercised the system-memory
branch; it did not fail a test.

Not run in this WIP gate: a live threshold-triggered production server dump,
Linux/Windows execution, workspace-wide tests, or `make lint`. Those are
reserved for the Ready profile before overall task completion or PR-readiness
is claimed.
