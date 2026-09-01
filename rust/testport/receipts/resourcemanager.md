# `pkg/resourcemanager` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

- `BUILD.bazel`
- `OWNERS`
- `rm.go`
- `schedule.go`
- `schedule_test.go`

The `pool`, `poolmanager`, `scheduler`, and `util` directories are distinct Go
packages. Scheduler and util have separate atomic receipts in this batch;
pool and poolmanager remain later package units.

## Rust ownership and integration

- `tidb-resourcemanager` owns the process-global manager, UUID-v4 test names,
  CPU observer, scheduler list, sharded pool registry, and the source start/stop,
  register/unregister/reset lifecycle.
- Start runs CPU observation and the 100 ms scheduling ticker asynchronously;
  stop closes both source lifecycles and joins their threads. Server startup
  starts the global manager after process resource admission and cleanup stops
  it before cgroup cleanup, matching the Go process lifecycle.
- Scheduling skips distributed-task pools, holds idle pools, suppresses an
  unsafe downclock at capacity one or while running exceeds capacity, and uses
  the first non-Hold scheduler decision. Execution observes the strict source
  minimum-interval boundary and changes concurrency by one, bounded by original
  concurrency plus the mutable maximum overclock count.
- `TestSchedulerOverloadTooMuch` retains the sole source test identity and exact
  one-to-two/no-further-increase assertions. No supplemental resource-manager
  tests or behavior were added. `OWNERS` is governance-only and requires no
  Rust runtime artifact.

## WIP validation

Commands and results are shared with `receipts/util_cpu.md`. Additionally, both
targeted `tidb-server` early-resource-admission tests passed, proving those
error paths return before starting the non-restartable process-global manager.

```text
cargo test --quiet --offline -p tidb-server --test all configured_sem_is_installed_before_startup_resource_admission
cargo test --quiet --offline -p tidb-server --test all impossible_spill_quota_fails_before_auth_listener_or_cluster_startup
```

Not run: a full server lifecycle, workspace-wide tests, or the Ready-profile
`make lint` gate.
