# `pkg/util/cgmon` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

- `BUILD.bazel`
- `cgmon.go`
- `cgmon_test.go`

## Rust ownership and integration

- `tidb-util::cgmon` owns the process-global monitor lifecycle, its immediate
  refresh and ten-second refresh loop, and the two shared Prometheus gauges.
  Their exported metric names and help strings are exactly
  `tidb_server_maxprocs` / `The value of GOMAXPROCS.` and
  `tidb_server_memory_quota_bytes` /
  `The value of memory quota bytes.`.
- CPU refresh starts from the affinity-visible logical CPU count and takes the
  smaller positive cgroup quota/period ratio rounded upward. Memory refresh
  starts from host physical memory and takes the smaller readable cgroup
  limit. As in Go, both default values are published before a cgroup-read
  error is returned.
- The last published CPU and memory values are process-global and survive a
  stop/start cycle. A changed value updates its gauge; an unchanged value does
  not. Initial read failures log at warning level and later failures at debug
  level.
- `tidb-server::run_configured_node` starts one monitor after process globals
  are installed and stops it through process-lifecycle cleanup. Repeated start,
  repeated stop, and all non-Linux calls are no-ops. A panic ends the monitor
  thread, matching the source goroutine's deferred panic recovery.
- `tidb-util::cgroup::logical_cpu_count` is the shared owner for both cgroup
  usage and cgmon. Linux uses the process affinity mask, matching
  `runtime.NumCPU`; non-Linux retains the native available-parallelism value.
- The sole Go source test is represented with its source identity. It injects
  cgroup errors and verifies that CPU and memory gauges still receive their
  host defaults while both refresh operations return the errors.

## WIP validation

Commands run from `rust/`:

```text
cargo fmt --all
cargo test --quiet --offline -p tidb-util --lib cgmon::tests
cargo test --quiet --offline -p tidb-util --lib cgroup::tests
cargo check --quiet --offline -p tidb-util -p tidb-server
```

Results: formatting completed, the one cgmon source test and four adjacent
cgroup tests passed on macOS, and both affected crates checked successfully.
The cgmon test was allowed to invoke macOS `sysctl` for its host-memory value.
Cargo emitted existing workspace warnings; no changed-file warning was added.

Not run in this WIP gate: Linux live monitor execution, Linux compilation,
Windows or other unsupported-target execution, workspace-wide tests, or
`make lint`. Those are reserved for the Ready profile before overall task
completion or PR-readiness is claimed.
