# `pkg/util/cpu` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

- `BUILD.bazel`
- `cpu.go`
- `cpu_test.go`
- `main_test.go`

## Rust ownership and integration

- `tidb-util::cpu` owns the process-global usage and unsupported state, the
  observer, the `mockNumCpu` failpoint, and the runtime CPU-count view.
- The observer performs the source cgroup preflight, samples cumulative process
  user/system milliseconds every 100 ms, normalizes the deltas by elapsed wall
  time and cgroup CPU shares, and publishes the shared 0.95-factor/10-sample
  exponential moving average.
- The shared metric is exactly `tidb_rm_ema_cpu_usage` with help text
  `exponential moving average of CPU usage`. A cgroup preflight error sets the
  process-global unsupported flag, logs `GetCgroupCPU`, and starts no sampler.
- Unix process CPU time uses `getrusage(RUSAGE_SELF)` and Windows uses
  `GetProcessTimes`, preserving the source millisecond boundary. Server startup
  installs the effective configured/environment/cgroup/affinity CPU count that
  is the Rust runtime equivalent of Go's current `GOMAXPROCS(0)`.
- `TestCPUValue` and `TestFailpointCPUValue` retain their source identities and
  workloads in separate process test carriers. The separation preserves the
  source package's process-global unsupported state without inventing a reset
  API. Explicit thread joins provide the source `TestMain` leak-check boundary.

## WIP validation

Commands run from `rust/`:

```text
cargo fmt --all
cargo check --quiet --offline -p tidb-util -p tidb-resourcemanager -p tidb-server
cargo test --quiet --offline -p tidb-resourcemanager --features failpoints,intest
cargo test --quiet --offline -p tidb-util --lib cgroup::tests
```

Results: formatting and the affected-crate check passed; both CPU source tests,
the resource-manager and sharded-map source tests, and four adjacent cgroup
tests passed on macOS. `TestCPUValue` took its source non-container skip path.
Cargo emitted existing workspace warnings and no changed-file warning.

Not run in this WIP gate: live Linux/container sampling, Linux compilation,
Windows compilation/execution, workspace-wide tests, or `make lint`.
