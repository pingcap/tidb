# `pkg/resourcemanager/scheduler` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

- `BUILD.bazel`
- `cpu_scheduler.go`
- `scheduler.go`

## Rust ownership and integration

- `tidb-resourcemanager::scheduler` owns the exact command values
  `Downclock = 0`, `Hold = 1`, and `Overclock = 2`, plus the scheduler contract
  and CPU scheduler.
- CPU tuning holds before the mutable minimum interval, holds when CPU sampling
  is unsupported, overclocks below 0.5, downclocks above 0.7, and otherwise
  holds. It consumes the ordinary shared `tidb-util::cpu` state; there is no
  Rust-only policy or alternate scheduler path.
- The pinned package contains no source tests. Its failpoint behavior is covered
  only where Go covers it: `pkg/util/cpu`'s `TestFailpointCPUValue`.

## WIP validation

Commands and results are shared with `receipts/util_cpu.md`; the failpoint CPU
source test exercised the scheduler's unsupported-to-Hold path successfully.

Not run: workspace-wide tests or the Ready-profile `make lint` gate.
