# `pkg/util/cpu` parity audit ExecPlan

## Objective

Keep the complete Go-master process-CPU observer and runtime CPU-count
package aligned with its Rust utility, resource-manager, and server startup
owners across platform variants.

## Progress

- [x] Read all four Go-master artifacts at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: `BUILD.bazel`, `cpu.go`,
  `cpu_test.go`, and `main_test.go` (308 lines; usage observer, CPU count,
  failpoint test, and goleak harness).
- [x] Confirm there are no Go package docs, fixtures, generated inputs/outputs,
  separate platform/build-tag files, benchmarks, fuzz targets, examples, or
  nested packages; `gosigar` supplies the source's platform implementation.
- [x] Compare the Rust Unix/Windows/fallback process-time variants,
  cgroup-preflight fail-closed state, 100 ms EMA observer, metric, failpoint,
  runtime CPU count, resource-manager scheduler, and server startup call path.
  No Rust-only diagnostics or policy was found.
- [x] Revalidate current and exact detached Go tests with the failpoint wrapper,
  both Rust source test carriers, owner/scheduler/server checks, formatting,
  and diff quality.
- [ ] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This rolling refresh uses the Ready documentation-only profile. No Go or
Bazel file changed, so `make bazel_prepare` is not required. Exact commands,
failpoint cleanup, and platform boundaries are recorded in
`rust/testport/receipts/util_cpu.md`.

## Next boundary

Any future CPU observer change must preserve cgroup fail-closed semantics,
100 ms sampling and EMA parameters, process-time units, metric identity,
`mockNumCpu`, observer lifecycle, and the startup/runtime CPU-count boundary
on Unix, Windows, and unsupported platforms.
