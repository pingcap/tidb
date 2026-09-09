# `pkg/util/signal` parity audit ExecPlan

## Objective

Keep the complete Go-master cross-platform signal adapter inventoried and its
Rust ownership boundary explicit until the server can migrate it atomically.

## Progress

- [x] Read all five Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: `BUILD.bazel`, `exit.go`,
  `signal_posix.go`, `signal_windows.go`, and `signal_wasm.go` (289 lines).
- [x] Confirm there are no package docs, source tests/support, fixtures,
  generated inputs/outputs, benchmarks, fuzz targets, examples, ownership
  files, or nested packages.
- [x] Inventory the full Bazel platform dependency matrix plus POSIX one-shot
  shutdown and SIGUSR1 stack dumps, non-Windows process signaling, Windows
  best-effort signaling, and WASM no-op handlers.
- [x] Compile the host, Windows, and JS/WASM file selections in the current
  and exact detached Go-master worktrees.
- [x] Verify adjacent `tidb-server` shutdown/exit-code owners compile and keep
  the package explicitly unclaimed because they lack the complete platform
  contract and shared startup ownership.
- [x] Run Ready formatting and diff hygiene.
- [x] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This is a docs-only Ready boundary refresh. No Go, Rust, Bazel, or module file
changed, so `make bazel_prepare` is not required. Exact commands and remaining
scope are recorded in `rust/testport/receipts/util_signal.md`.

## Next boundary

Any future implementation must migrate the complete POSIX, Windows, and WASM
matrix together with every server startup consumer. Preserve first-signal
ordering, SIGUSR1 dump bounds, process-signaling behavior, and platform no-ops;
do not add another partial signal thread or stack endpoint.
