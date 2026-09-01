# `pkg/session/metrics` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for session metric bindings and
record a safe package-atomic Rust ownership boundary. Read every Go source
and build artifact before editing; do not treat one label-only metric leaf as
the complete Prometheus/session metrics package.

## Completed this batch

1. Inventoried both artifacts (158 lines): all 49 exported counter/observer
   handles, their `InitMetricsVars` label bindings, the package initializer,
   and the 12-line Bazel target. No tests, fixtures, generated outputs,
   benchmarks, fuzz inputs, or platform variants were omitted.
2. Compiled the exact Go-master package; it reported `[no test files]`.
3. Compared the package with Rust. Rust's `tidb-exec::session_metrics` owns
   only the three non-transactional DML labels and not the Prometheus
   registration, observer handles, transaction, timing, or telemetry
   families.
4. Found no safe missing behavior to implement and no Rust-only behavior to
   remove. Recorded the inventory, hashes, validation evidence, and explicit
   SEED boundary in `rust/testport/receipts/session_metrics.md`.

## Validation gate

- [x] Complete Go source/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master package compilation passed (`[no test files]`).
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

The shared Prometheus metric families, session/executor instrumentation,
transaction timing and retry labels, and telemetry consumers remain explicit
cross-crate boundaries. The repository package loop continues after this
receipt; this plan does not claim whole-repository completion.
