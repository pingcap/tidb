# `pkg/resourcemanager/pool/workerpool` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 40 | `065bbfb852fa5dd529335d74a7844c66e26356c6` | `tidb-resourcemanager` module and its five source-derived tests; the server `failpoints` feature enables the package callback instrumentation |
| `main_test.go` | 34 | `6ba6d9d895ec8d1898a6319bd90703a7d0bd0899` | Rust tests join every worker and consumer thread directly; no detached thread survives the test target |
| `workerpool.go` | 379 | `fafa22654a07060435207fc5d67017fbbc30bf1c` | `tidb_resourcemanager::workerpool`: cancellable first-error context, reusable workers, panic conversion, custom channels, result-less pools, tuning, lifecycle, timestamps, counters, options, and constructor callback failpoint |
| `workpool_test.go` | 241 | `390adc1c575fcd45d0e42819cf451be2bccc859e` | five Rust tests preserve all source cases, including the three `TestTunePoolSize` subtests and exact random tuning workload |

There is no package doc, fixture, benchmark, generated source, or
build/platform variant in the pinned directory.

## Native integration decision

The workerpool is the canonical reusable implementation in
`tidb-resourcemanager`. `tidb-dxf-operator` now consumes it exactly as pinned
Go `pkg/dxf/operator` consumes this package; the previous cache of duplicate
context, panic, worker, channel, lifecycle, and tuning behavior was removed.
The shared closeable channel is the native owner used for the Go channel
fields. `NoResult` is the Rust spelling of Go `None`.

Go's `failpoint.InjectCall("NewWorkerPool", numWorkers)` accepts a callback.
The feature-gated native hook preserves that argument-bearing behavior, and
the ordinary server `failpoints` feature propagates into this crate. It is
not a production scheduling policy.

## WIP validation

Commands run from `rust/`:

```text
cargo fmt --all
cargo check --quiet --offline -p tidb-resourcemanager -p tidb-dxf-operator
cargo test --quiet --offline -p tidb-resourcemanager --lib workerpool::tests
cargo test --quiet --offline -p tidb-dxf-operator
cargo check --quiet --offline -p tidb-resourcemanager --features failpoints -p tidb-dxf-operator
cargo check --quiet --offline -p tidb-server --features failpoints
```

All commands passed. Cargo emitted only pre-existing workspace warnings.
The Go race-enabled Bazel target, non-native targets, workspace-wide tests,
and Ready-profile `make lint` were not run during this WIP package iteration.
