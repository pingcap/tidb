# `pkg/ddl/logutil` package receipt

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete package inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 12 | `6226c1609fd4f2af5cc8222474f5430648bb064e` | workspace crate `tidb-ddl-logutil` with only the pinned logging dependencies |
| `logutil.go` | 49 | `0f6076c6c9685a31b5017a7507f40a5ab3360c98` | all four constructors in `tidb-ddl-logutil/src/lib.rs` |

There is no package doc, test, test harness, benchmark, fixture, generated
source/input, build/platform variant, or ownership artifact in the pinned
directory.

## Behavior and integration decision

The crate composes the already completed `pkg/util/logutil` owner exactly as
Go does: `DDLLogger`, `DDLUpgradingLogger`, and `DDLIngestLogger` add only the
respective `category` field to the process background logger. `SampleLogger`
is one process-shared factory result with `category=ddl`, a one-minute window,
and the first three entries admitted per level/message bucket. No Rust-only
logger, fallback, or test behavior is added.

`tidb-ddl-serverstate` now consumes `ddl_logger()` instead of retaining a
second local constructor. Other DDL package callers can adopt the same owner
without duplicating logger policy.

## WIP validation

Run from `rust/`:

```text
cargo fmt --all -- --check
cargo check --offline -q -p tidb-ddl-logutil -p tidb-ddl-serverstate
cargo test --locked -q -p tidb-ddl-logutil
cargo test --locked -q -p tidb-ddl-serverstate --lib
```

The package has zero tests, matching the pinned inventory. The dependent
serverstate suite passes 6 tests with its one live-PD integration test ignored.
