# `pkg/statistics/handle/cache/internal` → `tidb-stats-handle-cache-internal`

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Rust owner |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 17 | `bfe8b29fcbfab5ba222a0bb984745a3ba19403bd` | workspace member and crate manifest |
| `inner.go` | 50 | `1a529bc6b3b833d3fefeab1762f3b7195fef8e06` | `src/lib.rs` |

The BUILD file publishes the same source once under the current import path
and once under its legacy import path. Both Go targets contain the identical
interface; one Rust crate is therefore the complete native owner. The package
has no generated, platform-specific, test, fixture, or benchmark artifacts.

## Behavior mapping

`StatsCacheInner` has exactly the pinned eleven-method surface. It operates on
shared `tidb_stats::Table` values, preserving Go `*statistics.Table` identity
without copying table contents. Every method takes a shared receiver because
both pinned implementations synchronize or otherwise mutate internally.

The former `tidb-stats` trait was removed because it generalized the table to
an arbitrary value type, exposed a source-absent `is_empty` method, required
exclusive mutable receivers, and was exercised only by two source-absent mock
tests. No production consumer depended on that shape.

## Validation

WIP profile: this package is a source-test-free interface package, so its
minimum meaningful gate is compilation of the complete crate contract.

- `cargo check --offline -p tidb-stats-handle-cache-internal`
- `cargo check --locked -p tidb-stats-handle-cache-internal`
- `cargo clippy --locked -p tidb-stats-handle-cache-internal --no-deps -- -D warnings`
- `rustfmt --edition 2021 --check crates/tidb-stats-handle-cache-internal/src/lib.rs`
- `git diff --check`

The concrete `lfu`, `mapcache`, and `testutil` subpackages remain separate Go
package units and are not claimed by this receipt.
