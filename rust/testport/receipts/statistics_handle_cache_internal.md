# `pkg/statistics/handle/cache/internal` → `tidb-stats-handle-cache-internal`

Pinned source: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (Go `master` at the
audit boundary).

## Atomic inventory

| Artifact | Lines | Git blob | SHA-256 | Rust owner |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 17 | `bfe8b29fcbfab5ba222a0bb984745a3ba19403bd` | `eb76d0ab8267a49b7f859def78a9339860771625c197ed73fe6c8bf3467282e4` | workspace member and crate manifest |
| `inner.go` | 50 | `1a529bc6b3b833d3fefeab1762f3b7195fef8e06` | `5e8273a5918dd6fa729483d642492542c598398a76fab15ba31d34a85bdfeb61` | `src/lib.rs` |

The BUILD file publishes the same source once under the current import path
and once under its legacy import path. Both Go targets contain the identical
interface; one Rust crate is therefore the complete native owner. The package
has no generated, platform-specific, test, fixture, or benchmark artifacts.

Every interface method and BUILD attribute was read; the current checkout is
byte-identical to this pin. The Rust trait matches all eleven pointer-oriented
operations over shared `tidb_stats::Table` values, with no source-vs-owner gap
or Rust-only production method.

## Behavior mapping

`StatsCacheInner` has exactly the pinned eleven-method surface. It operates on
shared `tidb_stats::Table` values, preserving Go `*statistics.Table` identity
without copying table contents. Mutating pointer-receiver methods use Rust
interior mutability in concrete implementations; the interface does not add a
Go-absent thread-safety bound.

The former `tidb-stats` trait was removed because it generalized the table to
an arbitrary value type, exposed a source-absent `is_empty` method, required
exclusive mutable receivers, and was exercised only by two source-absent mock
tests. No production consumer depended on that shape.

## Validation

Ready profile: this package is a source-test-free interface package, so its
minimum meaningful gate is compilation of the complete crate contract.

- `cargo check --offline -p tidb-stats-handle-cache-internal`
- `cargo check --locked -p tidb-stats-handle-cache-internal`
- `cargo clippy --locked -p tidb-stats-handle-cache-internal --no-deps -- -D warnings`
- `rustfmt --edition 2021 --check crates/tidb-stats-handle-cache-internal/src/lib.rs`
- `git diff --check`

The current and detached Go package probes both report `[no test files]`;
Rust check/clippy/format and pinned repository lint passed. `make bazel_prepare`
is not required because no Go or Bazel source changed.

The concrete `lfu`, `mapcache`, and `testutil` subpackages remain separate Go
package units and are not claimed by this receipt.
