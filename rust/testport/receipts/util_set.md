# `pkg/util/set` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All eleven package artifacts were read in full: five production files, five
test/benchmark files, and `BUILD.bazel`. They define the generic keyed set,
int/int64/string/float map sets, five memory-aware concrete types, seven unit
tests, three benchmarks, and the shared test harness. There is no package doc,
README, fixture, generated/platform variant, or ownership file. The checkout
is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/set.rs` is the sole owner. The seven retained tests
now correspond one-for-one to the seven Go unit tests. The three source
benchmarks are executable in `rust/crates/tidb-util/benches/set.rs` with every
source row-count boundary.

The audit replaced four generic type aliases with Go's five concrete
memory-aware types. Their constructors now accept initial values and return
the initial accounted bytes; only the two string maps and string set accept an
optional tracker, including Go's nil/detach case. The generic `MemoryMap` and
`MemorySet` implementation details are private. Primitive int and string sets
now use hash maps with unspecified iteration rather than publishing a
Rust-only sorted iteration policy.

HashAgg now consumes `StringSetWithMemoryUsage`, matching Go's executor, and
performs the same existence check before insertion.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `cargo test -p tidb-util --locked set::tests::` — passed (seven source tests).
- `cargo check -p tidb-util --bench set --locked` — passed.
- `cargo check -p tidb-executor --lib --locked` — passed.
- `cargo test -p tidb-executor --lib --locked hash_agg::tests::distinct_` —
  passed (two tests).
- `cargo test -p tidb-executor --lib --locked
  hash_agg::tests::global_binary_count_distinct_uses_direct_bytes` — passed.
- `cargo test -p tidb-util --locked` — passed.
- `cargo fmt --all -- --check` and `git diff --check` — passed.
- `go test ./pkg/util/set` — blocked before this package compiled by the
  workspace's existing missing `pkg/util/hack.checkMapABI` build selection and
  `google.golang.org/grpc/internal/transport` / `http2.TrailerPrefix`
  dependency mismatch.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; concrete constructor, tracker, map-equality, and
  source test behavior now match Go.
- Compatibility: the public Rust-only generic memory wrappers are removed;
  the only production consumer now uses Go's concrete string-set type.
- Performance: ordinary sets and HashAgg use hash tables rather than ordered
  trees; memory deltas continue through the existing Go-map accounting owner.
