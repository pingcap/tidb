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

The follow-up whole-package audit also removed the public generic
`NumericSet<T>` policy and replaced it with the source's concrete `IntSet` and
`Int64Set`. Keyed sets now use a hash map like Go, expose `ListToSet` and
`CombSet` as free operations rather than a Rust-only method, and return no
Rust-only result from `Add`/`Remove`/`Insert`. `ToList`, `String`, and `Clone`
re-read each retained item's current key before sorting or reinserting, as the
Go implementation does. `StringSet` exposes the source callback-shaped
`IterateWith` behavior rather than a public iterator-only substitute.

Memory-aware variadic constructors now pre-size the underlying table from the
exact input count before insertion, matching `make(map, len(ss))`, and do not
allocate a temporary collection. Their map iteration surface is lazy, matching
direct iteration over the embedded Go `MemAwareMap.M`, rather than materializing
a Rust-only vector. The shared `MemAwareMap` owner now uses explicit Go value
layouts for `GoString`, primitives, empty values, and boxed decimal pointers,
so these concrete set/map types feed the same source group sizes into the
checkpoint policy rather than Rust struct sizes.

HashAgg now consumes `StringSetWithMemoryUsage`, matching Go's executor, and
performs the same existence check before insertion.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `rustfmt --edition 2024 --check crates/tidb-util/src/set.rs` — passed.
- `cargo test -p tidb-util --locked set::tests:: --no-fail-fast` — passed; all
  seven source tests passed (the substring filter also selected nine existing
  `intset`/`disjointset` tests).
- `cargo check -p tidb-util --bench set --locked` — passed.
- `cargo check -p tidb-executor --lib --locked` — passed.
- `cargo test -p tidb-executor --lib --locked hash_agg::tests::distinct_` —
  passed (two tests).
- `cargo test -p tidb-executor --lib --locked
  hash_agg::tests::global_binary_count_distinct_uses_direct_bytes` — passed.
- `cargo test -p tidb-util --locked --no-fail-fast` — all set tests and 541
  other unit/integration/doc tests passed; one unrelated full-suite-only
  `memoryusagealarm::tests::test_if_need_do_record` assertion failed.
- `cargo test -p tidb-util --lib --locked
  memoryusagealarm::tests::test_if_need_do_record -- --exact` — passed in
  isolation, confirming the full-suite failure is order/timing dependent and
  outside this package.
- `git diff --check` — passed.
- `go test ./pkg/util/set` — blocked before this package compiled by the
  workspace's existing missing `pkg/util/hack.checkMapABI` build selection and
  `google.golang.org/grpc/internal/transport` / `http2.TrailerPrefix`
  dependency mismatch.

No Go or Bazel file changed, so `make bazel_prepare` is not required.
Workspace-wide `cargo fmt --all -- --check` is currently blocked by unrelated
pre-existing formatting drift in `tidb-datatype/src/mydecimal.rs`. Scoped
`cargo clippy` is also blocked by pre-existing lint failures in dependencies,
generated protobuf output, and unrelated `tidb-util` modules; neither blocker
was changed for this package checkpoint.

## Risk

- Correctness: reduced; current-key clone/order behavior, concrete constructor,
  tracker, map-equality, and source test behavior now match Go.
- Compatibility: the public Rust-only generic numeric and memory wrappers and
  non-source method shapes are removed; the production consumer uses Go's
  concrete string-set type.
- Performance: keyed and primitive sets use hash tables; constructor
  pre-sizing no longer pays for an untracked temporary vector; memory deltas
  continue through the existing Go-map accounting owner.
