# `pkg/util/set` — complete package transcreation

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The package is
unchanged from the earlier implementation and byte-for-byte identical to the
exact detached latest-master worktree.

## Complete inventory

All twelve current Go-master artifacts were read in full: five production
files, six test/benchmark files, and `BUILD.bazel`. They define the generic
keyed set, int/int64/string/float map sets, five memory-aware concrete types,
seven unit tests, three benchmarks, and the shared test harness. There is no
package doc, README, fixture, generated/platform variant, or ownership file.
The checkout is byte-identical to the current authority.

| Artifact | Lines |
| --- | ---: |
| `BUILD.bazel` | 40 |
| `float64_set.go` / `float64_set_test.go` | 43 / 43 |
| `int_set.go` / `int_set_test.go` | 71 / 70 |
| `main_test.go` | 33 |
| `set.go` / `set_test.go` | 184 / 96 |
| `set_with_memory_usage.go` / `set_with_memory_usage_test.go` | 157 / 108 |
| `string_set.go` / `string_set_test.go` | 92 / 64 |

Total: 1,001 lines, 60 declarations, seven unit-test functions, three
benchmarks, and no current source delta.

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

This authority refresh removed all 55 explicit Rust-only `#[must_use]`
annotations in the set owner, covering public constructors, queries, set
operations, and private memory-accounting helpers. Go permits callers to
discard each corresponding function result. The focused
`return_values_may_be_ignored_like_go` regression calls every public annotated
surface; its pre-fix compile failed with 47 `unused_must_use` errors and its
post-fix run passes.

## Validation

Profile: **Ready** for this focused parity fix within the continuing
package-by-package audit, not a repository-wide readiness claim.

- `git -c maintenance.auto=false -c gc.auto=0 fetch origin master --prune` —
  passed; `origin/master` is `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- `git diff --exit-code 5e8a1a229a7591ddac49a0cd3b795587c2595ab9..c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/set` — passed; no Go source drift.
- `git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/set` and full-file reads — passed; confirmed the twelve-artifact inventory.
- `cmp -s /tmp/tidb-go-latest-c605/pkg/util/set/<file> pkg/util/set/<file>` for all twelve artifacts — passed.
- Pre-fix focused Rust compile with the deny-lint regression — failed with
  the expected 47 `unused_must_use` errors.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib set::tests::return_values_may_be_ignored_like_go --offline --locked -- --exact` — passed; focused return-contract regression.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib set::tests --offline --locked -- --test-threads=1` — passed; 17 filtered owner/intset/disjointset tests.
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
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/set -count=1` — passed in the current and exact detached latest-master worktrees.

No Go, Bazel, module, or Cargo manifest file changed, so `make bazel_prepare`
was not required. Workspace-wide formatting and repository lint were rerun
under the Ready profile and passed.

## Risk

- Correctness: reduced; current-key clone/order behavior, concrete constructor,
  tracker, map-equality, and source test behavior now match Go.
- Compatibility: the public Rust-only generic numeric and memory wrappers and
  non-source method shapes are removed; the production consumer uses Go's
  concrete string-set type.
- Performance: keyed and primitive sets use hash tables; constructor
  pre-sizing no longer pays for an untracked temporary vector; memory deltas
  continue through the existing Go-map accounting owner.
