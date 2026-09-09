# `br/pkg/restore/utils` — Rust return-contract alignment

The complete Go-package inventory was established by the original package
landing at `e40dbe9f6a7a41f910450b9874fdfddeaacc484c`: exactly eight artifacts,
comprising `common.go`, `merge.go`, `misc.go`, `rewrite_rule.go`, their three
test files, and `BUILD.bazel`. That landing records the complete package
behavior and all source tests. There is no package doc, fixture, generated
source/input, platform variant, or other build artifact. This 2026-09-07
follow-up deliberately does not reopen Go code, following the user's Rust-only
direction.

## Complete Rust inventory

The implementation owner is `rust/crates/tidb-br/src/restore_utils.rs` plus
`src/restore_utils/{common,merge,misc,proto,rewrite_rule}.rs`. Before this edit
the six modules contained 77, 46, 506, 140, 122, and 1,394 lines respectively
(2,285 total) and were byte-identical to their complete package landing. They
cover created-table state, file-range merging and statistics, table/partition/
index ID maps, timestamp truncation, prefix encoding, flat protobuf boundary
types, rewrite-rule construction and lookup, raw/encoded key rewriting, range
validation and rewriting, timestamp filters, and normalized restore errors.

The Rust test surface had seventeen active inline source-derived tests plus one
ignored benchmark-shaped test: four merge/range cases, two misc key-transform
cases, and eleven rewrite-rule/file/range/time-filter cases. The 19-line shared
manifest, 72-line crate root, workspace member, lockfile entry, every function,
and every caller were inspected. `tidb-br` has no reverse Cargo dependency.
There is no feature, build script, generated output/input, platform variant,
fixture, example, separate test artifact, or external production caller.

## Alignment decision

Go permits callers to discard fifteen direct package results:
`GetPartitionIDMap`, `GetTableIDMap`, `GetIndexIDMap`, `TruncateTS`,
`EncodeKeyPrefix`, `RewriteRules.HasSetTs`, `RewriteRules.Clone`,
`RewriteRules.Equal`, `EmptyRewriteRulesMap`, `EmptyRewriteRule`,
`GetRewriteRules`, `GetRewriteRulesMap`, `GetRewriteRuleOfTable`,
`RewriteAndEncodeRawKey`, and `GetRewriteTableID`. Rust had added
`#[must_use]` to every counterpart. The focused regression calls all fifteen
under `#[deny(unused_must_use)]`; its pre-fix run failed with exactly fifteen
diagnostics. Removing only those fifteen annotations makes the same test pass.

Ten annotations remain intentionally: five nil-tolerant local protobuf
accessors, four Rust normalized-error boundary helpers, and
`FindMatchedRewriteRule`, whose `Option` result is already inherently must-use.
No key, rule, table-ID, filter, range, merge, error, or statistics behavior
changed. The post-edit owner is 2,302 lines including the focused regression;
the shared manifest and crate root bring the inspected integration surface to
2,393 lines.

## Validation

Profile: Ready, because this package batch is published as an independent
checkpoint.

- Focused regression: `cargo +nightly-2026-08-22 test --manifest-path
  rust/Cargo.toml --offline --locked -p tidb-br --lib
  restore_utils::return_contract_tests::direct_source_returns_may_be_ignored_like_go
  -- --exact --test-threads=1` — passed, 1 test. The pre-fix run failed with
  exactly fifteen `unused_must_use` diagnostics.
- Complete shared crate: `cargo +nightly-2026-08-22 nextest run
  --manifest-path rust/Cargo.toml --offline --locked -p tidb-br
  --no-fail-fast` — passed, 34 active tests; two unrelated benchmark-shaped
  tests skipped.
- Owner/build surface: `cargo +nightly-2026-08-22 check --manifest-path
  rust/Cargo.toml --offline --locked -p tidb-br --all-targets` — passed; only
  pre-existing dependency warnings were emitted.
- Scoped nightly `rustfmt --check`, repository `make lint`, and `git diff
  --check` — passed.

Only Rust owner source and parity documentation changed. No Go, Bazel, Cargo
metadata, module, import, generated, or build-target input changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: minimal; the edit changes compiler diagnostics only and the
  complete deterministic shared-crate suite passes.
- Compatibility: improved for direct source-shaped calls; local protobuf,
  error, and inherent `Option` contracts remain explicit.
- Performance: none; generated code and runtime instructions are unchanged.
- Not verified locally: no live BR/object-store integration was run because
  the affected owner has no live-service or fixture dependency.
