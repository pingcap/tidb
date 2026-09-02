# Rust `tidb-expr` collation-name boundary receipt

Status: bounded Rust-only alignment batch; this receipt does not claim the
entire `pkg/expression` transcreation is complete.

Comparison source: Go `origin/master` at `a85e0fd5dfa914e73eed97f17af584061252bc3c`
(2026-09-02). The source contract is `pkg/expression/collation.go:607-617`,
where `illegalMixCollationErr` indexes the seven-entry `coerString` slice
directly when formatting a two- or three-argument error.

## Complete package inventory

Before editing, every tracked root artifact under Go `pkg/expression` was
enumerated and read from the fetched tree: 137 artifacts and 128,744 lines
(68 production files, 60 tests, 7 generated Go sources, `BUILD.bazel`, and
`OWNERS`). The nested `aggregation`, `exprctx`, `expropt`, `exprstatic`,
`generator`, `integration_test`, `sessionexpr`, and test-fixture packages are
included in the manifest but remain their own package boundaries. There are
no root platform-specific files or fixture directories.

The Rust owner inventory was likewise enumerated and read before editing:
`rust/crates/tidb-expr` has 175 tracked artifacts and 104,964 lines, including
`Cargo.toml`, 107 source files with in-module tests, 65 standalone source-test
fixtures, and the `tests/all.rs`/benchmark and support inputs. Its Cargo build
input is the shared `rust/scripts/aggregate-tests.rs`, which emits the
untracked `OUT_DIR/all_tests.rs`; no platform-specific implementation exists.
The edited production files are `src/expr_collation.rs` and
`src/collation_derive.rs`, with the in-module collation tests read before the
change. No Go, Bazel, generated output, fixture, or platform file changed.

## Alignment

Go's `coerString[c]` is a trusted enum-to-name lookup. The prior Rust
`Coercibility::name` returned `None` for negative/out-of-range values, and the
collation error formatter silently converted that refusal to `EXPLICIT`. Rust
now converts the signed value to an index and directly indexes the same seven
names, so invalid values panic at the same boundary as Go. The formatter now
uses the direct name without a fallback. Valid names and all collation hash,
derivation, and error-message behavior remain unchanged.

The focused regression retains the seven valid names and asserts that both a
positive out-of-range value and a negative value panic. The old source-shaped
assertions (`None` for those values) are removed, so the test would fail against
the pre-fix implementation.

The same inventoried package also had two explicit unchecked-accessor gaps in
`pkg/expression/util.go`: `GetFuncArg` indexed a function's argument slice
directly, and `ExtractColumnsFromColOpCol` type-asserted both arguments after
the two-argument check. Rust now returns `None` only for non-functions or
non-two-argument shapes, while function-index and two-argument type mismatches
panic like Go. The focused `expr_util` regressions cover both cases. Applied to
a clean pre-fix `ac81699c05` tree, the mixed-argument panic assertion failed
because Rust returned `None`; it passes after the change.

## Validation

Profile: Ready for this bounded Rust package batch.

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib coercibility_names -- --nocapture` — focused valid/invalid-name regression passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,077 passed, 9 pre-existing failures, and 139 documented gap tests ignored out of 1,225 after this follow-up. The same failures remain in compare refinement, constant folding/const-level, and duration operand paths; none exercises the changed unchecked accessors or collation-name formatter.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --offline --locked -p tidb-expr --all-targets` — required cross-target compile gate.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib expr_util::tests::is_col_op_col_needs_two_columns -- --nocapture` — mixed-argument panic and non-two-argument `None` regression passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib expr_util::tests::get_func_arg_panics_on_an_out_of_range_function_index_like_go -- --nocapture` — direct-index panic regression passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint`, and `git diff --check` — Ready formatting, lint, and whitespace gates.

## Risks and boundaries

- Correctness: malformed coercibility values now fail loudly rather than
  producing a misleading `EXPLICIT` diagnostic, matching Go's trusted lookup.
- Compatibility: the public method changes from `Option<&str>` to `&str`; all
  current Rust call sites are in `tidb-expr` and were updated together.
- Performance: valid formatting remains one bounds-checked array access.
- The rest of `pkg/expression` (including expression-node construction,
  session/executor integration, and nested packages) remains an explicit
  dependency boundary in the existing historical receipts.
