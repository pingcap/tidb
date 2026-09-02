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

## Rust follow-up: duration-to-YEAR statement-date conversion

The rolling Go comparison is `origin/master` at
`17daba3dfde858eebef60f6e4e1bb37268269225`; the root `pkg/expression`
objects are unchanged from the earlier `a85e0fd5df` inventory. The complete
root package remains 137 direct artifacts and 128,744 lines (68 production,
60 test, seven generated, `BUILD.bazel`, and `OWNERS`); its recursive tree is
208 artifacts and 146,247 lines after including the seven nested package
boundaries. The Rust owner recheck covers all 175 `tidb-expr` artifacts and
104,998 lines, including the aggregate-test build input and every source,
inline test, standalone fixture, benchmark, and support artifact. No Go,
Bazel, generated, fixture, or platform artifact changed.

Go's `builtinCastDurationAsIntSig.evalInt` calls
`Duration.ConvertToYear(typeCtx(ctx))` for a YEAR target. With the default
flag it mixes the elapsed duration into `now.In(ctx.Location())`'s calendar
date; with `CastTimeToYearThroughConcat` it converts the TIME fields to the
packed year number before applying `AdjustYear`. Rust's `cast_to_year`
previously fell through to its signed-integer helper for every duration, so
`12:59:59` produced `125959` instead of the statement year. Rust now routes
duration operands through the existing `MySqlDuration::convert_to_year`,
constructing the statement instant from `Columns::now()` and projecting it
through the session `SessionTimeZone`; the new context seam carries the Go
concat flag while retaining the disabled default.

The focused regression was made active in
`aggregation_arithmetic_cast_source`: on a clean pre-fix `af1f9a6a3e` tree it
failed with `Int(125959)` versus `Int(2020)`. The fixed tests pin both the
statement-date path and the concat path (`00:20:12` -> `2012`).

Validation for this follow-up used the Ready profile:

- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib test_cast_duration_as_year -- --nocapture` — both focused regressions passed.
- `cargo +nightly-2026-08-22 check --offline --locked -p tidb-expr --all-targets` — owner all-target compile passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,079 passed, nine pre-existing failures, and 138 documented gap tests ignored; none is in the new duration-to-YEAR tests.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint`, and `git diff --check` — Ready formatting, lint, and whitespace gates.

Correctness risk is limited to duration operands of `CAST(... AS YEAR)`:
they now require a statement clock, as Go does, and use the session zone when
the clock crosses a local date boundary. Other YEAR source domains retain the
existing parser/integer rules. The duration conversion still reports the
datatype's existing out-of-range error rather than inventing a new warning
policy.

## Rust follow-up: duration-column comparison with folded constants

The rolling Go comparison remains `origin/master` at
`17daba3dfde858eebef60f6e4e1bb37268269225`. Before this follow-up, the complete
root `pkg/expression` inventory was rechecked: 137 direct artifacts and
128,744 lines (68 production files, 60 tests, seven generated sources,
`BUILD.bazel`, and `OWNERS`), with 208 artifacts and 146,247 lines across its
seven nested package boundaries. The Rust owner inventory covers all 175
`tidb-expr` artifacts and 104,998 lines, including source tests, standalone
fixtures, benchmark/support inputs, and the shared aggregate-test build input.
No Go, Bazel, generated, fixture, or platform artifact changed.

Go's `GetAccurateCmpType` receives a `*Constant` after `NewFunction` runs
`foldConstant`; therefore `duration_col = CONCAT('1:00', ':00')` selects the
ETDuration signature just like a literal constant, while a VARCHAR column keeps
ETString. Rust's comparison wrapper previously classified only a literal
`Expression::Constant` as constant. The concat subtree consequently selected
ETString and the duration column later reached numeric conversion with its
unspecified FSP, which panicked. Rust now uses the existing
`constant_fold::folds_to_constant` predicate when constructing `CmpOperand`,
so foldable scalar subtrees follow Go's post-fold dispatch. The runtime
duration-constant arm also compares two duration values directly after the
cast and returns the Go NULL/`<=>` result when an invalid constant casts to
NULL, avoiding numeric conversion of an unspecified-FSP duration.

The focused `operand_dispatch` regression covers the literal, foldable
`CONCAT`, duration-column/VARCHAR-column, invalid constant, and NULL-safe
invalid-constant rows. A clean pre-fix `368ab79bb1` worktree failed the test
with `nonnegative duration FSP: TryFromIntError(NegOverflow)` in
`duration::to_number`; the fixed tree passes all rows.

Validation for this follow-up used the Ready profile:

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib tests::operand_dispatch::a_duration_compares_as_a_duration_only_against_a_constant -- --nocapture` — all focused rows passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --offline --locked -p tidb-expr --all-targets` — owner all-target compile passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,080 passed, eight pre-existing failures (compare refinement, constant folding/const-level, and an external JSON schema resource), and 138 documented gap tests ignored.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint`, and `git diff --check` — Ready formatting, lint, and whitespace gates.

Correctness risk is limited to comparison dispatch for a duration column and a
constant or foldable constant subtree. Non-constant duration comparisons still
use the Go string domain, and other temporal/numeric comparison domains are
unchanged. Compatibility risk is confined to replacing a Rust panic with the
Go duration comparison or NULL result; there is no new externally visible
state or performance-sensitive path.

## Rust follow-up: comparison signature cast folding

The rolling Go authority is `origin/master` at
`17daba3dfde858eebef60f6e4e1bb37268269225`. Before editing, the complete
`pkg/expression` root was re-inventoried: 137 direct artifacts and 128,744
lines (68 production files, 60 tests, seven generated sources, `BUILD.bazel`,
and `OWNERS`), with 208 artifacts and 146,247 lines across its nested package
boundaries. The Rust `tidb-expr` owner was rechecked at 175 artifacts and
104,998 lines, including all production/test/support files, standalone
fixtures, generated inputs, platform variants, build metadata, and the shared
`rust/scripts/aggregate-tests.rs` input. No Go, Bazel, generated output,
fixture, platform, or build artifact was changed.

Go's `BuildCastFunctionWithCheck` (`pkg/expression/builtin_cast.go`) calls
`FoldConstant` for every non-JSON cast returned while
`compareFunctionClass.generateCmpSigs` builds its comparison signature. Rust's
comparison wrapper previously retained `cast_time`, `cast_duration`,
`cast_double`, and similar scalar nodes because it had no evaluation context;
this was Rust-only observable shape and moved conversion warnings to row
evaluation. The real function-builder path now carries its concrete `Columns`
context through comparison signature generation and folds the newly-created
cast in the same normal mode. The no-context AST rewriter and compatibility
tree walk remain structural, and JSON casts remain intentionally unfurled,
matching Go's exception.

The focused source-derived regressions cover valid and invalid duration casts
(including NULL-safe equality), DATE/DATETIME string casts (including one
1292 warning and a folded NULL for an invalid value), and an inexact numeric
equality whose unrefined string is cast to a REAL constant. A clean pre-fix
`a65edecc10` worktree failed
`builtin_compare::tests::invalid_duration_constants_are_rewritten_once_on_either_side`
because the valid operand was still `ScalarFunction(cast_time(Const:...))`
instead of a `Constant`; the fixed tree passes all 13 focused comparison tests.
The first cut also folded casts in the older no-context compatibility walk;
the source helper regressions for nullable integer and ENUM operands failed on
`43764fc6f8` because their unchanged arguments became typed constants. The
final split limits folding to the real builder, and both compatibility tests
pass without weakening the new cast-folding assertions.

Validation used the Ready profile:

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib builtin_compare::tests -- --nocapture --test-threads=1` — 13 focused tests passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib tests::compare_control_source::test_refine_args_with_ -- --nocapture --test-threads=1` — both compatibility-walk helper regressions passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --offline --locked -p tidb-expr --all-targets` — owner all-target compile passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,085 passed, six remaining failures (two compare-control shapes, two constant-fold rows, one const-level row, and the shared unary-minus hex expectation), and 135 documented gap tests ignored.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint`, and `git diff --check` — formatting, lint, and whitespace gates passed.

Correctness risk is limited to non-JSON casts created by comparison signature
generation with a real context; it restores Go's constant value and warning
timing without changing the context-free rewriter path. Compatibility risk is
that invalid temporal/numeric constants now become NULL or typed constants at
build time, as in Go. JSON and non-constant operands retain their prior paths;
there is no new performance-sensitive loop.
