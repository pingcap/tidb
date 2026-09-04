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

## Rust follow-up: canonical expression semantic equality

The rolling Go authority is `origin/master` at
`049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5` (2026-09-03). Before editing, the
complete root `pkg/expression` inventory was rechecked: 137 direct artifacts
and 128,744 lines (68 production files, 60 tests, seven generated sources,
`BUILD.bazel`, and `OWNERS`), with 208 artifacts and 146,247 lines across the
seven nested package boundaries. There is no source delta for this package
between the prior `a85e0fd5df` authority and the current master. The Rust
`tidb-expr` owner was rechecked at 175 tracked artifacts and 105,478 lines;
its pre-edit source and support inventory includes all production files,
in-module and standalone tests, fixture/support inputs, generated-test inputs,
benchmarks, Cargo metadata, and the shared aggregate-test build input. No Go,
Bazel, generated, fixture, platform, or build artifact changed.

Go's `ExpressionsSemanticEqual` delegates to `CanonicalHashCode`, and
`simpleCanonicalizedHashCode` (`pkg/expression/scalar_function.go:622-682`)
sorts commutative child hashes, reverses the directed `LE`/`LT` forms, and
rewrites `NOT` over the four directed comparisons. Rust previously had no
canonical path: the source-shaped `TestExpressionSemanticEqual` was an empty
ignored test and callers could only compare ordinary structural hash bytes.
The Rust `Expression`, `Constant`, and `ScalarFunction` owners now expose
canonical bytes on demand and `expressions_semantic_equal` compares those
bytes. The implementation preserves Go's scalar-function flag/name encoding,
typed literal/parameter/deferred leaves, commutative ordering, directed
comparison identities, cast result-type suffix, and the source's empty inner
`NOT` default for an unknown scalar child.

The focused source-derived `test_expression_semantic_equal` now covers
`LT`/`GT`, `LE`/`GE`, all four `NOT` rewrites, `PLUS`/`MUL`/`EQ`/`AND`/`OR`
commutativity, nested canonical children, and negative direct-order/name
cases. On the pre-fix tree, activating this test failed to compile because
`Expression::canonical_hash_code` did not exist; the fixed test passes.
At that point grouping metadata remained an explicit gap; the follow-up below
closes that gap while the separate `Values` parity gap remains unchanged.

Validation for this follow-up used the Ready profile:

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib scalar_function_semantics_source -- --nocapture` — the source module ran eight tests, with all four live tests passing and four documented gap tests ignored.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --all-targets` — owner all-target compile passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,089 tests passed, five pre-existing failures (comparison-control shapes, constant folding/const-level, and the shared unary-minus hex expectation), and 134 documented gap tests ignored; none exercises the new canonical bytes.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint`, and `git diff --check` — Ready formatting, lint, and whitespace gates passed.

Correctness risk is limited to canonical plan-key bytes: semantic equality now
recognizes exactly the Go commutative/comparison identities, while ordinary
`HashCode` remains unchanged. Compatibility risk is that callers relying on a
Rust-only absence of canonical bytes now receive owned allocations; the bytes
are derived per call rather than cached, so performance-sensitive callers may
need a later cache once the Rust expression tree is fully integrated.

## Rust follow-up: scalar-function Hash64 and Equals

The rolling Go authority remains `origin/master` at
`049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5`. The complete root
`pkg/expression` inventory remains 137 direct artifacts and 128,744 lines,
with 208 artifacts and 146,247 lines across its nested package boundaries;
there is no Go source delta from the prior authority. Before this edit, the
Rust `tidb-expr` owner was rechecked at 175 tracked artifacts and 105,478
lines, including production files, in-module and standalone tests,
fixture/support and generated-test inputs, benchmarks, Cargo metadata, and
the aggregate-test build input. No Go, Bazel, generated, fixture, platform,
or build artifact changed.

Go's `ScalarFunction.Hash64` writes the scalar-function tag, lower-case
function name, nullable return type, argument count, and ordered recursive
argument hashes into the cascades FNV-1a hasher. `Equals` compares the same
function name, return type, and ordered argument trees while ignoring caches
and collation metadata. Rust previously exposed these methods only for leaf
nodes; the source-shaped `TestScalarFunctionHash64Equals` was an empty ignored
test. The Rust scalar owner now implements both methods and reuses the
existing expression-tree hash/equality recursion for nested arguments.

The focused source-derived test covers identical trees plus changed function
name, argument value, and return type. On the pre-fix `5af00badf9` tree, the
activated test failed to compile with missing `ScalarFunction::hash64` and
`ScalarFunction::equals` methods; the fixed test passes. The implementation
uses the crate's existing FNV helper, so the established leaf hash/equality
behavior and cache lifecycle remain unchanged.

Validation for this follow-up used the Ready profile:

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib scalar_function_semantics_source -- --nocapture` — five live source tests passed and three documented gap tests remained ignored.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --all-targets` — owner all-target compile passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,090 tests passed, five pre-existing failures (comparison-control shapes, constant folding/const-level, and the shared unary-minus hex expectation), and 133 documented gap tests ignored; none exercises the scalar `Hash64`/`Equals` methods.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint`, and `git diff --check` — Ready formatting, lint, and whitespace gates passed.

Correctness risk is limited to structural plan-key identity: the scalar
function's ordered argument and return-type fields now participate in the
same way as Go, while canonical semantic equality remains a separate path.
Compatibility risk is limited to the existing Rust FNV helper's established
encoding for leaf fields; no evaluator or execution path changes.

## Rust follow-up: cast target nullability

The rolling Go authority remains `origin/master` at
`17daba3dfde858eebef60f6e4e1bb37268269225`. Before this follow-up, the
dependency-closed `pkg/expression` owner was rechecked file by file: 137 direct
artifacts and 128,744 lines at the root (68 production files, 60 tests, seven
generated sources, `BUILD.bazel`, and `OWNERS`), 208 artifacts and 146,247
lines recursively. The Rust `tidb-expr` owner contains 175 tracked artifacts;
after the focused regression it has 105,358 lines. Its production, test,
fixture/support, generated-input, platform, and build metadata surfaces were
read before editing; no Go, Bazel, generated output, fixture, platform, or
build artifact changed.

Go's `BuildCastFunctionWithCheck` (`pkg/expression/builtin_cast.go:2616-2619`)
deep-copies the requested cast target and removes `NotNullFlag` from that copy
when the source expression is nullable. The old Rust
`simple_expr::build_cast_function` retained the flag, making a nullable
`cast(a as signed)` report NOT NULL and leaving the source-shaped planner test
ignored. Rust now strips only the copied target when `expr.static_type()` is
nullable; a NOT NULL source keeps the flag and the `BuildOptions` target is
never mutated.

The focused `tidb-expr` regression checks nullable and NOT NULL sources,
independent built ret types, and target immutability. The planner source test
`core_expression_eval_source::cast_ret_type_clones_share_nothing_across_builds`
is now active. A clean pre-fix `bc00456157` worktree with only the regression
test failed on the retained NOT_NULL assertion; the fixed tree passes both the
expression and planner tests.

Validation used the Ready profile:

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib simple_expr::tests::cast_target_not_null_follows_source_nullability_without_mutating_target -- --nocapture --test-threads=1` — focused regression passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-planner --test all cast_ret_type_clones_share_nothing_across_builds -- --nocapture --test-threads=1` — source-shaped planner regression passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --offline --locked -p tidb-expr --all-targets` and `... check --offline --locked -p tidb-planner --all-targets` — owner all-target checks passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint`, and `git diff --check` — Ready formatting, lint, and whitespace gates passed.

Correctness risk is limited to cast result metadata for nullable versus NOT
NULL sources. Compatibility risk is limited to restoring Go's nullable result
flag while preserving caller-owned target metadata; no evaluation algorithm or
performance-sensitive loop changes.

## Rust follow-up: BETWEEN shared coercion

The rolling Go authority remains `origin/master` at
`17daba3dfde858eebef60f6e4e1bb37268269225`. Before this follow-up, the
dependency-closed `pkg/expression` root and nested owner inventory was already
complete (137 direct artifacts / 208 recursive artifacts, including tests,
generated inputs and build metadata); the Rust `tidb-expr` owner was likewise
complete at 175 tracked artifacts. No Go, Bazel, generated output, fixture,
platform, or build artifact changed.

Go's `ResolveType4Between` and `expressionRewriter.wrapExpWithCast`
(`pkg/expression/builtin_compare.go:395-423`,
`pkg/planner/core/expression_rewriter.go:2746-2785`) derive one comparison
domain across the subject and both bounds before constructing GE/LE. The old
Rust `Expr::Between` rewrite built each comparison directly from its two raw
arms, so a string subject, DATETIME lower bound, and string upper bound used a
separate string upper comparison and returned 0 instead of Go's 1. Rust now
resolves the six Go domains and applies the matching `WrapWithCastAs*` adapter
to all three operands before either arm is built; a caller's connection
charset is passed through the string wrapper.

The source-derived planner regression
`between_string_subject_with_datetime_bound_uses_shared_coercion` is active.
A clean pre-fix `dca53c865d` worktree with the test body restored failed with
Rust result `0`; the fixed test passes. The existing integer/datetime BETWEEN
rows remain covered by the neighboring source test.

Validation used the Ready profile for the dependency-closed owner:

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-planner --test all between_string_subject_with_datetime_bound_uses_shared_coercion -- --nocapture --test-threads=1` — focused source regression passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-expr --lib rewriter::tests -- --nocapture --test-threads=1` — blocked by unrelated concurrent edits to `constant.rs`, `expression.rs`, `scalar_function.rs`, and `tests/scalar_function_semantics_source.rs` (`canonical_hash_code` compile errors); no those files were changed here.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check` — formatting and whitespace checks are clean for this batch.

Correctness risk is limited to BETWEEN's pre-comparison coercion domain and
the corresponding cast wrappers. Compatibility risk is limited to cases where
the three operands previously inferred different pairwise domains; non-BETWEEN
comparisons and the existing NOT rewrite shape are unchanged. The temporary
context-free fallback for unsupported future eval types preserves their old
shape until a corresponding Go cast is transcreated.

## Rust follow-up: arithmetic construction types, deferred folding, and binary-literal unary minus

The rolling Go authority is `origin/master` at
`049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5` (2026-09-03). Before editing, the
complete root `pkg/expression` inventory was rechecked: 137 direct artifacts
and 128,744 lines (68 production files, 60 tests, seven generated sources,
`BUILD.bazel`, and `OWNERS`), with 208 artifacts and 146,247 lines across the
recursive package boundaries. The Rust `tidb-expr` owner was rechecked at 175
tracked artifacts and 105,744 lines before this batch, including production,
in-module and standalone tests, fixture/support and generated-test inputs,
benchmarks, Cargo metadata, and the aggregate test build input. No Go, Bazel,
generated output, fixture, platform, or build artifact changed.

Three source-derived gaps were closed in one `pkg/expression` batch. Go's
`newFunctionImpl` lets each arithmetic function class replace the caller's
placeholder return type with its inferred integer/real/decimal result. Rust's
direct `RealFunctionBuilder` path previously left `plus(Column, constant)` as
`Unspecified`, so comparison refinement inserted a Rust-only `cast_signed`
around the arithmetic node. Rust now consults the existing arithmetic result
inference with the statement's unsigned-subtraction and division-precision
settings before the generic builtin table. Go's `FoldConstant` also carries a
`ParamMarker`/`DeferredExpr` provenance bit onto a folded replacement; Rust
previously dropped it and reported a strict constant. The construction fold
now marks a result with deferred provenance whenever any constant argument is
context-only. Finally, the source unary-minus table classifies a
`BinaryLiteral` as REAL (`-0x1A` -> `-26.0`); the Rust AST-tier expectation had
been stale at `DEC:-26` and is aligned with the Go table while the CHUNK tier
already agreed.

Focused regressions now cover arithmetic result typing before comparison
refinement, preservation of context-only provenance after folding a parameter,
the complete constant-folding operator-argument table, the Go `ConstLevel`
case table, and the binary/bit literal unary-minus rows. On the clean pre-fix
`d92766fd6d` worktree, the constant-folding regression failed with
`left: "cast_signed", right: "plus"`; the const-level regression failed with
`left: ConstLevel(2), right: ConstLevel(1)`; and the binary-literal row failed
with `left: "FLOAT:-26", right: "DEC:-26"`. The fixed focused tests pass.

Validation for this follow-up uses the Ready profile:

- Focused source and unit regressions: `arithmetic_builder_infers_result_type_before_comparison_refinement`, `folding_a_parameter_keeps_context_only_provenance`, `constant_folding_operator_arguments_reduce_in_place`, `test_const_level_case_table`, and `hex_and_bit_literals_are_binary_literals_in_a_numeric_context` — all pass.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --all-targets` — owner all-target compile passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,095 tests passed, two unchanged baseline comparison-control shape failures (`test_compare` and `test_compare_function_with_refine`), and 133 documented gap tests ignored.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, the pinned detached `make lint` command, and `git diff --check` — Ready formatting, lint, and whitespace gates (run before commit).

Correctness risk is limited to construction metadata and plan-cache
provenance: arithmetic nodes now carry the same inferred type Go uses, and
context-only constants cannot be frozen as strict literals. Compatibility risk
is limited to callers that intentionally depended on an unspecified arithmetic
type or the stale DECIMAL test label; value evaluation and the existing binary
literal conversion are unchanged. The new provenance marker retains a cloned
expression for later context evaluation, which adds allocation only when a
folded subtree contains a parameter/deferred argument.

## Rust follow-up: comparison refinement before AST signature casts

The rolling Go authority remains `origin/master` at
`049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5` (2026-09-03). Before editing, the
complete direct `pkg/expression` inventory was rechecked at 137 artifacts and
128,744 lines (68 production files, 60 tests, seven generated sources,
`BUILD.bazel`, and `OWNERS`); the recursive package boundary is 208 artifacts
and 146,247 lines. The Rust `tidb-expr` owner remains 175 tracked artifacts
and 105,908 lines, including production modules, in-module and standalone
tests, generated-test inputs, fixtures/support, benchmarks, Cargo metadata,
and the aggregate test build input. No Go, Bazel, generated output, fixture,
platform, or build artifact changed.

Go's `compareFunctionClass.getFunction` calls `refineArgs` before
`generateCmpSigs` (`pkg/expression/builtin_compare.go:1769-1991`). The old Rust
AST rewriter selected `GetAccurateCmpType`-equivalent casts first and only then
ran the compatibility refinement walk, so an integer column compared with a
string numeric constant stayed in the DOUBLE domain (`a < '1.0'` became
`lt(cast_double(a), cast_double('1.0'))`) and could not receive Go's floor or
ceiling rewrite. Rust now runs the context-independent integer/non-integer
constant rule before comparison wrappers. The later context-aware pass still
owns temporal conversion and warning reporting; the context-free helper uses
the warning-discarding `NoColumns` sink because AST rewriting has no statement
context.

The source-derived `ast_rewrite_refines_integer_constant_before_comparison_casts`
regression asserts the ordering directly, and the complete
`test_compare_function_with_refine` table now passes, including mirrored
operators and floor/ceiling boundaries. The signature-tail expectations in
`test_compare` were also corrected to describe the compatibility walk's
structural DECIMAL, DATETIME, JSON, and DOUBLE casts rather than claiming
context-aware constant folding on a context-free tree. A clean pre-fix
`702fb48550` worktree failed the focused source table before the first row:
`left: "lt(cast_double(col(Some(Long))), cast_double(Const:STR:1.0))"`,
`right: "lt(col(Some(Long)), Const:INT:1)"`. The fixed focused regressions
pass.

Ready validation for this follow-up used the dependency-closed `tidb-expr`
owner and repository gates:

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib compare_control_source -- --nocapture --test-threads=1` — 12 passed, 5 documented gap tests ignored.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --all-targets` — pass.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,098 passed, 0 failed, 133 documented gap tests ignored.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint`, and `git diff --check` — pass.

Correctness risk is limited to conversion warnings during the context-free AST
pass: those are intentionally discarded until a statement-aware refinement is
run, while the rounded constant and comparison domain now match Go.
Compatibility risk is limited to callers that inspect the retained structural
casts for Go's still-unmodeled exceptional constant folds; value comparison
semantics are unchanged.

## Rust follow-up: DECIMAL-to-DOUBLE type propagation

The rolling Go authority remains `origin/master` at
`049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5` (2026-09-03). Before editing, the
complete direct `pkg/expression` inventory was rechecked at 137 artifacts and
128,744 lines (68 production files, 60 tests, seven generated sources,
`BUILD.bazel`, and `OWNERS`); the recursive package boundary is 208 artifacts
and 146,247 lines. The Rust `tidb-expr` owner remains 175 tracked artifacts
and 105,908 lines, including production modules, in-module and standalone
tests, generated-test inputs, fixtures/support, benchmarks, Cargo metadata,
and the aggregate test build input. No Go, Bazel, generated output, fixture,
platform, or build artifact changed.

Go's `castAsRealFunctionClass.getFunction` invokes `PropagateType` for a
DECIMAL operand before returning the `builtinCastDecimalAsRealSig`
(`pkg/expression/builtin_cast.go:219`, `pkg/expression/expression.go:1238-1308`).
That propagation changes the nested DECIMAL's display width/scale to the
DOUBLE domain (capped at `flen=48`, `decimal=30`), preserving enough integer
digits while exposing the full 30-digit non-fixed fractional metadata. The
Rust cast wrapper previously converted the value correctly but left the
child's declared `(flen, decimal)` untouched, which was a Rust-only metadata
divergence visible to nested casts and explain/type consumers. Rust now applies
the same width/scale calculation to its owned child before constructing
`cast_double`; the Go-style clone is unnecessary because Rust's by-value
expression is already unaliased.

The focused source regression
`cast_decimal_as_real_propagates_child_metadata` fails on a clean pre-fix
`05451eccc7` worktree with `left: 5`, `right: 48` and passes after the change.
The existing 59-row aggregation/cast source module now reports 33 passed and
26 documented gaps, with no value regressions. The complete `test_cast_func_sig_as_real`
value table remains active alongside the metadata assertion.

Ready validation for this follow-up used the dependency-closed `tidb-expr`
owner and repository gates:

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib aggregation_arithmetic_cast_source -- --nocapture --test-threads=1` — 33 passed, 26 documented gap tests ignored.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --all-targets` — pass.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,099 passed, 0 failed, 133 documented gap tests ignored.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint`, and `git diff --check` — pass.

Correctness risk is limited to result-type metadata: the propagated child
remains a DECIMAL value and only its declared DOUBLE-compatible width/scale is
adjusted. Compatibility risk is limited to callers that intentionally inspect
the old unpropagated child metadata; the cast value and all non-DECIMAL cast
paths are unchanged.

## Rust follow-up: expression decorrelation and ETReal propagation

The rolling Go authority remains `origin/master` at
`049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5` (2026-09-03). Before editing, the
complete direct `pkg/expression` inventory was rechecked at 137 artifacts and
128,744 lines (68 production files, 60 tests, seven generated sources,
`BUILD.bazel`, and `OWNERS`); the recursive package boundary remains 208
artifacts and 146,247 lines. The Rust `tidb-expr` owner was rechecked at 175
tracked artifacts and 105,908 lines, including production modules, in-module
and standalone tests, generated-test inputs, fixtures/support, benchmarks,
Cargo metadata, and the aggregate test build input. No Go, Bazel, generated
output, fixture, platform, or build artifact changed.

Go's `Constant.Decorrelate` is an identity, `Column.Decorrelate` preserves a
plain column, `CorrelatedColumn.Decorrelate` replaces a column contained by the
outer schema, and `ScalarFunction.Decorrelate` recursively rewrites arguments
then calls `CleanHashCode` (`pkg/expression/constant.go:539`,
`column.go:207,691`, `scalar_function.go:451`). Rust previously had only the
leaf column methods; an expression tree had no generic decorrelation carrier.
Rust now rebuilds all four owned node variants, recursively decorrelating
scalar arguments and invalidating every argument-derived cache on the rebuilt
function. The optional schema represents Go's `nil` call for schema-independent
nodes; a supplied schema follows the correlated-column membership rule, and a
correlated node with `None` panics on the same invalid nil dereference as Go.

Go's `Expression.PropagateType` currently implements only `ETReal`, using
`setDataTypeDouble` and DECIMAL precision/scale safeguards before the decimal
cast (`pkg/expression/expression.go:1238-1308`). Rust previously kept this
behavior private to the aggregate cast wrapper. The calculation is now the
shared expression-level helper and the wrapper delegates to it, so the
constant and nested expression paths expose Go's `flen=48`, `decimal=30`
metadata while preserving the DECIMAL value domain.

The focused regression was applied to a clean pre-fix `06e5a9af6d` worktree;
it failed to compile with missing `Expression::decorrelate` and
`expression::propagate_type` symbols. The fixed tests cover constant identity,
DECIMAL metadata propagation, recursive correlated-column replacement, the
invalid nil-schema boundary, and preservation of the input tree; all six tests
pass.

Ready validation for this follow-up used the dependency-closed `tidb-expr`
owner and repository gates:

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib expression_null_const_source -- --nocapture --test-threads=1` — 6 passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --all-targets` — pass.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,102 passed, 0 failed, 132 documented gap tests ignored.
- `env PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — pass.
- Direct `rustfmt +nightly-2026-08-22 --edition 2021 --check` over the five edited Rust source files and `git diff --check` — pass. The repository-wide `cargo fmt --all -- --check` was also rerun and reports only unrelated uncommitted `tidb-stmtsummary` formatting hunks, which this Rust expression batch does not own.

Correctness risk is limited to expression-tree ownership and metadata: the
decorrelator returns a rebuilt tree and drops stale scalar caches, while
propagation changes only declared type width/scale. Compatibility risk is
limited to callers that relied on the prior absence of a generic API or on
unpropagated DECIMAL metadata; value evaluation and non-ETReal paths are
unchanged. Performance impact is one expression-tree clone during explicit
decorrelation, matching Go's scalar rebuild and avoiding aliasing.

## Rust follow-up: grouping metadata and hash identity

The rolling Go authority remains `origin/master` at
`049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5` (2026-09-03). Before editing, the
complete direct `pkg/expression` inventory was rechecked at 137 artifacts and
128,744 lines (68 production files, 60 tests, seven generated sources,
`BUILD.bazel`, and `OWNERS`); the recursive package boundary remains 208
artifacts and 146,247 lines. The Rust `tidb-expr` owner was rechecked at 175
tracked artifacts and 105,908 lines, including all production modules,
in-module and standalone tests, generated-test inputs, fixtures/support,
benchmarks, Cargo metadata, and the aggregate test build input. The relevant
Go sources and tests were read in full before editing:
`pkg/expression/builtin_grouping.go`, `scalar_function.go`,
`scalar_function_test.go`, `builtin.go`, and `util.go`. No Go, Bazel, generated,
fixture, platform, or build artifact changed.

Go's `BuiltinGroupingImplSig.SetMetadata` validates and stores the grouping
mode/marks, `defaultScalarFunctionCheck` rejects an uninitialized `GROUPING`
node, and `ReHashCode` appends the mode, mark count, each mark's size, and its
sorted keys (`pkg/expression/builtin_grouping.go:73-91`,
`scalar_function.go:298-305,757-785`). The Rust `GroupingFunction` already
implemented the pure bit-and, numeric-compare, and numeric-set algorithms, but
`ScalarFunction` had no metadata carrier: `NewFunction` silently accepted an
uninitialized grouping node, hashes omitted metadata, substitution could not
prove metadata preservation, and scalar evaluation had no grouping branch.
Rust now carries validated `GroupingMetadata` on `ScalarFunction`, exposes the
source `SetMetadata`/initialization guard, appends deterministic Go-compatible
hash bytes, preserves the metadata through clone/substitution, marks builder
results unsigned, and evaluates grouping IDs with NULL propagation.

The focused source-derived regressions activate
`TestColumnSubstituteGroupingCleansHashCode` and add the construction guard.
They verify metadata survives column substitution, stale argument hashes are
discarded and recomputed to the expected column, canonical identity changes
with the column, and bit-and evaluation returns the Go grouping flags for IDs
1 and 0. On a clean pre-fix `1e9681ed23` worktree, the guard test failed because
`new_function("grouping", ...)` returned `Ok(ScalarFunction { ... })` instead
of the required initialization error; the fixed tests pass.

Validation for this follow-up used the Ready profile:

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib test_column_substitute_grouping_cleans_hash_code -- --nocapture --test-threads=1` — focused substitution/hash/evaluation regression passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib grouping_construction_requires_metadata -- --nocapture --test-threads=1` — uninitialized construction guard passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --all-targets` — owner all-target compile passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,104 passed, 0 failed, and 131 documented gap tests ignored.
- `rustfmt +nightly-2026-08-22 --edition 2021 --check` over the four edited Rust source files, `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint`, and `git diff --check` — formatting, lint, and whitespace gates.

Correctness risk is limited to `GROUPING` nodes: metadata is validated before
use, hash bytes sort each mark's keys exactly as Go does, and NULL grouping IDs
remain NULL. Compatibility risk is that callers which relied on the prior
Rust-only acceptance of uninitialized grouping nodes now receive the source
initialization error. The standalone `GroupingFunction` API remains intact;
there is no new performance-sensitive loop beyond the source-required mark
encoding and evaluation.

## Rust follow-up: COALESCE temporal result FSP

The rolling Go authority remains `origin/master` at
`049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5` (2026-09-03). Before editing, the
complete direct `pkg/expression` inventory was rechecked at 137 artifacts and
128,744 lines (68 production files, 60 tests, seven generated sources,
`BUILD.bazel`, and `OWNERS`); the recursive package boundary remains 208
artifacts and 146,247 lines. The Rust `tidb-expr` owner remains 175 tracked
artifacts and 105,908 lines, including production modules, in-module and
standalone tests, generated-test inputs, fixtures/support, benchmarks, Cargo
metadata, and the aggregate test build input. The relevant Go implementation
and source table were read in full: `pkg/expression/builtin_compare.go`'s
`coalesceFunctionClass`, `builtinCoalesceTimeSig`,
`builtinCoalesceDurationSig`, and `pkg/expression/builtin_compare_test.go`'s
`TestCoalesce`. No Go, Bazel, generated, fixture, platform, or build artifact
changed.

Go's COALESCE builder merges the result type's temporal decimal, while its
`builtinCoalesceTimeSig.evalTime` and `builtinCoalesceDurationSig.evalDuration`
stamp the selected value with that FSP after evaluating each argument. The
`newBaseBuiltinFuncWithTp` path does not cast temporal arguments, so a first
non-NULL `TIME(0)`/`DATETIME(0)` value must render with the merged
`TIME(3)`/`DATETIME(3)` metadata. Rust's generic result-family check treated
both values as already compatible and returned the first argument unchanged,
leaving the `.000` suffix out. `ScalarFunction::coerce_to_ret_type` now has a
COALESCE-only temporal tail that updates FSP metadata without rounding the
instant, matching Go's `SetFsp` behavior; other functions continue through
the existing family conversion path.

The focused source regression `test_coalesce_fraction_promotion` covers both
duration and datetime rows through the rewritten chunk evaluator. A clean
pre-fix `b542b953ea` worktree with the temporary active assertions failed on
`left: "DUR:12:59:59"`, `right: "DUR:12:59:59.000"`; the fixed test passes and
also verifies the datetime `.000` rendering. The context-free AST helper is
not used for this assertion because it intentionally carries no static
FieldType/FSP metadata, whereas Go's bug is in the typed signature path.

Ready validation for this follow-up used the dependency-closed `tidb-expr`
owner and repository gates:

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib compare_control_source -- --nocapture --test-threads=1` — 13 passed, 4 documented gap tests ignored.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --all-targets` — pass.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,105 passed, 0 failed, 130 documented gap tests ignored.
- `rustfmt +nightly-2026-08-22 --edition 2021 --check` over the two edited Rust files and `git diff --check` — pass.

Correctness risk is limited to typed COALESCE temporal results: the selected
instant is unchanged and only its source-compatible FSP metadata is updated;
invalid metadata is not introduced because result types come from validated
expression inference. Compatibility risk is limited to callers that depended
on the prior Rust-only omission of trailing zero temporal digits. Performance
impact is one small metadata copy on each non-NULL COALESCE temporal result.

## Rust follow-up: expression-level `STR_TO_DATE` punctuation

The rolling Go authority is `origin/master` at `fc7788ff517c3407dc7e000be989ab23e6648211`.
Before editing, the complete `pkg/expression` tree was re-inventoried: 208
tracked artifacts, 146,291 lines (137 direct root artifacts, 68 production
files, 60 tests, seven generated sources, `BUILD.bazel`, and `OWNERS`, plus
the nested expression packages and their build/test/support inputs). The Rust
`tidb-expr` owner and its aggregate test inputs were rechecked before editing;
no Go, generated, fixture, platform, or Bazel file changed.

Go's `STR_TO_DATE` `%.'` token calls `unicode.IsPunct` through
`skipAllPunct`, consuming Unicode punctuation such as U+00BF INVERTED
QUESTION MARK while excluding ASCII symbols such as `+`. The datatype Rust
parser already had the source-version classifier, but the independent
expression implementation in `time_fn/calendar.rs` still used
`char::is_ascii_punctuation`. The expression-level regression
`time_fn::tests::str_to_date_punctuation_token_uses_go_unicode_categories`
failed before the change (`2013¿5` returned NULL); after the change it accepts
the Unicode punctuation and rejects `2013+5`, matching Go. The classifier is
now shared from `tidb-datatype`, keeping the Unicode 15 table and its explicit
Unicode-16 exclusions in one owner.

No Go source or generated/Bazel artifact changed. This is a bounded expression
parser fix; the broader expression package remains non-atomic while its
documented PB/vectorized/deferred gaps remain.

## Validation for expression-level `STR_TO_DATE` punctuation

- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib time_fn::tests::str_to_date_punctuation_token_uses_go_unicode_categories -- --exact --nocapture` — pre-fix failed (`NULL` instead of `2013-05-00`); after the fix, 1 focused test passed.
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype -p tidb-expr` — passed (existing warnings only).
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` and `git diff --check` — passed.
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib -- --test-threads=1` — 1,118 tests passed, 130 documented gaps were ignored, and the known loopback HTTP JSON-schema fixture failed with `WouldBlock`; this is unrelated to the punctuation change and reproduced the same resource error in the isolated retry.
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --lib -- --test-threads=1` — passed (385 tests), covering the shared classifier owner.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed.

Risks are limited to sharing a pure Unicode-category predicate across the two
Rust parser owners. The `%.'` path performs one table lookup per consumed
character; all other `STR_TO_DATE` tokens and expression evaluation paths are
unchanged.

## Rust follow-up: negative `DATE_FORMAT` week-year rendering

The same current Go authority remains `origin/master` at
`fc7788ff517c3407dc7e000be989ab23e6648211`. Before this edit, the complete
`pkg/expression` inventory was already reread (208 tracked artifacts; 137
direct-root artifacts; 68 production, 60 test, seven generated sources,
`BUILD.bazel`, `OWNERS`, and nested package/build/support inputs). Because the
formatter delegates its week-year calculation to `types.Time`, the complete
`pkg/types` owner was also inventoried: 61 tracked artifacts, 30 production
files, 29 tests, and two `BUILD.bazel` files, including the nested
`parser_driver` package; no platform or fixture files are present.

Go's `pkg/types/time.go` `convertDateFormat` writes `%X`/`%x` week-years
through `uint32` when `YearWeek` returns a negative year, producing the literal
`4294967295` (`math.MaxUint32`). Rust's `tidb-expr` formatter instead emitted a
signed `-001` for `%x` at `0000-01-01`. The focused regression
`time_fn::tests::date_format_negative_week_year_uses_go_uint32_sentinel`
failed before the change (`0000 -001`) and passes after the formatter uses the
same sentinel for negative `%X`/`%x` years. Positive week-years and all other
format tokens remain unchanged.

No Go, generated, fixture, platform, or Bazel source changed. This bounded
expression/type-formatting fix does not claim the wider package complete;
documented typed temporal and warning-state boundaries remain in the receipt.

## Validation for negative `DATE_FORMAT` week-year rendering

- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib time_fn::tests::date_format_negative_week_year_uses_go_uint32_sentinel -- --exact --nocapture` — pre-fix failed (`0000 -001` vs Go's sentinel); post-fix passed (1 focused test).
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --lib time_fn::tests::date_format_source_vectors -- --exact --nocapture` — positive and existing zero-year vectors pass.
- `OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-expr --all-targets` — passed (existing warnings only).
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` and `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed.

The compatibility risk is limited to the documented negative week-year
sentinel. The formatter now allocates the same short-lived decimal string for
negative years that it already allocates for positive year formatting; no
execution or storage path changes.

## Rust follow-up: `FROM_BASE64` packet limit

The rolling Go authority remains `origin/master` at
`fc7788ff517c3407dc7e000be989ab23e6648211`. Before editing, the complete
`pkg/expression` tree was re-inventoried: 208 tracked artifacts, 146,291 lines
(137 direct-root artifacts, 68 production files, 60 tests, seven generated
sources, `BUILD.bazel`, and `OWNERS`, plus eight nested package/build/test
boundaries). The Rust `tidb-expr` owner was also rechecked at 176 tracked
artifacts and 107,196 lines, including production modules, source-derived
tests, fixtures/support, benchmarks, Cargo metadata, and aggregate test build
inputs. The relevant Go `builtinFromBase64Sig` row/vector implementations and
`TestFromBase64Sig` table were read in full. No Go, generated, fixture,
platform, or Bazel file changed.

Go estimates the decoded length from the original input byte length before
removing spaces/tabs and CR/LF, returns NULL silently if the `int`-sized
estimate overflows, and routes an estimate above `maxAllowedPacket` through
`handleAllowedPacketOverflowed` (NULL plus warning 1301 at warning level, or a
statement error at error level). Rust's `FROM_BASE64` value helper previously
had no context and always decoded the value, so the packet-boundary rows were
an unimplemented Rust-only acceptance. The new context-aware entry point
performs the source estimate and calls `Columns::handle_allowed_packet_overflowed`
before the shared decoder; AST and chunk evaluation now use the same policy,
while the value-only helper remains available for pure source vectors.

The focused regression `tests::builtin_string_time_source::test_from_base64_sig`
covers the Go packet table (`3`, `2`, `70`, and `69` byte limits), including the
long input's embedded whitespace and the exact 1301 warning text. With the
context arm removed, the test failed before the fix because the packet-2 row
returned `Bytes([97, 98, 99])` instead of NULL; after the fix it passes all four
rows. The previously ignored `DAYOFMONTH` zero-date and `%x` week-year carrier
rows are now active as well: existing context and formatter fixes make those
source assertions executable, and their stale gap-ledger entries were removed
from `receipts/b071.md`.

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib tests::builtin_string_time_source::test_from_base64_sig \
  -- --exact --nocapture
# pre-fix: failed (packet=2 returned decoded bytes); after fix: 1 passed

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib from_base64 -- --nocapture --test-threads=1
# passed: 4 tests (value vectors plus packet-boundary carrier)
```

Ready validation for this package batch:

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets
# passed (existing warnings only)

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib -- --test-threads=1
# passed: 1,125; failed: 0; ignored: 127

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
# both passed

PATH=... GOPATH=... TMPDIR=/tmp/tidb-codex make lint
# passed
```

## Rust follow-up: `FORMAT` precision and `WEIGHT_STRING` truncation warnings

This batch stays inside the complete `pkg/expression` inventory at Go
`origin/master` `fc7788ff517c3407dc7e000be989ab23e6648211`: 208 tracked
artifacts, 146,291 Go lines (68 production files, 60 tests, seven generated
sources, `BUILD.bazel`, `OWNERS`, and eight nested package/build/test
boundaries). The Rust `tidb-expr` owner remains 176 tracked artifacts and
107,196 lines, including production modules, source-derived tests,
fixtures/support, benchmarks, Cargo metadata, and aggregate test inputs. The
Go `evalNumDecArgsForFormat`, integer coercion, and `builtinWeightStringSig`
branches plus their complete test rows were reread before editing. No Go,
generated, fixture, platform, or Bazel file changed.

`FORMAT`'s second argument is an `ETInt` in Go. Malformed string precision
therefore goes through `Context.HandleTruncate`, contributing a 1292 warning
while retaining the parsed prefix; the Rust helper previously parsed the
prefix silently. It now routes string and byte precision through the shared
warning-aware signed-integer conversion while preserving all numeric and NULL
branches. The focused regression pins the exact warning order and text for
one-warning and two-warning rows, including an empty precision.

`WEIGHT_STRING(... AS BINARY(n))` already had the source warning operation in
the Rust chunk evaluator, but its documentary test was ignored because the
default value-only helper had no warning sink. The test now evaluates the
rewritten scalar with a statement context and pins all three Go cut rows (`ab`
at 1 byte and `中` at 1/2 bytes), each with one exact 1292 message; the stale
gap receipt entry is removed.

The `FORMAT` focused test was reproduced failing before the conversion change
(the malformed precision row emitted zero warnings), then passed after it.

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib tests::builtin_string_time_source::test_format_precision_side_truncate_warning_counts \
  -- --exact --nocapture
# pre-fix: failed (-12332.123444/A emitted 0 instead of 1); after fix: 1 passed

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib tests::builtin_string_time_source::test_weight_string_binary_cut_warning \
  -- --exact --nocapture
# passed: 1 test covering all three warning rows
```

Ready validation for the `FORMAT`/`WEIGHT_STRING` package batch:

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets
# passed (existing warnings only)

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib -- --test-threads=1
# passed: 1,128; failed: 0; ignored: 125

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
# both passed

PATH=... GOPATH=... TMPDIR=/tmp/tidb-codex make lint
# passed
```

## Rust follow-up: `FROM_UNIXTIME` real-input rounding

The same complete `pkg/expression` inventory at Go `origin/master`
`fc7788ff517c3407dc7e000be989ab23e6648211` remains the package boundary:
208 tracked artifacts and 146,291 Go lines (137 direct-root artifacts, 68
production files, 60 tests, seven generated sources, `BUILD.bazel`, `OWNERS`,
and eight nested package/build/test boundaries). The Rust `tidb-expr` owner
remains 176 tracked artifacts and 107,196 lines, including production modules,
source-derived tests, fixtures/support, benchmarks, Cargo metadata, and
aggregate test inputs. The Go `evalFromUnixTime` path, `Constant.EvalDecimal`
conversion, and the complete `TestFromUnixTime` table were reread before this
edit. No Go, generated, fixture, platform, or Bazel file changed.

Go converts `KindFloat64` (and `KindFloat32`) through
`MyDecimal.FromFloat64`, which uses `strconv.FormatFloat(value, 'g', -1, 64)`;
the resulting decimal retains the shortest significant digits before
`evalFromUnixTime` rounds the complete value at FSP 6. Rust's
`unix_arg_nanos` previously formatted real values with fixed nine decimals,
which can move a value across a half-up microsecond boundary. The evaluator
now uses the shared `Decimal::from_f64` Go-shortest formatter for both real
datum kinds, preserving the existing FSP and range handling.

The focused regression uses `1451606400.0363455`: Go's shortest decimal
rounds to `2016-01-01 00:00:00.036346`, while the pre-fix Rust fixed-nine
spelling returned `.036345`. The test failed before the production change and
passes after it; the existing integral/decimal/format source vectors remain
green.

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib tests::builtin_string_time_source::test_from_unixtime_real_uses_go_shortest_decimal_before_rounding \
  -- --exact --nocapture
# pre-fix: failed (.036345 instead of .036346); after fix: 1 passed

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib tests::builtin_string_time_source::test_from_unixtime_utc_fixed \
  -- --exact --nocapture
# passed

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib time_fn::session_tz::tests::from_unixtime_goeval_vectors \
  -- --exact --nocapture
# passed
```

Ready validation for this `FROM_UNIXTIME` package batch:

```text
OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --all-targets
# passed (existing warnings only)

OPENSSL_DIR=... DYLD_FALLBACK_LIBRARY_PATH=... \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-expr --lib -- --test-threads=1
# passed: 1,126; failed: 0; ignored: 127

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
# both passed

PATH=... GOPATH=... TMPDIR=/tmp/tidb-codex make lint
# passed
```
