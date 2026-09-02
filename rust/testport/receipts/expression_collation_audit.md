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
Grouping metadata remains an explicit gap because the Rust node model does not
yet carry Go's `BuiltinGroupingImplSig` metadata, and the separate `Values`
and `Hash64` parity gaps are unchanged.

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
