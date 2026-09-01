# `pkg/util/stringutil` — complete package transcreation

Pinned Go source: `origin/master` at
`db35d47066648fe73abce6318d53fc625df51490`.

## Complete inventory

The package has exactly four artifacts, all read in full: `string_util.go`
(573 lines), `string_util_test.go` (292 lines), `main_test.go` (33 lines), and
`BUILD.bazel` (29 lines). There is no package doc, fixture, generated
input/output, platform variant, README, or ownership file. Go master differs
from the hparser integration source only by adding the `escape byte` parameter
to `CompileLike2Regexp` and passing the SQL-default backslash from its source
test rows; no build metadata changes.

Production behavior comprises quoted-string decoding; Unicode and binary LIKE
pattern compilation and matching; LIKE-to-regexp conversion; exact-pattern
detection; byte-string copying; display closures, memoization, and displayable
strings; SQL-mode-aware identifier quoting; deterministic label rendering;
UTF-8 position helpers; ASCII classification/lowercasing; and glob question
mark escaping.

`string_util_test.go` has seven unit tests and three benchmarks:
`TestUnquote`, `TestPatternMatch`, `TestCompileLike2Regexp`,
`TestIsExactMatch`, `TestBuildStringFromLabels`, `TestEscapeGlobQuestionMark`,
and `TestMemoizeStr`, plus `BenchmarkDoMatch`,
`BenchmarkDoMatchNegative`, and `BenchmarkBuildStringFromLabels`.
`main_test.go` only installs the ordinary TiDB test environment and goleak
harness around the package tests; it contains no package behavior or
independently runnable test.

## Rust ownership and audit result

`rust/crates/tidb-util/src/stringutil.rs` is the sole package owner. Go strings
can hold arbitrary bytes, so `unquote`, `copy`, binary patterns, identifier
escaping, and trailing-space operations use byte slices and vectors. Unicode
pattern operations decode invalid UTF-8 one byte at a time to Go's replacement
rune, matching `[]rune(string)`.

The earlier audit removed the public Rust-only `compile_pattern_with_escape` option
that disabled escape handling, the UTF-8-only copy and identifier wrappers,
the duplicate byte-suffixed APIs, the `StringerFn::new` constructor, and the
public memoization implementation type. It restored the two exported Go inner
pattern entry points, the source-shaped `StringerFunc`, the canonical syntax
error value, and Go's empty-input precondition for `UnquoteChar`.

The inline suite now contains exactly the seven source tests and exactly their
rows. Supplemental malformed-byte, disabled-escape, customized-comparator,
empty-memoization, and untested-helper assertions were removed. The three Go
benchmarks are executable in `benches/stringutil.rs` with the exact source
cases. The expression LIKE consumer now calls the ordinary `compile_pattern`
entry point; no cache-only or optional-escape path remains.

The Go-master delta is now carried by `compile_like_to_regexp(pattern, escape)`;
the Rust owner compiles with the supplied byte rather than hard-coding a
backslash. Every Rust call site was searched: there are no production callers
of this conversion helper, while `tidb-expr::like` independently consumes the
ordinary `compile_pattern` path and already forwards its escape byte. The
source-shaped default rows remain unchanged, and a focused custom-escape
regression covers escaped `%`, `_`, and a non-escape backslash.

Other Go packages consume these utilities across planner, executor, expression,
privilege, types, table, DDL, and collation boundaries. Existing Rust packages
that already own equivalent higher-level behavior are not given duplicate
wrappers merely to mirror Go imports; their package audits remain responsible
for integration behavior. This receipt claims the complete utility package,
not completion of every consumer package.

## Validation

Profile: Ready for this package batch; the repository-wide package audit is
still continuing.

- Go-master source inventory and diff against `origin/hparser-integration` — passed; only the escape-parameter delta described above.
- `go test ./pkg/util/stringutil -count=1` (current hparser checkout) — passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-util stringutil::tests --lib` — passed, 8 tests including the custom-escape regression.
- `cargo check --offline --locked -p tidb-expr` — passed with existing warnings in `tidb-chunk`.
- `cargo test --offline --locked -p tidb-expr like` — passed, 37 tests and 10 pre-existing ignored gaps.
- `cargo bench --offline --locked -p tidb-util --bench stringutil --no-run` — passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `make lint` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: all source test rows pass, and both Unicode and raw-byte Go
  string semantics remain explicit. Untested exported helpers were compared
  directly with their pinned implementations but have no invented Rust tests.
- Compatibility: removes only in-tree-unused Rust-only public APIs; the sole
  production caller was migrated to the Go-shaped compiler entry point.
- Performance: matching remains the source linear greedy algorithm. The
  benchmark carrier adds no production policy or crossover threshold.
