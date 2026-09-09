# `pkg/util/stringutil` — Go-master parity audit receipt

Status: complete dependency-closed audit at the current Go-master authority;
the Go helper now accepts an explicit LIKE escape while retaining legacy
one-argument callers, and the Rust owner has Go's discardable return-value
contract.

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). Relative to the
hparser integration checkout, this authority carries the explicit escape-byte
parameter on `CompileLike2Regexp`; the Go implementation uses a variadic
compatibility parameter so existing one-argument planner callers continue to
compile while new callers can pass the SQL escape byte. The Rust owner already
forwards that byte through its canonical pattern compiler.

## Complete inventory

The package has exactly four artifacts, all read in full: `string_util.go`
(573 lines), `string_util_test.go` (292 lines), `main_test.go` (33 lines), and
`BUILD.bazel` (29 lines), 927 lines total. There is no package doc, fixture, generated
input/output, platform variant, README, or ownership file. Go master differs
from the hparser integration source only by adding the `escape byte` parameter
to `CompileLike2Regexp` and passing the SQL-default backslash from its source
test rows; this checkout carries that contract via a variadic parameter for
source compatibility. No build metadata changes.

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

The inline suite now contains the seven source tests and exactly their rows,
plus the focused return-contract regression. Supplemental malformed-byte,
disabled-escape, customized-comparator, empty-memoization, and untested-helper
assertions were removed. The three Go
benchmarks are executable in `benches/stringutil.rs` with the exact source
cases. The expression LIKE consumer now calls the ordinary `compile_pattern`
entry point; no cache-only or optional-escape path remains.

The Go-master delta is carried by `compile_like_to_regexp(pattern, escape)`;
the Rust owner compiles with the supplied byte rather than hard-coding a
backslash. Every Rust call site was searched: there are no production callers
of this conversion helper, while `tidb-expr::like` independently consumes the
ordinary `compile_pattern` path and already forwards its escape byte. The
source-shaped default rows remain unchanged. The Go regression now exercises
both the legacy default and a custom escape (`+`) covering escaped `%`.

Other Go packages consume these utilities across planner, executor, expression,
privilege, types, table, DDL, and collation boundaries. Existing Rust packages
that already own equivalent higher-level behavior are not given duplicate
wrappers merely to mirror Go imports; their package audits remain responsible
for integration behavior. This receipt claims the complete utility package,
not completion of every consumer package.

The Rust owner previously marked 15 public helpers with explicit
`#[must_use]`, which is Rust-only at this Go boundary. The new
`return_values_may_be_ignored_like_go` regression failed before the fix with
15 `unused_must_use` errors. Removing those annotations makes all 15 calls
compile and pass without weakening lint settings globally.

## Validation

Profile: Ready for this package batch; the repository-wide package audit is
still continuing.

- Go-master source inventory and diff against `origin/hparser-integration` — passed; only the escape-parameter delta described above.
- Pre-fix `go test ./pkg/util/stringutil -run '^TestCompileLike2Regexp$' -count=1` — failed as expected because the one-argument helper could not accept the new escape byte.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/stringutil -run '^(TestCompileLike2Regexp|TestPatternMatch)$' -count=1` — passed, including the custom-escape regression.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/stringutil -count=1` (current hparser checkout) — passed.
- Same Go command from detached `/tmp/tidb-go-latest-c605` — passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib stringutil::tests::return_values_may_be_ignored_like_go --offline --locked -- --exact` — passed after the 15-error pre-fix failure.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib stringutil::tests --offline --locked -- --test-threads=1` — passed, 9 tests including the custom-escape and return-contract regressions.
- `cargo check --offline --locked -p tidb-expr` — passed with existing warnings in `tidb-chunk`.
- `cargo test --offline --locked -p tidb-expr like` — passed, 37 tests and 10 pre-existing ignored gaps.
- `cargo bench --offline --locked -p tidb-util --bench stringutil --no-run` — passed.
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

Go source and test files changed in this follow-up, so `make bazel_prepare` was
required but is blocked locally because the `bazel` executable is not installed.
Failpoint toggling is not applicable to this package.

## Risk

- Correctness: all source test rows pass, and both Unicode and raw-byte Go
  string semantics remain explicit. Untested exported helpers were compared
  directly with their pinned implementations but have no invented Rust tests.
- Compatibility: removes only in-tree-unused Rust-only public APIs; the sole
  production caller was migrated to the Go-shaped compiler entry point.
- Performance: matching remains the source linear greedy algorithm. The
  benchmark carrier adds no production policy or crossover threshold.
