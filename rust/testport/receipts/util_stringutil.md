# `pkg/util/stringutil` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full: `string_util.go`,
`string_util_test.go`, `main_test.go`, and `BUILD.bazel`. There is no package
doc, fixture, generated input/output, platform variant, README, or ownership
file. The local Go package is byte-identical to the pin.

Production behavior comprises quoted-string decoding; Unicode and binary LIKE
pattern compilation and matching; LIKE-to-regexp conversion; exact-pattern
detection; byte-string copying; display closures, memoization, and displayable
strings; SQL-mode-aware identifier quoting; deterministic label rendering;
UTF-8 position helpers; ASCII classification/lowercasing; and glob question
mark escaping.

`string_util_test.go` has seven unit tests and three benchmarks. `main_test.go`
only installs the ordinary TiDB test environment and goleak harness around the
package tests; it contains no package behavior or independently runnable test.

## Rust ownership and audit result

`rust/crates/tidb-util/src/stringutil.rs` is the sole package owner. Go strings
can hold arbitrary bytes, so `unquote`, `copy`, binary patterns, identifier
escaping, and trailing-space operations use byte slices and vectors. Unicode
pattern operations decode invalid UTF-8 one byte at a time to Go's replacement
rune, matching `[]rune(string)`.

The audit removed the public Rust-only `compile_pattern_with_escape` option
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

Other Go packages consume these utilities across planner, executor,
expression, privilege, types, table, DDL, and collation boundaries. Existing
Rust packages that already own equivalent higher-level behavior are not given
duplicate wrappers merely to mirror Go imports; their package audits remain
responsible for integration behavior. This receipt claims the complete utility
package, not completion of every consumer package.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/stringutil` — passed.
- `go test ./pkg/util/stringutil -count=1` — blocked before package execution by the existing missing `checkMapABI` symbol in `pkg/util/hack`.
- `cargo test --offline --locked -p tidb-util stringutil::tests --lib` — passed, 7 tests.
- `cargo check --offline --locked -p tidb-expr` — passed with existing warnings in `tidb-chunk`.
- `cargo test --offline --locked -p tidb-expr like` — passed, 37 tests and 10 pre-existing ignored gaps.
- `cargo bench --offline --locked -p tidb-util --bench stringutil --no-run` — passed.
- `cargo fmt -p tidb-util -p tidb-expr` — passed.
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
