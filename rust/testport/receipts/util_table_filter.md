# `pkg/util/table-filter` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly ten artifacts, all read in full:

- `README.md` — filter grammar, imports, comments, allow/block rules,
  escaping, quoted identifiers, Go regular expressions, default rejection,
  and last-match precedence;
- `column_filter.go` — case-insensitive column filtering;
- `compat.go` — table/config types, exact schema/table filters, lowercase
  conversion, and legacy MySQL-replication rules;
- `matchers.go` — string, universal, and regexp matchers;
- `parser.go` — table/column grammar and nonrecursive file imports;
- `table_filter.go` — table/schema filtering, case folding, and the all filter;
- `column_filter_test.go` — four tests;
- `compat_test.go` — four tests;
- `table_filter_test.go` — six tests;
- `BUILD.bazel` — one library and one test target containing the five
  production and three test files.

There is no `doc.go`, ownership file, generated/platform source, fixture,
benchmark, example test, or additional test harness. The checkout is
byte-identical to the pin.

## Rust ownership and audit result

The five modules under `rust/crates/tidb-util/src/table_filter` own the complete
package. Their fourteen Go-named owner tests reproduce every source test and
table row; `tests/table_filter_contract.rs` retains supplementary checks for
the source's concurrency-safe immutable filters, serialized field names, and
Go simple Unicode lowercase behavior.

The audit found one executable mismatch. Go `regexp` defines `\d`, `\w`,
`\s`, their negations, and word boundaries over ASCII, while Rust `regex`
uses Unicode semantics for the same spellings. Consequently Rust accepted
non-ASCII digits and letters that Go rejects. The regression failed before the
fix on Arabic-Indic digits. The existing Go-regexp rewriter is now a shared
crate-private authority used by both TiDB filter implementations and the
regexp router, so table, column, regular-expression, and legacy-wildcard paths
all receive Go semantics without duplicating policy.

The audit also removed the public Rust-only `Table::new`, `FilterError::Clone`,
and `MySQLReplicationRules::Clone` APIs. Go exposes struct construction, an
ordinary non-clonable error interface, and a slice-backed rules struct whose
copy would be shallow; the removed Rust rules clone was deep and therefore not
equivalent.

## Validation

Profile: WIP; this is one completed package within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/table-filter` — passed (14 tests).
- Before the fix,
  `cargo test -p tidb-util --locked table_filter::matchers::tests::go_perl_character_classes_are_ascii`
  — failed because `\d` matched Arabic-Indic digits; the same command passes
  after the fix.
- `cargo test -p tidb-util --locked table_filter` — 15 owner tests passed (14
  source tests plus the regexp regression).
- `cargo test -p tidb-util --locked --test table_filter_contract` — 3
  supplementary source-contract tests passed.
- `cargo test -p tidb-util --locked filter::` — 26 table-filter and dependent
  filter tests passed.
- `cargo test -p tidb-util --locked regexpr_router::` — passed.
- `cargo test -p tidb-util --locked` — passed.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; all regexp entry points now share Go's ASCII Perl-class
  and word-boundary semantics.
- Compatibility: Rust-only public conveniences were removed; all workspace
  consumers use the source-shaped struct fields and compile in validation.
- Performance: regexp rewriting happens only at filter construction; matching
  still uses compiled regexes and unchanged rule order.
