# `pkg/util/table-filter` — complete package transcreation

Status: complete dependency-closed audit at the current Go-master authority;
the concrete column-rule API missing from the hparser branch has been
restored, with the legacy interface-returning parser retained.

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). This refresh
supersedes the earlier `0bc44483e3e41a8ea917d4382dc202369468d200` pin.

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
- `column_filter_test.go` — five tests;
- `compat_test.go` — four tests;
- `table_filter_test.go` — six tests;
- `BUILD.bazel` — one library and one test target containing the five
  production and three test files.

There is no `doc.go`, ownership file, generated/platform source, fixture,
  benchmark, example test, or additional test harness. Go master differs from
  the hparser integration source only by exporting the `ColumnFilterRules`
  concrete rule-list type, adding `ParseColumnFilterRules`, and making
  `ParseColumnFilter` delegate to it; this batch restores that source delta and
  adds one focused API regression. The build target is unchanged.

## Rust ownership and audit result

The five modules under `rust/crates/tidb-util/src/table_filter` own the complete
package. Their fourteen Go-named owner tests reproduce every source test and
table row; `tests/table_filter_contract.rs` retains supplementary checks for
the source's concurrency-safe immutable filters, serialized field names, and
Go simple Unicode lowercase behavior. `ColumnFilterRules` is now the public,
non-clonable parsed rule list, with an inherent `match_column` method and the
existing `ColumnFilter` trait implementation; `parse_column_filter` preserves
the interface-returning Go entry point by boxing that concrete value.

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
equivalent. All Rust call sites were searched: no production Rust consumer
currently invokes `ParseColumnFilterRules`; the exported API is nevertheless
part of the Go package contract and is exercised by the focused regression.

The concrete `ColumnFilterRules::match_column` method also carried one
Rust-only `#[must_use]` diagnostic. The existing
`filter_return_values_may_be_ignored_like_go` contract regression now discards
that result under `#[deny(unused_must_use)]`: the detached pre-fix owner failed
with exactly one diagnostic, while the corrected table-filter owner passes.

## Validation

Profile: Ready for this package batch; the repository-wide package audit is
still continuing.

- Go-master source inventory and diff against `origin/hparser-integration` — passed; only the concrete column-rule API delta described above.
- Pre-fix detached worktree run with the new API regression — failed at compile time because `ParseColumnFilterRules` was undefined.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/util/table-filter -run '^TestParseColumnFilterRules$' -count=1` — passed.
- `go test ./pkg/util/table-filter -count=1` (current hparser checkout) — passed (15 tests, including the concrete API regression).
- Before the fix,
  `cargo test -p tidb-util --locked table_filter::matchers::tests::go_perl_character_classes_are_ascii`
  — failed because `\d` matched Arabic-Indic digits; the same command passes
  after the fix.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-util table_filter --lib` — 16 owner tests passed (14 source tests plus the regexp and concrete-API regressions).
- `cargo test -p tidb-util --locked --test table_filter_contract` — 3
  supplementary source-contract tests passed.
- `cargo test -p tidb-util --locked filter::` — 26 table-filter and dependent
  filter tests passed.
- `cargo test -p tidb-util --locked regexpr_router::` — passed.
- `cargo test -p tidb-util --locked` — passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, `make lint`, and
  `git diff --check` — passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --test table_filter_contract filter_return_values_may_be_ignored_like_go -- --exact --nocapture` — passed; the detached pre-fix owner failed with exactly one `unused_must_use` diagnostic.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util table_filter --lib -- --test-threads=1` — 16 owner tests passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --test table_filter_contract -- --test-threads=1` — 4 contract tests passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --all-targets` — passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util filter:: -- --test-threads=1` — 23 table/filter dependent tests passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util regexpr_router:: -- --test-threads=1` — 9 regexp-router dependent tests passed.

Go production and test files changed, including a new top-level test, so
`make bazel_prepare` was required and is blocked locally because the `bazel`
executable is not installed.

## Risk

- Correctness: reduced; all regexp entry points now share Go's ASCII Perl-class
  and word-boundary semantics.
- Compatibility: Rust-only public conveniences were removed; all workspace
  consumers use the source-shaped struct fields and compile in validation.
- Performance: regexp rewriting happens only at filter construction; matching
  still uses compiled regexes and unchanged rule order.
