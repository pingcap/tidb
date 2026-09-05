# Rust session `ON UPDATE` destination validation receipt

Status: bounded Rust-only alignment batch; this receipt covers the DDL
destination-type gate for `ON UPDATE CURRENT_TIMESTAMP`.

Comparison source: Go `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723` (2026-09-06).

## Complete package inventory

Before editing, every tracked root artifact in the relevant Go packages was
enumerated and read from the fetched tree: 293 artifacts under `pkg/ddl`
(262 Go files, including 124 production files and 138 tests, plus fixture,
generated, platform, build, and metadata files), 208 under `pkg/expression`
(195 Go files, including 117 production files and 78 tests, plus generated and
build inputs), and 92 under `pkg/session` (70 Go files, including 25
production files and 45 tests, plus fixture, generated, platform, build, and
metadata files). No Go, generated, fixture, platform, or Bazel file changed.

The Rust owners were inventoried before editing: `tidb-executor` has 291
tracked files and `tidb-session` has 222, including every production source,
inline and standalone test, generated test harness input, fixture, platform
variant, Cargo/build artifact, and package metadata. The changed Rust files
are `tidb-executor/src/column_default.rs` and
`tidb-session/src/tests_column_defaults.rs`.

## Alignment

Go's `pkg/ddl/add_column.go` and `pkg/ddl/modify_column.go` reject an
`ON UPDATE` clause unless the destination field is `TIMESTAMP` or `DATETIME`.
Only after that type check do they call
`expression.IsValidCurrentTimestampExpr`, which checks the
`CURRENT_TIMESTAMP` spelling and fractional-second precision. The rejection
is `ErrInvalidOnUpdate` (1294), for CREATE, ADD COLUMN, and MODIFY COLUMN.

Rust's shared `tidb_expr::is_valid_current_timestamp_expr` intentionally
mirrors the expression helper only: it validates the function spelling and
FSP metadata and does not know which DDL path is consuming it. The DDL caller
therefore accepted `INT ON UPDATE CURRENT_TIMESTAMP`, returning success where
Go returned 1294.

`validate_on_update_current_timestamp` now applies the DDL-only type gate for
`FieldTypeCode::Timestamp | FieldTypeCode::Datetime` before invoking the
shared predicate. The existing FSP checks remain unchanged. Focused
regressions cover the direct validator and Go's 1294 boundary for CREATE,
ALTER MODIFY, and ALTER ADD, while a valid `DATETIME(3)` clause remains
accepted and preserved by `SHOW CREATE TABLE`.

## Validation

Focused validation:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib column_default::tests::on_update_validation_uses_current_timestamp_predicate -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_column_defaults::invalid_on_update_clauses_keep_tidbs_error_boundary -- --exact --nocapture --test-threads=1`
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_column_defaults::alter_column_options_preserve_computed_default_and_on_update_semantics -- --exact --nocapture --test-threads=1`

All focused tests passed (one validator regression and two session DDL
regressions).

Ready validation for this batch:

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `git diff --check`
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --all-targets`
- `GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint`

## Risks and boundaries

Only the DDL acceptance boundary for `ON UPDATE` changes. The shared
expression helper's syntax/FSP behavior, valid TIMESTAMP/DATETIME clauses,
default processing, and all non-DDL expression evaluation remain unchanged.
No Go source, generated output, fixture, platform variant, or build artifact
was modified.
