# `pkg/ddl` multi-schema conflict preflight parity receipt

Status: completed Rust-only alignment for Go's `checkOperateSameColAndIdx`
(`pkg/ddl/multi_schema_change.go:330-379`) at Go-master
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

Go collects affected column and index names by operation category before
sub-jobs run. A name appearing in two categories returns 8200
`Unsupported modify column: operate same column/index '<name>'` atomically;
the statement does not expose a later 1054/1060/1091 or leave earlier
sub-jobs applied.

Rust now performs that same category-ordered preflight in
`ddl/alter_table.rs`. It covers ADD/DROP/POSITION/MODIFY/relative column
dependencies, ADD/DROP/ALTER index names, CHANGE/RENAME expansion, grouped
ADD constraints, generated-column expressions, and expression-index column
dependencies. Non-conflicting multi-spec ALTERs retain the existing
synchronous source-order execution path.

Focused regressions in `tests_ddl_multi_schema_change_sql.rs` now assert the
exact 8200 code/message and unchanged table metadata for conflicting rename,
alter-default, CHANGE, index-visibility, index-rename, and expression-index
combinations. The successful multi-spec ALTER and duplicate-entry backfill
cases remain live to guard against over-rejecting independent operations.

Validation:

- focused conflict and success regressions: pass
- `cargo fmt --check` and `git diff --check`: pass
- locked offline `tidb-executor` all-target check: pass
- full Ready lint profile: pass

The existing modify/drop metadata test still exercises a separate rowcodec
handle edge when run without its storage carrier; it is unrelated to this
preflight and is not changed by the production path.

No Go, generated, platform, Bazel, or module files changed.
