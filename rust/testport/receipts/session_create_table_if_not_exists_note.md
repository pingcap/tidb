# `pkg/ddl` CREATE TABLE IF NOT EXISTS note parity receipt

Status: completed Rust-only alignment for Go's `CreateTableWithInfo` ignore
path (`pkg/ddl/executor.go:1217`) at Go-master
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

Go appends the swallowed `ErrTableExists` exactly once as a `Note` in the
statement context when `CREATE TABLE IF NOT EXISTS` finds an existing table.
Rust's executor already recorded that note in its `StmtContext`, but the
session dispatch layer appended a second copy when it observed `Done(false)`.
The dispatch duplicate append is removed; the normal context drain now carries
one 1050 note with the exact `Table 'test.<name>' already exists` text. The
same path covers duplicate `CREATE TABLE ... LIKE` statements.

Focused regressions:

- `tests_core::ddl::create_table_if_not_exists_records_one_note` asserts one
  `Note|1050|Table 'test.once' already exists` and one wire warning.
- `tests_core::ddl::if_exists_demotes_the_error_it_swallowed_to_a_note` keeps
  the broader CREATE/DROP/VIEW warning matrix and verifies the corrected
  single-note behavior alongside the per-name DROP notes.

Validation:

- focused session regressions: pass
- `cargo fmt --check` and `git diff --check`: pass
- locked offline `tidb-executor` all-target check: pass
- full Ready lint profile: pass

No Go, generated, platform, Bazel, or module files changed.
