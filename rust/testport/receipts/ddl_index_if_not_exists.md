# `pkg/ddl` ordinary index IF NOT EXISTS parity receipt

Status: completed Rust-only alignment for Go's duplicate ordinary-index
guard. The Go authority is `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; the source inventory is recorded
in `receipts/b102.md`.

Go's `checkIndexNameAndColumns` checks the index name before building hidden
expression columns. A duplicate name is 1061; with `IF NOT EXISTS`, Go files
that error as a Note and returns without changing the existing index. The
same flag is accepted for both `CREATE INDEX` and `ALTER TABLE ... ADD INDEX`.

Rust now carries the parsed guard through `IndexSpec` for both entry points,
performs the duplicate-name check before hidden-column construction, and
appends the mapped 1061 as a Note when guarded. The focused
`index_if_not_exists_skips_duplicate_create_and_alter` regression verifies
both statements leave exactly one index and produce two Note warnings.

The concurrent DDL job race and columnar-index guard variants remain explicit
boundaries. No Go, generated, platform, Bazel, or module files changed.
