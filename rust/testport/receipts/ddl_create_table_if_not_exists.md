# `pkg/ddl` CREATE TABLE IF NOT EXISTS note receipt

Status: completed Rust-only alignment for Go's duplicate-table warning. The
Go authority is `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; the source inventory is recorded
in `receipts/b102.md`.

Go's duplicate `CREATE TABLE IF NOT EXISTS` path turns
`infoschema.ErrTableExists` (1050) into a session Note and returns without
replacing the existing table. The same behavior applies to the
`CREATE TABLE ... LIKE` spelling.

Rust now creates the same mapped 1050 error before the duplicate early return
and appends it through the statement context. The focused
`create_table_if_not_exists_like_suppresses_and_copies` regression exercises
both LIKE and plain duplicate forms, checks both Note warnings, and confirms
the copied table remains intact.

No Go, generated, platform, Bazel, or module files changed.
