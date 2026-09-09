# `pkg/ddl` generated-column prior-order receipt

Status: completed Rust-only alignment for Go's `CreateNewColumn` generated
column validation. The Go authority is `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; the source inventory is recorded
in `receipts/b102.md`.

Go's `checkDependedColExist` resolves every generated-expression dependency
against the whole existing table and reports unknown names as 1054. Only after
that does `verifyColumnGenerationSingle` compare a generated dependency's
existing offset with the requested insertion position and report 3107 when it
would be defined later than the new column. Rust had resolved against the
preceding prefix, so `AS (c+1) FIRST` incorrectly surfaced 1054 even though
`c` existed and was a later generated column.

Rust now builds the added expression against all existing columns, then checks
the recorded dependency names against the requested position. The focused
`depended_generated_column_prior2_generated_column_checks` regression retains
the unknown-name 1054 assertion and now asserts 3107 for `AS (c+1) FIRST`,
while covering the legal `AFTER` and grouped forms.

No Go, generated, platform, Bazel, or module files changed.
