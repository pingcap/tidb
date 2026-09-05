# `pkg/ddl` DROP DATABASE foreign-key owner parity receipt

Status: completed Rust-only alignment for the synchronous `DROP DATABASE`
owner check. Go authority: `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; the complete recursive package
inventory and per-file walk are recorded in
`receipts/ddl_foreign_key_owner_inventory.md` (293 tracked artifacts,
262 Go sources, 30 BUILD/Bazel inputs, and 1 OWNERS file).

## Behavior restored

Go's `checkDatabaseHasForeignKeyReferred` (the `DropSchema` owner path in
`pkg/ddl/executor.go`) checks every table in the schema before scheduling the
drop. A parent table is refused with `ErrForeignKeyCannotDropParent` (3730)
when a child table in another schema still refers to it. Children in the
schema being dropped disappear in the same operation and are not blockers;
`foreign_key_checks=0` bypasses the check. Rust previously called the
boolean `Catalog::drop_database` primitive directly, so the cross-schema
owner diagnostic and its atomic refusal were missing.

Rust now scans the catalog referral graph before `DROP DATABASE`, ignores
children in the dropped schema, honors the live session switch, and renders
Go's exact parent/constraint/child message:

```text
[ddl:3730]Cannot drop table 't2' referenced by a foreign key constraint 'fk_b' on table 't3'.
```

## Focused regressions

`tidb-session::tests_foreign_key::drop_database_is_refused_by_an_outside_schema_foreign_key`
creates `test.t1`/`test.t2`, an external `test2.t3` child, and asserts the
exact 3730 diagnostic. It proves the failed operation leaves `test.t2`
available, then removes the external child and confirms the database can be
dropped. The companion
`foreign_key_checks_off_allows_dropping_database_with_external_reference`
regression proves the session switch bypasses the owner check without
removing the external child. The source-shaped executor regression
`foreign_key_ddl_owner_checks_source::drop_database_with_foreign_key_referred_reports_3730`
asserts the same code and message from the catalog owner helper.

## Ready validation

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all \
  foreign_key_ddl_owner_checks_source::drop_database_with_foreign_key_referred_reports_3730 \
  -- --exact --test-threads=1
# 1 passed; 0 failed

cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
-p tidb-session --lib tests_foreign_key -- --test-threads=1
# 53 passed; 0 failed

cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor -p tidb-session --all-targets
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint
# READY_LINT_RC=0
```

No Go, generated, platform, Bazel, or module file changed. The remaining
foreign-key parity boundaries (such as partial-index validation) are outside
this owner batch.
