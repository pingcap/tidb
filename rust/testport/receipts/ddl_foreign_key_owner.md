# `pkg/ddl` foreign-key table-owner parity receipt

Status: completed Rust-only alignment for the synchronous table lifecycle
owner checks. Go authority: `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; full package inventory and
per-file walk are recorded in
`receipts/ddl_foreign_key_owner_inventory.md`.

## Behavior restored

Go's `checkTruncateTableHasForeignKeyReferredInOwner` and
`checkDropTableHasForeignKeyReferredInOwner` both raise
`ErrTruncateIllegalForeignKey` (1701) with the child schema/table and
constraint inside the diagnostic. Rust previously let `TRUNCATE TABLE`
through without any referral check and rendered `DROP TABLE` through the
row-level 1451 path. The Rust owner now scans the same catalog referral set,
ignores a self-reference or a child dropped in the same statement, honors the
session `foreign_key_checks` switch for TRUNCATE, and emits the exact Go
message. DELETE/UPDATE retain their distinct 1451 row-level variant.

The legacy `run_truncate_table_in` entry remains an ON wrapper for direct
executor callers; the session route now passes its live session switch to
`run_truncate_table_in_with_foreign_key_checks`.

## Focused regressions

`tidb-session::tests_foreign_key::drop_table_is_refused_while_a_foreign_key_still_points_at_it`
now asserts code 1701 and the exact Go message, then verifies an atomic pair
drop still succeeds. The new
`truncate_table_is_refused_while_a_foreign_key_still_points_at_it` regression
asserts the same code/message, proves the failed truncate leaves parent rows
untouched, proves `foreign_key_checks=0` bypasses the owner check, and proves
a self-referencing table truncates successfully.

## Ready validation

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_foreign_key -- --test-threads=1
# 51 passed; 0 failed

cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor -p tidb-session --all-targets
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
PATH=... GOPATH=... TMPDIR=/tmp/tidb-codex make lint
```

The existing repository warnings remain unchanged. No Go, generated,
platform, Bazel, or module file changed.

## Boundary retained

`DROP DATABASE`'s cross-schema 3730 referral check was intentionally kept as
a separate owner batch because `Catalog::drop_database` is a boolean catalog
primitive. It is now closed by the follow-up receipt
`receipts/ddl_foreign_key_database_owner.md`; this receipt's production scope
remains limited to TRUNCATE/DROP TABLE and the shared foreign-key renderer.
