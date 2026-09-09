# `pkg/ddl` ALTER ADD FOREIGN KEY validation parity receipt

Status: completed Rust-only alignment for the ALTER-side parent validation
owner. Go authority: `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`. The full recursive `pkg/ddl`
inventory and per-file walk (293 tracked artifacts: 262 Go sources, 30
BUILD/Bazel inputs, and 1 OWNERS file) are recorded in
`receipts/ddl_foreign_key_owner_inventory.md`; this batch touches only the
Rust ALTER carrier and its focused regressions.

## Behavior restored

Go's `checkTableForeignKeyValid` (`pkg/ddl/foreign_key.go:186-211`) rejects a
same-column self-reference with `ErrCannotAddForeign` (1215). Its
`checkTableForeignKey` parent-side index rule (`:290-297`) requires the
referenced columns to be covered by a full-length index, or by the clustered
primary handle in the single-column case, and otherwise returns
`ErrForeignKeyNoIndexInParent` (1822). Rust's ALTER path previously validated
the child side and auto-created a child support index but admitted both
parent-side gaps and same-column self-reference.

`add_foreign_key_action` now runs those owner checks before allocating a
foreign-key ID or mutating metadata. Prefix indexes that do not hold the full
referenced column remain insufficient; a missing parent is still deferred when
`foreign_key_checks=0`, matching Go's deferred-reference path. CREATE TABLE's
separate historical builder and the concurrency-only TestAddForeignKey2
missing-index race are unchanged.

The exact diagnostics are:

```text
[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_1' in the referenced table 't1'
[schema:1215]Cannot add foreign key constraint
```

## Focused regressions

`tidb-executor::fk_alter_meta_and_privilege_source::alter_add_foreign_key_refuses_missing_parent_index_and_self_reference`
now runs the source-shaped ladder: an unindexed parent fails 1822 without
leaving a constraint, adding the parent index allows the FK, and a same-column
self-reference fails 1215 while the existing constraint remains. The session
regression
`tidb-session::tests_foreign_key::alter_add_foreign_key_checks_parent_index_and_self_reference`
asserts both exact code/message pairs through SQL and checks the failed add's
metadata is untouched.

## Ready validation

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all \
  fk_alter_meta_and_privilege_source::alter_add_foreign_key_refuses_missing_parent_index_and_self_reference \
  -- --exact --test-threads=1
# 1 passed; 0 failed

cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all fk_alter_meta_and_privilege_source -- --test-threads=1
# 4 passed; 8 ignored; 0 failed

cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_foreign_key::alter_add_foreign_key_checks_parent_index_and_self_reference \
  -- --exact --test-threads=1
# 1 passed; 0 failed

cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor -p tidb-session --all-targets
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex make lint
# READY_LINT_RC=0
```

No Go, generated, platform, Bazel, or module file changed. Partial-index
predicate safety remains the explicit neighboring gap in the shared
`fk_alter_meta_and_privilege_source` ledger; multi-action ALTER atomicity is
recorded in `receipts/ddl_foreign_key_alter_atomicity.md`.
