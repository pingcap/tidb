# `pkg/ddl` ALTER foreign-key atomicity parity receipt

Status: completed Rust-only alignment for the multi-action ALTER ADD FOREIGN
KEY transaction boundary. Go authority: `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`,
`pkg/ddl/tests/fk/foreign_key_test.go:1145-1153`. The complete recursive
`pkg/ddl` inventory and per-file walk (293 tracked artifacts: 262 Go sources,
30 BUILD/Bazel inputs, and 1 OWNERS file) remain recorded in
`receipts/ddl_foreign_key_owner_inventory.md`; this batch touches only the
Rust ALTER carrier, focused regression, and parity receipts.

## Behavior restored

Go's `TestAddForeignKey` builds three FK actions in one `ALTER TABLE` and makes
the final action invalid (`references t1(unknown_col)`). The first two
constraints and their support indexes must not survive the failed statement.
The same test also drops `idx_c` while adding an FK that needs its `(c)`
prefix; Go returns `ErrDropIndexNeededInForeignKey` (1553) and leaves the index
in place.

Rust's public `run_alter_table_in` now detects multi-action ALTER statements
that add a foreign key, clones the catalog, and commits the clone only when
the inner action loop succeeds. Failed validation therefore rolls back both
FK metadata and automatically-created child indexes. Before running the inner
loop, a same-statement `DROP INDEX` is checked against top-level FK additions;
an index whose leading columns cover the FK's child columns is rejected with
1553 before any metadata is changed.

## Focused regression

`tidb-executor::fk_alter_meta_and_privilege_source::a_failed_multi_add_leaves_no_constraint_behind`
asserts Go's 3734 missing-column error, zero declared FKs, and retention of the
original support index after the failed three-FK statement. It then asserts the
same-statement drop/add form returns 1553 and retains `idx_c`.

## Ready validation

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all \
  a_failed_multi_add_leaves_no_constraint_behind -- --nocapture
# 1 passed; 0 failed

cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all fk_alter_ -- --test-threads=1
# 10 passed; 3 ignored; 0 failed

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --all-targets
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
# READY_LINT_RC=0
```

No Go, generated, platform, Bazel, or module file changed. Partial-index
predicate safety remains the next explicit FK ALTER boundary.
