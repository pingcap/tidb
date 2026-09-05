# `pkg/ddl` CREATE FOREIGN KEY partitioning validation

Status: completed Rust-only alignment for Go's partitioning rows in
`TestCreateTableWithForeignKeyError`. Go authority is `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; the full `pkg/ddl` inventory is
recorded in `receipts/ddl_foreign_key_owner_inventory.md`.

## Behavior restored

The shared CREATE/ALTER foreign-key builder now checks the relationship after
the parent resolves and before column/index validation. If either the child
statement/table carries a partition clause or the referenced `KvTable` is
partitioned, it returns Go's schema 1506 diagnostic:
`Foreign key clause is not yet supported in conjunction with partitioning`.
The check remains behind `foreign_key_checks` for unresolved parents, matching
Go's deferred checks-off behavior.

## Focused regression

`tidb-executor::fk_create_error_matrix_source::fk_create_refuses_partitioning_on_either_side`
is now live (previously ignored). It runs both Go rows: a non-partitioned child
referencing a partitioned parent, and a partitioned child referencing an
ordinary parent, asserting the exact 1506 errno/message pair.

The corresponding `TestAddForeignKey` ALTER rows are also live in
`fk_alter_meta_and_privilege_source::alter_add_foreign_key_refuses_partitioning_on_either_side`,
covering both parent and child partitioned tables before any FK metadata is
staged.

## Ready validation

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all \
  fk_create_error_matrix_source::fk_create_refuses_partitioning_on_either_side \
  -- --exact --nocapture
# 1 passed; 0 failed

cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all fk_create_error_matrix_source -- --test-threads=1
# 5 passed; 0 failed; 4 ignored

cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all alter_add_foreign_key_refuses_partitioning_on_either_side \
  -- --exact --nocapture
# 1 passed; 0 failed

cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --all-targets
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
# READY_LINT_RC=0
```

No Go, generated, platform, Bazel, or module file changed. Self-reference,
deferred-parent, temporary-table, and pass-matrix rows remain explicit
ignored boundaries in `fk_create_error_matrix_source.rs`.
