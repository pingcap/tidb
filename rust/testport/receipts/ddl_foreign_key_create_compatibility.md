# `pkg/ddl` CREATE FOREIGN KEY compatibility validation

Status: completed Rust-only alignment for the CREATE-time compatibility rows
in Go's `TestCreateTableWithForeignKeyError`. Go authority is `origin/master`
at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; the complete recursive
`pkg/ddl` inventory remains in `receipts/ddl_foreign_key_owner_inventory.md`.

## Behavior restored

Go's `checkTableForeignKey` validates each child/reference pair before the
constraint is staged. Rust's shared `build_foreign_key` now carries the
resolved child `FieldType` and performs the same CREATE/ALTER checks:

- missing child columns return 1072;
- referenced columns require a full-length leading parent index (or the
  single-column clustered integer handle), returning 1822; prefix indexes do
  not cover the reference;
- `ON DELETE/UPDATE SET NULL` rejects a NOT NULL child column with 1830; and
- type code, unsigned flag, charset, and collation mismatches return 3780 with
  Go's child/parent/constraint names.

The checks run only behind `foreign_key_checks`; deferred references remain
allowed with the switch off. Existing virtual-column and missing-reference
diagnostics retain their earlier ordering and exact messages.

## Focused regression

`tidb-executor::fk_create_error_matrix_source::fk_create_reference_compatibility_rows_match_go`
is now live (previously an ignored measured gap). It runs ten independent
source-shaped CREATE pairs and asserts every errno/message listed above,
including the `(a(5))` prefix-index rejection.

## Ready validation

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all \
  fk_create_error_matrix_source::fk_create_reference_compatibility_rows_match_go \
  -- --exact --nocapture
# 1 passed; 0 failed

cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all fk_create_error_matrix_source -- --test-threads=1
# 4 passed; 0 failed; 5 ignored

cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --all-targets
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
# READY_LINT_RC=0
```

No Go, generated, platform, Bazel, or module file changed. Temporary-table
FK refusals remain the only explicit CREATE-matrix ignored family in the
source-shaped test; self-reference, deferred-parent, constraint-name,
partitioning, and pass-matrix rows are live carriers.
