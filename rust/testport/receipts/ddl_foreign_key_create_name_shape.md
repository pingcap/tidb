# `pkg/ddl` CREATE FOREIGN KEY name and shape validation

Status: completed Rust-only alignment for Go's name/shape rows in
`TestCreateTableWithForeignKeyError`. Go authority is `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; the full `pkg/ddl` inventory is
recorded in `receipts/ddl_foreign_key_owner_inventory.md`.

## Behavior restored

The CREATE path now matches Go for the remaining isolated constraint-shape
checks: empty FK names return 1280, duplicate child columns return 1060,
duplicate referenced-column shapes without a matching parent index return
1822, and a generated child support index refuses a same-named explicit index
with Go's lowercase 1061 text. FK names, referenced schemas/tables, and
referenced columns over 64 bytes return 1059 before metadata is staged. These
checks share the existing builder and therefore apply to ALTER's FK carrier
where it reaches the same shape.

## Focused regression

`tidb-executor::fk_create_error_matrix_source::fk_create_name_shape_rows_match_go`
is now live (previously ignored). It runs ten independent rows covering both
empty-name spellings, duplicate child/reference shapes, support-index name
collision, and all five over-length identifier locations, asserting exact
errno/message pairs.

## Ready validation

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all \
  fk_create_error_matrix_source::fk_create_name_shape_rows_match_go \
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

No Go, generated, platform, Bazel, or module file changed. The complete FK
CREATE matrix is live in `fk_create_error_matrix_source.rs`, including
self-reference, deferred-parent, temporary-table, partition, and pass rows.
