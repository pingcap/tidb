# `pkg/ddl` temporary `CREATE TABLE IF NOT EXISTS` warning receipt

Status: completed Rust-only test/receipt alignment for Go's local-temporary
duplicate warning. Go authority: `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`,
`pkg/ddl/tests/serial/serial_test.go:352-360`.

`createSessionTemporaryTable` checks the session-local temporary namespace;
when `IF NOT EXISTS` finds the target it returns success (`false` at the
catalog helper boundary) and appends the swallowed `ErrTableExists` (1050) as
one Note. Rust already carried the correct duplicate branch in
`run_create_table_in`; its source-shaped regression was still ignored and
described a warning gap.

The regression
`serial_create_table_like_source::create_temporary_if_not_exists_over_existing_table_files_a_1050_warning`
now builds a permanent source and local temporary copy, reruns the guarded
`LIKE`, and asserts the false result plus the exact single Note tuple. The
neighboring pre-split and shard-row-bit temporary-copy checks are also live:
the shared `CREATE TABLE ... LIKE` path refuses each option with Go's exact
8006 diagnostic before temporary-copy creation. Only the physical-region
split/SHOW TABLE REGIONS carrier remains outside this tier.

## Ready validation

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all \
  serial_create_table_like_source::create_temporary_if_not_exists_over_existing_table_files_a_1050_warning \
  -- --exact --nocapture
# 1 passed; 0 failed

cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all serial_create_table_like_source -- --test-threads=1
# 6 passed; 1 ignored; 0 failed

cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --all-targets
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
# READY_LINT_RC=0
```

No Go, generated, platform, Bazel, or module file changed.
