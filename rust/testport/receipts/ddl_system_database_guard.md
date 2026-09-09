# `pkg/ddl` protected `mysql` database receipt

Status: completed Rust-only alignment for Go's protected system-database
guard. Go authority: `origin/master` at
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`,
`pkg/executor/ddl.go:359-366` (`DDLExec.executeDropDatabase`).

## Inventory and behavior

The owning Go production path checks the normalized database name before
`DropSchema` runs and returns `ErrForbiddenDDL` (8267) for `mysql`, including
when the statement carries `IF EXISTS`. The Rust walk covered the session
DDL dispatcher, the catalog drop primitive, the shared `DriverError`/MySQL
mapping, the system-schema unit module, and the existing DDL receipts. The
catalog remains policy-neutral; the session statement arm is the matching
policy boundary, so direct catalog callers can still exercise generic drop
semantics without silently changing the shared primitive.

Rust now returns the exact Go error before foreign-key checks or mutation:

```text
[ddl:8267]Drop 'mysql' database is forbidden
```

The guard is case-insensitive at the SQL boundary and leaves the bootstrap
schema available after refusal.

## Focused regression

`tidb-session::tests_system_schemas::dropping_the_mysql_schema_is_refused`
asserts both `DROP DATABASE mysql` and `DROP DATABASE IF EXISTS mysql`, checks
the exact errno/message, and confirms `USE mysql` still succeeds. The prior
documentary divergence and its stale catalog/session comments were removed.
`information_schema` remains outside this batch because Go's executor guard
only names `mysql`.

## Ready validation

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_system_schemas::dropping_the_mysql_schema_is_refused \
  -- --exact --nocapture
# 1 passed; 0 failed

cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session -p tidb-executor --all-targets
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
# READY_LINT_RC=0
```

No Go, generated, platform, Bazel, or module file changed.
