# `pkg/ddl` foreign-key partial-index safety

## Go authority and inventory

The source contract is `pkg/ddl/tests/fk/foreign_key_test.go:1185-1220`,
backed by partial-index validation in `pkg/ddl/index.go:4050-4250`.
The Go test exercises three non-partitioned shapes:

- an unsafe child predicate (`c IS NOT NULL`) that requires an automatic
  `fk_b` support index, returns 1451 on parent delete, permits dropping the
  unsafe user index, and returns 1553 when dropping `fk_b`;
- a safe child predicate (`b IS NOT NULL`) that covers the FK columns and does
  not create `fk_b`, while parent delete still returns 1451;
- a safe parent predicate (`a IS NOT NULL`) that supports the parent key and
  returns 1452 for an orphan child insert.

The package inventory was read before editing: the Go production path is
`pkg/ddl/index.go` plus FK validation in `pkg/ddl/foreign_key.go`; the focused
Go test and the Rust source-shaped carriers are under
`pkg/ddl/tests/fk/foreign_key_test.go` and
`rust/crates/tidb-executor/tests/` respectively. No Go, generated, platform,
Bazel, or build-artifact file was changed.

## Rust change

`KvTable` stores compiled partial predicates by index id. Index backfill and
all row-maintenance paths evaluate the predicate and omit rows for which it is
false or NULL. Updates handle all four predicate transitions (false/true in
either direction), so a row entering or leaving the predicate updates the
index exactly once. Dropping an index removes only entries that were actually
materialized by its predicate.

DDL accepts non-partitioned partial indexes for `CREATE TABLE`, `CREATE INDEX`,
and `ALTER TABLE ... ADD INDEX`; partitioned tables remain rejected, matching
Go. FK index coverage consults the predicate: an `IS NOT NULL` conjunction over
every FK column is safe, while other predicates are unsafe. This preserves the
Go auto-support-index and drop-index behavior above.

## Focused validation

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all partial_index_safety_rules_match_go -- --nocapture
# 1 passed; 0 failed

cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --test all fk_alter_ -- --test-threads=1
# 11 passed; 2 ignored; 0 failed
```

The broader `TestPartialIndex` carrier remains ignored because the complete
literal/type compatibility matrix, generated-column and primary-key checks,
affect-column offset maintenance, and DDL reorganization lifecycle are not
fully transcreated. The focused FK predicate semantics are no longer an
ignored gap.

## Ready profile

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --all-targets
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
```
