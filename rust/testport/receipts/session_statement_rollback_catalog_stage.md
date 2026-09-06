# `pkg/executor` / `pkg/session` — statement rollback catalog staging receipt

## Scope and complete owner inventory

This batch follows Go-master (`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`,
2026-09-06) for the statement-level rollback contract exercised by the Rust
`tidb-session` driver. Before editing, the complete tracked package trees were
inventoried with `git ls-tree -r --name-only origin/master`:

| Go package tree | Tracked files | Go production | Go tests | non-Go/build/fixture inputs |
| --- | ---: | ---: | ---: | ---: |
| `pkg/executor` (including subpackages and test trees) | 502 | 198 | 244 | 60 Bazel files, 8 JSON fixtures, 9 `OWNERS`, 1 README |
| `pkg/session` (including subpackages and test trees) | 92 | 25 | 45 | 21 Bazel files, 1 generated/bootstrap marker, 1 `OWNERS` |

The owner review covered every listed production/test file, all nested
executor/session subpackages, Bazel build inputs, JSON fixtures, and metadata.
There is no checked-in `zz_dump_stmt_rollback_test.go`; the Rust module records
the real-TiDB captures that are not staged in this Go checkout. The direct Go
rollback owners are:

* `pkg/session/tidb.go:238-255` — statement execution calls `StmtRollback` on
  error and `StmtCommit` on success;
* `pkg/executor/adapter.go:852-903,1458-1464` — executor lifecycle and retry
  paths select the statement commit/rollback hook;
* `pkg/session/txn.go:742-765` and `pkg/session/txnmanager.go:293-327` — the
  hooks delegate to the transaction manager's `OnStmtCommit` /
  `OnStmtRollback` implementation;
* `pkg/executor/test/txn/txn_test.go:596-637` — batch insert/update/delete and
  duplicate-update savepoint coverage.

## Rust owners and failure

`Session::with_staged_catalog_for_table` in
`rust/crates/tidb-session/src/txn.rs` previously treated every `KvTable` in a
session without an internal transaction as cluster-backed and skipped the
catalog image. That was wrong for `Session::new` and the pipeline front end:
their `KvTable`s use the in-process `MemTableStorage`, and no outer mutation
buffer checkpoint exists. A failing multi-row statement therefore left rows
written before the error visible to the next statement.

The focused `tests_statement_rollback` captures exposed both manifestations:

* a primary-key-colliding UPDATE left rows under handles `101` and `102`;
* a duplicate-in-the-middle INSERT left the first two rows.

## Change

`TableStorage` now advertises whether its owner supplies an external statement
savepoint. Only `ClusterTableStorage` returns true. `Catalog` exposes
table-level and whole-catalog checks, and the session staging doors now skip
the image only for catalogs/tables with that external owner. Standalone and
pipeline `KvTable`s consequently use the same clone-and-restore image path as
the existing matrix-backed tests; cluster sessions retain their outer
`MutationBuffer::checkpoint`/`restore` path without an extra deep image.

Regression coverage in
`rust/crates/tidb-session/src/tests_statement_rollback.rs`:

* `a_five_row_update_colliding_on_the_third_row_leaves_the_table_unchanged`;
* `a_multi_row_insert_with_a_duplicate_in_the_middle_writes_nothing`;
* `a_shared_in_process_catalog_rolls_back_a_failed_insert` (new shared
  pipeline-shaped guard); and
* the surrounding transaction, REPLACE, panic, and auto-increment rollback
  contracts.

Focused command:

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_statement_rollback:: -- --nocapture --test-threads=1
```

Result: **7 passed, 0 failed**.

The external-owner path was also checked with
`cluster_session_node::tests::transactions::a_failed_statement_leaves_no_bytes_of_its_own_in_the_mutation_buffer`
(**1 passed, 0 failed**), confirming cluster-backed statements continue to
use the mutation-buffer savepoint.

Ready validation profile:

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --all-targets
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
```

All four commands passed; the existing workspace warnings remain non-fatal.
