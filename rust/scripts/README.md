# `rust/scripts`

Operational tooling for the Rust workspace.

## Running tests

Run scoped tests from `rust/` with 12 build jobs:

```bash
cargo test --offline --locked --release -j12 -p tidb-session --lib <test_filter>
```

Go test cases and their fixtures are the correctness reference. Do not add
Rust source-shape, call-count, file-size, or historical test-count gates.

## Shared SQL server checks

Both storage engines use the ordinary session and shared catalog. Static table
descriptors and the one/two-table campaign engines have been removed. Use:

- `run-realtikv-convergence.sh` for SQL, joins, ordering and shared catalog DDL.
- `run-live-write-proof.sh` for binary prepared reads/writes and sysbench.
- `run-realtikv-catalog-load.sh`, `run-realtikv-ddl.sh` and
  `run-realtikv-ddl-notify.sh` for metadata and cross-node schema visibility.
- `run-realtikv-multi-statement-txn.sh` for transaction visibility and locking.
- `run-realtikv-region-retry.sh`, `run-realtikv-transport-retry.sh` and
  `run-realtikv-lock-recovery.sh` for storage recovery.

The private campaign harness, its six selection/range/prepared/topology/join
runners, and three older read-only/multicolumn/concurrent-auth runners were
retired with their private planner telemetry. Their old receipts
are historical; the runners above do not establish that the combined persistent
connection, leader-transfer and blocked-shutdown campaign has passed on the shared
server. That live composition still requires validation. The removal and test
coverage inventory is in `../docs/parity/current-audit/shared-server-session-repair.md`.

## When the machine gets slow, it is usually disk

Builds and tests thrash long before the disk reports itself full. Free space in
this order, cheapest first:

```bash
go clean -cache                          # 16-36GB of gorun/goeval capture cruft
rm -rf rust/target/debug/incremental     # ~53GB, keeps every compiled dependency
git worktree list                        # agent worktrees run 4-11GB EACH
```

`cargo clean` frees the same space as the second line but costs a full
workspace rebuild — reach for it last. Remove an agent worktree as soon as its
work is cherry-picked rather than batching the cleanup.
