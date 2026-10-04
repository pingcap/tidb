# `rust/scripts`

Operational tooling for the Rust workspace.

## Running tests

Run Cargo from `rust/` so the workspace configuration is applied. In Cloud,
activate `/workspace/.cloud-setup/env.sh` in each shell and group related filters:

```bash
CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-session --lib -- <filter_a> <filter_b> --test-threads=1
```

For integration suites in `tidb-exec`, `tidb-planner` and `tidb-txnkv`, use
one aggregate target with module filters, for example:

```bash
cargo test --locked -p tidb-exec --test all -- prepared_dml_lowering_source cluster_ddl_source --test-threads=1
```

The duplicate standalone registrations for 11 suites and the private-source
relation-binding target are retired. Historical receipts retain their original
`--test <suite>` commands; use `--test all -- <suite>` to select those suites
now. Deliberately standalone targets listed in Cargo.toml remain supported.

Use the pinned toolchain and existing profile/cache. Heavy Cloud links use one
build job; do not force an unrelated release build just to run a focused check.

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

## Build artifacts and retired tooling

Check disk capacity before heavy builds. Remove only identified inactive,
regeneratable artifacts when needed, retaining current dependencies, binaries,
logs, source and recovery bundles. Broad cache deletion or worktree removal is
not an ordinary test step.

The source-line-count gate and its Cargo wrapper are retired: Go behavior and
ownership determine acceptance, not a 2,200-line threshold. The private
`select-one-profile` executable is also retired; its manually reconstructed
statement phases no longer model the ordinary session. Use the real server and
maintained workload runners for measurements. No workload speedup is established
by removing these tools.
