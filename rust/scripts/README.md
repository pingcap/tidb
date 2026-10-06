# `rust/scripts`

Operational tooling for the Rust workspace. Historical retirement details and
validation receipts are indexed in [the current audit](../docs/parity/current-audit/README.md).

## Build and test commands

Run Cargo from `rust/` so workspace configuration applies. In Cloud, source
`/workspace/.cloud-setup/env.sh` in every shell. Group related test filters:

```bash
CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-session --lib -- <filter_a> <filter_b> --test-threads=1
cargo test --locked -p tidb-exec --test all -- prepared_dml_lowering_source cluster_ddl_source --test-threads=1
```

Live runners inherit `CARGO_BUILD_JOBS`; they do not override it with fixed
parallelism. Use one job for heavy Cloud links. Build/test commands use
`--locked`. The prewrite-recovery test uses the ordinary test profile; server
runners with explicit release binary paths retain their release builds.
Choose an existing server-binary override where the runner provides one.

Planner primitive source translations belong to the planner's existing aggregate:

```bash
cargo test --locked -p tidb-planner --test all -- primitives:: --test-threads=1
```

Transaction primitives use `tidb-txnkv --test all -- primitives::`.
Live transaction/DistSQL cases use `tidb-distsql --test all` under
`transaction_runtime::`; use the maintained `run-realtikv-*.sh` runners for
cluster setup and cleanup. The former transaction test package is retired;
shared fixtures remain under `difftests/transaction-tests/fixtures`.

Module-safe integration suites use their crate's `--test all -- <module>`
aggregate. Register new suites explicitly in `tests/all.rs`; keep helper
modules owned by another suite out of the aggregate root. Intentional isolated
targets belong in Cargo.toml. Keep production code generation; ordinary tests
do not need Go-oracle regeneration.

These suites retain separate binaries or serialized execution because they
change shared state:

- Utility `--test printer_contract` changes the logger; `--test systimemon_source`
  starts a persistent monitor.
- Snapshot lock-wait, transaction-size settings and lock-resolver metrics use
  process-global configuration or counters.
- Session TopN/collation and statistics-loading tests use global collation
  modes and async statistics queues.

Select tests at the actual owner. Hash joins, statement pushdown, used-statistics
formatting and window execution belong to `tidb-executor`, for example:

```bash
cargo test --locked -p tidb-executor --test all -- window_executor_source --test-threads=1
cargo test --locked -p tidb-executor --lib -- hash_agg::tests::min_max_skip_nulls hash_agg::tests::max_min_count hash_agg::parallel::tests::typed_count_distinct --test-threads=1
```

Error-level policy belongs to `tidb-error::errctx`; dynamic initial values run
under `tidb-vardef --lib global_sysvar_initial::`. Shared differential helpers,
including result formatting, run under `difftest-result-tests --lib`.

Go cases and fixtures are the correctness reference. Keep meaningful Rust
correctness regressions. Do not add source-shape, call-count, file-size or
historical test-count gates. Gate test-only modules at their parent declaration
so server builds do not load them. Unicode runtime tests use the checked-in Go
fixture; when maintaining its table, run from repository root:

```bash
python3 rust/scripts/generate-go-simple-case.py --check
```

## Shared SQL server checks

Both storage engines use the ordinary session and shared catalog. Use:

- `run-realtikv-convergence.sh` for SQL, joins, ordering and shared catalog DDL.
- `run-live-write-proof.sh` for binary prepared reads/writes and sysbench.
- `run-realtikv-catalog-load.sh`, `run-realtikv-ddl.sh` and
  `run-realtikv-ddl-notify.sh` for metadata and cross-node schema visibility.
- `run-realtikv-multi-statement-txn.sh` for transaction visibility and locking.
- `run-realtikv-region-retry.sh`, `run-realtikv-transport-retry.sh` and
  `run-realtikv-lock-recovery.sh` for storage recovery.

A runner passing alone does not establish that persistent connections,
leader transfer and blocked shutdown pass together. Keep those combined live
obligations explicit. Retain cleanup-path safety, MySQL authentication and
protobuf-sync tests; they protect the maintained runners and tools.

## Native synchronization and build artifacts

Native client fixes belong in `client-rust` first. Use the maintained
`sync-tikv-client-rs.sh` workflow from the repository root after validation;
keep patches and protobuf regeneration, and reconcile failed patches. Do not
hand-copy vendored client code. Use the shared Cloud target directory to avoid
building a second dependency cache.

Check capacity before heavy builds. Remove only identified inactive,
regeneratable artifacts when needed, retaining current dependencies, binaries,
logs, source and recovery bundles. Do not run `cargo clean`, purge dependency
caches or remove worktrees as an ordinary test step. Measure workloads with
the real server and maintained runners; tooling cleanup alone does not prove
a workload speedup.

Parser and lexer comparisons share `difftest`: from `rust/`, run
`cargo test --locked -p difftest --test all -- --test-threads=1`.
Source-inventory generation/checking is an explicit maintenance operation; see
[the inventory workflow](../difftests/INTEGRATION_PLAN_INVENTORY.md).
