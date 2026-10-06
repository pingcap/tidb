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

Standalone catalog-reload, scan-limit, CTE storage, engine classification
and resource-group-tag suites now run in
their crate's `all` target as well. Historical receipts retain their original
`--test <suite>` commands; use `--test all -- <suite>` to select retained cases.
Utility and protocol suites use `--test all -- <module>` as well. The utility
printer suite keeps its own binary because it changes the process logger; the
system-time monitor keeps its own binary because its background loop never ends.
Those two retain `--test printer_contract` and `--test systimemon_source`.
New utility/protocol module-safe suites must be registered in `tests/all.rs`;
intentional isolated targets belong in their crate's Cargo.toml. Their existing
custom build scripts remain unchanged. The repeated-statfs equality check is
retired: unrelated filesystem writes can change capacity between two samples.
Go's positive-capacity case and the OS-error regression remain.

Snapshot lock-wait, transaction-size settings and lock-resolver metric suites
remain isolated because they touch process-global configuration or counters.
The session TopN/collation and statistics-loading suites also retain isolation
for their global collation mode and async statistics queues.

The source-name transaction guard, kvcache documentation-path test binary and
two mocked DDL-runner self-checks are retired. The Go LRU contract, ordinary
transaction tests and maintained live DDL runners remain the validation owners.

Aggregate, DISTINCT and sort execution belong to `tidb-executor`; the unused
`tidb-exec` state models and three result-set wrappers are retired. Select
existing live owner tests together, for example:

```bash
cargo test --locked -p tidb-executor --lib -- hash_agg::tests::min_max_skip_nulls hash_agg::tests::max_min_count hash_agg::parallel::tests::typed_count_distinct --test-threads=1
```

The variance result-metadata assertion remains in `result_field_resolver_source`.

The shell snippet self-tests for access-path readiness/statistics and grant
convergence are retired. Use the live runners that call those shared helpers;
cleanup-path safety, MySQL authentication and protobuf sync tests remain.

Use the pinned toolchain and existing profile/cache. Heavy Cloud links use one
build job; do not force an unrelated release build just to run a focused check.

Window behavior is owned by `tidb-executor::window`. The unused integer-only
window models and peer geometry in `tidb-exec` are retired, together with
checks of their private state layout. Their useful vectors run in the existing
live suite:

```bash
cargo test --locked -p tidb-executor --test all -- window_executor_source --test-threads=1
```

Test-only modules are gated at their parent declarations so ordinary server builds do not load their source. Unicode runtime tests consume the checked-in Go fixture; run `python3 rust/scripts/generate-go-simple-case.py --check` from the repository root when maintaining the generated table, rather than spawning a generator check from each ordinary test run.

Go test cases and their fixtures are the correctness reference. Do not add
Rust source-shape, call-count, file-size, or historical test-count gates.
Statistics discard-return lint tests and comment-only test/benchmark shells
are retired; use the existing owning behavioral suites. Original Go obligations
remain in the cleanup receipts, not as empty executable test registrations.
Do not treat absence of a Rust lint annotation or an empty passing test as SQL
or lifecycle parity.

The unused `tidb-exec` JSON array/object and percentile models are retired.
Their Go-backed value/merge/reset vectors run through the existing
`tidb-executor::hash_agg` and session JSON suites. Do not restore private
string-fragment accumulators or tests of their native structure sizes.

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
