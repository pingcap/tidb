# `pkg/statistics/handle/autoanalyze/priorityqueue` parity receipt

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

The parent package has 22 artifacts and 8,132 lines. Every artifact was read
before the Rust decision. The two nested packages, `calculatoranalysis` and
`intervaltimezone`, remain separate package units for subsequent audits.

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 88 | `6abdf25c503e50c1aac91544e66158e66a2c2fc0` |
| `analysis_job_factory.go` | 402 | `2d7b16bb6f9a556962c4593ab86b7805716bfd5a` |
| `analysis_job_factory_test.go` | 527 | `f8f121a0e92f2a31c8c58b5335337d63731939d5` |
| `calculator.go` | 73 | `e79c46b6931e4c06c223e0fcc6b13cdf01865109` |
| `calculator_test.go` | 148 | `35f65b5b139a4c2a5d3172fa4a93a0d6c88493b6` |
| `dynamic_partitioned_table_analysis_job.go` | 373 | `fa49137d237c3cb9470b73eb025e4c9aedaabef1` |
| `dynamic_partitioned_table_analysis_job_test.go` | 228 | `f6da9fb202bb20f72c2bef26cc1b4dea067bf105` |
| `heap.go` | 202 | `0055af44927a10c0ff37c92a5fe6577c63fe1dbe` |
| `heap_test.go` | 320 | `13590237bd6a346ccf5403ae6ce83444875dee61` |
| `interval.go` | 161 | `0497091358cd2367c259c65be3036204aacb9f66` |
| `interval_test.go` | 419 | `67117724f605d0ba653eba1842dd599bfc1788f7` |
| `job.go` | 200 | `bf01a000d88a71953097dc60d90ad1cc55c4f40c` |
| `job_test.go` | 175 | `9d8dbf292caf60b3dd40fea438fcd17487205878` |
| `main_test.go` | 34 | `91c7de7a6817a5a39d76babc6a90e3cb7592311f` |
| `non_partitioned_table_analysis_job.go` | 278 | `5bd07bf8f69a67b2474f01c927510fba23247469` |
| `non_partitioned_table_analysis_job_test.go` | 207 | `621b3614ac00e6d4a3ce8c472cec50784ddb6f63` |
| `queue.go` | 1,274 | `540af225d5fd2c925b68b0058499150bac196af1` |
| `queue_ddl_handler.go` | 479 | `a7a64a1d822a0bab23f3c2e80c1b1ee1855ccbf9` |
| `queue_ddl_handler_test.go` | 1,213 | `60a761a734a50661365c70f229dc2f7c0b653c96` |
| `queue_test.go` | 812 | `706ff3308b2d985f48261fd9e7788deca46c65e2` |
| `static_partitioned_table_analysis_job.go` | 314 | `a9e9e0244e33d7a75ca27ebef8e25a8b3e877a97` |
| `static_partitioned_table_analysis_job_test.go` | 205 | `f179a922a9816ec3b1b6bb0d6002fe941930cd9b` |

The 11 test artifacts contain 78 assertion tests plus the shared leak-checking
`TestMain`, and no benchmark.

## Go behavior

Go owns one synchronized, live analysis scheduler. It scans real InfoSchema
tables through the statistics handle, constructs concrete table/partition
jobs, ranks them in a keyed max heap, tracks running and retry identities,
processes versioned DML changes and table locks, refreshes durations, and runs
background maintenance tickers. Its DDL notifier mutates that same queue for
all supported schema events. Concrete jobs validate against current metadata
and `mysql.analyze_jobs`, execute through `autoanalyze/exec`, refresh the stats
cache, publish hooks and warnings, and preserve Go's SQL, failure, string, and
JSON contracts.

The tests exercise this integrated behavior through the mock store, domain,
session, statistics handle, DDL notifier, failpoints, concurrency, lifecycle,
and post-ANALYZE cache/storage effects. The heap arithmetic and SQL templates
are private pieces of that package, not independent public APIs.

## Rust integration

`tidb-stats-handle-autoanalyze-priorityqueue` owns the keyed heap, all three
concrete job forms, priority calculator, job factory, interval and retry
policy, running identities, DML watermark, queue lifecycle, and DDL mutations.
The production server source scans the shared catalog/statistics/lock/global
variable state, receives DDL events after publication, recreates jobs from
current state, and executes generated ANALYZE through the ordinary session
executor. The refresher and root auto-analyze packages consume that same live
queue instead of reconstructing caller-supplied snapshots.

Source comparison initially corrected four observable divergences during integration:
the DML watermark is captured before the cache scan, static retry recreates the
whole logical table, concurrent close callers wait for the single worker reset,
and duration strings use Go's nanosecond/microsecond/millisecond units. The
unchanged validation query also drove the ordinary index-reader execution fix;
no queue-only SQL workaround remains.

A later complete reread of the shared `types.AnalysisJobJSON` contract found
two more serialization divergences in this package's `AsJSON` output. Rust's
integer-keyed `HashMap` serialized in randomized order rather than Go's sorted
decimal-text key order, and Serde rendered integral weights as `1.0` and
non-finite weights as `null` rather than Go's `1` and serialization error. The
field serializers now preserve Go's float cutovers/exponents/negative zero,
reject non-finite values, and sort integer keys. The exact JSON regression was
observed failing first on map order and then on integral-float rendering.

The 78 original assertion tests are mapped across the crate and its production
server receipts by behavior: job construction/validation and SQL, keyed heap
ordering, retry and duration rules, rebuild/refresh/close lifecycle, running
identity, DML and lock changes, DDL mutation, concurrent access, and ordinary
ANALYZE/cache effects. Go's package-level `TestMain` only installs the standard
leak checker. The separately inventoried `calculatoranalysis` child is now
complete with its exact golden matrix; `intervaltimezone` remains a distinct
open package and is not claimed by this receipt.

## Validation

- `cargo test -p tidb-stats-handle-autoanalyze-priorityqueue`
- `cargo test --manifest-path rust/Cargo.toml -p tidb-stats-handle-autoanalyze-priorityqueue tests::source_sql_and_json_shapes -- --exact --nocapture`
- `cargo check -p tidb-server --tests`
- `cargo fmt --all -- --check`
- `git diff --check`

No Go or Bazel source changed, so `make bazel_prepare` was not required.
