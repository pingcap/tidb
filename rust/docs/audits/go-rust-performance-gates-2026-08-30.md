# TiDB Go/Rust performance gate report (2026-08-30)

## Scope and environment

- Testbed: `testbed-tidb-rust-go-tps-8180973-1-85`
- Plan execution: `8228803`
- Topology: 3 Go TiDB listeners + 3 Rust TiDB listeners, sharing 3 TiKV
  stores. `lbhover` HAProxy listeners were used for engine selection:
  `tiup-peer:3391` (Go) and `tiup-peer:3392` (Rust).
- Concurrency: 10 threads.
- Private kubeconfig: `/tmp/tc8228803-new.MMk3QB/kubeconfig.yml` (mode 600).
- Rust source: `hparser-integration`, pushed commit `7a1f15d`.
- Rust binary: built on the TiUP Pod NVMe volume (`/tiup/rust-target`),
  SHA-256 `29d413c7ece3821f632d592d1630514f991630f5ff609b31135f0e366866c889`.
- Git was installed and SSH access was configured inside the TiUP Pod; no
  workstation Git state was used for the TiUP checkout.

BR restore was performed once per logical dataset. Later benchmark cells
reused the restored data. Sysbench `oltp_insert.lua` and `bulk_insert.lua`
used engine-specific empty tables with an isolated tag; existing
`test.sbtest*` tables were not dropped or used by those insert cells.

## Implemented changes

The pushed Rust change contains:

1. A clean, unpartitioned, no-residual-predicate IndexLookUp window with up
   to four integer handles now uses direct projected `BatchGet`, avoiding the
   second coprocessor table request and its transport handoff.
2. Cached single-row UPDATE/DELETE sources are promoted to `PhysicalPointGet`.
3. Repeated pessimistic lock keys are omitted from the lock RPC, matching Go's
   held-key behavior.
4. Common-handle range detachment retains bounded ranges when projection
   prunes trailing primary-key columns.
5. StreamAgg/HashAgg attachment keeps the bounded scan as a root task while
   the current partial-aggregate lowering can lose the range.

## Results

### Sysbench round 1 (threshold 0.80)

All 10 requested Lua subtypes ran serially, one sample per engine, in a
203-second benchmark window (under the 300-second limit). The latest
post-push receipt is:

`/tmp/tc8228803-new.MMk3QB/sysbench-r1-postpush`

| Subtype | Go QPS | Rust QPS | Rust/Go | Gate |
|---|---:|---:|---:|---|
| `oltp_read_write.lua` | 537.27 | 222.56 | 0.4142 | FAIL |
| `oltp_read_only.lua` | 777.46 | 849.97 | 1.0933 | PASS |
| `oltp_write_only.lua` | 2126.93 | 838.44 | 0.3942 | FAIL |
| `oltp_point_select.lua` | 18643.50 | 23840.17 | 1.2787 | PASS |
| `select_random_points.lua` | 8225.39 | 9206.97 | 1.1193 | PASS |
| `select_random_ranges.lua` | 8181.50 | 5289.91 | 0.6466 | FAIL |
| `oltp_insert.lua` (isolated empty tables) | 7936.39 | 7626.21 | 0.9609 | PASS |
| `oltp_update_index.lua` | 3155.93 | 2241.17 | 0.7101 | FAIL |
| `oltp_update_non_index.lua` | 6376.14 | 4582.27 | 0.7187 | FAIL |
| `bulk_insert.lua` (isolated empty tables) | 354595.74 | 166040.13 | 0.4683 | FAIL |

The first-round sysbench gate is not yet passed. The direct lookup change
improved the read/point paths and an earlier focused YCSB-style range test;
write-heavy and bulk paths still require optimization.

### TPCC round 1 (threshold 0.80)

The current binary reused the existing `tpcc` restore and completed in a
123-second benchmark window:

`/tmp/tc8228803-new.MMk3QB/tpcc-r1-postpush`

| Workload | Go QPS | Rust QPS | Rust/Go | Gate |
|---|---:|---:|---:|---|
| TPCC | 4229.3 | 3647.7 | 0.8625 | PASS |

The Rust TPCC log also contains `invalid connection` retries while executing
the ORDER_STATUS statement near the end of the run. The throughput ratio
clears the 0.80 threshold, but this transaction/error signal must be resolved
before treating TPCC as a clean acceptance result.

### YCSB round 1 (threshold 0.80)

`test.usertable` was restored once (100 million rows) and reused. All A–F
subtypes completed with three samples in a 90-second benchmark window:

`/tmp/tc8228803-new.MMk3QB/ycsb-r1-postpush-final`

| Subtype | Go QPS | Rust QPS | Rust/Go | Gate |
|---|---:|---:|---:|---|
| workloada | 5715.3 | 5006.1 | 0.8759 | PASS |
| workloadb | 7640.4 | 7640.8 | 1.0001 | PASS |
| workloadc | 7963.2 | 8772.8 | 1.1017 | PASS |
| workloadd | 8120.0 | 8519.2 | 1.0492 | PASS |
| workloade | 5941.5 | 5705.3 | 0.9602 | PASS |
| workloadf | 6250.8 | 5629.4 | 0.9006 | PASS |

The focused post-push E rerun also passed at 1.1015 (`/tmp/tc8228803-new.MMk3QB/ycsb-r1-postpush-e`).

### BenchmarkSQL round 1 (threshold 0.80)

The 1k-warehouse database was restored once; restore took about 16 minutes
and was outside the benchmark window. The client pair itself stayed within
the 300-second budget. Receipt:

`/tmp/tc8228803-new.MMk3QB/benchmarksql-r1-parsed`

| Workload | Go tpmTOTAL | Rust tpmTOTAL | Rust/Go | Gate |
|---|---:|---:|---:|---|
| BenchmarkSQL | 28559.71 | 3480 | 0.1218 | FAIL |

Rust repeatedly reports `Unsupported("expression form is not yet supported by
the rewriter")` for the standard STOCK_LEVEL `s_i_id IN (SELECT ol_i_id …)`
subquery, followed by a communication failure in ORDER_STATUS. This is a
correctness/support gap, not a valid passing performance result; the gate
must remain failed until that query shape is implemented.

## Acceptance status

- Round 1 (0.80): **not passed** because Sysbench and BenchmarkSQL still fail;
  TPCC and YCSB pass.
- Rounds 2 (0.90) and 3 (1.00): not started after the current push because
  round 1 has not passed.
- Every executed benchmark window stayed within the requested five-minute
  budget. No benchmark triggered a repeat BR restore.

## Next optimization targets

1. Explicit transaction write/lock paths (`oltp_read_write`, `write_only`,
   `update_non_index`, and the current update-index regression).
2. Bulk insert throughput on isolated empty tables.
3. Full Go-aligned physical-plan/cache support for random ranges.
4. `IN` subquery/Apply support required by BenchmarkSQL STOCK_LEVEL.
