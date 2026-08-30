# TiDB Go/Rust performance gate report (2026-08-30)

## Scope and environment

- Testbed: `testbed-tidb-rust-go-tps-8180973-1-85`
- Plan execution: `8228803`
- Topology: 3 Go TiDB listeners + 3 Rust TiDB listeners, sharing 3 TiKV
  stores. `lbhover` HAProxy listeners were used for engine selection:
  `tiup-peer:3391` (Go) and `tiup-peer:3392` (Rust).
- Concurrency: 10 threads.
- Private kubeconfig: `/tmp/tc8228803-new.MMk3QB/kubeconfig.yml` (mode 600).
- Rust source: `hparser-integration`; the tested binary was built from
  commit `0d79610` (planner subquery lowering plus bounded join-row append
  fixes). The branch subsequently advanced to report commit `d2ef241`.
- Rust binary: built on the TiUP Pod NVMe volume (`/tiup/rust-target-lto`),
  with the repository's default thin-LTO profile, SHA-256
  `320764be80a156cefccfea0608b9a349b59d7fad881c9c1514b1e5aad4c29dd2`.
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
6. Filter subqueries are recognized per top-level `AND` conjunct, allowing
   BenchmarkSQL's `IN (SELECT ...)` STOCK_LEVEL shape to use semi-apply
   lowering instead of the unsupported scalar rewriter.
7. Join residual-condition and partial-row assembly now bound copies to the
   visible destination schema, preventing hidden child columns from causing a
   connection-thread panic.

## Results

### Sysbench round 1 (threshold 0.80)

All 10 requested Lua subtypes ran serially, one sample per engine, in a
203-second benchmark window (under the 300-second limit). Insert and bulk
insert used fresh engine-specific empty tables; restored `test.sbtest*` data
was unchanged. The latest receipt after the join fix is:

`/tmp/tc8228803-new.MMk3QB/sysbench-r1-joinfix`

| Subtype | Go QPS | Rust QPS | Rust/Go | Gate |
|---|---:|---:|---:|---|
| `oltp_read_write.lua` | 525.37 | 247.39 | 0.4709 | FAIL |
| `oltp_read_only.lua` | 781.34 | 838.48 | 1.0731 | PASS |
| `oltp_write_only.lua` | 2167.02 | 699.59 | 0.3228 | FAIL |
| `oltp_point_select.lua` | 18887.65 | 22028.54 | 1.1663 | PASS |
| `select_random_points.lua` | 8201.92 | 9287.04 | 1.1323 | PASS |
| `select_random_ranges.lua` | 8410.07 | 4858.95 | 0.5778 | FAIL |
| `oltp_insert.lua` (isolated empty tables) | 8155.22 | 7729.37 | 0.9478 | PASS |
| `oltp_update_index.lua` | 2372.98 | 2289.97 | 0.9650 | PASS |
| `oltp_update_non_index.lua` | 6398.50 | 4393.08 | 0.6866 | FAIL |
| `bulk_insert.lua` (isolated empty tables) | 302412.67 | 177959.36 | 0.5885 | FAIL |

The first-round sysbench gate is not yet passed. Point/read and insert paths
clear the threshold, and update-index is now above 0.80 in this run. Read-write,
write-only, non-index update, random-range, and bulk paths still require
optimization.

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
the 300-second budget. Latest receipt after the planner and join fixes:

`/tmp/tc8228803-new.MMk3QB/benchmarksql-r1-joinfix`

| Workload | Go tpmTOTAL | Rust tpmTOTAL | Rust/Go | Gate |
|---|---:|---:|---:|---|
| BenchmarkSQL | 30414.62 | 12352.60 | 0.4061 | FAIL |

The planner fix accepts the standard STOCK_LEVEL `s_i_id IN (SELECT ol_i_id
...)` shape, and the latest pair has no new `connection_panic` or SQL
unsupported-expression errors. Rust throughput remains below the gate, so the
BenchmarkSQL performance gate is still failed and requires optimization.

### Thin-LTO Sysbench follow-up

To separate code-generation effects from source changes, the same
`hparser-integration` binary was rebuilt with the repository's default thin LTO
profile on the TiUP Pod NVMe disk. The binary was installed on all three Rust
listeners and each copy matched SHA256
`320764be80a156cefccfea0608b9a349b59d7fad881c9c1514b1e5aad4c29dd2`. The
full ten-subtype sweep completed in 202 seconds, with no restore and isolated
empty tables for INSERT/bulk INSERT:

| Subtype | Go QPS | Rust QPS | Rust/Go | Gate |
|---|---:|---:|---:|---|
| `oltp_read_write.lua` | 580.59 | 204.96 | 0.3530 | FAIL |
| `oltp_read_only.lua` | 780.97 | 853.40 | 1.0927 | PASS |
| `oltp_write_only.lua` | 2205.25 | 719.32 | 0.3262 | FAIL |
| `oltp_point_select.lua` | 18399.76 | 22875.93 | 1.2433 | PASS |
| `select_random_points.lua` | 8079.22 | 9198.56 | 1.1385 | PASS |
| `select_random_ranges.lua` | 8149.06 | 1.60 | 0.0002 | FAIL* |
| `oltp_insert.lua` (isolated empty tables) | 8181.14 | 7666.08 | 0.9370 | PASS |
| `oltp_update_index.lua` | 4385.97 | 2838.69 | 0.6472 | FAIL |
| `oltp_update_non_index.lua` | 6364.54 | 4293.76 | 0.6746 | FAIL |
| `bulk_insert.lua` (isolated empty tables) | 306461.04 | 194029.27 | 0.6331 | FAIL |

`select_random_ranges.lua` hit a transient TiKV `MissingLeader` error in this
short sweep. A focused retry after the listeners settled completed in 28
seconds and produced Go 8310.41 / Rust 4483.90 (ratio 0.5396), still below the
round-1 threshold. The LTO run is therefore diagnostic rather than an
acceptance result; it did not close the remaining write/range gaps.

## Acceptance status

- Round 1 (0.80): **not passed** because Sysbench and BenchmarkSQL still fail;
  TPCC and YCSB pass. The previous BenchmarkSQL join panic is fixed, but the
  0.4061 throughput ratio is not yet sufficient.
- Rounds 2 (0.90) and 3 (1.00): not started after the current push because
  round 1 has not passed.
- Every executed benchmark window stayed within the requested five-minute
  budget. No benchmark triggered a repeat BR restore.

## Next optimization targets

1. Explicit transaction write/lock paths (`oltp_read_write`, `write_only`,
   `update_non_index`, and the current update-index regression).
2. Bulk insert throughput on isolated empty tables.
3. Full Go-aligned physical-plan/cache support for random ranges.
4. Reduce BenchmarkSQL transaction overhead after correctness parity is
   established; the STOCK_LEVEL `IN` subquery is now supported.
