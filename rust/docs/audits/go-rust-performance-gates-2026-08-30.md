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
  commit `50d9de2` plus the then-uncommitted index short-circuit change, now
  pushed as `edd0441` (`executor: skip untouched secondary index rewrites`).
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

### Untouched-index write follow-up

For `UPDATE` statements that leave every indexed column unchanged, the write
path now skips secondary-index key/value encoding and storage calls entirely;
the condition is based on indexed column offsets and preserves generated,
unique, partitioned, and handle-changing cases. The change is pushed as
`edd0441` and was compiled on the TiUP Pod NVMe volume (binary SHA-256
`29eef156b53137612967a5e5c3d8cc5e6684f6a54d9d10e66fcf1e14027bbeda`). A
three-sample focused run completed in 53 seconds without restore:

| Subtype | Go median QPS | Rust median QPS | Rust/Go | Gate |
|---|---:|---:|---:|---|
| `oltp_update_non_index.lua` | 6361.95 | 4250.53 | 0.6681 | FAIL |

The measured ratio remains below 0.80, so the dominant cost is still the
transaction/read/commit path rather than untouched-index maintenance.

## Latest follow-up (2026-08-31)

The `hparser-integration` branch was fast-forwarded to `64aaf2e` (tablesampler
and coretestsdk parity) before this run. A thin-LTO `tidb-server` binary was
built on the TiUP Pod NVMe volume and deployed to all three Rust listeners;
the binary SHA-256 was `43fb24938d30076acc8af3d9e3ba246d6cb403bc381950dfb7c60a905efba432`.
The Go listeners were not restarted. The Rust listener smoke query passed and
no `panic`, `FATAL`, or `connection_panic` was observed in the three TiDB logs.

The ten Sysbench subtypes were rerun serially with one 10-thread sample per
engine in a 224-second window (under the 300-second limit), without BR restore:

| Subtype | Go QPS | Rust QPS | Rust/Go | Gate |
|---|---:|---:|---:|---|
| `oltp_read_write.lua` | 544.51 | 206.93 | 0.3800 | FAIL |
| `oltp_read_only.lua` | 764.70 | 863.02 | 1.1286 | PASS |
| `oltp_write_only.lua` | 2193.81 | 689.74 | 0.3144 | FAIL |
| `oltp_point_select.lua` | 18841.17 | 22615.11 | 1.2003 | PASS |
| `select_random_points.lua` | 7423.00 | 9392.86 | 1.2654 | PASS |
| `select_random_ranges.lua` | 8314.85 | 5661.04 | 0.6808 | FAIL |
| `oltp_insert.lua` (isolated empty tables) | 7979.52 | 7636.15 | 0.9570 | PASS |
| `oltp_update_index.lua` | 4897.05 | 2461.29 | 0.5026 | FAIL |
| `oltp_update_non_index.lua` | 6356.25 | 4259.06 | 0.6701 | FAIL |
| `bulk_insert.lua` (isolated empty tables) | 289507.01 | 132497.91 | 0.4577 | FAIL |

The focused write-only experiment identifies the current highest-leverage
gap: prepared `index_updates + delete_inserts` is about 850 txn/s versus about
2040 txn/s over text protocol, while non-index update combinations are about
2060 txn/s. This is evidence for a prepared explicit-transaction/index-write
interaction; changing pessimistic transaction semantics without a matching
retry/lock implementation would be unsafe. Receipt: `/tmp/tc8228803-new.MMk3QB/sysbench-r1-latest64aaf`.

### Autocommit prepared point-write follow-up (2026-08-31)

The prepared autocommit DML path was changed to open a pessimistic statement
transaction for point `UPDATE`/`DELETE` and to acquire the record key with
return-values before the first source read. This reuses the existing
lock-value cache (the text protocol already used this fold) and keeps the
prelock key classifier fail-closed for non-point writes. The change is pushed
as `db26c1b` on `hparser-integration`; `cargo check -p tidb-server --bin
tidb-server` passed, and the release binary was linked on the TiUP Pod NVMe
volume with SHA-256
`4ad25505753382a06c0b7fb20b6a8e0c2f3d5f7d54d77c1af75a7a1dbb2a81da`.

All three Rust listeners ran that binary. A serial ten-subtype Sysbench sweep
used one 10-thread sample per engine, reused the restored data, used isolated
empty tables for insert/bulk-insert, and completed in 212 seconds (under the
300-second budget):

| Subtype | Go QPS | Rust QPS | Rust/Go | Gate |
|---|---:|---:|---:|---|
| `oltp_read_write.lua` | 567.82 | 221.93 | 0.3908 | FAIL |
| `oltp_read_only.lua` | 758.68 | 832.63 | 1.0975 | PASS |
| `oltp_write_only.lua` | 2246.97 | 604.51 | 0.2690 | FAIL |
| `oltp_point_select.lua` | 18837.18 | 23197.50 | 1.2315 | PASS |
| `select_random_points.lua` | 8132.71 | 9266.78 | 1.1394 | PASS |
| `select_random_ranges.lua` | 8191.45 | 8421.26 | 1.0281 | PASS |
| `oltp_insert.lua` (isolated empty tables) | 8185.80 | 7554.97 | 0.9229 | PASS |
| `oltp_update_index.lua` | 4588.04 | 3481.30 | 0.7588 | FAIL |
| `oltp_update_non_index.lua` | 6444.14 | 3676.04 | 0.5704 | FAIL |
| `bulk_insert.lua` (isolated empty tables) | 284590.44 | 192455.32 | 0.6763 | FAIL |

The prelock fold improved the focused one-thread `oltp_update_index` sample
from 359 to 404 events/s and `oltp_update_non_index` from 361 to 384 events/s
without errors. The 10-thread gate remains below 0.80 for the write-heavy
subtypes, so round 1 is still not accepted. Receipt:
`/tmp/tc8228803-new.MMk3QB/sysbench-r1-autoprelock`.

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
