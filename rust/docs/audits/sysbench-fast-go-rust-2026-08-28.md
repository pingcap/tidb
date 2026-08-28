# Sysbench Go/Rust baseline and fast validation (2026-08-28)

## Scope

- Testbed: `benchbot-amd64-4xl-sysbench-tps-8228803-1-421`
- Shape: three Go TiDB listeners and three Rust listeners sharing three TiKV
  stores; `lbhover` listeners were used for engine selection.
- Concurrency: 10 threads.
- Rust source: `hparser-integration` at `1cd8fc3507e0070f532c2d5a50bbb7a5c07ff0ff`.
- Rust release binary SHA256: `bb32f8ef92cf3a0ac42b624618ab02228dc7679271dba939be3030e001f00452`.

## Changes validated

1. Prepared query metadata is planned without executing a NULL-parameter probe,
   so a range prepared statement does not open a snapshot or scan rows.
2. Statement/transaction snapshots reuse a cancellation carrier and avoid an
   extra point-read key allocation on the hot path.
3. `preparing_a_range_only_plans_metadata` passes on the latest branch.

## Results

The reproducible 30-second read-only run (existing restored data, no BR
restore between engines) measured:

| Workload | Go QPS | Rust QPS | Rust/Go |
|---|---:|---:|---:|
| `oltp_read_only.lua` | 870.21 | 1006.28 | 1.1564 |

After rebuilding the exact commit above and fixing the Rust restart/temporary
storage lock race, the final-binary focused run through `lbhover` measured:

| Workload | Go QPS | Rust QPS | Rust/Go |
|---|---:|---:|---:|
| `oltp_read_only.lua` | 875.66 | 1031.41 | 1.1779 |

Receipt: `/tmp/tc8228803.JvwO2R/sysbench-final-ro30-retry2`.
The empty-table insert checks use isolated tables and do not touch `sbtest*`:

| Workload | Go QPS | Rust QPS | Rust/Go |
|---|---:|---:|---:|
| `oltp_insert.lua` (`insert_sbtestN`) | 8080.30 | 9088.08 | 1.1247 |
| `bulk_insert.lua` (`bulk_sbtestN`) | 223028.16 | 153495.89 | 0.6882 |

The prepared integer `LIMIT ?` execution path was then fixed and unit-tested
(`tidb-executor`: 2 tests passed). A focused YCSB Workload E run now completes
all 10,000 operations without `SCAN_ERROR`, but remains a performance failure:

| Workload | Go QPS | Rust QPS | Rust/Go | Receipt |
|---|---:|---:|---:|---|
| `workloade` (95% scan) | 7814.5 | 602.4 | 0.0771 | `/tmp/tc8228803.JvwO2R/ycsb-workloade-postfix` |

The remaining gap is scan/range execution (Rust scan latency is roughly 10 ms
versus Go's 1.2 ms), not a parser or client accounting error.

The same five-minute-oriented sweep was repeated for the other gate thresholds
without another BR restore. Round 2 (threshold 0.90) took 154 seconds and
round 3 (threshold 1.00) took 154 seconds; both failed. Their full receipts
are `/tmp/tc8228803.JvwO2R/sysbench-fast-round2-postfix` and
`/tmp/tc8228803.JvwO2R/sysbench-fast-round3-postfix`.

| Round | Threshold | Failing subtypes |
|---:|---:|---|
| 1 | 0.80 | read_write, update_index, random_points, random_ranges, bulk_insert |
| 2 | 0.90 | read_only, update_index, random_ranges, bulk_insert |
| 3 | 1.00 | read_only, point_select, update_index, bulk_insert |

Because the requested workflow intentionally reuses the restored data, these
2-second write-heavy sweeps are directional rather than clean-room formal
acceptance runs. A later 30-second read-only sample after the write/restore
sequence measured Go 330.72 QPS and Rust 200.02 QPS (0.6048), receipt
`/tmp/tc8228803.JvwO2R/sysbench-ro30-postfix`; this confirms the testbed was
under materially different load than the earlier stable 30-second sample.

The first TPCC and BenchmarkSQL probes also remain failing: TPCC reports the
Rust executor's unsupported `IndexJoin` lowering, while BenchmarkSQL does not
complete a Rust transaction window and emits no final tpmC. Receipts:
`/tmp/tc8228803.JvwO2R/tpcc-round1` and
`/tmp/tc8228803.JvwO2R/benchmarksql-round1`.

`FAST_MODE=1 FAST_RUN_SECONDS=2` covered all ten Lua subtypes in 162 seconds
of benchmark-window time (one BR restore and cluster setup are outside this
window). Its post-fix receipt is `/tmp/tc8228803.JvwO2R/sysbench-fast-postfix`:

| Subtype | Go QPS | Rust QPS | Rust/Go | Gate 0.80 |
|---|---:|---:|---:|---|
| `oltp_read_write.lua` | 278.53 | 182.01 | 0.6535 | FAIL |
| `oltp_read_only.lua` | 419.60 | 416.89 | 0.9935 | PASS |
| `oltp_write_only.lua` | 146.36 | 129.64 | 0.8858 | PASS |
| `oltp_point_select.lua` | 11640.29 | 14039.19 | 1.2061 | PASS |
| `select_random_points.lua` | 4046.37 | 2626.20 | 0.6490 | FAIL |
| `select_random_ranges.lua` | 3421.00 | 1980.36 | 0.5789 | FAIL |
| `oltp_insert.lua` | 4637.02 | 5307.63 | 1.1446 | PASS |
| `oltp_update_index.lua` | 485.63 | 282.79 | 0.5823 | FAIL |
| `oltp_update_non_index.lua` | 509.79 | 565.87 | 1.1100 | PASS |
| `bulk_insert.lua` | 118547.04 | 86417.38 | 0.7290 | FAIL |

The fast sweep is an iteration signal, not a replacement for the formal
30-second/multi-sample gates: write workloads share one dataset and can show
lock or state contamination, while insert/bulk use the isolated empty tables
above. The full three-round 0.8/0.9/1.0 acceptance sequence remains pending.
