# Sysbench Go/Rust baseline and fast validation (2026-08-28)

## Scope

- Testbed: `benchbot-amd64-4xl-sysbench-tps-8228803-1-421`
- Shape: three Go TiDB listeners and three Rust listeners sharing three TiKV
  stores; `lbhover` listeners were used for engine selection.
- Concurrency: 10 threads.
- Rust source: `hparser-integration` at `f2385a296b0fbb6c20a87288b9e97a5c6c4a9b07`.
- Rust release binary: `ead17693773eed45e3e5e72ab8772c42a1746b6986d991ee15e766f35294d368`.

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

The empty-table insert checks use isolated tables and do not touch `sbtest*`:

| Workload | Go QPS | Rust QPS | Rust/Go |
|---|---:|---:|---:|
| `oltp_insert.lua` (`insert_sbtestN`) | 8080.30 | 9088.08 | 1.1247 |
| `bulk_insert.lua` (`bulk_sbtestN`) | 223028.16 | 153495.89 | 0.6882 |

`FAST_MODE=1 FAST_RUN_SECONDS=2` covered all ten Lua subtypes in 143 seconds
of benchmark-window time (one BR restore and cluster setup are outside this
window). Its receipt is `/tmp/tc8228803.JvwO2R/sysbench-fast-final`.
The fast sweep is an iteration signal, not a replacement for the formal
30-second/multi-sample gates: write workloads share one dataset and can show
lock or state contamination, while insert/bulk use the isolated empty tables
above. The full three-round 0.8/0.9/1.0 acceptance sequence remains pending.

