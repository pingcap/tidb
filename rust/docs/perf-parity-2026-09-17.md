# Rust node vs Go, same box, 2026-09-17

Measured on the shared benchmark VM; every comparison is same-box, interleaved, and answer-checked where the harness supports it. Harness scripts and raw results live in the session scratchpad (`tpch-run.sh`, `matrix-go.sh`, `cpu-both.sh`, `ab-futex.sh`, `cop-inflight.sh`).

Environment: new VM (kernel 6.18.44-fc-v33), 4 cores @ 2.8 GHz, 15 GB, block device throttled
(~11 MB/s reads at 95% busy). Absolute times are not comparable with earlier VMs; every
comparison below is same-box, interleaved, and answer-checked where the harness supports it.

## TPC-H SF1, warm, min of 2 counter-balanced rounds, `tiup bench tpch --check`, 0 errors
Three-way: Go / pre-sync 4746dbdb / head ceaaa790 (results/tpch-sf1-3way):
total Go 28.14 s, pre 31.33 s (1.113x), head 31.01 s (1.102x). The 122 upstream commits are
net slightly positive; the one real regression is Q13 (hash aggregate): head 1.67x vs pre
1.23x (0.50 s vs 0.37 s, Go 0.30 s). Q4 1.30->1.14, Q12 1.31->1.20, Q16 1.28->1.00 improved.
Scan-bound queries are behind for pre and head alike on this box (Q10 1.31, Q12 1.20-1.31,
Q14 1.14, Q19 1.13, Q15 1.12-1.18): the slower TiKV exposes the node's shallower cop
pipelining (task #29 territory) rather than any executor change.

## sysbench, 4 threads, 20 s, mean of 2 ABBA rounds, fresh tables per run (results/matrix-sync-4t*)
Rust/Go throughput, latency, p95: point_select 1.21 / 0.82 / 0.71; write_only 1.18 / 0.85 /
0.85; insert 1.15 / 0.87 / 0.84; delete 1.15 / 0.87 / 0.85; read_only 1.12 / 0.89 / 0.87;
update_index 1.08 / 0.92 / 0.88; read_write 1.06 / 0.94 / 0.97; update_non_index 0.97 / 1.04 /
1.06; select_random_points 0.98 / 1.03 / 0.97; select_random_ranges 0.97 / 1.03 / 0.97;
bulk_insert 0.60 / 1.67 (4.0 vs 2.4 tps over 20 s on the throttled disk; noisy).
TPC-C (2 warehouses, 60 s): tpmC 0.96; NEW_ORDER 0.96 / 1.02 / 1.12 (p99); PAYMENT 1.01 /
0.95 / 1.15; ORDER_STATUS 1.04 / 1.27 / 1.85; DELIVERY 1.01 / 1.12 / 1.15; STOCK_LEVEL 0.95 /
0.73 / 0.55.

## Where the remaining TPC-H CPU goes (Q10, node CPU Rust 1140 ms vs Go 760 ms)
perf self%: tidb-exec-pool 33.8, tidb-index-join 31.4, tikv-query-work 12.2, hash-join-probe
7.9, tidb-sql-connec 7.9, tikv-execution 5.2. Index-join thread cumulative: send_index_task
17.1 -> LookupForkTemplate::open 6.1 (12 deep clones per lookup task), open_index_task 5.7,
transport 5.9, scan_result 4.9; IndexHashState::drain 11.8 -> per-cell output appends
(append_cell_from 7.6, memmove 5.3). Pool thread: hash-agg PartialLane 17.6, FinalWorker 10.8.
No spin or yield hotspot. Q10, Q3, Q21 and Q18 all take this index-join path and show CPU
ratios of 1.34-1.60 against Go on this box.

## Fixes (each A/B'd same-box before commit)
1. Persistent partial-aggregate lanes (hash_agg/parallel.rs): the upstream rewrite parked the
   lane worker on an empty queue and re-enqueued it as a fresh boxed task through the shared
   compute pool per chunk; Go's HashAggPartialWorker.run is a goroutine that never leaves its
   receive loop. The lane now runs as one long-lived task on worker_pool::LanePool (reused OS
   threads, the primitive upstream added for index-join workers), blocking on its own channel
   like partialInputChs[i]. Result: neutral (below); reverted.
2. LookupForkTemplate shares its immutable per-task state by Arc (Go innerWorker holds
   lookup *IndexLookUpJoin and keyOff2IdxOff by pointer). Result: neutral (below); reverted.

### Result of fix 1 (persistent partial lanes), same-box go/head/fix (results/tpch-sf1-fix)
head 1.065x, fix 1.072x. Q13 1.00 -> 1.14 (0.44 -> 0.50 s; Go 0.44 s this run, 0.30 s in the
three-way earlier -- the box's run-to-run variance is of the same size as the effect), Q12
1.35 -> 1.47, Q8 1.00 -> 1.08 worse; Q18 1.20 -> 1.13, Q20 1.09 -> 1.00, Q7 1.09 -> 1.04 better.
Tests 1332/1332. No measured win -> reverted, recorded. The parked/re-enqueued lane is not what
costs Q13 on this box; the earlier 1.67x reading was variance plus a slow-disk restart.

### Result of fix 2 (Arc-shared LookupForkTemplate state), same-box go/head/fix2 (results/tpch-sf1-fix2)
head 1.049x, fix2 1.050x. Q3 1.05 -> 1.00, Q12 1.68 -> 1.54, Q9 1.11 -> 1.07 better; Q16 1.14 ->
1.30, Q14 1.00 -> 1.07 worse. Node CPU vs Go on the index-join queries (cpu-both-fix2): Q3 1.66,
Q10 1.41, Q21 1.46, Q18 1.31, Q9 1.19 -- no consistent reduction against head's 1.60 / 1.50 /
1.39 / 1.34 / 1.14. Tests 250 lookup + 1332 full, green. No measured win -> reverted, recorded.
KvTable could not be shared (pushdown_row_cursor_with_context / stage_rows_by_handles_filtered
take &mut self); the seven other fields were. The 6.1% "LookupForkTemplate::open" in the
profile was therefore not the clones' cost, or the cost moved elsewhere.

## In-flight coprocessor requests (Little's law from TiKV's grpc counters), Go vs Rust, 2 runs each
Q10: Go 9.2-10.8, Rust 6.8-8.5 (wall 0.78-0.84 vs 0.89-0.94 s); Q12: Go 6.5-7.9, Rust 1.1-6.2;
Q3: Go 7.9-13.5, Rust 3.8-9.9; Q19: Go 8.0-9.1, Rust 7.7-8.7 (wall equal); Q14: Go 7.8-8.6,
Rust 5.2-12.6. Mean TiKV service time is the same for both nodes where the request mix is the
same. Weak, consistent-direction signal that the Rust node keeps fewer cop requests in flight
on the index-join / scan-bound queries; request counts also differ (paging), and the scatter
(Q12 Go 52 vs 176 requests between runs, coprocessor cache) is too large for two runs to settle
it. Needs a longer, cache-controlled run before any fix is aimed at it.

## TPC-H go/head, four same-box counter-balanced runs on 2026-09-17 (warm min of 2 rounds each)
3way 1.102x, fix 1.065x, fix2 1.049x, base-run 1.067x: the synced tree is about 1.06x Go on this
box with +-0.03 run-to-run, matching 09-14's 1.056x. Any change below ~5% on a single query is
inside this scatter, which is why the two fixes above read "neutral" and why the micro-bench
suite (benches/pipeline.rs) exists.

## Pre-campaign base 8123bb1 on this cluster: unmeasurable
Built here with a one-line libc cast (statvfs.f_bsize is i64 on this VM). It answers Q1-Q4 and
then stalls (Q5 in round 1, Q10 in round 2); its connections end with "MySQL packet failed:
packet stream reached EOF" and each TPC-H pass hits the 3600 s timeout. The goal's 25% therefore
has no same-box baseline here; Go is the reference that can be measured.

## Micro-benchmark baseline at HEAD (`cargo bench -p tidb-executor --bench pipeline`, bench profile, `setarch -R`, run 1)
```
calibration ns_per_op 0.6093
calibration cal_per_op 0.9975
read_task_split_1024 ns_per_range 258.6
read_task_split_1024 cal_per_range 422.0498
read_task_prepare_1024 ns_per_range 716.2
read_task_prepare_1024 cal_per_range 1169.7286
read_task_split_20000 ns_per_range 260.4
read_task_split_20000 cal_per_range 426.7004
read_task_prepare_20000 ns_per_range 762.4
read_task_prepare_20000 cal_per_range 1240.5322
row_copy_keys_only ns_per_row 34.5
row_copy_keys_only mib_per_s 442
row_copy_keys_only cal_per_row 56.2459
row_copy_wide ns_per_row 85.4
row_copy_wide mib_per_s 2212
row_copy_wide cal_per_row 139.7056
agg_groups_10 total_ns_per_row 140.0
agg_groups_10 fold_ns_per_row 54.4
agg_groups_10 total_cal_per_row 229.0008
agg_groups_10 fold_cal_per_row 88.9071
agg_groups_1k total_ns_per_row 156.1
agg_groups_1k fold_ns_per_row 70.5
agg_groups_1k total_cal_per_row 254.5504
agg_groups_1k fold_cal_per_row 116.7793
agg_groups_60k total_ns_per_row 157.2
agg_groups_60k fold_ns_per_row 71.5
agg_groups_60k total_cal_per_row 255.7231
agg_groups_60k fold_cal_per_row 116.7615
append_int64 ns_per_cell 18.10
append_int64 cal_per_cell 29.77460
append_bytes_8 ns_per_cell 15.79
append_bytes_8 cal_per_cell 25.90825
append_bytes_32 ns_per_cell 14.68
append_bytes_32 cal_per_cell 23.94901
append_bytes_128 ns_per_cell 15.99
append_bytes_128 cal_per_cell 26.14814
append_decimal_40 ns_per_cell 20.00
append_decimal_40 cal_per_cell 32.74836
cop_encode skipped Unbound
join_probe_int_key ns_per_row 604.7
join_probe_int_key total_cal_per_row 1149.5106
join_probe_int_key source_cal_per_row 111.2286
join_probe_composite_key ns_per_row 7871.5
join_probe_composite_key total_cal_per_row 11966.1988
join_probe_composite_key source_cal_per_row 106.9120
```
**join_probe_composite_key 7,872 ns/row against join_probe_int_key 605 ns/row: a two-column key costs 13x per probe row.** That is the serialised-key table every composite equi-join uses (TPC-H Q9 joins lineitem to partsupp on `l_partkey, l_suppkey`), and it matches the per-key `Vec<u8>` allocations and jemalloc traffic the Q3/Q16/Q8 profile showed. `cop_encode` is skipped until the bench can bind a transport.

## Fix 3: join output from the sparse candidate list (found by the micro-bench)

Bisecting the 13x composite-key cost with a third shape, `join_probe_bytes_key`
(a single unique 25-byte VARCHAR key), showed the cost is the general (non
exact-int) probe path itself, not composite serialisation: bytes 7,419 ns/row,
composite 7,413, int 594. A flat profile of that shape alone
(`BENCH_ONLY=bytes`) put 32% in `Column::copy_selected_rows`, 26% in its
`SharedBytes::append_owned` closure and 7% in memmove, and the cost did not
move with table size (7.1 us at 25k rows, 7.9 us at 100k), so it was per-probe-row
work in the output copy: `selected_chunk_matches` took a `Vec<bool>` painted
over the whole 1024-row build chunk for every probe row, counted it, and rescanned
it once per output column (~6k iterations to emit one match). Go never selects
over the build chunk; its join copies the matched rows (`CopyRows`,
`appendBuildRowToChunk`). Both probe paths now collect the accepted candidate
rows as ascending physical indexes and the output copies them with
`chunk_util::copy_rows`.

```
                          before      after
join_probe_int_key         594 ns     569 ns
join_probe_bytes_key     7,419 ns   1,901 ns   (3.9x)
join_probe_composite_key 7,413 ns   1,641 ns   (4.5x)
```
The int-key shape was unaffected because it never reaches this copy: its flat
profile shows the per-row `append_cell_from` path (`probe_exact_int_many`,
`index_chunk_selected`) and no `selected_chunk_matches` at all; the remaining 3x of the byte-key shapes
over the int one is the per-candidate re-verification, which builds two `Datum`s
and two collation sort keys per candidate where Go's `EqualChunkRow` compares
encoded keys in place; that is the next step. Executor tests: 1,332 + 329 + 6
pass; clippy clean on the crate.
