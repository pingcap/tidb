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
