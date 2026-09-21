# TPC-H SF50 Rust vs Go-master perf baseline (2026-09-22)

Environment: benchbot 172.16.6.48, tiup playground nightly tikv-slim
(TIUP_HOME=/data2/tmp/rust, tag tpch50), data restored from
qe-testing/performance-testing/tpch50-new via br (300M-row lineitem),
all eight tables analyzed. Go master binary from origin/master, Rust
release binary from this branch's hparser-integration head. One client,
`go-tpc tpch run --use-explain` (EXPLAIN ANALYZE), sequential laps.

## Wall-clock gate (first lap, seconds)

| query | go | rust | rust/go |
| --- | --- | --- | --- |
| q1 | 47.21 | 45.20 | 0.96x |
| q2 | 10.77 | 18.76 | 1.74x |
| q3 | 20.77 | 21.98 | 1.06x |
| q4 | 11.58 | 12.85 | 1.11x |
| q5 | 38.82 | 38.96 | 1.00x |
| q6 | 20.37 | 20.70 | 1.02x |
| q7 | 32.72 | 32.78 | 1.00x |
| q8 | 16.81 | 18.69 | 1.11x |
| q9 | 77.61 | 72.85 | 0.94x |
| q10 | 17.95 | 141.13 | 7.86x |
| q11 | 7.82 | 7.62 | 0.97x |
| q12 | 43.25 | 43.18 | 1.00x |
| q13 | 30.50 | 73.45 | 2.41x |
| q14 | 21.64 | 22.31 | 1.03x |
| q16 | 8.22 | 7.82 | 0.95x |
| q17 | 196.96 | 591.90 | 3.01x |
| q18 | 53.99 | 70.83 | 1.31x |
| q19 | 30.70 | 31.37 | 1.02x |
| q20 | 22.65 | 24.13 | 1.07x |
| q21 | 41.91 | 89.09 | 2.13x |
| q22 | 7.21 | 6.01 | 0.83x |

(q15's go-tpc view lifecycle raced on the Go side; rust = 44.93s.)

## Hotspot attribution (perf + operator times)

* **q10 7.86x**: plans now match shape; the remaining cost is the
  IndexHashJoin inner probe over lineitem (probe + its runtime filter take
  ~90s of the 109-141s) plus the root HashAgg. Raising
  `tidb_mem_quota_query` to 8GB only recovers ~30s: the probe path itself,
  not spill, dominates.
* **q13 2.41x**: root HashAgg grouping 7.5M rows into 7.5M groups costs
  73s under the default 1GB `tidb_mem_quota_query` because the parallel
  pipeline maps spill. With an 8GB quota the same plan runs **17.9s —
  faster than Go master's 30.5s**. The group map's memory footprint per
  group is the gap, not CPU.
* **q17 3.01x**: the pushed-down coprocessor aggregation over 300M rows is
  common to both sides; with 8GB quota rust improves to 357s and remains
  ~1.8x behind, implicating the IndexHashJoin inner-probe row fetch.
* **q21 2.13x** (was 1.08x at 34b57852) and **q10's former 2.51x** regressed
  across the group-key / index-probe EXPLAIN + runtime commits
  (b497d04188fb, 403c269c66c8, e94c5159..b49c1a7e4b); needs bisect
  attribution before further executor changes.
* **q2 1.74x**: same plan shape; profiler points at the hash-join probe and
  group-key resolve path shared with q13.

## Plan-parity status

20/22 queries explain identically (`EXPLAIN FORMAT='brief'`, normalized).
Remaining: q10's greedy node-assembly order (equal-condition argument
direction only; shapes and build sides now match) and q18's internal
Column# allocator numbering.

## Next levers, in expected-value order

1. IndexHashJoin inner-probe batching/prefetch (q10, q17): Go fetches the
   inner handle ranges in worker-sized batches through
   `pkg/executor/internal/exec` IndexJoinRuntime with per-region
   concurrency; profile where the rust probe serializes.
2. Parallel HashAgg group-map footprint (q13, q2): 7.5M-group maps exceed
   the 1GB query quota and spill while Go's equivalent stays resident;
   shrink the per-group key/state representation.
3. Bisect q21/q10 regressions across the group-key commit series
   (e94c515913d3..b49c1a7e4b49) and the index-probe EXPLAIN series
   (b497d04188fb..403c269c66c8).
4. q10 greedy node-assembly order and q18 column-number allocator for the
   last two plan-text mismatches.
