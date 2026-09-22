# TPC-H SF50 Rust vs Go-master perf baseline (2026-09-22)

Environment: benchbot 172.16.6.48, tiup playground nightly tikv-slim
(TIUP_HOME=/data2/tmp/rust, tag tpch50), data restored from
qe-testing/performance-testing/tpch50-new via br (300M-row lineitem),
all eight tables analyzed. Go master binary from origin/master, Rust
release binary from this branch's hparser-integration head. One client,
`go-tpc tpch run --use-explain` (EXPLAIN ANALYZE), sequential laps.

## Wall-clock gate -- back-to-back final (2026-09-22, head 9cb4d25ca2)

Both gates ran sequentially on the same machine state (go-tpc, one client,
EXPLAIN ANALYZE, warm stats). Total 769.70s (Go) vs 1403.72s (Rust) =
1.82x. Sixteen queries sit at parity (0.82x-1.09x, six faster), and the
regressions concentrate in five shapes:

| query | go | rust | rust/go | shape |
| --- | --- | --- | --- | --- |
| q10 | 17.48 | 139.92 | 8.00x | IndexHashJoin probe + root agg |
| q17 | 190.52 | 597.81 | 3.14x | pushed cop agg + IndexHashJoin probe |
| q13 | 30.30 | 72.58 | 2.40x | LEFT JOIN + 7.5M-group HashAgg (spill) |
| q21 | 41.44 | 72.65 | 1.75x | semi-join probes over lineitem |
| q2 | 10.17 | 17.62 | 1.73x | partsupp/part/supplier join + group |
| q18 | 51.44 | 66.20 | 1.29x | LEFT JOIN + group |

The original first table below is the earlier gate (9cb4d25's ancestors)
kept for history.

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
* **q21 ~2x** is a pre-existing executor gap, NOT a commit regression: a
  bisect across ed32149a/403c269c66/dc389f42a3 measured 96.8s / 75.0s /
  76.6s — the original 45.33s first-lap number was a warm-cache artifact.
  Steady state puts q21 at roughly 1.8-2x Go master on every revision.
* **q2 1.74x**: same plan shape; profiler points at the hash-join probe and
  group-key resolve path shared with q13.

## Plan-parity status

20/22 queries explain identically (`EXPLAIN FORMAT='brief'`, normalized).
Remaining: q10's greedy node-assembly order (equal-condition argument
direction only; shapes and build sides now match) and q18's internal
Column# allocator numbering.

## q10 probe profile on the current head (1695754376, 2026-09-22)

A clean perf capture (binary not replaced mid-run) of the q10
IndexHashJoin probe shows the dispatcher's limiter/waiter machinery, not
the data path, as the top cost:

* 11.1% `Mutex::lock_contended` under `CoprRequestLimiter::register_waker`
* 6.7% `register_waker` itself (the `will_wake` linear dedup scan)
* 8.3% `QueryCopStoreLimiter::get_store_limiter` (RwLock read + Arc clone
  + hash, contended across the probe workers)
* 6.7% `acquire_request_attempt_limiter` + 6.5%
  `with_request_selection`/`dispatch_attempts` (region-cache selection)

Together roughly a third of the query sits in request-admission overhead.
The rust direct-unary driver yields and registers a waker when the
per-store limiter is saturated (blocking the runtime thread would strand
the response's own in-flight tokens), then the next release wakes every
registered driver. Go's worker blocks on a native semaphore instead --
zero waker traffic. A wake-one variant was attempted and reverted
(52f7c601906b4): with stale entries in the waiters list, popping the most
recent registrant starves the live ones (q10 hung 1700s+). A correct
cheaper design needs deregistration-aware waker tracking or handing the
token directly to a parked driver.

## Next levers, in expected-value order

1. Coprocessor request-admission redesign for the index-join probe
   (q10/q17): replace the yield+register+thundering-herd limiter path with
   Go-style blocking admission on the dedicated worker threads (or a
   deregistration-aware waiter queue); the profile above puts ~30% of q10
   in that admission machinery alone.
2. Parallel HashAgg group-map footprint (q13, q2): 7.5M-group maps exceed
   the 1GB query quota and spill while Go's equivalent stays resident;
   shrink the per-group key/state representation.
3. q21's ~2x join execution gap (long-standing, not commit-specific): the
   semi-join probes over lineitem via EXISTS predicates.
4. q10 greedy node-assembly order and q18 column-number allocator for the
   last two plan-text mismatches.
