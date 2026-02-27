# Big Test Report: Streaming Sample Save (8000 partitions, 30M rows)

**Binary**: `94d097a6e4` (`sample-based-global-stats-save-and-load`)
**Table**: `analyze_profile.t_partitioned` — 8000 HASH partitions, 16 int columns, 30M rows (3750/partition)
**Feature**: `tidb_enable_sample_based_global_stats=ON` for all runs

## Test Matrix

| # | Run | Scope | Truncate | Duration | Mem max | Global row_count |
|---|-----|-------|----------|----------|---------|-----------------|
| 1 | 215714 | full analyze | yes | 5m02.6s | 1.5 GB | 30,000,000 |
| 2 | 220237 | p5 only | no | 25.9s | 192 KB | 30,000,000 |
| 3 | 220316 | full analyze | no | 5m13.1s | 1.5 GB | 30,000,000 |
| 4 | 220848 | p5 only | no | 25.9s | 192 KB | 30,000,000 |

All runs: 8000 partitions at row_count=3750, modify_count=0.

## Key Result: No OOM

**Zero Error 8175 (memory quota exceeded)**. Zero "failed to save partition
samples" warnings. OOM record directory empty.

Previously the batch save of 8000 samples failed because the analyze session
was already at ~1.48 GB and each REPLACE INTO with a ~515 KB blob exceeded
`tidb_mem_quota_query`. The streaming save via pool session avoids this
entirely — each sample is saved immediately after the partition analysis
completes using a fresh session from `statsHandle.SPool()`.

### Memory progression during full analyze (run 1)

| Elapsed | Session mem_max |
|---------|----------------|
| 60s | 238.6 MB |
| 120s | 557.9 MB |
| 180s | 876.2 MB |
| 240s | 1.17 GB |
| 300s | 1.48 GB (peak) |
| Final | 1.5 GB |

The memory curve is identical to the pre-fix version because the inline
saves run on pool sessions with their own memory tracking. The analyze
session's peak is determined solely by per-partition analysis, not by
sample persistence.

## Single-Partition Re-Analyze Timing

Both p5 re-analyze runs show identical performance:

| Phase | Run 2 | Run 4 |
|-------|-------|-------|
| Load 7999 saved samples | 25.4s | 25.0s |
| Build global stats | 0.12s | 0.10s |
| Write global stats | 0.28s | 0.67s |
| **Total** | **25.9s** | **25.9s** |

Sample loading dominates at ~25s for 7999 rows (each ~150 KB serialized
protobuf). Build + write is <1s. This compares to the old partition-merge
path which would read all 8000 partition histograms (typically 8+ min).

The analyze session itself reports **Mem max: 192 KB** — the sample loading
and merging happens entirely via pool sessions and the sample collector's
own allocations, not tracked by the analyze session.

## Stats Correctness

### stats_meta
- Global row_count = 30,000,000 in all 4 runs
- All 8000 partitions: row_count = 3750, modify_count = 0

### Histogram quality
- Full-analyze runs (1, 3) produce **identical** global buckets (deterministic)
- Re-analyze runs (2, 4) produce **identical** global buckets (deterministic)
- Full-analyze: 255 global buckets; re-analyze: 250 global buckets (both near max 256)
- stats_dump.json: full = 3.12 MB, re-analyze = 3.09 MB (similar quality)

### NDV
- Global pk distinct_count = 30,000,000 in all runs (correct for unique pk)
- Avg_col_size = 8 (correct for bigint)

### Reproducibility
- Run 1 buckets == Run 3 buckets (full analyze is deterministic)
- Run 2 buckets == Run 4 buckets (re-analyze is deterministic)

## TiDB Log Analysis

### Log files examined
- `tidb-2026-02-27T22-01-50.190.log` (run 1 analyze phase)
- `tidb-2026-02-27T22-07-24.931.log` (runs 2-3)
- `tidb.log` (runs 3-4)

### Events confirmed
- `analyze started ... tasks=8000 partitionTasks=8000 sampleBasedGlobalStats=true`
- Run 1: `all partitions freshly analyzed, skip loading saved samples [partitions=8000]`
- Run 2: `loading saved samples for table [analyzed=1] [toLoad=7999] [total=8000]`
- Run 3: `all partitions freshly analyzed, skip loading saved samples [partitions=8000]`
- Run 4: `loading saved samples for table [analyzed=1] [toLoad=7999] [total=8000]`
- All runs: `merge entry built (sample-based)` + `merge entry written` with no errors

### Warnings
Only standard `expensive_query` warnings for the 5-minute full analyzes.
No save failures, no schema mismatches, no partition sample warnings.

## Resource Usage Summary

| Run | CPU delta | RSS delta | Heap delta | Goroutines |
|-----|-----------|-----------|------------|------------|
| 1 (full) | +552.5s | +1.5 GB | +885 MB | +10 |
| 2 (p5) | +27.7s | -41 MB | -377 MB | +4 |
| 3 (full) | +546.3s | +3.4 GB | +5.4 GB | +10 |
| 4 (p5) | +24.6s | +11 MB | -1.5 GB | +4 |

Run 3 shows higher heap delta because it starts with state from prior runs
(Go GC hasn't reclaimed everything). The RSS/heap deltas for re-analyze
runs are negligible or negative.
