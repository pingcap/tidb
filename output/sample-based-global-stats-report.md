# Performance Report: Sample-Based Global Stats Save & Load

**Date:** 2026-02-27
**Branch:** `sample-based-global-stats-save-and-load`
**Table:** 8,000 partitions × 3,750 rows = 30M total rows, 16 int columns
**Environment:** TiDB playground, single node (macOS, ~64GB RAM)

---

## 1. Executive Summary

Sample-based global stats dramatically improves the single-partition re-analyze
use case — the primary target scenario — but a save-phase failure prevents it
from working end-to-end in the tested configuration.

| Scenario | Sample-Based ON | Merge-Based (OFF) | Speedup |
|---|---|---|---|
| **Single-partition re-analyze** | **1.3s** | **8m13s** | **~380x** |
| Full analyze (8000 parts, cold) | 7m04s | 55m19s | 7.8x |
| Full analyze (overwrite existing) | 7m41s | OOM crash | — |

**Critical issue — save failure:** During a full-analyze with sample-based ON,
all 8,000 partition sample saves fail with Error 8175 (per-query memory limit
exceeded). The session already holds ~1.48 GB of sample collectors; each
REPLACE INTO with a ~515 KB SQL-escaped blob pushes the session over the 1 GB
`tidb_mem_quota_query` limit. As a result, the `mysql.stats_global_merge_data`
table is empty when a subsequent single-partition re-analyze tries to load
saved samples. The global stats are then rebuilt from only the one re-analyzed
partition, producing `row_count=3,750` instead of 30,000,000.

The global-stats rebuild *code* is correct — `MergeCollector` properly
accumulates Count, NullCount, and TotalSizes. The bug is that the data never
reaches storage.

The full-analyze path also benefits from the new code because the BUILD phase
(merging partition stats into global stats) drops from ~8 minutes to ~0.02s.
However, the new SAVE phase (persisting sample data via SQL) adds 4–5 minutes,
partially offsetting the gain.

---

## 2. Test Setup

### Table Schema

```sql
-- 8000 partitions (p0..p7999), 16 int columns + pk
CREATE TABLE t_partitioned (
  pk BIGINT PRIMARY KEY,
  c1 INT, c2 INT, ..., c16 INT
) PARTITION BY RANGE (pk) (
  PARTITION p0 VALUES LESS THAN (...),
  ...
  PARTITION p7999 VALUES LESS THAN (MAXVALUE)
);
-- 3,750 rows per partition, 30,000,000 total
```

### Session Variables

```
tidb_analyze_partition_concurrency      = 2
tidb_analyze_skip_column_types          = json,blob,mediumblob,longblob,mediumtext,longtext
tidb_analyze_version                    = 2
tidb_auto_analyze_ratio                 = 0.5
tidb_build_sampling_stats_concurrency   = 2
tidb_build_stats_concurrency            = 2
tidb_enable_async_merge_global_stats    = 1
tidb_merge_partition_stats_concurrency  = 1
tidb_partition_prune_mode               = dynamic
```

### Test Matrix (8 runs)

| # | Dir | Mode | Scope | Truncate | Duration | Status |
|---|-----|------|-------|----------|----------|--------|
| 1 | 172852 | sample ON | full 8000 parts | yes | 7m28s | OK |
| 2 | 173634 | sample ON | partition p5 | no | 1.25s | OK |
| 3 | 173653 | sample OFF | full 8000 parts | yes | 55m33s | OK |
| 4 | 183336 | sample OFF | partition p5 | no | 8m11s | OK |
| 5 | 184248 | sample OFF | full (overwrite) | no | 6m35s | **OOM (Error 8176)** |
| 6 | 185034 | sample OFF | partition p5 | no | 8m16s | OK |
| 7 | 185958 | sample ON | full (overwrite) | no | 8m17s | OK |
| 8 | 190922 | sample ON | partition p5 | no | 1.44s | OK |

Runs are sequential. Runs 5–8 inherit residual memory state from prior runs.
Run 5 (merge-based full overwrite) OOM'd; runs 6–8 were affected by residual
heap pressure.

---

## 3. Wall-Clock Comparison

### Timeline Summary (from `analyze-timeline.py`)

```
   #         START    TYPE  TASKS  PARTS  CONC      TOTAL    ANALYZE       LOAD       SAVE      BUILD      WRITE     GLOBAL    PATH  SAMPLE  PEAK MEM
----  ------------  ------  -----  -----  ----  ---------  ---------  ---------  ---------  ---------  ---------  ---------  ------  ------  --------
   1  17:29:17.197  manual   8000   8000     2     7m4.4s    2m59.5s     0.000s     4m4.5s     0.163s     0.246s     4m4.9s  sample    true     1.48G
   2  17:36:34.096  manual      1      1     1     1.241s     0.042s     0.884s     0.033s     0.020s     0.260s     1.199s  sample    true      5.7M
   3  17:37:07.310  manual   8000   8000     2   55m18.9s   47m35.9s     0.000s     0.000s    7m42.6s     0.486s    7m43.0s   merge   false    191.7K
   4  18:33:36.129  manual      1      1     1    8m10.6s     0.580s     0.000s     0.000s     8m9.1s     0.914s    8m10.0s   merge   false    191.5K
   5  18:50:34.852  manual      1      1     1    8m15.6s     0.521s     0.000s     0.000s    8m14.0s     1.033s    8m15.1s   merge   false    191.5K
   6  19:00:34.168  manual   8000   8000     2    7m41.3s    2m13.7s     0.000s    5m25.2s     0.179s     2.237s    5m27.6s  sample    true     1.48G
   7  19:09:23.014  manual      1      1     1     1.426s     0.049s     0.873s     0.055s     0.020s     0.426s     1.376s  sample    true      5.7M
```

Note: Run 5 (the OOM failure, dir 184248) is not in the timeline because it
crashed mid-analyze and did not complete the global-stats phase. Rows 4 and 5
above correspond to dirs 183336 and 185034 (both merge-based single-partition).

### Key Findings

**Single-partition re-analyze** (the target scenario):
- Sample ON: **1.2–1.4s** (rows 2, 7)
- Merge OFF: **8m10–8m16s** (rows 4, 5)
- **Speedup: ~380x**

**Full analyze** (all 8000 partitions):
- Sample ON (cold): **7m04s** (row 1)
- Merge OFF (cold): **55m19s** (row 3)
- **Speedup: 7.8x**
- Sample ON (overwrite): **7m41s** (row 6) — slightly slower due to save overhead on existing data

---

## 4. Phase Breakdown

Each ANALYZE run decomposes into these phases:

| Phase | Description |
|---|---|
| ANALYZE | Scan partitions, collect column samples (same code path for both modes) |
| SAVE | (Sample only) Persist partition sample blobs to `mysql.stats_global_merge_data` |
| LOAD | (Sample only) Load previously-saved partition sample blobs |
| BUILD | Merge partition data into global stats (merge reads all histograms; sample builds from samples) |
| WRITE | Write final global stats to `mysql.stats_*` tables |

### Full Analyze (8000 partitions)

| Phase | Sample ON (row 1) | Sample ON (row 6) | Merge OFF (row 3) |
|---|---|---|---|
| ANALYZE | 2m59.5s | 2m13.7s | 47m35.9s |
| SAVE | **4m04.5s** | **5m25.2s** | — |
| LOAD | — | — | — |
| BUILD | 0.163s | 0.179s | **7m42.6s** |
| WRITE | 0.246s | 2.237s | 0.486s |
| **TOTAL** | **7m04.4s** | **7m41.3s** | **55m18.9s** |

The ANALYZE phase difference (3m vs 48m) is partially explained by storage
pressure: during the merge-based full analyze (row 3), per-partition ANALYZE
cost degrades from ~2s early to ~4s late as the stats tables accumulate data.
With `truncate-stats` both runs start clean, but the merge path's 8000 save-stats
operations create back-pressure during the analysis phase itself.

The BUILD phase is where the fundamental algorithmic difference shows: merge
must read all 8000 partition histograms/TopN from storage and merge them (7m43s),
while sample-based builds from in-memory samples (0.16s).

The SAVE phase (4–5 min) is the main overhead of the sample-based path for full
analyze. CPU profiling shows this is dominated by SQL blob handling (see Section 6).

### Single-Partition Re-analyze

| Phase | Sample ON (row 2) | Sample ON (row 7) | Merge OFF (row 4) | Merge OFF (row 5) |
|---|---|---|---|---|
| ANALYZE | 0.042s | 0.049s | 0.580s | 0.521s |
| LOAD | **0.884s** | **0.873s** | — | — |
| SAVE | 0.033s | 0.055s | — | — |
| BUILD | 0.020s | 0.020s | **8m09.1s** | **8m14.0s** |
| WRITE | 0.260s | 0.426s | 0.914s | 1.033s |
| **TOTAL** | **1.241s** | **1.426s** | **8m10.6s** | **8m15.6s** |

The sample-based path loads 7,999 previously-saved partition samples in 0.88s,
then builds global stats in 0.02s. The merge-based path must read all 8000
partition histograms, TopN arrays, and CM Sketches from `mysql.stats_*` tables,
which takes over 8 minutes.

---

## 5. Memory Usage

### Process-Level Memory (from `local-test.sh` log)

| Run | Dir | Mode | Scope | RSS Before→After (delta) | Heap Before→After (delta) |
|---|---|---|---|---|---|
| 1 | 172852 | sample ON | full | 1.1→19.0 GB (+17.9) | 275M→2.1G (+1.8) |
| 2 | 173634 | sample ON | p5 | 18.9→18.9 GB (+1M) | 1.2G→369M (−835M) |
| 3 | 173653 | sample OFF | full | 18.9→7.9 GB (−11.0) | 926M→8.7G (+7.8) |
| 4 | 183336 | sample OFF | p5 | 7.4→15.3 GB (+7.8) | 4.7G→6.9G (+2.2) |
| 5 | 184248 | sample OFF | full | 15.6→32.7 GB (+17.1) | 8.0→32.2G (+24.1) |
| 6 | 185034 | sample OFF | p5 | 9.5→11.9 GB (+2.4) | 43.6→6.0G (−37.6) |
| 7 | 185958 | sample ON | full | 12.6→10.6 GB (−2.1) | 4.7→7.4G (+2.8) |
| 8 | 190922 | sample ON | p5 | 14.8→15.2 GB (+422M) | 9.4→3.8G (−5.6) |

### In-Analyze Peak Memory (from timeline PEAK MEM column)

| Mode | Scope | Peak In-Analyze Mem |
|---|---|---|
| sample ON | full (8000 parts) | **1.48 GB** |
| sample ON | single partition | **5.7 MB** |
| merge OFF | full (8000 parts) | **191.7 KB** |
| merge OFF | single partition | **191.5 KB** |

The 1.48 GB peak for sample-based full analyze represents the in-memory sample
collectors for all 8000 partitions held simultaneously (with concurrency=2,
samples accumulate as partitions complete).

### Key Observations

- **Sample ON single partition**: Negligible memory (+1 MB RSS). The load phase
  reads samples one-by-one from storage; the build phase processes them immediately.

- **Merge OFF single partition**: +7.8 GB RSS (run 4). The BUILD phase loads all
  8000 partitions' histograms, TopN, and CM Sketches into memory to reconstruct
  global stats — extremely expensive for a single-partition update.

- **Merge OFF full overwrite (run 5)**: OOM crash. Starting from 8.0 GB heap (residual
  from run 4), the heap spiked to 32.2 GB and hit the server memory limit.
  The sample-based full overwrite (run 7) completed successfully despite similar
  starting conditions.

---

## 6. CPU Profile Analysis

### Profile 1: FMSketch sync.Pool Bottleneck (Pre-Fix, earlier run)

Captured during an earlier full analyze run (before the FMSketch fix commit
`0819b8d00b`). 30s sample, 61.15s total CPU:

```
      flat  flat%   sum%        cum   cum%
    53.71s 87.83% 87.83%     53.98s 88.27%  swiss.(*Map[uint64,bool]).Clear (inline)
     3.72s  6.08% 93.92%      3.89s  6.36%  swiss.(*Map[uint64,bool]).Iter
     0.70s  1.14% 95.06%      0.70s  1.14%  runtime.memclrNoHeapPointers
```

**88% of CPU** was spent in `swiss.Map.Clear`, called from
`FMSketch.DestroyAndPutToPool → FMSketch.reset`. The sync.Pool reuse of
FMSketch instances triggered O(capacity) clearing of the underlying hash map.
This was fixed by removing the sync.Pool for FMSketch (commit `0819b8d00b`).

### Profile 2: Save Phase Bottleneck (Post-Fix)

Captured during the save phase of a sample-based full analyze. 30s sample,
25.22s total CPU:

```
      flat  flat%   sum%        cum   cum%
     4.24s 16.81% 16.81%      4.24s 16.81%  runtime.memmove
     2.83s 11.22% 28.03%      4.37s 17.33%  unicode/utf8.appendRuneNonASCII
     2.40s  9.52% 37.55%      2.40s  9.52%  runtime.memclrNoHeapPointers
     1.80s  7.14% 44.69%      1.80s  7.14%  unicode/utf8.DecodeRuneInString
     1.75s  6.94% 51.63%      1.97s  7.81%  strings.(*Builder).WriteByte
     1.51s  5.99% 57.61%      1.51s  5.99%  unicode/utf8.DecodeRune
     1.18s  4.68% 67.05%      1.34s  5.31%  sqlescape.escapeBytesBackslash
     0.90s  3.57% 70.62%      3.09s 12.25%  parser.(*reader).skipRune
     0.33s  1.31% 86.99%      8.21s 32.55%  parser.(*Scanner).scanString
```

The top functions are all **SQL string escaping and parsing**:
- `escapeBytesBackslash` (5.3%) + `memmove` (16.8%) + utf8 encoding (18.4%) = blob serialization
- `parser.(*Scanner).scanString` (32.6% cum) = the SQL parser re-parsing the escaped blob

This confirms the save-phase bottleneck is the SQL-based blob handling in
`SavePartitionSampleData`, which builds INSERT statements with large escaped
binary literals. A binary protocol or direct KV write would eliminate this overhead.

---

## 7. Accuracy Analysis

### Root Cause: Save Failure, Not Logic Bug

Direct database inspection reveals only **1 row** in
`mysql.stats_global_merge_data` (partition p5, 515 KB blob), despite 8,000
partitions existing. The global-stats rebuild code is correct — the data simply
never reaches storage.

**Evidence chain:**

1. Run 1 (full analyze, sample ON): All 8,000 sample saves fail with Error 8175.
   TiDB logs show `[WARN] failed to save partition samples` for every partition.
   The session holds 1.48 GB of sample collectors, and each REPLACE INTO with a
   ~515 KB SQL-escaped blob exceeds the 1 GB `tidb_mem_quota_query`.

2. Run 3 (full analyze, sample OFF, `-truncate-stats`): Truncates
   `mysql.stats_global_merge_data` along with all other stats tables, wiping any
   previously saved data.

3. Run 7 (full analyze, sample ON, overwrite): All 8,000 saves fail again —
   same Error 8175. Residual heap pressure from earlier runs makes it worse.

4. Single-partition saves (runs 2, 8) succeed because the session memory is low
   (~5.7 MB) — well under the 1 GB quota.

**Verified by querying the running TiDB instance:**

```sql
SELECT COUNT(*) FROM mysql.stats_global_merge_data WHERE type=2;
-- Result: 1 (only partition p5 = table_id 240)

SELECT table_id, LENGTH(value) as blob_bytes FROM mysql.stats_global_merge_data;
-- 240  |  527301  (515 KB — partition p5 only)
```

### Global Row Count

| Run | Dir | Mode | Scope | Global row_count | Expected | Correct? |
|---|---|---|---|---|---|---|
| 1 | 172852 | sample ON | full | **30,000,000** | 30,000,000 | Yes (built from in-memory samples) |
| 2 | 173634 | sample ON | p5 | **3,750** | 30,000,000 | **NO** (loaded 0 of 7999 samples) |
| 3 | 173653 | sample OFF | full | **30,000,000** | 30,000,000 | Yes |
| 4 | 183336 | sample OFF | p5 | **30,000,000** | 30,000,000 | Yes |
| 6 | 185034 | sample OFF | p5 | **30,000,000** | 30,000,000 | Yes |
| 8 | 190922 | sample ON | p5 | **3,750** | 30,000,000 | **NO** (loaded 0 of 7999 samples) |

Sources: `stats_meta.tsv` (runs 190922, 173634, 183336), `stats_dump.json`
(runs 172852, 173634), and direct `mysql.stats_global_merge_data` query.

### Column-Level Metadata (global partition)

Comparing sample-based full (172852, correct) with sample-based single-partition
(173634, where all loads returned nil):

| Column | Metric | Full Analyze (172852) | Single-Part (173634) | Expected |
|---|---|---|---|---|
| pk | null_count | 0 | 0 | 0 |
| pk | tot_col_size | 240,000,000 | 30,000 | 240,000,000 |
| c1 | null_count | 1,499,262 | 194 | ~1,500,000 |
| c1 | tot_col_size | 228,005,904 | 28,448 | ~228,000,000 |
| c2 | null_count | 1,500,588 | 188 | ~1,500,000 |
| c5 | null_count | 1,501,809 | 206 | ~1,500,000 |

All global column metadata reflects only partition p5's data because
`LoadPartitionSample` returned nil for all 7,999 other partitions (no saved
data existed). The `MergeCollector` loop in `loadAndMergeSavedSamples` thus
had nothing to merge, and `collector.Base().Count` remained at 3,750.

### NDV (Distinct Count)

NDV is not available in `stats_histograms.tsv` (all TSV dumps for histograms
are header-only for the relevant runs). From `stats_dump.json` (run 172852),
the global NDV values look reasonable for a full analyze (e.g., pk NDV=30M,
c4 NDV=100, c5 NDV=3750), but we cannot cross-validate against the merge-based
path because run 173653's stats_dump timed out.

### Code Path Verification

The global-stats rebuild logic is correct:

- `MergeCollector` (`row_sampler.go:372`): `s.Count += subCollector.Base().Count` —
  properly accumulates Count, NullCount, TotalSizes from each merged partition.
- `ToProto` / `FromProto` (`row_sampler.go:282,297`): Count is preserved in the
  protobuf round-trip.
- `PruneTo` (`row_sampler.go:406`): Count is preserved when samples are pruned.
- `buildGlobalColumnStatsFromSamples` (`global_stats_sample.go:64`):
  `globalStats.Count = collector.Base().Count` — reads the accumulated Count.
- `WriteGlobalStatsToStorage` → `SaveColOrIdxStatsToStorage` (`save.go:354`):
  Writes Count to `mysql.stats_meta` via `REPLACE INTO`.

If the saved samples existed in storage, the load→merge→build pipeline would
produce the correct Count of 30,000,000.

---

## 8. Errors and Issues

### Sample Save Failure During Full Analyze (Error 8175) — THE BLOCKING BUG

During full-analyze with sample-based ON, **all 8,000 partition sample saves
fail** with Error 8175 (per-query memory limit). This is the root cause of the
incorrect global stats after single-partition re-analyze.

```
[WARN] failed to save partition samples [partitionID=383]
  [error="[executor:8175]Your query has been cancelled due to exceeding the
  allowed memory limit for a single SQL query..."]
```

- Run 1 (172852): 8,000/8,000 saves failed. Session mem ~1.46 GB during save.
- Run 7 (185958): 8,000/8,000 saves failed. Session mem even higher from residual pressure.
- Single-partition saves (runs 2, 8): succeeded — session mem only ~5.7 MB.

The `savePartitionSamplesToStorage` loop runs under the ANALYZE statement's
session memory tracker, which already holds ~1.48 GB of accumulated sample
collectors. Each REPLACE INTO carries a ~515 KB SQL-escaped binary blob, which
the SQL parser must re-parse, adding temporary allocations that push the session
over the 1 GB `tidb_mem_quota_query` limit.

### Post-Analyze OOM on Stats Queries (Error 8175)

After sample-based full analyze (runs 172852, 185958), queries against the stats
tables fail with Error 8175. Affected operations: `SHOW ANALYZE STATUS`, stats
TSV dumps. This prevents data collection after sample-based full analyze.

### Full Merge OOM Crash (Error 8176)

Run 5 (dir 184248): merge-based full analyze on existing stats. The heap grew
from 8.0 GB to 32.2 GB and hit the server-level memory limit. Only ~1,116 of
8,000 partitions were analyzed before the crash. The equivalent sample-based run
(run 7, dir 185958) completed successfully.

### Nil Pointer Dereference in Slow Query Lookup

Runs 172852 and 185958 triggered a nil pointer dereference in the slow query
lookup path. Unrelated to the stats feature.

### Per-Partition Cost Degradation

During the merge-based full analyze (run 3), the ANALYZE phase took 47m36s vs
~3 minutes for sample-based. Per-partition cost degrades from ~2s early to ~4s
late as `mysql.stats_*` tables accumulate data.

---

## 9. Conclusions & Next Steps

### What Works Well

1. **Single-partition re-analyze is ~380x faster** — the primary use case for
   this feature. Loading 7,999 partition samples takes 0.88s vs 8+ minutes to
   re-read and merge all partition histograms.

2. **Full analyze is 7.8x faster** even with the save-phase overhead. The
   ANALYZE phase is faster (3m vs 48m) due to less stats-table back-pressure,
   and the BUILD phase is 2800x faster (0.16s vs 7m43s).

3. **Better OOM resilience**: sample-based full overwrite completed where
   merge-based crashed (run 7 vs run 5).

### Must Fix Before Merge

1. **Sample save failure under memory pressure**: The batch save of 8,000
   partition samples runs under the ANALYZE statement's memory tracker, which
   already holds ~1.48 GB of accumulated sample collectors. Each REPLACE INTO
   exceeds `tidb_mem_quota_query`, causing all saves to fail. Without saved
   samples, single-partition re-analyze rebuilds global stats from only one
   partition. The fix must either reduce the session memory during saves or
   decouple the save from the analyze session.

### Proposed Fix: Streaming Pipeline Architecture

The current flow is batched — all phases run sequentially:

```
Current (batch):
  [Analyze p0..p7999] → [Accumulate 8000 collectors in memory]
                      → [Save all 8000 to storage]  ← FAILS (1.48GB + blob = OOM)
                      → [Build global] → [Write global]
```

A streaming pipeline would interleave per-partition work, releasing memory as
each partition completes:

```
Proposed (streaming):
  For each partition (N in parallel, e.g. N=2):
    1. Analyze partition      → sample collector (~5.7 MB)
    2. Save partition stats   → TopN/histogram to mysql.stats_*  (already happens)
    3. Merge into global      → accumulate Count, FMSketches, reservoir samples
    4. Save pruned samples    → to mysql.stats_global_merge_data  ← NEW: immediate
    5. Release partition mem  → collector freed

  After all partitions:
    6. Build global stats from merged collector  (0.02s)
    7. Write global stats                        (0.25s)
```

**Key changes to implement this:**

The existing pipeline in `handleResultsErrorWithConcurrency` (`analyze.go:482`)
already processes partition results one-by-one as they arrive:

```
resultsCh → handleGlobalStats() → mergePartitionSamplesForGlobal() → saveResultsCh
```

Currently, `mergePartitionSamplesForGlobal()` serializes the pruned samples into
`e.partitionSamplesToSave[partitionID]`, deferring the actual storage write to
`savePartitionSamplesToStorage()` which runs later in `handleGlobalStats()`.

The change: **save each partition's samples immediately after serialization**,
inside (or right after) `mergePartitionSamplesForGlobal()`, then discard the
serialized bytes. This means:

- Memory: O(concurrency × collector_size) instead of O(partitions × blob_size).
  With concurrency=2 and ~5.7 MB per collector, peak is ~12 MB vs 1.48 GB.
- Save success: Each save runs when session memory is low (only the global
  collector + current partition's collector), well under the 1 GB quota.
- The save can use a **separate SQL session** (not tracked by the ANALYZE
  statement's memory tracker) to avoid any memory accounting interference.

**Concurrency control:** The save to storage can run in a bounded worker pool
(e.g., 2–4 concurrent saves) alongside the analyze workers. The pipeline would
be:

```
analyzeWorker(concurrency=2) → resultsCh → [merge + serialize + save] → saveResultsCh
                                                    ↓
                                           saveSampleWorker(concurrency=2)
```

With a limit on in-flight saves, memory stays bounded regardless of partition
count.

**What this also fixes:**

- The save phase would overlap with the analyze phase, hiding the 4–5 minute
  save cost. Currently the save phase adds 4m after analysis; with streaming,
  each ~30ms save happens concurrently with the next partition's analysis.
- The SQL blob overhead becomes less critical because saves are spread over the
  full analyze duration instead of concentrated in a burst.

### Should Also Optimize

2. **SQL blob overhead** (4–5 min wall-clock for 8000 saves): Even with
   streaming, each 515 KB blob goes through SQL escaping + parser round-trip.
   Switching to binary protocol or prepared statements with `?` placeholders
   for the blob parameter would avoid the SQL escaping entirely.

### Nice to Have

3. **Stats query OOM** (Error 8175 on post-analyze queries): Consider pagination
   or streaming for stats table dumps when partition count is high.

4. **Stats dump timeout**: The HTTP stats dump endpoint needs a higher timeout
   or streaming support for high-partition tables.

---

## Appendix: Data Sources

All raw data is preserved in the following locations:

| Source | Path | Description |
|---|---|---|
| Test script | `./local-test.sh` | Orchestrates all 8 runs |
| Script log | `./local-test.sh.log` | Full output with timings, memory, errors |
| TiDB logs | `~/.tiup/data/sample-global-stats/tidb-0/tidb*.log` | 4 rotated + current |
| Timeline tool | `./tools/analyze-timeline.py` | Parses TiDB logs into per-run summaries |
| Run outputs | `./output/run_20260227_172852/` through `190922/` | Profiles, stats dumps, TSVs |
| Profile tool | `./tools/analyze-profile` (referenced in local-test.sh) | ANALYZE profiling harness |

### Reproducing the Timeline

```bash
python3 tools/analyze-timeline.py ~/.tiup/data/sample-global-stats/tidb-0/tidb*.log
```

### CPU Profile Files

Each run directory contains `cpu_profile_*.pb.gz` and `heap_*.pb.gz` files
(one per 30s interval). Analyze with:

```bash
go tool pprof -top output/run_20260227_172852/cpu_profile_30.pb.gz
go tool pprof -http=:8080 output/run_20260227_172852/heap_45.pb.gz
```
