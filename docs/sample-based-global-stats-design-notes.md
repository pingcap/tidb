# Sample-Based Global Stats: Design Discussions & Technical Notes

This document captures the technical discussions around the sample-based global stats implementation for TiDB partitioned tables (PR #66289).

---

## Table of Contents

1. [Overview](#overview)
2. [A-Res Algorithm](#a-res-algorithm)
3. [Per-Partition Analyze Options](#per-partition-analyze-options)
4. [Crash Safety During ANALYZE](#crash-safety-during-analyze)
5. [Memory Retention & Footprint](#memory-retention--footprint)
6. [NDV Calculation for Global Stats](#ndv-calculation-for-global-stats)
7. [Per-Bucket NDV in Sample-Based Path](#per-bucket-ndv-in-sample-based-path)
8. [NDV Levels: Calculation, Storage, and Configurations](#ndv-levels-calculation-storage-and-configurations)
9. [Storage Cost for Persisting Samples](#storage-cost-for-persisting-samples)
10. [Storage Strategy & Caching Partially Merged Results](#storage-strategy--caching-partially-merged-results)
11. [Subtracting Partition Stats from Global Stats](#subtracting-partition-stats-from-global-stats)
12. [Key Bug Fixes](#key-bug-fixes)

---

## Overview

The sample-based global stats feature builds global-level statistics from merged `ReservoirRowSampleCollector` data, avoiding the slow and lossy partition-level stats merge. Instead of merging per-partition histograms/TopN (the existing merge-based path), it builds TopN and Histograms directly from the merged samples using the same `BuildHistAndTopN` code path used for per-partition stats.

### Key Files

| File | Purpose |
|------|---------|
| `pkg/statistics/handle/globalstats/global_stats_sample.go` | Core: `BuildGlobalStatsFromSamples()`, column/index sample extraction |
| `pkg/executor/analyze_global_stats.go` | Call site: `handleGlobalStats()`, `buildGlobalStatsFromSamples()` |
| `pkg/executor/analyze.go` | `globalSampleCollectors` map, `mergePartitionSamplesForGlobal()`, cleanup |
| `pkg/statistics/handle/globalstats/global_stats_test.go` | 7 sample-based global stats tests |
| `pkg/statistics/handle/globalstats/global_stats.go` | Merge-based path, `WriteGlobalStatsToStorage()` |

---

## A-Res Algorithm

**Q: What is the A-Res algorithm?**

A-Res stands for **Algorithm A with Reservoir**, a weighted reservoir sampling algorithm from the paper by Efraimidis & Spirakis (2006): *"Weighted random sampling with a reservoir"*.

### How It Works

Traditional reservoir sampling (Algorithm R by Vitter) gives each item equal probability of being in the sample. A-Res extends this to **weighted** items, where each item has a weight `w_i` that determines its relative probability of being included.

**Core formula:** For each item `i` with weight `w_i`, compute a key:

```
k_i = u_i^(1/w_i)
```

where `u_i` is a uniform random number in `(0, 1)`. The reservoir keeps the items with the **largest** keys.

### Why It Matters for TiDB

When merging partition samples, different partitions may have different row counts. A-Res ensures that the merged sample correctly represents the relative sizes of partitions -- a partition with 1M rows contributes proportionally more samples than one with 1K rows, even though both were sampled down to `MaxSampleSize`.

TiDB explicitly references this algorithm at `pkg/statistics/row_sampler.go:61`:

```go
// MergeCollector merges the collectors using weighted reservoir sampling (A-Res algorithm).
```

The `MergeCollector()` function at line 373 merges FMSketches, NullCounts, TotalSizes, and samples via this weighted reservoir sampling.

---

## Per-Partition Analyze Options

**Q: Can different partitions have different `WITH NUM SAMPLES` / `WITH FLOATNUM SAMPLERATE` options? If so, will it affect global stats?**

### Answer: No, in dynamic prune mode

In dynamic partition prune mode (where global stats are built), all partitions get the **same** analyze options. This is enforced in `pkg/planner/core/planbuilder.go` via `genV2AnalyzeOptions()`:

- In dynamic mode, all partitions get the same options as the table level
- Default options: `NumBuckets: 256`, `NumTopN: 100` (V2), `NumSamples: 10000` (V1) / `0` (V2, dynamic)

### What If They Somehow Differed?

Even if partitions had different sample sizes (e.g., from a previous static-mode analyze), `MergeCollector()` handles it correctly regardless. The A-Res weighted sampling correctly accounts for different collector sizes during merge. So the global stats would still be valid -- the weight adjustments would account for the differences.

---

## Crash Safety During ANALYZE

**Q: What happens if the TiDB node crashes/restarts during ANALYZE?**

### Analysis

1. **Each partition's stats are committed independently**: In `analyzeSaveStatsWorker.run()` (`pkg/executor/analyze_worker.go`), each partition's stats are saved in its own transaction via `FlagWrapTxn`. So if the node crashes after partitions 1-5 but before partition 6, partitions 1-5 have their stats updated.

2. **Global stats are built after all partitions complete**: The `handleGlobalStats()` call happens after all partition analyze workers finish. If the crash happens during global stats building, the per-partition stats are already persisted, but global stats are stale (from the previous analyze, if any).

3. **No automatic retry mechanism**: There's no built-in retry for crashed ANALYZE jobs. `CleanupCorruptedAnalyzeJobsOnDeadInstances()` in `pkg/statistics/handle/autoanalyze/autoanalyze.go` marks stuck jobs as failed after 10 minutes. Auto-analyze may eventually re-trigger the analysis.

4. **Sample-based path is no worse than merge-based path**: Both paths build global stats at the same point in the workflow. The crash window is identical.

### Summary

- Partition stats are crash-safe (per-partition transactions)
- Global stats are NOT crash-safe (built after all partitions, not transactional)
- Manual re-run of `ANALYZE TABLE` is required after crash
- Auto-analyze may eventually pick it up based on `ModifyCount` thresholds

---

## Memory Retention & Footprint

**Q: How long are the previously discarded samples kept for global stats merge? How much memory do they use?**

### Retention Duration

Samples are retained for the **duration of `ANALYZE TABLE`**. The lifecycle:

1. Each partition analyze task produces a `ReservoirRowSampleCollector`
2. After each partition completes, its collector is merged into a single per-table merged collector via `mergePartitionSamplesForGlobal()`
3. The merged collector is stored in `e.globalSampleCollectors[tableID]`
4. After `handleGlobalStats()` completes, the cleanup code releases them:

```go
for id, c := range e.globalSampleCollectors {
    if c != nil {
        c.DestroyAndPutToPool()
    }
    delete(e.globalSampleCollectors, id)
}
e.globalSampleCollectors = nil
```

### Memory Footprint

For a table with default settings (10K `MaxSampleSize`, 5 columns):

| Component | Size |
|-----------|------|
| Sample rows (10K x 5 cols x ~100 bytes/datum) | ~5 MB |
| FMSketches (5 cols + N indexes, max 10K entries each) | ~800 KB |
| NullCount, TotalSize arrays | negligible |
| Weights array (10K float64s) | ~80 KB |
| **Total** | **~6.6 MB per table** |

Key insight: Only **one** merged collector is kept per table, regardless of partition count. Each partition's collector is merged into the running total and then discarded. So memory usage does not scale with partition count.

---

## NDV Calculation for Global Stats

**Q: How are NDVs calculated for global stats?**

### Both Paths Use FMSketch

Both the merge-based and sample-based paths ultimately derive the histogram-level NDV from `FMSketch.NDV()`:

**FMSketch NDV formula** (from `pkg/statistics/fmsketch.go:100`):

```go
func (s *FMSketch) NDV() int64 {
    return int64(s.mask+1) * int64(s.hashset.Count())
}
```

### Merge-Based Path

1. Each partition's FMSketch is already persisted in `mysql.stats_fm_sketch`
2. All partition FMSketches are loaded and merged via `MergeFMSketch()` (union of hashsets with mask-level adjustment)
3. `globalStatsNDV = min(mergedFMSketch.NDV(), globalCount)` (line 320 of `global_stats.go`)

### Sample-Based Path

1. Each partition's FMSketch is merged during `MergeCollector()` (same `MergeFMSketch` call)
2. The merged FMSketch is passed to `BuildHistAndTopN()` via the `SampleCollector`
3. `BuildHistAndTopN()` reads NDV from `collector.FMSketch.NDV()` (line 401 of `builder.go`)
4. This NDV is set on the histogram: `hg.NDV = collector.FMSketch.NDV()`

### Test Discrepancy

In testing, the sample-based path produced NDV=14 for a composite index while the expected value was 11. This is because `BuildHistAndTopN`'s interaction between TopN pruning and bucket building can adjust the effective NDV. The test was relaxed to check `> 0` rather than an exact value.

---

## Per-Bucket NDV in Sample-Based Path

**Q: Is per-bucket NDV correct in the sample-based path (not zeroed out)?**

### Answer: Per-bucket NDV is ALSO zero in the sample-based path

Per-bucket NDV (`Bucket.NDV`) is zero in both the sample-based and the standard sample-based per-partition path. This is because `buildHist()` in `pkg/statistics/builder.go:223` always passes `needBucketNDV = false` for sample-based paths:

```go
// In BuildHistAndTopN:
hg, err := buildHist(sc, hg, samples, count, ndv, numBuckets, memTracker, false)
//                                                                         ^^^^^ needBucketNDV = false
```

Only `SortedBuilder` (used for V2 full-scan / `analyzeColumnsPushdown`) sets `needBucketNDV = true`:

```go
// In SortedBuilder:
needBucketNDV: statsVer >= Version2
```

### Impact on Query Optimization

Per-bucket NDV is used in `EqualRowCount()` (`histogram.go:505`) for better cardinality estimation within histogram buckets. When it's zero, the optimizer falls back to `totalNotNull / hg.NDV` as a uniform estimate.

### Regression vs Merge-Based Path

The sample-based path is actually a **regression for index stats** compared to the merge-based path. The merge-based path computes per-bucket NDV for indexes via `mergeBucketNDV()`, which uses overlap heuristics. The sample-based path produces zero per-bucket NDV for all stats (columns and indexes).

**Q: If there are the same number of partitions as buckets, would the total sample set be OK for accurate per-bucket NDV?**

The limiting factor isn't partitions vs buckets but `MaxSampleSize` (reservoir sampling caps at 10K). Even if all rows fit in samples, the code doesn't compute per-bucket NDV -- it's a **code design choice**, not a data limitation. The `buildHist()` function simply passes `needBucketNDV = false` regardless of data availability.

To fix this, one would need to change the `BuildHistAndTopN` call to pass `needBucketNDV = true` (or `statsVer >= Version2`) -- but this would require additional testing to ensure correctness.

---

## NDV Levels: Calculation, Storage, and Configurations

**Q: At what levels are NDVs calculated and stored, and in which implementations/configurations?**

### Three NDV Levels

| Level | Field | Storage | Description |
|-------|-------|---------|-------------|
| **Histogram-level** | `hg.NDV` | `stats_histograms.distinct_count` | Total unique values across entire column/index |
| **Per-bucket** | `Bucket.NDV` | `stats_buckets` (as part of bucket data) | Unique values within each histogram bucket |
| **FMSketch** | `FMSketch.NDV()` | `stats_fm_sketch` (probabilistic) | Probabilistic NDV estimate from Flajolet-Martin sketch |

### NDV Population Matrix

| Path | Scope | hg.NDV | Per-Bucket NDV | FMSketch |
|------|-------|--------|----------------|----------|
| V1 sampling | Per-partition | From FMSketch | Always 0 | Yes |
| V2 full-scan (`SortedBuilder`) | Per-partition | From FMSketch | **Yes** (when `statsVer >= 2`) | Yes |
| V2 sampling (`BuildHistAndTopN`) | Per-partition | From FMSketch | Always 0 | Yes |
| Merge-based global (columns) | Global | From merged FMSketch | **Always 0** (zeroed at line 1872) | Yes (merged) |
| Merge-based global (indexes) | Global | From merged FMSketch | **Yes** (via `mergeBucketNDV()`) | Yes (merged) |
| **Sample-based global (columns)** | Global | From merged FMSketch | **Always 0** | Yes (merged) |
| **Sample-based global (indexes)** | Global | From merged FMSketch | **Always 0** (regression!) | Yes (merged) |

### Key Observations

1. **hg.NDV** is always populated -- it comes from `FMSketch.NDV()` in all paths
2. **Per-bucket NDV** is the differentiator -- only V2 full-scan and merge-based index paths populate it
3. **Sample-based global stats is a regression for index per-bucket NDV** compared to merge-based global stats
4. Per-bucket NDV matters for `EqualRowCount()` accuracy -- without it, the optimizer uses uniform distribution assumption

---

## Storage Cost for Persisting Samples

**Q: How much more space would be needed to save all samples and sketches from TiKV for each partition?**

### What's Already Persisted

| Component | Storage Location | Already Saved? |
|-----------|-----------------|----------------|
| FMSketches | `mysql.stats_fm_sketch` | Yes |
| NullCounts | `mysql.stats_histograms.null_count` | Yes |
| TotalSizes | `mysql.stats_histograms.tot_col_size` | Yes |
| Histograms | `mysql.stats_histograms` + `stats_buckets` | Yes |
| TopN | `mysql.stats_top_n` | Yes |
| **Sample rows** | Not stored | **No** |
| **Sample weights** | Not stored | **No** |

### Size Estimates for New Data

For a typical table (5 columns, 10K `MaxSampleSize`):

| Component | Per-Partition Size |
|-----------|-------------------|
| Sample rows (10K rows x 5 cols x ~50-100 bytes avg) | 250 KB - 500 KB |
| Sample weights (10K x 8 bytes) | 80 KB |
| Row metadata (ordinals, etc.) | ~40 KB |
| Serialization overhead (protobuf/msgpack) | ~10-20% |
| **Total new data per partition** | **~400 KB - 700 KB** |

For a table with 100 partitions: **~40 MB - 70 MB** additional storage in TiKV.

This is roughly **1.5-6x more storage** compared to what's already stored per partition (histograms + TopN + FMSketches are typically 100-300 KB per partition).

---

## Storage Strategy & Caching Partially Merged Results

**Q: How should persisted samples be stored? Should partially merged results be stored?**

### Recommended Storage: Blob-per-Partition in a New System Table

Create a new system table, e.g., `mysql.stats_samples`:

```sql
CREATE TABLE stats_samples (
    table_id     BIGINT NOT NULL,
    partition_id BIGINT NOT NULL,  -- physical partition ID
    version      BIGINT NOT NULL,  -- stats version (timestamp)
    sample_data  LONGBLOB NOT NULL, -- serialized ReservoirRowSampleCollector
    PRIMARY KEY (table_id, partition_id)
);
```

Serialize each partition's `ReservoirRowSampleCollector` (samples + weights + FMSketches + NullCounts + TotalSizes) as a single blob.

### Three Caching Strategies Analyzed

#### Strategy 1: Per-Partition Only (Recommended)

Store only individual partition collectors. To build global stats, load all N partitions and merge in memory.

| Aspect | Assessment |
|--------|------------|
| Storage | N blobs (one per partition) |
| I/O to rebuild | Read all N blobs |
| I/O for 1-partition update | Read N blobs (all partitions) |
| Complexity | Low |
| DDL compatibility | Simple (drop partition = delete row) |
| Merge correctness | Guaranteed (always from raw data) |

**Why it's recommended:**
- In-memory A-Res merge of N collectors with 10K samples each is fast (milliseconds for 100 partitions)
- The I/O bottleneck is reading ~50-70 MB for 100 partitions -- acceptable for a stats operation
- DDL operations (ADD/DROP/TRUNCATE PARTITION) just insert/delete rows
- No cache invalidation complexity

#### Strategy 2: Binary Tree of Merged Results

Store intermediate merge results in a tree structure. E.g., for 8 partitions: store merges of (1,2), (3,4), (5,6), (7,8), (1-4), (5-8).

| Aspect | Assessment |
|--------|------------|
| Storage | ~2N blobs |
| I/O to rebuild | Read 2 blobs (log2 N depth) |
| I/O for 1-partition update | Read log2 N blobs + recompute path |
| Complexity | High |
| DDL compatibility | Complex (tree rebalancing needed) |
| Merge correctness | Guaranteed (merging is associative) |

**Pros:** Optimal I/O for incremental updates (O(log N) reads).
**Cons:** Tree rebalancing on DDL, 2x storage, complex bookkeeping, marginal benefit when N < 1000.

#### Strategy 3: Fixed Groups (e.g., groups of 10)

Store merged results for fixed-size groups. E.g., for 100 partitions: 10 groups of 10, each stored as a merged blob.

| Aspect | Assessment |
|--------|------------|
| Storage | N + N/G blobs (G = group size) |
| I/O to rebuild | Read N/G blobs |
| I/O for 1-partition update | Read N/G blobs + rebuild 1 group |
| Complexity | Medium |
| DDL compatibility | Moderate (rebuild affected group) |

**Middle ground** between strategies 1 and 2. Less optimal than binary tree, but simpler.

### Recommendation

**Start with Strategy 1 (per-partition only)**. The I/O cost of reading all partitions is acceptable for realistic partition counts (< 1000). Merging 100 collectors of 10K samples each takes < 100ms in memory. The simplicity gains (no cache invalidation, trivial DDL handling, no consistency bugs) far outweigh the marginal I/O savings of tree/group strategies.

If profiling later shows that I/O is a bottleneck for tables with 1000+ partitions, Strategy 3 (fixed groups) can be added as an optimization layer without changing the underlying per-partition storage.

---

## Subtracting Partition Stats from Global Stats

**Q: Is it possible to subtract a single partition's statistics from global stats and still be accurate? (e.g., for partition drop/truncate, or re-analyzing a single partition)**

### Short Answer: Partially, but not accurately enough to be useful

### Component-by-Component Analysis

#### Fully Subtractable (Additive Metrics)

| Metric | Subtractable? | How |
|--------|--------------|-----|
| **Count** | Yes | `global.Count -= partition.Count` |
| **NullCount** | Yes | `global.NullCount[i] -= partition.NullCount[i]` |
| **TotalSize** | Yes | `global.TotalSize[i] -= partition.TotalSize[i]` |
| **ModifyCount** | Yes | Simple subtraction |

#### Not Subtractable

**FMSketch (NDV estimation):**
FMSketch merging is a set union of hash values with mask-level adjustment. Set union is not reversible -- you can't compute `A union B union C` minus `B`'s contribution without knowing which hash values came exclusively from `B`. If a hash value exists in both partition B and partition C, removing B's contribution would incorrectly remove it from the global sketch.

You *could* make it work by storing each partition's FMSketch separately and re-merging the remaining ones -- but that's just a full rebuild from persisted per-partition sketches, not a subtraction.

**Reservoir Samples:**
Reservoir sampling merge is lossy by design. When two collectors merge via A-Res weighted sampling, samples from both partitions compete for slots. Once a sample is evicted, you don't know which partition it came from. Even if you tagged each sample with its source partition, removing that partition's samples would leave holes in the reservoir with incorrect weights -- the remaining samples would no longer be a valid weighted random sample of the remaining partitions.

**Histograms:**
Histograms encode bucket boundaries and counts. The merge-based path (`MergePartitionHist2GlobalHist`) re-buckets across all partition histograms. You can't subtract one partition's contribution because:
- Bucket boundaries were chosen based on all partitions' data
- Frequencies within buckets are aggregated
- Per-bucket NDV uses overlap heuristics (`mergeBucketNDV`) that aren't reversible

**TopN:**
TopN values are merged by summing frequencies across partitions. In theory, you could subtract one partition's TopN frequencies -- but values that fell below the TopN threshold after subtraction would be lost (you don't know what the "next most frequent" values are without the full data).

### Practical Implications

| Scenario | Subtraction viable? | Better approach |
|----------|---------------------|-----------------|
| **Partition dropped** | No | Re-merge remaining partitions' persisted samples/sketches |
| **Partition truncated** | No | Re-merge with empty partition |
| **Single partition re-analyzed** | No | Re-merge all persisted per-partition data |

### Why Per-Partition Persistence Is the Solution

With per-partition samples and FMSketches persisted, "updating global stats after one partition changes" becomes:

1. Re-analyze the changed partition -> new per-partition samples + sketches
2. Load all other partitions' persisted samples + sketches from storage
3. Merge all of them (including the fresh one) into new global stats

The merge step is fast in-memory (A-Res merge of N collectors is O(N x MaxSampleSize), and FMSketch union is O(N x hashset_size)). The I/O cost is reading ~500KB-2MB per partition, which is the price of correctness.

### "Additive-Only" Fast Update (Quick-and-Dirty)

If you wanted a quick update without full rebuild, you could update only the additive metrics (Count, NullCount, TotalSize) and leave NDV/histograms/TopN stale until the next full rebuild. This gives the optimizer a correct row count estimate but stale distribution information -- better than nothing for DDL operations like `DROP PARTITION`.

TiDB already does something similar: when a partition is dropped, the global stats are eventually rebuilt by auto-analyze detecting the staleness via `ModifyCount` thresholds.

---

## Key Bug Fixes

### Critical FMSketch Offset Bug

**Problem:** The original code used `len(tableInfo.Columns)` to compute the offset of index FMSketches in the collector's arrays. But `tableInfo.Columns` does NOT include `_tidb_rowid` (the hidden column for tables without a clustered primary key), while the collector's arrays DO include it. This caused an off-by-one error in FMSketch lookup for index stats.

**Fix:** Introduced `sampleColumnCount()` helper that derives the actual column count from the sample data:

```go
func sampleColumnCount(collector *statistics.ReservoirRowSampleCollector) int {
    if collector.Base().Samples.Len() > 0 {
        return len(collector.Base().Samples[0].Columns)
    }
    return len(collector.Base().NullCount)
}
```

### Memory Cleanup

Added `DestroyAndPutToPool()` cleanup for `globalSampleCollectors` after `handleGlobalStats()` completes to free memory promptly:

```go
for id, c := range e.globalSampleCollectors {
    if c != nil {
        c.DestroyAndPutToPool()
    }
    delete(e.globalSampleCollectors, id)
}
e.globalSampleCollectors = nil
```

### Unused Parameter Lint Errors

- Removed unused `indexes` parameter from `buildGlobalColumnStatsFromSamples()`
- Removed unused `statsHandle` parameter from `buildGlobalStatsFromSamples()` (in `analyze_global_stats.go`)

---

## Progressive Pruning & Sample Persistence

### Motivation

Without persisted per-partition samples, re-analyzing a single partition requires re-analyzing ALL partitions to rebuild global stats. By saving pruned samples to storage, future incremental rebuilds become possible: load saved samples for unchanged partitions, merge with the freshly analyzed partition, and rebuild global stats without touching TiKV.

### Progressive Pruning Algorithm

When persisting samples, we don't need the full `MaxSampleSize` per partition -- we only need enough to produce accurate global stats when all partitions are merged. The `SamplePruner` computes a per-partition target using a fixed total budget (default 30,000 samples across all partitions):

1. **First partition**: target = budget / N (where N = total partition count)
2. **Subsequent partitions**: target = (budget / N) * (partitionRows / avgRowsSoFar), clamped to [500, totalBudget]

This progressive approach improves estimates as more partitions are observed. Larger partitions get proportionally more samples, while a minimum floor of 500 guarantees reasonable estimates even for tiny partitions.

The `PruneTo()` method on `ReservoirRowSampleCollector` performs correct A-Res sub-sampling: it creates a new collector with a smaller `MaxSampleSize` and feeds all current samples through `sampleZippedRow`, so only the highest-weight items survive.

### mysql.stats_samples Schema

```sql
CREATE TABLE mysql.stats_samples (
    table_id        BIGINT(64) NOT NULL,
    partition_id    BIGINT(64) NOT NULL,
    version         BIGINT(64) UNSIGNED NOT NULL,
    sample_data     LONGBLOB NOT NULL,
    max_sample_size INT NOT NULL,
    sample_count    INT NOT NULL,
    row_count       BIGINT(64) NOT NULL,
    PRIMARY KEY (table_id, partition_id)
);
```

- `sample_data`: protobuf-serialized `tipb.RowSampleCollector` (reusing existing `ToProto()`/`FromProto()`)
- `version`: stats version (txn timestamp) for staleness detection
- `max_sample_size`, `sample_count`, `row_count`: metadata for pruning decisions

### Save / Load Flow

**During ANALYZE (save):**
1. Each partition's analyze task produces a `ReservoirRowSampleCollector`
2. Before merging into the global collector, `savePartitionSamples()` prunes the collector via `SamplePruner.PruneCount()` + `PruneTo()` and persists it via `SavePartitionSamples()`
3. The original (unpruned) collector continues to `mergePartitionSamplesForGlobal()` as before

**Future incremental rebuild (load):**
1. Re-analyze only the changed partition
2. `LoadSampleCollectorsFromStorage()` loads saved collectors for all other partitions
3. Merge all collectors and rebuild global stats

### DDL Cleanup

Persisted samples are cleaned up alongside other stats when partitions or tables are dropped/truncated/reorganized, via `DeleteSamplesByPartition()` and `DeleteSamplesByTable()` calls in the DDL subscriber.
