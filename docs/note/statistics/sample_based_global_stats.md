# Sample-Based Global Statistics for Partitioned Tables

## Problem Statement

When TiDB uses dynamic partition pruning (`tidb_partition_prune_mode = 'dynamic'`), it
needs global-level statistics (spanning all partitions) for the query optimizer. Currently,
global stats are built by merging the already-constructed per-partition TopN and Histogram
structures. This merge process is:

1. **Very slow** — O(P² × T × H) for TopN, O(nBkt × P × log(nBkt)) for Histograms
2. **Lossy** — merging two approximations introduces compounding errors
3. **Complex** — ~500 lines of intricate bucket overlap/splitting/repeat-recalculation code

This document proposes building global statistics from the same raw sampled data used for
per-partition stats, via the existing `BuildHistAndTopN()` code path.

---

## Current Architecture

### Per-Partition Stats Pipeline (already good)

```
TiKV (per region)             TiDB (merge regions)           TiDB (build stats)
─────────────────             ────────────────────           ──────────────────
Full scan region        →     Weighted reservoir merge  →    Sort samples by value
A-Res reservoir sample        across all regions for         BuildHistAndTopN():
  MaxSampleSize rows          this partition                   → TopN (bounded min-heap)
Build FM-Sketch from          Merge FM-Sketches                → Histogram (equi-depth)
  ALL scanned rows                                             → FM-Sketch (NDV)
Return via protobuf                                          Scale counts by sampleFactor
```

**Key files:**
- `pkg/executor/analyze_col_v2.go` — orchestrates sampling and building
- `pkg/statistics/row_sampler.go` — `ReservoirRowSampleCollector`, `MergeCollector()`
- `pkg/statistics/builder.go:362` — `BuildHistAndTopN()` builds TopN + Histogram from samples
- `pkg/statistics/builder.go:223` — `buildHist()` builds Histogram buckets from sorted samples

**What each structure is built from:**

| Structure | Built From | Data Quality |
|---|---|---|
| **TopN** | Sorted sample array, bounded min-heap, scaled by `sampleFactor = totalRows/sampleSize` | Approximate (sampled) |
| **Histogram** | Same sorted sample array (excluding TopN ranges), equi-depth bucketing, scaled by `sampleFactor` | Approximate (sampled) |
| **FM-Sketch** | ALL rows scanned in TiKV (not just samples) | Good NDV estimate |
| **CM-Sketch** | V1 only, built during sampling | Approximate |

### Global Stats Pipeline (the slow, lossy part)

```
Per-partition stats saved to mysql.stats_* tables
                          ↓
                  (samples discarded)
                          ↓
handleGlobalStats() → MergePartitionStats2GlobalStats()
                          ↓
              Load partition stats from storage
                          ↓
           ┌──────────────┴──────────────┐
           │                             │
    Merge TopN across partitions   Merge Histograms
    O(P² × T × H)                 O(B_total × log(B_total))
    Cross-reference each TopN      + O(nBkt × P × log(nBkt))
    value with all other           Bucket overlap detection,
    partitions' TopNs and          splitting, repeat recalc
    histograms                     (has TODO: optimize)
           │                             │
           └──────────────┬──────────────┘
                          ↓
                   Global stats saved
```

**Key files:**
- `pkg/executor/analyze_global_stats.go` — `handleGlobalStats()` entry point
- `pkg/statistics/handle/globalstats/global_stats.go` — `MergePartitionStats2GlobalStats()`
- `pkg/statistics/handle/globalstats/topn.go` — `MergePartTopN2GlobalTopN()` (O(P² × T × H))
- `pkg/statistics/histogram.go:1611` — `MergePartitionHist2GlobalHist()` (O(nBkt × P × log(nBkt)))

### Why the current merge is slow

**TopN merging** (`topn.go:136-211`):
For each TopN value in partition `i`, search every other partition `j`:
1. Look up in `j`'s TopN via `FindTopN()` — O(log T)
2. If not found, look up in `j`'s histogram via `EqualRowCount()` — O(H)
3. If found in histogram, remove via `BinarySearchRemoveVal()` — O(H)

Total: O(P × T × P × (log(T) + H)) — with P=100, T=500, H=256: **~1.3 billion operations**

**Histogram merging** (`histogram.go:1611-1877`):
1. Collect all partition buckets + popped TopN values
2. Sort by upper bound — O(B_total × log(B_total))
3. Iteratively merge with overlap detection/splitting — complex O(B_total²) worst case
4. Recalculate repeat values — O(nBkt × P × log(nBkt)), flagged with `TODO: optimize`

### Why the current merge is lossy

The per-partition stats are **already approximate** — TopN counts and histogram bucket counts
are projections via `sampleFactor = totalRows / sampleSize`. The merge then:

1. Cross-references these approximate counts between partitions (approximate × approximate)
2. Uses `calcFraction4Datums()` to estimate bucket overlap — a linear interpolation heuristic
3. Recalculates repeat values by searching all partition histograms — but the histograms
   themselves only have approximate bucket boundaries
4. **Misses globally frequent values**: a value that appears in 1% of each partition (below
   each partition's TopN threshold) but 1% of the entire table (above the global TopN
   threshold) is **never discovered** by the current approach

---

## Proposed Solution: Sample-Based Global Stats

### Core Idea

Instead of discarding per-partition samples after building per-partition stats, **keep them
and merge them** into a global sample using the same weighted reservoir sampling algorithm
(`MergeCollector()`) that already merges region-level samples within a partition. Then call
the same `BuildHistAndTopN()` to produce global stats.

### Architecture

```
Partition p0: TiKV → merge regions → BuildHistAndTopN() → per-partition stats
                                   ↘ keep rootRowCollector ──┐
Partition p1: TiKV → merge regions → BuildHistAndTopN() → per-partition stats
                                   ↘ keep rootRowCollector ──┤
  ...                                                         ├→ MergeCollector()
Partition pN: TiKV → merge regions → BuildHistAndTopN() → per-partition stats
                                   ↘ keep rootRowCollector ──┘     ↓
                                                          globalRowCollector
                                                                   ↓
                                                          Decode + Sort samples
                                                                   ↓
                                                          BuildHistAndTopN()
                                                                   ↓
                                                            Global TopN +
                                                            Global Histogram +
                                                            Global FM-Sketch
```

### Why this works correctly

**Weighted reservoir sampling is composable.** This is already proven in TiDB's own codebase:
region-level samples are merged via `MergeCollector()` into a partition-level reservoir. The
same operation applied at the partition level produces a valid global reservoir.

Each sampled row has a random weight (`rng.Int63()`). `MergeCollector()` keeps the top-
MaxSampleSize samples by weight using a min-heap. A partition with more rows produces samples
with statistically higher weights (the top-100K from 10M random draws are higher than the
top-100K from 1K draws), so larger partitions naturally contribute proportionally more samples
to the global reservoir. This is a known property of the A-Res algorithm.

**Key code:** `pkg/statistics/row_sampler.go:372-396` — `MergeCollector()` already handles:
- Sample merging via min-heap (`sampleZippedRow`)
- FM-Sketch merging (`MergeFMSketch`)
- Null count accumulation
- Total size accumulation
- Row count accumulation

### Memory efficiency

**Incremental merging** keeps memory constant regardless of partition count:

```go
globalCollector := NewReservoirRowSampleCollector(MaxSampleSize, totalColLen)

for each partition {
    // ... analyze partition, build per-partition stats ...
    globalCollector.MergeCollector(partitionRootRowCollector)
    partitionRootRowCollector.DestroyAndPutToPool()  // free immediately
}
// Memory: only ~2 × MaxSampleSize rows at peak (global + one partition being merged)
```

This is the **same** memory as analyzing a single partition.

### Speed comparison

| Operation | Current merge | Sample-based |
|---|---|---|
| TopN | O(P² × T × H) | O(S × log(S)) sort + O(S) scan |
| Histogram | O(B_total × log(B_total)) + O(nBkt × P × log(nBkt)) | O(S) single pass |
| FM-Sketch | O(P × maxSize) | O(P × maxSize) — same |
| Sample merge | N/A | O(P × S) |
| **Total (P=100, S=100K, T=500, H=256)** | **~1.3 billion ops** | **~1.7 million ops** |

**~750× faster** for the global stats building step.

### Accuracy comparison

| Aspect | Current merge | Sample-based |
|---|---|---|
| TopN item discovery | Only finds values already in some partition's TopN. Globally frequent values spread evenly across partitions are **missed**. | Finds global heavy hitters directly from merged samples. |
| TopN counts | Cross-references already-scaled counts (approximate × approximate) | Single scaling: `sample_count × globalSampleFactor` — same quality as per-partition |
| Histogram boundaries | Heuristic overlap splitting via `calcFraction4Datums()`, compounds errors | Built fresh from merged samples — same quality as per-partition |
| Histogram repeat | Known-inaccurate O(nBkt × P) recalculation (flagged TODO) | Computed directly from sample adjacency — no cross-partition approximation |
| NDV | FM-Sketch merge | FM-Sketch merge (same) |

**Summary:** The sample-based approach produces global stats of the **same quality** as
per-partition stats (single-level approximation from sampling), whereas the current approach
compounds approximation errors across two levels (sampling + merging heuristics). It also
discovers globally frequent values that the current approach misses.

---

## Configuration

### Toggle variable: `tidb_enable_async_merge_global_stats`

The existing variable `tidb_enable_async_merge_global_stats` already controls the global
stats merge strategy and has a deprecation warning ("will always be enabled in a future
release"). There are already two implementations behind this toggle (async vs blocking).

**Proposal:** Add a **new session variable** to opt into sample-based global stats:

```
tidb_enable_sample_based_global_stats  (Boolean, default: false initially)
```

This variable controls whether the sample-based approach is used during ANALYZE. When
enabled, the ANALYZE executor keeps per-partition samples and merges them for global stats.
When disabled, the existing merge-based approach is used (respecting
`tidb_enable_async_merge_global_stats` for async vs blocking).

The existing merge code path remains untouched and is used as fallback for:
- Non-ANALYZE global stats rebuilds (e.g., DDL-triggered, manual `ANALYZE TABLE ... PARTITION`)
- Backward compatibility during rollout
- Cases where samples are unavailable (loaded from storage)

### Relevant existing variables

| Variable | Default | Purpose |
|---|---|---|
| `tidb_enable_async_merge_global_stats` | `true` | Async vs blocking merge (existing, to be deprecated) |
| `tidb_merge_partition_stats_concurrency` | `1` | TopN merge concurrency (existing, unused in new path) |
| `tidb_analyze_partition_concurrency` | `2` | Concurrent partition save workers |
| `tidb_analyze_version` | `2` | V1 vs V2 stats — new approach only works with V2 |
| `tidb_persist_analyze_options` | `true` | Persists analyze options |

---

## Implementation Plan

### Phase 1: Plumbing — pass samples through the ANALYZE pipeline

**Goal:** Make per-partition `rootRowCollector` samples available at the point where global
stats are built, without changing any existing behavior.

#### Step 1.1: Add global sample collector to AnalyzeExec

**File:** `pkg/executor/analyze.go`

Add a `globalSampleCollector` field to `AnalyzeExec` that accumulates partition samples:

```go
type AnalyzeExec struct {
    // ... existing fields ...
    globalSampleCollectors map[int64]*statistics.ReservoirRowSampleCollector // tableID → collector
}
```

Initialize it in `Next()` when `needGlobalStats && sampleBasedEnabled`.

#### Step 1.2: Attach samples to AnalyzeResults

**File:** `pkg/statistics/analyze.go`

Add an optional field to `AnalyzeResults` to carry the raw row collector:

```go
type AnalyzeResults struct {
    // ... existing fields ...
    // RowCollector carries the raw sample collector for sample-based global stats.
    // Only populated when tidb_enable_sample_based_global_stats is enabled.
    // Nil otherwise to avoid memory overhead.
    RowCollector statistics.RowSampleCollector
}
```

#### Step 1.3: Populate RowCollector in analyzeColumnsPushDownV2

**File:** `pkg/executor/analyze_col_v2.go`

After `buildSamplingStats()` returns and per-partition stats are built, if sample-based
global stats are enabled, attach the `rootRowCollector` to the results instead of
discarding it. The samples at this point are already decoded and have handles computed.

Key change near line 299: conditionally skip the `defer e.memTracker.Release(...)` and
instead pass the collector through.

#### Step 1.4: Accumulate samples in handleResultsErrorWithConcurrency

**File:** `pkg/executor/analyze.go`

In `handleResultsErrorWithConcurrency()` (line 481), after calling
`handleGlobalStats(needGlobalStats, globalStatsMap, results)`, merge the row collector
into the global accumulator:

```go
if results.RowCollector != nil {
    e.globalSampleCollectors[results.TableID.TableID].MergeCollector(results.RowCollector)
    results.RowCollector.DestroyAndPutToPool()
    results.RowCollector = nil
}
```

This is the incremental merge — constant memory regardless of partition count.

### Phase 2: Build global stats from merged samples

**Goal:** Use the accumulated global sample collector to build global stats via
`BuildHistAndTopN()`.

#### Step 2.1: Add sample-based global stats builder

**File:** `pkg/statistics/handle/globalstats/global_stats_sample.go` (new file)

Create a function that takes a `ReservoirRowSampleCollector` and produces `GlobalStats`:

```go
func BuildGlobalStatsFromSamples(
    sc sessionctx.Context,
    globalCollector *statistics.ReservoirRowSampleCollector,
    tableInfo *model.TableInfo,
    opts map[ast.AnalyzeOptionType]uint64,
    colsInfo []*model.ColumnInfo,
    indexes []*model.IndexInfo,
    histIDs []int64,
    isIndex bool,
) (*GlobalStats, error)
```

This function:
1. Extracts per-column `SampleCollector` from the `ReservoirRowSampleCollector` (same as
   `analyze_col_v2.go:352-359` does for per-partition stats)
2. Calls `statistics.BuildHistAndTopN()` for each column/index
3. Packages results into `GlobalStats` struct
4. FM-Sketch comes from the merged collector's `FMSketches` field

The code is largely extracted from the existing `subBuildWorker` in `analyze_col_v2.go`.

#### Step 2.2: Wire into handleGlobalStats

**File:** `pkg/executor/analyze_global_stats.go`

Add a branch in `handleGlobalStats()` that uses the sample-based path when the global
sample collector is available:

```go
func (e *AnalyzeExec) handleGlobalStats(statsHandle *handle.Handle, globalStatsMap globalStatsMap) error {
    for tableID, collector := range e.globalSampleCollectors {
        if collector != nil && collector.Base().Count > 0 {
            // Sample-based path
            globalStats, err := globalstats.BuildGlobalStatsFromSamples(...)
            if err != nil {
                return err
            }
            globalstats.WriteGlobalStatsToStorage(statsHandle, globalStats, info, tableID)
            collector.DestroyAndPutToPool()
            continue
        }
        // Fall through to existing merge-based path
        statsHandle.MergePartitionStats2GlobalStatsByTableID(...)
    }
    return nil
}
```

### Phase 3: Session variable and feature toggle

#### Step 3.1: Add session variable

**File:** `pkg/sessionctx/vardef/tidb_vars.go`

```go
const TiDBEnableSampleBasedGlobalStats = "tidb_enable_sample_based_global_stats"
// Default: false (opt-in during initial rollout)
const DefTiDBEnableSampleBasedGlobalStats = false
```

**File:** `pkg/sessionctx/variable/sysvar.go`

Register the new variable with validation.

**File:** `pkg/sessionctx/variable/session.go`

Add `EnableSampleBasedGlobalStats bool` to `SessionVars`.

#### Step 3.2: Gate the feature

In `analyze.go:Next()`, check the variable to decide whether to initialize the global
sample collector:

```go
sampleBasedGlobalStats := needGlobalStats &&
    sessionVars.EnableSampleBasedGlobalStats &&
    sessionVars.AnalyzeVersion >= 2
if sampleBasedGlobalStats {
    e.globalSampleCollectors = make(map[int64]*statistics.ReservoirRowSampleCollector)
}
```

### Phase 4: Handle edge cases

#### Step 4.1: Index-only analysis

When only indexes are analyzed (not columns), the samples come from index data, not row
data. The sample-based approach still works — `BuildHistAndTopN()` handles index samples
the same way. However, the global collector needs to be per-analysis-type (column vs index).

#### Step 4.2: Partial partition analysis

When only a subset of partitions is analyzed (e.g., `ANALYZE TABLE t PARTITION p0, p1`),
the sample-based approach can only build global stats from the analyzed partitions. For
unanalyzed partitions, fall back to the existing merge approach or skip global stats
(matching current behavior).

#### Step 4.3: Virtual columns

Virtual column samples are filled via `decodeSampleDataWithVirtualColumn()` in
`analyze_col_v2.go:307-315`. This happens before samples are passed to the global
collector, so virtual columns are handled transparently.

#### Step 4.4: Multi-valued indexes

Multi-valued indexes are analyzed separately (`AnalyzeIndexExec`) and have row counts
potentially higher than the table row count. These should continue using the existing
merge path.

#### Step 4.5: V1 stats

The sample-based approach requires V2 stats (which uses row-based reservoir sampling).
V1 stats use column-based sampling with CM-Sketch and have a different data flow. Gate
the feature on `AnalyzeVersion >= 2`.

### Phase 5: Testing

#### Step 5.1: Unit tests

**File:** `pkg/statistics/handle/globalstats/global_stats_sample_test.go` (new)

- Test that `BuildGlobalStatsFromSamples` produces valid TopN and Histograms
- Test with varying partition counts (1, 10, 100)
- Test with skewed data distributions
- Test with empty partitions
- Test with partitions of vastly different sizes

#### Step 5.2: Integration tests

**File:** `tests/integrationtest/t/statistics/global_stats_sample.test` (new)

- Create partitioned table, insert data, ANALYZE with sample-based enabled
- Verify global stats are correct via `SHOW STATS_HISTOGRAMS`, `SHOW STATS_TOPN`
- Compare query plans between sample-based and merge-based global stats
- Test the toggle variable works correctly

#### Step 5.3: Accuracy comparison tests

- Generate known data distributions
- Compare TopN accuracy: sample-based vs merge-based
- Specifically test the "hidden globally frequent value" case
- Compare histogram boundary accuracy

#### Step 5.4: Performance benchmarks

Extend existing benchmarks in `topn_bench_test.go`:
- Benchmark sample-based vs merge-based at P=100, 1000, 5000
- Measure wall-clock time and memory usage
- Verify the expected speedup

### Phase 6: Deprecation path

Once the sample-based approach is validated:
1. Change default to `true`
2. Add deprecation warning to `tidb_enable_async_merge_global_stats` and
   `tidb_merge_partition_stats_concurrency` when used during ANALYZE
3. Eventually remove the merge-based path for the ANALYZE case (keep it for non-ANALYZE
   global stats rebuilds where samples aren't available)

---

## Files to modify (summary)

| File | Change |
|---|---|
| `pkg/sessionctx/vardef/tidb_vars.go` | Add `TiDBEnableSampleBasedGlobalStats` variable definition |
| `pkg/sessionctx/variable/sysvar.go` | Register new sysvar |
| `pkg/sessionctx/variable/session.go` | Add `EnableSampleBasedGlobalStats` field |
| `pkg/statistics/analyze.go` | Add `RowCollector` field to `AnalyzeResults` |
| `pkg/executor/analyze.go` | Add `globalSampleCollectors` field, accumulate samples in result handler |
| `pkg/executor/analyze_col_v2.go` | Conditionally keep `rootRowCollector` instead of discarding |
| `pkg/executor/analyze_global_stats.go` | Branch between sample-based and merge-based paths |
| `pkg/statistics/handle/globalstats/global_stats_sample.go` | **New:** `BuildGlobalStatsFromSamples()` |
| `pkg/statistics/handle/globalstats/global_stats_sample_test.go` | **New:** Unit tests |
| `tests/integrationtest/t/statistics/global_stats_sample.test` | **New:** Integration test |

---

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Memory pressure from keeping samples | Incremental merge: only 2 × MaxSampleSize in memory at peak |
| Regression in stats quality | Feature-gated, default off; comparison tests before enabling |
| V1 stats incompatibility | Gate on `AnalyzeVersion >= 2` |
| Partial partition analyze | Fall back to merge-based path |
| Index-only analyze | Same BuildHistAndTopN code handles index samples |

---

## Appendix: Code References

- **Weighted reservoir sampling:** `pkg/statistics/row_sampler.go:62-65` (`ReservoirRowSampleCollector`)
- **Sample merging (A-Res):** `pkg/statistics/row_sampler.go:372-396` (`MergeCollector()`)
- **TopN + Histogram building:** `pkg/statistics/builder.go:362-566` (`BuildHistAndTopN()`)
- **Histogram building:** `pkg/statistics/builder.go:223-334` (`buildHist()`)
- **FM-Sketch merging:** `pkg/statistics/fmsketch.go:178-196` (`MergeFMSketch()`)
- **Current TopN merge (slow):** `pkg/statistics/handle/globalstats/topn.go:136-211`
- **Current Histogram merge (slow):** `pkg/statistics/histogram.go:1611-1877`
- **ANALYZE entry point:** `pkg/executor/analyze.go:90-211` (`AnalyzeExec.Next()`)
- **Per-partition sampling:** `pkg/executor/analyze_col_v2.go:280-360` (`buildSamplingStats` result handling)
- **Global stats entry:** `pkg/executor/analyze_global_stats.go:40-92` (`handleGlobalStats()`)
- **Merge dispatch:** `pkg/statistics/handle/globalstats/global_stats.go:96-125` (`MergePartitionStats2GlobalStats()`)
- **Async toggle:** `pkg/sessionctx/vardef/tidb_vars.go:986` (`TiDBEnableAsyncMergeGlobalStats`)
- **Merge concurrency:** `pkg/sessionctx/vardef/tidb_vars.go:984` (`TiDBMergePartitionStatsConcurrency`)
