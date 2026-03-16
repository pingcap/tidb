# Sample-Based Global Partition Statistics

- Author(s): [Mattias Jonsson](http://github.com/mjonss)
- Discussion PR: https://github.com/pingcap/tidb/pull/66289
- Tracking Issue: https://github.com/pingcap/tidb/issues/66220

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
    * [Current Approach: Merge-Based Global Stats](#current-approach-merge-based-global-stats)
    * [Proposed Approach: Sample-Based Global Stats](#proposed-approach-sample-based-global-stats)
    * [Weighted Reservoir Sampling (A-Res)](#weighted-reservoir-sampling-a-res)
    * [Persisting Samples for Incremental Rebuild](#persisting-samples-for-incremental-rebuild)
    * [Progressive Pruning](#progressive-pruning)
    * [Single-Partition Re-Analyze](#single-partition-re-analyze)
    * [DDL Handling](#ddl-handling)
    * [User Interface](#user-interface)
    * [Fallback and Compatibility](#fallback-and-compatibility)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This proposal introduces a sample-based approach for building global-level statistics on partitioned tables. Instead of merging per-partition histograms and TopN arrays — which is both lossy and slow — global histograms and TopN are built directly from merged reservoir samples, reusing the same histogram construction logic that already handles the region merge of stats to table and per-partition level TopN and Histograms.

For non-partitioned tables, TiDB already builds statistics this way: each TiKV region returns a reservoir sample collector, all region collectors are merged via A-Res weighted sampling into a single collector, and histograms and TopN are built from the merged samples. The current partition-to-global path bypasses this proven infrastructure and instead attempts to merge pre-built histograms and TopN arrays — a fundamentally lossier operation. This proposal replaces that merge with the same sample-based approach already used for regions, applied at the partition-to-global level. The only addition is persisting pruned samples per partition so that future single-partition re-analyzes can load them instead of re-scanning unchanged partitions from TiKV.

By also persisting pruned samples per partition, this design enables incremental global stats rebuilds: re-analyzing a single partition no longer requires re-scanning all other partitions from TiKV.

## Motivation or Background

TiDB uses dynamic partition pruning by default, which requires global-level statistics spanning all partitions. Today there are two performance problems with how these global stats are produced.

### Problem 1: Global Stats Merge Is Slow and Lossy

After all partitions are analyzed, global stats are built by merging per-partition histograms and TopN arrays. This merge step has two issues (P = partitions, T = TopN entries per partition, B = histogram buckets per partition, B_global = target global bucket count, NDV = number of distinct values):

**It is slow for many partitions.** The TopN merge is O(P² × T × log B) in the worst case: for each unique TopN value (up to P × T when partitions have non-overlapping TopN sets, common for hash partitions), it searches every partition's TopN via binary search and, if not found, queries that partition's histogram via binary search. The histogram merge collects all P × B buckets into memory, sorts them in O(P × B × log(P × B)), then re-buckets with overlap detection and repeat recalculation in O(B_global × P × log B). Both steps must load all P partitions' histograms and TopN arrays from storage simultaneously, requiring O(P × (T + B)) memory per column. In benchmarks on 10,000 partitions (500 TopN entries, 500 buckets each), the full merge took 4.5 seconds and allocated 1.9 GB — per column. For tables with thousands of partitions and dozens of columns, the merge step dominates ANALYZE time while the actual per-partition data scanning may finish in seconds.

**It is lossy.** The merge compounds approximation errors in three ways:

1. **TopN count inflation.** When a TopN value from one partition is not in another partition's TopN, the merge estimates its count from the histogram using `totalRows/NDV` (uniform assumption). This estimate accumulates across partitions. Testing with 20 partitions showed a value with true count=120 inflated to 1,920 (16×), causing the merge to pick the wrong value for global TopN.

2. **Histogram boundary misalignment.** Bucket boundaries optimized for individual partitions do not align across partitions. The merge uses linear interpolation to estimate bucket overlap — a heuristic that compounds errors when applied across many partitions. The TopN merge also mutates partition histograms (removing values via binary search) before they are merged, further reducing histogram quality.

3. **Globally frequent values missed.** A value appearing in 1% of each partition (below each partition's TopN threshold) but 1% of the entire table (above the global TopN threshold) is never discovered by the current approach, because neither partition promoted it to TopN.

Measured on a table with 8,000 partitions and 30M rows, building histograms directly from sample data instead of merging produced 3–5× more uniform buckets (CV 0.000–0.048 vs 0.062–0.158) and 1,000× better value range accuracy (+7 overshoot vs +7,999). The sample-based path also computes column correlation (the merge path always returns 0) and avoids the O(P × (T + B)) memory spike that caused a second full ANALYZE to crash with OOM in testing.

### Problem 2: Single-Partition ANALYZE Requires Full Rebuild

When only one partition has changed and is re-analyzed via `ANALYZE TABLE t PARTITION p5`, global stats still need to reflect all partitions. Today, this triggers a full merge of all P partitions' stored histograms — the same expensive merge step as a full table analyze, with the same O(P² × T × log B) TopN merge, O(P × B × log(P × B)) histogram re-bucketing, and O(P × (T + B)) memory per column. No intermediate data is saved that would allow rebuilding global stats from just the changed partition plus cached results from other partitions.

For a table with 8,000 partitions, this means that even a single-partition re-analyze must read and merge histogram data for all 8,000 partitions, even though 7,999 of them have not changed. In testing, this took over 8 minutes for a single-partition re-analyze on a table where the partition scan itself completed in under 2 seconds.

### Proposed Solution

Each partition's ANALYZE already produces a reservoir sample collector containing weighted random samples. These collectors can be merged across partitions using weighted reservoir sampling (A-Res algorithm), and the merged samples can be used to build global histograms and TopN directly — bypassing the lossy histogram merge entirely.

By persisting pruned samples per partition, future single-partition re-analyzes can load saved samples for unchanged partitions from storage and merge them with the fresh partition's samples in memory. This avoids re-scanning unchanged partitions from TiKV.

## Detailed Design

### Current Approach: Merge-Based Global Stats

```text
ANALYZE TABLE t

  For each partition Pi (in parallel, up to concurrency limit):
    1. Scan partition data from TiKV (reservoir sampling)
    2. Build partition histogram + TopN from samples
    3. Save partition stats to mysql.stats_* tables
                              │
                              ▼
  After all partitions complete:
    4. Load all P partitions' histograms + TopN from storage
    5. Merge TopN: sum frequencies, drop below-threshold values
    6. Merge histograms: re-bucket across all partitions (lossy)
    7. Union FMSketches for NDV
    8. Save global stats
```

The merge in steps 5-7 is the bottleneck for tables with many partitions: it requires O(P) reads from storage and produces results that are structurally inferior to stats built directly from data.

### Proposed Approach: Sample-Based Global Stats

```text
ANALYZE TABLE t

  For each partition Pi (in parallel, up to concurrency limit):
    1. Scan partition data from TiKV (reservoir sampling)
    2. Build partition histogram + TopN from samples
    3. Save partition stats to mysql.stats_* tables
    4. Prune samples and persist to storage          ◄── NEW
    5. Merge samples into running global collector   ◄── NEW
       (A-Res weighted merge, O(MaxSampleSize) per partition)
                              │
                              ▼
  After all partitions complete:
    6. Build global histogram + TopN from merged samples   ◄── NEW
       (same code path as step 2 — not a histogram merge)
    7. Save global stats
```

The key differences:

- **Step 5** merges each partition's sample collector into a single accumulator as it completes. The previous partition's collector is discarded, so memory does not grow with partition count.
- **Step 6** builds global stats from actual sample data using the same histogram construction logic as per-partition stats. There is no histogram merging step.
- **Step 4** persists a pruned copy of each partition's samples to enable future incremental rebuilds.

### Weighted Reservoir Sampling (A-Res)

When merging samples from partitions with different row counts, proportional representation is required. A partition with 1M rows should contribute more samples than one with 1K rows, even though both were sampled down to the same reservoir size.

The A-Res algorithm (Efraimidis & Spirakis, 2006) achieves this. For each item with weight `w_i`, a key is computed:

```text
k_i = u_i^(1/w_i)    where u_i ~ Uniform(0, 1)
```

The reservoir keeps items with the largest keys. TiDB already implements this algorithm for per-partition sampling. This design reuses it for cross-partition merging.

Key property: merging is **associative** — `merge(merge(A, B), C) == merge(A, merge(B, C))`. This allows streaming: each partition's collector is merged into a running accumulator and then discarded, so memory usage is constant regardless of partition count.

### Persisting Samples for Incremental Rebuild

Each partition's pruned sample collector is serialized via protobuf and stored in `mysql.stats_table_data`, keyed by the partition's physical table ID:

```sql
CREATE TABLE mysql.stats_table_data (
    table_id  BIGINT(64) NOT NULL,   -- physical partition ID
    type      INT(11) NOT NULL,      -- data type (sample = 2)
    hist_id   BIGINT(64) NOT NULL,   -- 0 for sample data
    value     LONGBLOB NOT NULL,     -- protobuf-serialized collector
    PRIMARY KEY (table_id, type, hist_id)
);
```

Each partition stores one row containing all samples to be persisted for that partition. The serialized blob is a `ReservoirRowSampleCollector` — the same structure returned by TiKV during ANALYZE. Each sample row contains values only for the columns included in the ANALYZE request (controlled by `ANALYZE TABLE ... ALL COLUMNS`, `PREDICATE COLUMNS`, or `COLUMNS c1, c2, ...`), not all table columns. The collector also includes per-column FMSketches, per-column null counts, per-column total sizes, the total row count, and A-Res weights. `hist_id` is 0 because the blob is partition-scoped — individual columns are extracted from the full-row samples only when building histograms. `REPLACE INTO` overwrites stale samples atomically.

The per-partition blob size depends primarily on the number of samples and the number and types of analyzed columns. For a table with 20 integer columns and 500 pruned samples, the blob is roughly 50–100 KB. For 50 mixed-type columns (integers, strings, timestamps) with 3,000–4,000 samples, it grows to 400–700 KB. Tables analyzed with `PREDICATE COLUMNS` will have smaller blobs since fewer columns are sampled.

### Progressive Pruning

Persisting the full reservoir (up to 10,000 samples) per partition would use excessive storage. Instead, a target budget (default 30,000 samples) determines proportional allocation:

```text
First partition:       target = budget / totalPartitions
Subsequent partitions: target = (budget / totalPartitions) × (partitionRows / avgRowsSoFar)
                       clamped to [500, MaxSampleSize]
```

Larger partitions get proportionally more samples. The per-partition minimum (500) ensures each partition retains enough samples for valid A-Res sub-sampling. For tables with many partitions, the minimum takes precedence over the target budget — e.g., 8,000 partitions × 500 = 4M total samples. In practice this is acceptable because pruned blobs are much smaller than full reservoirs (see blob size estimates above).

The pruning performs correct A-Res sub-sampling: a smaller reservoir is created and all current samples compete for slots via weighted selection, preserving statistical validity.

Pruning applies **only to the persisted copy**. The full unpruned collector is used for the in-memory global merge during the current ANALYZE.

### Single-Partition Re-Analyze

When `ANALYZE TABLE t PARTITION p5` is executed:

```text
  1. Analyze partition p5 (scan from TiKV, build stats)
  2. Load saved samples for all other partitions from mysql.stats_table_data
  3. Validate schema compatibility (see below)
  4. Merge all collectors (fresh p5 + loaded others) via A-Res
  5. Build global histogram + TopN from merged samples
  6. Save global stats
  7. Persist p5's pruned samples (replacing old entry)
```

**Step 3 — Schema validation**: Saved collectors are position-based — each sample row's `Columns[i]` corresponds to the i-th column in the original ANALYZE request. On load, the FMSketch array length of the saved collector is compared against the current ANALYZE's collector. If they differ (columns added or dropped), the saved samples are discarded and the partition falls back to merge-based global stats. This check does not detect column type or collation changes with the same column count; a schema fingerprint (column IDs + types) could strengthen this in a future iteration.

**Partial coverage**: If some partitions lack saved samples (e.g., newly created by `REORGANIZE PARTITION`, or never analyzed with the sample-based path), those partitions are skipped during the merge. Global stats are built from the available samples, which may underrepresent the missing partitions. To ensure complete coverage, a full `ANALYZE TABLE` (without partition restriction) should be run after schema changes that add new partitions.

This avoids re-scanning unchanged partitions entirely. The cost is:
- **I/O**: reading ~500 KB per partition from TiKV (pruned sample blobs)
- **CPU**: merging N collectors in memory (milliseconds for 1,000 partitions)

Compared to today's approach of re-merging all P partitions' histograms, this is both faster and produces higher-quality global stats.

### DDL Handling

Persisted samples must be cleaned up when partitions change:

| DDL Operation | Cleanup |
|--------------|---------|
| `DROP TABLE` | All samples deleted via stats GC |
| `TRUNCATE TABLE` | All samples deleted (table gets new IDs) |
| `DROP PARTITION` | Partition's samples deleted |
| `TRUNCATE PARTITION` | Partition's samples deleted (new partition ID) |
| `EXCHANGE PARTITION` | Both sides' samples deleted |
| `REORGANIZE PARTITION` | Old partitions' samples deleted; new partitions have no samples until analyzed |

Cleanup is handled by the existing stats GC path (`GCStats`), which deletes rows by `table_id` from all `mysql.stats_*` tables when a physical table or partition ID becomes orphaned. `mysql.stats_table_data` must be added to this GC sweep. Since rows are keyed by `table_id` (the physical partition ID), the same `table_id`-based deletion used for other stats tables applies directly — no histogram-level or `hist_id`-based cleanup is needed.

### User Interface

A new session variable controls the feature:

```sql
SET SESSION tidb_enable_sample_based_global_stats = ON;
SET GLOBAL tidb_enable_sample_based_global_stats = ON;
```

| Property | Value |
|----------|-------|
| Scope | SESSION, GLOBAL |
| Default | OFF |
| Type | Boolean |
| Prerequisite | Analyze Version 2 with dynamic partition pruning |

When enabled, ANALYZE for partitioned tables will collect samples, persist pruned copies, and build global stats from merged samples instead of merging histograms.

### Fallback and Compatibility

The sample-based path falls back transparently to the merge-based path when:
- The variable is disabled
- No sample collector is available (e.g., all partition analyses failed)
- Schema mismatch detected when loading saved samples (columns were added/dropped between analyzes)
- Some partitions lack saved samples (e.g., newly created partitions or first analysis) — those partitions are skipped during the sample merge and global stats are built from available samples only (see [Partial coverage](#single-partition-re-analyze))

**Upgrade**: No samples exist yet. First ANALYZE with the flag enabled populates them. The merge-based path works as fallback until then.

**Downgrade**: Saved sample rows in `mysql.stats_table_data` are harmlessly ignored by older versions. The merge-based path works without samples.

**BR backup/restore**: Samples in `mysql.stats_table_data` are included in full backups. After restore, incremental rebuilds work from saved samples.

**Global indexes**: Global indexes are not affected by this change. Unlike regular (local) indexes, a global index physically spans all partitions as a single B-tree and is analyzed as an independent task — its statistics already represent the full table's distribution without any per-partition merge step. The sample-based path applies only to local indexes and columns, which are analyzed per-partition and then merged.

## Test Design

### Functional Tests

- Global histograms and TopN built from samples have valid NDV, bucket counts, and row counts for both columns and indexes.
- Composite (multi-column) index stats are correctly encoded via the sample path.
- Save/load round-trip for serialized collectors produces identical results.
- Progressive pruning allocates budgets proportionally across partitions of varying sizes.
- Merge order does not affect resulting global stats (within sampling variance).

### Scenario Tests

1. **Large partition count (8,000 partitions)**: Memory stays bounded during sample merge — only one accumulated collector in memory at a time.
2. **Skewed partition sizes**: Partitions with vastly different row counts (e.g., 1K vs 1M rows) produce proportionally representative global samples.
3. **Schema change between analyzes**: Adding/dropping columns between a full analyze and a single-partition re-analyze triggers clean fallback to merge-based path.
4. **DDL during ANALYZE**: Partition drop/truncate during concurrent ANALYZE does not leave orphan samples.
5. **Empty partitions**: Partitions with zero rows are handled gracefully during merge.

### Compatibility Tests

- **Upgrade**: Cluster analyzed with merge-based path upgrades, enables flag, re-analyzes one partition — global stats are rebuilt correctly.
- **Downgrade**: Cluster with saved samples downgrades — merge-based path works, samples ignored.
- **BR**: Full backup with stats included preserves samples. After restore, incremental rebuild works.

### Benchmark Tests

A deterministic benchmark compares the sample-based and merge-based paths across multiple table configurations:

| Table | Partitions | Columns | Rows | Purpose |
|-------|-----------|---------|------|---------|
| tp8000CM50I3R30M | 8,000 | 50 mixed | 30M | High partition count stress test |
| tSSp256CM50I3R10M | 256 | 50 mixed | 10M | Size-skewed partitions |
| tp256CM20MS8kI3R10M | 256 | 20 (8KB strings) | 10M | Large column values |
| tp8000CI16R30M | 8,000 | 16 int | 30M | Many partitions, simple schema |

Each configuration is tested with full ANALYZE, single-partition ANALYZE, and a ground truth comparison against a non-partitioned clone of the same data.

Measurements: wall-clock duration, CPU time, memory usage, and accuracy (row count, NDV, TopN, histogram bucket count vs ground truth).

## Impacts & Risks

### Expected Impacts

- **Faster global stats for many partitions**: Building histograms from merged samples avoids the expensive O(P) histogram re-bucketing merge. For tables with thousands of partitions, this should significantly reduce ANALYZE time.
- **Faster single-partition re-analyze**: Rebuilding global stats after a single-partition ANALYZE requires only I/O (loading saved samples) + in-memory merge, not re-merging all partitions' histograms.
- **Improved global stats quality**: Histograms built from actual sampled data avoid the information loss inherent in histogram merging, potentially improving cardinality estimation.
- **Additional storage**: Persisted samples add ~50–700 KB per partition depending on pruned sample count, number of analyzed columns, and column types (see blob size estimates in the Persisting Samples section). For a table with 1,000 partitions and 50 mixed-type columns, this is ~400–700 MB in TiKV; for narrower tables or predicate-column analysis, storage is proportionally less.

### Risks

- **Per-bucket NDV is zero**: The sample-based path produces zero per-bucket NDV for all histogram buckets. In practice this is not a regression — the merge-based path also zeroes per-bucket NDV after computing it, with the comment "after merging bucket NDVs have the trend to be underestimated, so for safe we don't use them." The non-partitioned index path similarly zeroes per-bucket NDV via `StandardizeForV2AnalyzeIndex` after region merge. Per-bucket NDV is therefore zero in all current V2 stats paths. Future improvement: since the sample-based path builds histograms from sorted samples, counting distinct values per bucket during construction would be straightforward and could provide per-bucket NDV where neither the merge-based nor the current non-partitioned path does.
- **Sample staleness**: If a partition's data changes significantly but is not re-analyzed, its saved samples become stale. An incremental rebuild using stale samples produces outdated distribution information for that partition. Mitigation: staleness detection via `ModifyCount` thresholds should trigger re-analyze of modified partitions.
- **Memory for very wide tables**: The merged collector holds up to 10,000 rows with all analyzed column values in memory. For tables with hundreds of columns and large string values, this could be significant. Mitigation: existing column type exclusions (`tidb_analyze_skip_column_types`) already filter blob/json columns from sampling.

## Investigation & Alternatives

### Alternative 1: Improve the Merge-Based Path

A combined TopN+histogram merge was prototyped ([PR #66221](https://github.com/pingcap/tidb/pull/66221)) that replaces the O(P² × T × H) TopN merge with an O(T × P) hash-map merge and extracts histogram bucket-upper-bound repeats into TopN counters during bucket collection. This eliminated the count inflation problem and achieved 5–11× speedup on the merge step itself (e.g., 4.85s → 432ms for 10,000 partitions).

**Rejected because**: Even with the improved merge, the fundamental limitation remains — bucket boundaries optimized for individual partitions cannot be perfectly combined, and the merge still uses heuristic overlap estimation. The improved merge also does not solve the single-partition re-analyze performance problem, since it still requires loading and re-merging all P partitions' histograms. The sample-based approach sidesteps both issues entirely by building histograms from raw data rather than merging pre-built approximations.

### Alternative 2: Full-Scan Global Stats

Scan all partition data in a single pass to build global stats directly, without per-partition intermediate steps.

**Rejected because**: This requires scanning the entire table for every ANALYZE, even when only one partition changed. For tables with billions of rows across thousands of partitions, this is prohibitively expensive.

### Alternative 3: Persist Full Samples (No Pruning)

Store the full 10,000-sample collector per partition instead of pruning.

**Rejected because**: Storage cost would be ~5 MB per partition (vs ~500 KB pruned). For 1,000 partitions, this is 5 GB vs 500 MB. Progressive pruning preserves statistical validity within a bounded storage budget.

### Alternative 4: Binary Tree of Merged Results

Store intermediate merge results in a tree structure, enabling O(log N) incremental updates instead of O(N).

**Rejected for initial implementation because**: The I/O cost of reading all N partitions' pruned samples is acceptable for realistic partition counts. Merging N collectors in memory takes < 100ms for 1,000 partitions. The simplicity of per-partition-only storage (no cache invalidation, trivial DDL handling) outweighs the marginal I/O savings. This optimization can be added later if profiling shows I/O is the bottleneck.

## Unresolved Questions

1. **Per-bucket NDV**: Per-bucket NDV is currently zero in all V2 stats paths (the merge-based global path computes it then discards it; the non-partitioned index path zeroes it after region merge). The sample-based path could be the first to actually provide per-bucket NDV by counting distinct values during histogram construction from sorted samples. Should this be pursued, and would the optimizer benefit from it?

2. **Staleness policy**: When loading saved samples for incremental rebuild, should there be a staleness threshold (e.g., skip partitions whose `ModifyCount` exceeds X% of `Count`)? Currently, any saved sample is used regardless of age.

3. **Default ON criteria**: What benchmarks and quality checks should be satisfied before changing the default from OFF to ON?

4. **Interaction with async merge**: The existing `tidb_enable_async_merge_global_stats` merges partition stats asynchronously. How should the sample-based path interact with this? Should sample persistence also be async?
