# Sample-Based Global Partition Statistics

- Author(s): [Mattias Jonsson](http://github.com/mjonss)
- Discussion PR: https://github.com/pingcap/tidb/pull/66960
- Tracking Issue: https://github.com/pingcap/tidb/issues/66220

## Table of Contents

* [Introduction](#introduction)
* [Background and Motivation](#background-and-motivation)
* [Detailed Design](#detailed-design)
    * [Current Approach: Merge-Based Global Stats](#current-approach-merge-based-global-stats)
    * [Proposed Approach: Sample-Based Global Stats](#proposed-approach-sample-based-global-stats)
    * [Sampling and Cross-Partition Merging](#sampling-and-cross-partition-merging)
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

This proposal introduces a sample-based approach for building global-level statistics on partitioned tables. Instead of merging per-partition histograms and TopN arrays — which is both lossy and slow — global histograms and TopN are built directly from merged samples, reusing the same histogram construction logic that already handles merging region-level samples into table-level and per-partition-level TopN and Histograms.

For non-partitioned tables, TiDB already builds statistics this way: each TiKV region returns a sample collector, all region collectors are merged into a single collector, and histograms and TopN are built from the merged samples. The current partition-to-global path bypasses this proven infrastructure and instead attempts to merge pre-built histograms and TopN arrays — a fundamentally lossier operation. This proposal replaces that merge with the same sample-based approach already used for regions, applied at the partition-to-global level. To support both Bernoulli (`SampleRate > 0`) and A-Res (`NumSamples > 0`) uniformly, each retained sample is treated as carrying a comparable priority key in `RowSample.Weight`, where larger values are better. Cross-partition pruning and merging then keep the highest-priority samples regardless of how they were originally collected. Beyond reusing existing infrastructure, the only new step this proposal adds is persisting pruned samples per partition to storage, so that future single-partition re-analyzes can load them instead of re-scanning unchanged partitions from TiKV.

## Background and Motivation

TiDB uses dynamic partition pruning by default, which requires global-level statistics spanning all partitions. Today there are two performance problems with how these global stats are produced.

### Problem 1: Global Stats Merge Is Slow and Lossy

After all partitions are analyzed, global stats are built by merging per-partition histograms and TopN arrays. This merge step has two issues (P = partitions, T = TopN entries per partition, B = histogram buckets per partition, B_global = target global bucket count, NDV = number of distinct values):

**It is slow for many partitions.** The TopN merge is O(P² × T × log B) in the worst case: for each unique TopN value (up to P × T when partitions have non-overlapping TopN sets, common for hash partitions), it searches every partition's TopN via binary search and, if not found, queries that partition's histogram via binary search. The histogram merge collects all P × B buckets into memory, sorts them in O(P × B × log(P × B)), then re-buckets with overlap detection and repeat recalculation in O(B_global × P × log B). Both steps must load all P partitions' histograms and TopN arrays from storage simultaneously, requiring O(P × (T + B)) memory per column. In benchmarks on 10,000 partitions (500 TopN entries, 500 buckets each), the full merge took 4.5 seconds and allocated 1.9 GB — per column. For tables with thousands of partitions and dozens of columns, the merge step dominates ANALYZE time while the actual per-partition data scanning may finish in seconds.

**It is lossy.** The merge compounds approximation errors in three ways:

1. **TopN count inflation.** When a TopN value from one partition is not in another partition's TopN, the merge estimates its count from the histogram using `totalRows/NDV` (uniform assumption). This estimate accumulates across partitions. Testing with 20 partitions showed a value with true count=120 inflated to 1,920 (16×), causing the merge to pick the wrong value for global TopN ([test](https://github.com/mjonss/tidb/commit/fb7e5208172ecfa8fd6805893c3edf0c951acdd2)).

2. **Histogram boundary misalignment.** Bucket boundaries optimized for individual partitions do not align across partitions. The merge uses linear interpolation to estimate bucket overlap — a heuristic that compounds errors when applied across many partitions. The TopN merge also mutates partition histograms (removing values via binary search) before they are merged, further reducing histogram quality.

3. **Globally frequent values missed.** A value appearing in 1% of each partition (below each partition's TopN threshold) but 1% of the entire table (above the global TopN threshold) is never discovered by the current approach, because neither partition promoted it to TopN.

Measured on a table with 8,000 partitions and 30M rows ([accuracy report](https://github.com/mjonss/tidb/blob/8e6c61ffca811dbb92251a520eeafe03170a4268/sample-based-accuracy.md)), building histograms directly from sample data instead of merging produced 3–5× more uniform buckets (CV 0.000–0.048 vs 0.062–0.158) and 1,000× better value range accuracy (+7 overshoot vs +7,999). The sample-based path also computes column correlation (the merge path always returns 0) and avoids the O(P × (T + B)) memory spike that caused a second full ANALYZE to crash with OOM in testing.

### Problem 2: Single-Partition ANALYZE Requires Full Rebuild

When only one partition has changed and is re-analyzed via `ANALYZE TABLE t PARTITION p5`, global stats still need to reflect all partitions. Today, this triggers a full merge of all P partitions' stored histograms — the same expensive merge step as a full table analyze, with the same O(P² × T × log B) TopN merge, O(P × B × log(P × B)) histogram re-bucketing, and O(P × (T + B)) memory per column. No intermediate data is saved that would allow rebuilding global stats from just the changed partition plus cached results from other partitions.

For a table with 8,000 partitions, this means that even a single-partition re-analyze must read and merge histogram data for all 8,000 partitions, even though 7,999 of them have not changed. In testing, this took over 8 minutes for a single-partition re-analyze on a table where the partition scan itself completed in under 2 seconds.

### Proposed Solution

Each partition's ANALYZE already produces a sample collector containing random samples. These collectors can be pruned or bounded according to the current sample target and then merged across partitions, and the resulting samples can be used to build global histograms and TopN directly — bypassing the lossy histogram merge entirely.

By persisting pruned samples per partition, future single-partition re-analyzes can load saved samples for unchanged partitions from storage and merge them with the fresh partition's samples in memory. This avoids re-scanning unchanged partitions from TiKV.

## Detailed Design

### Current Approach: Merge-Based Global Stats

```text
ANALYZE TABLE t

  For each partition Pi (in parallel, up to concurrency limit):
    1. Scan partition data from TiKV (sampling)
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
    1. Scan partition data from TiKV (sampling)
    2. Build partition histogram + TopN from samples
    3. Save partition stats to mysql.stats_* tables
    4. Prune/retain samples and persist              ◄── NEW
    5. Merge retained samples into global accumulator ◄── NEW
                              │
                              ▼
  After all partitions complete:
    6. Build global histogram + TopN from merged samples   ◄── NEW
       (same code path as step 2 — not a histogram merge)
    7. Save global stats
```

The key differences:

- **Step 5** merges each partition's retained samples into a global accumulator. The partition's full collector is discarded after pruning/bounding, so the accumulator is bounded by the total budget (~110K samples). Peak memory during a full ANALYZE is `concurrency × per_partition_samples + budget`, since each concurrently analyzed partition holds its full sample set before pruning. This is a net improvement over the merge-based path, which requires loading all P partitions' histograms and TopN arrays into memory for the global merge — O(P × (T + B)) per column. The sample-based accumulator replaces that with a fixed ~110K budget regardless of partition count.
- **Step 6** builds global stats from actual sample data using the same histogram construction logic as per-partition stats. There is no histogram merging step. Global NDV is computed by unioning the per-partition FMSketches from the collectors (same lossless union as the current merge path), not by rebuilding FMSketches from the pruned sample data.
- **Step 4** persists a pruned copy of each partition's samples to enable future incremental rebuilds.

### Sampling and Cross-Partition Merging

TiDB V2 stats supports two row-level sampling methods:

- **Bernoulli sampling** (the V2 default): Each row is independently included with probability `SampleRate`. No coordination between TiKV regions is needed — each region samples independently at the same rate. The auto-calculated rate is `min(1, 110000 / rowCount)`, targeting ~110K samples per partition. Small tables get higher coverage (up to rate=1), large tables get lower rates but still enough samples for good histograms. Used when `SampleRate > 0` (default: auto-calculated, `NumSamples = 0`).

- **A-Res (weighted reservoir sampling)**: Maintains exactly K samples via a min-heap of random keys. Fixed output size regardless of input. Used when `NumSamples > 0` (e.g., `ANALYZE TABLE t WITH 10000 SAMPLES`).

Cross-partition merging requires **bounded memory** and **proportional representation**. This design achieves both by treating every retained sample as carrying a comparable priority key in `RowSample.Weight`, where larger values are better. For A-Res, this is the existing reservoir key. For Bernoulli, it is derived from the Bernoulli inclusion value `u` so that smaller `u` maps to larger `Weight`:

1. TiKV collects samples using the configured method (Bernoulli or A-Res), and every retained sample carries a comparable priority key in `Weight`
2. After region merge within a partition, the full sample set is used to build partition histograms and TopN (unchanged quality)
3. Samples are pruned or bounded according to the current target (see Progressive Pruning)
4. The retained samples are merged across partitions using the same higher-weight-wins priority semantics

Proportional representation follows naturally from the priority-key distribution (see Progressive Pruning): larger partitions contribute more retained samples because more of their accepted rows compete for the global budget. The accumulator grows to at most the total budget (~110K) — bounded.

**Bernoulli weights**: Currently Bernoulli samples have `Weight = 0` in the `tipb.RowSample` proto. We propose that TiKV derive the weight from the same value `u` used for the Bernoulli include/exclude decision, transformed so that larger `Weight` means higher priority. One simple mapping is `Weight = (1 - u) × MaxInt64`, with Bernoulli acceptance still defined as `u <= SampleRate`. This makes Bernoulli weights comparable to the existing A-Res reservoir keys, which already use the convention that larger keys win. One way to obtain `u` deterministically is to use a hash of the row key, as prototyped in [tikv#19414](https://github.com/tikv/tikv/pull/19414/files#diff-4aad4af2afccbb967f83555af3ab4820c6995c3f896f5d0d7c51993bd6d7662cR126-R133) — the hash is already computed for the Bernoulli gate and can be stored as `RowSample.Weight` at zero additional cost.

For accepted Bernoulli samples (`u <= SampleRate`), the resulting priority is uniformly distributed in `[(1 - SampleRate) × MaxInt64, MaxInt64]`. This enables Bernoulli samples to participate in the same higher-weight-wins pruning and merge algorithm as A-Res. The same samples always survive deterministic pruning because the decision reuses the original priority key, not a new random draw. TiDB already copies the weight from the proto response (`pbSample.Weight`), so no TiDB-side deserialization changes are needed.

**TiKV version compatibility**: If TiDB receives Bernoulli samples with `Weight = 0` from an old TiKV (pre-weight-fix), TiDB can assign a replacement priority on the TiDB side. Because the row has already passed the Bernoulli acceptance test, the synthetic value must be drawn from the accepted conditional range `u ~ Uniform(0, SampleRate)`, then mapped with the same formula (for example `Weight = (1 - u) × MaxInt64`). This gives a compatible priority range `[(1 - SampleRate) × MaxInt64, MaxInt64]`. Using the original TiKV-side value is preferred because it is deterministic across rebuilds, but a TiDB-side replacement remains statistically valid.

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

Each partition stores one row containing all samples to be persisted for that partition. The serialized blob stored in `mysql.stats_table_data.value` contains the `tipb.RowSampleCollector` protobuf payload together with the metadata needed for reuse. Each sample row contains values only for the columns included in the ANALYZE request (controlled by `ANALYZE TABLE ... ALL COLUMNS`, `PREDICATE COLUMNS`, or `COLUMNS c1, c2, ...`), not all table columns. The collector also includes per-column FMSketches, per-column null counts, per-column total sizes, the total row count, and per-sample weights. `hist_id` is 0 because the blob is partition-scoped — individual columns are extracted from the full-row samples only when building histograms. `REPLACE INTO` overwrites stale samples atomically.

In addition to the collector itself, the persisted payload should carry the metadata needed to validate whether the saved samples can be reused safely:

- `minWeight`: the saved retention cutoff, expressed in the same `Weight` priority space as the samples
- a schema compatibility fingerprint covering the saved sample layout

This `minWeight` should mean "samples with `Weight < minWeight` were not retained in the saved partition sample." For Bernoulli, it is the cutoff implied by the `pruneRate` used when saving the partition, not the least observed saved sample. For A-Res, it is the local top-K cutoff.

The schema compatibility fingerprint should be based on the exact ANALYZE sample layout, not just the table schema version. This is a correctness safeguard for saved-sample reuse, not a separate feature goal. It is already stricter than a length-only check and avoids reusing incompatible saved samples. At minimum it should include:

- the ordered list of analyzed column IDs
- the ordered list of analyzed index / column-group definitions
- each sampled column's type and collation-sensitive attributes needed to decode and compare the saved values correctly

Only saved collectors with an exact or explicitly-compatible fingerprint match may participate in incremental rebuild. The default rule should be exact match; compatibility exceptions must be intentionally defined and limited to cases where the saved encoded values remain valid for the current ANALYZE request (for example some widening changes). Existing DDL compatibility logic for `MODIFY/CHANGE COLUMN` can be used as a reference when deciding which widenings are safe enough to treat as compatible (for example `checkModifyTypes`, `noReorgDataStrict`, and charset/collation compatibility checks), but the saved-sample fingerprint remains a separate validation mechanism because it is based on ANALYZE sample layout and encoded sample reuse rather than DDL execution semantics. This avoids positional mismatches when columns are added/dropped/reordered, when one column is dropped and another added with the same total count, or when type/collation changes make the saved encoded values incompatible with the current ANALYZE request.

The per-partition blob size depends on the number of pruned samples (determined by the ~110K total budget distributed proportionally across partitions) and the number and types of analyzed columns. Since the total budget is fixed, total storage is roughly constant regardless of partition count — approximately 10–20 MB for 50 mixed-type columns, distributed across all partitions. Narrower tables or `PREDICATE COLUMNS` analysis produce proportionally less.

### Progressive Pruning

After region merge within a partition, the sample set can be large (~110K for the default Bernoulli rate). Persisting this full set per partition would use excessive storage, and merging full sets from all partitions would exceed the memory budget. Instead, samples are pruned by keeping only the highest-priority samples under a global budget:

```text
for Bernoulli:
  pruneRate = budget / estimatedTableRowCount
  threshold = (1 - pruneRate) × MaxInt64
  keep sample if Weight >= threshold

for A-Res:
  keep the top-K samples by Weight using the same higher-weight-wins heap merge
```

The total pruning budget should match the same sample target used for non-partitioned tables (currently ~110K via `DefRowsForSampleRate`). This ensures the global histogram is built from the same sample density that already produces good histograms for non-partitioned tables. It also opens the path for a future unified sample target that adapts consistently across non-partitioned tables, per-partition stats, and partitioned table global stats.

The `estimatedTableRowCount` is computed once before partition scanning begins, using `mysql.stats_meta` (adjusted by DML `modify_count` tracking) or PD's approximate region-based row count as a fallback — the same estimation already used for calculating the Bernoulli `SampleRate` per partition (via `getAdjustedSampleRate`). When both sources are available, `stats_meta` takes precedence since it tracks DML changes. As each partition completes analysis, TiDB learns its actual row count from the collector's `Count`. The `pruneRate` can be dynamically recomputed after each partition finishes, using `actualRowsSoFar + estimatedRowsRemaining` as the denominator. This progressively replaces stale estimates with ground truth, making the pruning threshold more accurate as the analyze progresses. For early partitions pruned with a less accurate rate, the `minWeight` mechanism detects if they end up undersampled relative to the final target.

Proportional representation follows naturally. For Bernoulli, partitions with more rows contribute more accepted samples, and a single global threshold keeps the highest-priority ones without a per-partition target. For A-Res, the same higher-weight-wins merge already provides the correct top-K behavior. No per-partition target calculation is needed. Bernoulli pruning is O(N) per partition — a single pass with no sorting or heap, and deterministic when the original priority key is preserved.

The same priority-key semantics apply for both persistence and the in-memory global merge: the pruned copy is saved to `mysql.stats_table_data` for future incremental rebuilds, and the retained samples are merged across partitions with the same higher-weight-wins rule. The full unpruned collector is used only for building per-partition histograms and TopN.

Before a saved partition collector participates in a rebuild, TiDB must verify whether the current target would require samples with priority below the saved partition's `minWeight`. If so, that partition is undersampled for the current rebuild target: rows that would have been eligible under the current target were already discarded when the partition sample was saved. TiDB must not proceed silently in this case. It should log and return a SQL warning identifying the undersampled partitions. The initial policy is to warn but continue using the saved samples rather than falling back immediately.

### Single-Partition Re-Analyze

When `ANALYZE TABLE t PARTITION p5` is executed:

```text
  1. Analyze partition p5 (scan from TiKV, build stats)
  2. Load saved samples for all other partitions from mysql.stats_table_data
  3. Validate schema compatibility (see below)
  4. Apply the current target's pruning/bounding rule to fresh p5 samples and merge with loaded others
  5. Build global histogram + TopN from merged samples
  6. Save global stats
  7. Persist p5's pruned samples (replacing old entry)
```

**Step 3 — Schema validation**: Saved collectors are position-based — each sample row's `Columns[i]` corresponds to the i-th entry in the original ANALYZE sample layout. On load, TiDB must compare the saved collector's schema compatibility fingerprint against the current ANALYZE request. If the fingerprint is not exact or explicitly-compatible, the saved samples are discarded and the partition is treated as having no reusable saved samples for this rebuild. Length-only checks such as comparing FMSketch counts are not sufficient: they miss same-count add+drop changes, reordered analyzed columns, and type/collation changes that keep the same column count but make the saved encoded values incompatible.

**Missing samples**: If any partition has no saved sample data in `mysql.stats_table_data` but has existing TopN/histograms, the merge-based path is used for global stats instead (a warning is logged). If no partition has existing TopN/histograms, global stats are built from whatever samples are available. In both cases, the analyzed partition's samples are still saved for future use. To enable the sample-based path for all partitions, run a full `ANALYZE TABLE`.

This avoids re-scanning unchanged partitions entirely. The cost is:
- **I/O**: reading pruned sample blobs from TiKV (~10–20 MB total for 50 mixed-type columns, distributed across all partitions)
- **CPU**: merging N collectors in memory (milliseconds for 1,000 partitions)

Compared to today's approach of re-merging all P partitions' histograms, this is both faster and produces higher-quality global stats.

### DDL Handling

Persisted samples must be cleaned up when partitions change:

| DDL Operation | Cleanup |
|--------------|---------|
| `DROP TABLE` | Old table/partition IDs become orphaned; samples deleted by GC |
| `TRUNCATE TABLE` | Old table/partition IDs become orphaned; samples deleted by GC. Recreated table and partitions get new IDs with no samples until analyzed |
| `DROP PARTITION` | Old partition ID becomes orphaned; samples deleted by GC |
| `TRUNCATE PARTITION` | Old partition ID becomes orphaned; samples deleted by GC. Recreated partition gets a new ID with no samples until analyzed |
| `EXCHANGE PARTITION` | Both sides' old IDs become orphaned; samples deleted by GC |
| `REORGANIZE PARTITION` | Old partition IDs become orphaned; samples deleted by GC. New partitions get new IDs with no samples until analyzed |

Cleanup is handled by the existing stats GC path (`GCStats`), which deletes rows by `table_id` from all `mysql.stats_*` tables when a physical table or partition ID becomes orphaned. `mysql.stats_table_data` must be added to this GC sweep. Since rows are keyed by `table_id` (the physical partition ID), the same `table_id`-based deletion used for other stats tables applies directly — no histogram-level or `hist_id`-based cleanup is needed.

### User Interface

A session variable `tidb_sample_based_global_stats` controls the feature as a three-level enum, allowing staged rollout and safe fallback:

| Property | Value |
|----------|-------|
| Scope | SESSION, GLOBAL |
| Default | ON |
| Type | Enum: OFF, SAVE, ON |
| Prerequisite | Analyze Version 2 with dynamic partition pruning |

| Value | Behavior |
|-------|----------|
| `OFF` | No new code exercised. Merge-based path used for global stats. No samples saved. |
| `SAVE` | Samples are saved to `mysql.stats_table_data` after each partition ANALYZE, but merge-based path is still used for global stats. This validates the persistence code without affecting stats quality. |
| `ON` | Samples are saved and used for building global stats. Falls back to merge-based when needed (see Fallback and Compatibility). |

The variable defaults to `ON` and serves as a fail-safe: if issues are discovered after release, operators can set it to `SAVE` (continues populating samples without using them, allowing investigation) or `OFF` (disables all new code paths) without requiring a version rollback. SESSION scope allows testing individual ANALYZE runs. The goal is to eventually remove the variable (effectively always `ON`).

**Interaction with `tidb_enable_async_merge_global_stats`**: The existing `tidb_enable_async_merge_global_stats` variable continues to control only the legacy merge-based path used in fallback scenarios (`OFF`, `SAVE`, upgrade transition, missing saved samples, or policy-driven fallback). Once merge-based fallback is retired and sample-based global stats becomes the only path, `tidb_enable_async_merge_global_stats` should be removed.

### Fallback and Compatibility

The sample-based path falls back transparently to the merge-based path when:
- The variable is set to `OFF` or `SAVE`
- No sample collector is available (e.g., all partition analyses failed)
- Saved sample layout is not exact or explicitly-compatible with the current ANALYZE request (per-column: columns not covered by a partition's saved samples fall back individually — see note below)
- Any partition has no saved sample data in `mysql.stats_table_data` but has existing TopN/histograms from a prior analyze (see Gradual Transition below)

**Per-column fallback for `PREDICATE COLUMNS`**: If the workload's predicate columns evolve over time, auto-analyze may produce saved samples with different column layouts across partitions. Rather than falling back entirely to merge-based, the sample-based path can operate per-column: for each column/index in the current ANALYZE request, compute the intersection of partitions whose saved fingerprints include that column. Columns present in all partitions' saved samples use the sample-based path; columns missing from some partitions fall back to merge-based for those columns only. Since `BuildHistAndTopN` already operates per-column and FMSketches are per-column, this requires mapping column positions across different fingerprints (using the column IDs stored in each fingerprint) but no structural changes to the histogram construction path. This avoids the all-or-nothing fallback that would otherwise make the sample-based path rarely activate for tables with evolving predicate columns.

When a fallback occurs, a warning should be logged and returned to the client as a SQL warning (visible via `SHOW WARNINGS`), indicating the reason (e.g., "N partitions have no saved samples but have existing stats, falling back to merge-based global stats"). When TiDB continues with saved samples even though some partitions are undersampled for the current target, it should also return a warning (for example "partition p42 saved samples are sparser than the current sample target; continuing with saved samples"). This ensures the user running `ANALYZE TABLE` can see when the configured sample target was not fully met, and operators can identify tables that may benefit from denser saved samples.

**Gradual transition after upgrade or DDL**: After upgrade or when new partitions are added, no saved samples exist. Building global stats from only the analyzed partition's samples would be a regression — worse than the current merge-based path which uses all partitions' histograms. To avoid this, the sample-based path uses a hybrid approach during the transition:

1. Each partition's ANALYZE saves its pruned samples to `mysql.stats_table_data` regardless of whether all partitions have saved sample data yet (a partition with zero rows or very few rows still gets an entry — it just contains zero or few samples)
2. For building global stats, if any partition has no saved sample data in `mysql.stats_table_data` but has existing TopN/histograms from a prior analyze, the merge-based path is used instead — the existing stats are more representative than partial sample coverage
3. If no partition has existing TopN/histograms (e.g., a freshly created table), global stats are built from whatever samples are available — there is nothing to fall back to
4. Once all partitions have saved sample data in `mysql.stats_table_data` (after a full `ANALYZE TABLE` or after auto-analyze has covered every partition), the sample-based path takes over

This means auto-analyze gradually populates samples partition by partition, while global stats quality is maintained by the merge-based fallback when existing stats are available.

**Upgrade**: No saved sample data exists in `mysql.stats_table_data` yet, but existing TopN/histograms are available from prior analyzes. The merge-based path is used for global stats while auto-analyze gradually populates saved samples. A full `ANALYZE TABLE` populates all partitions at once.

**Downgrade**: Saved sample rows in `mysql.stats_table_data` are harmlessly ignored by older versions. The merge-based path works without samples.

**BR backup/restore**: Samples in `mysql.stats_table_data` are included in full backups. After restore, incremental rebuilds work from saved samples.

**Global indexes**: Global indexes are not affected by this change. Unlike regular (local) indexes, a global index physically spans all partitions as a single index and is analyzed as an independent task — its statistics already represent the full table's distribution without any per-partition merge step. The sample-based path applies only to local indexes and columns, which are analyzed per-partition and then merged.

## Test Design

### Functional Tests

- Global histograms and TopN built from samples have valid NDV, bucket counts, and row counts for both columns and indexes.
- Compare stats accuracy for a single column across global index, local index, and column stats — column and local index for the same column should produce identical stats; global index for a single column should have similar accuracy.
- Composite (multi-column) index stats are correctly encoded via the sample path.
- Save/load round-trip for serialized collectors produces identical results.
- Save/load round-trip preserves `minWeight` correctly.
- Save/load round-trip preserves the schema compatibility fingerprint correctly.
- Progressive pruning allocates budgets proportionally across partitions of varying sizes.
- Merge order does not affect resulting global stats (within sampling variance).
- Bernoulli sub-sampling for pruning produces proportionally representative global samples across partitions of different sizes.
- Both Bernoulli and A-Res samples participate correctly in the same higher-weight-wins global merge.
- Bernoulli samples from old TiKV (`Weight = 0`) get compatible replacement priorities and still produce valid global stats.

### Scenario Tests

1. **Large partition count (8,000 partitions)**: Memory stays bounded during sample merge — only one accumulated collector in memory at a time.
2. **Skewed partition sizes**: Partitions with vastly different row counts (e.g., 1K vs 1M rows) produce proportionally representative global samples.
3. **Schema change between analyzes — add/drop columns**: Adding/dropping columns between a full analyze and a single-partition re-analyze causes the schema compatibility fingerprint to mismatch, so saved samples are not reused.
4. **Schema change between analyzes — type changes**: Incompatible type changes while keeping the same column count cause the schema compatibility fingerprint to mismatch, so saved samples are not reused.
4b. **Schema change between analyzes — add+remove columns**: Dropping one column and adding another (same total count, different columns) also causes the schema compatibility fingerprint to mismatch, so positional mismatches in saved samples are avoided.
5. **DDL during ANALYZE**: Partition drop/truncate during concurrent ANALYZE does not leave orphan samples.
6. **Empty partitions**: Partitions with zero rows are handled gracefully — they get a saved sample entry with zero samples and do not force fallback to merge-based global stats.
7. **Undersampled saved partition**: Saved samples from a partition were collected at a lower density than the current rebuild target, so `minWeight` shows that eligible rows are missing. Verify warning behavior and the selected policy (continue with saved samples or fallback).
8. **Gradual transition after upgrade**: Auto-analyze of individual partitions saves samples but falls back to merge-based global stats when other partitions have TopN/histograms but no saved sample data in `mysql.stats_table_data`. A warning is logged. After all partitions have saved sample data, the sample-based path activates.
9. **All partitions new (no prior stats)**: A freshly created table where no partition has TopN/histograms — sample-based path is used directly since there is nothing to fall back to.

### Compatibility Tests

- **Upgrade**: Cluster analyzed with merge-based path upgrades, auto-analyzes one partition — samples are saved, merge-based path used for global stats, warning logged. After full `ANALYZE TABLE`, sample-based path takes over.
- **BR**: Full backup with stats included preserves samples. After restore, incremental rebuild works.

Note: Downgrade is not supported, so no downgrade testing is needed. Saved sample rows in `mysql.stats_table_data` are harmlessly ignored by older versions.

### Benchmark Tests

A deterministic benchmark compares the sample-based and merge-based paths across multiple table configurations:

| Table | Partitions | Columns | Rows | Purpose |
|-------|-----------|---------|------|---------|
| tp8000CM50I3R30M | 8,000 | 50 mixed | 30M | High partition count stress test |
| tSSp256CM50I3R10M | 256 | 50 mixed | 10M | Size-skewed partitions |
| tp256CM20MS8kI3R10M | 256 | 20 (8KB strings) | 10M | Large column values |
| tp8000CI16R30M | 8,000 | 16 int | 30M | Many partitions, simple schema |

Each configuration is tested with full ANALYZE, single-partition ANALYZE, and a ground truth comparison against a non-partitioned clone of the same data. The [analyze-profile](https://github.com/mjonss/tidb-dev-hacks) tool can be used for deterministic test data generation and accuracy comparison.

Measurements: wall-clock duration, CPU time, memory usage, and accuracy (row count, NDV, TopN, histogram bucket count vs ground truth).

## Impacts & Risks

### Expected Impacts

- **Faster global stats for many partitions**: Building histograms from merged samples avoids the expensive O(P) histogram re-bucketing merge. For tables with thousands of partitions, this should significantly reduce ANALYZE time.
- **Faster single-partition re-analyze**: Rebuilding global stats after a single-partition ANALYZE loads saved samples from storage and concatenates them in memory, instead of loading and re-merging all partitions' histograms and TopN arrays — which has been shown to cause high CPU usage and OOM in tables with many partitions.
- **Improved global stats quality**: Histograms built from actual sampled data avoid the information loss inherent in histogram merging, potentially improving cardinality estimation.
- **Additional storage**: Since the total pruning budget is fixed (~110K samples), total storage is roughly constant regardless of partition count — approximately 10–20 MB for 50 mixed-type columns (see blob size table in the Persisting Samples section). Narrower tables or predicate-column analysis produce proportionally less.
- **Reduced dependence on `mysql.stats_fm_sketch`**: FMSketches are only read from `stats_fm_sketch` when merging partition stats into global stats — non-partitioned tables and normal query planning never read them. In the steady-state sample-based path, the persisted collector already contains per-column FMSketches, so the partition-to-global rebuild no longer needs per-column reads from `stats_fm_sketch`. This can eliminate substantial I/O in the common case (e.g., 8,000 partitions × 50 columns = 400,000 individual queries in the current merge path, replaced by one blob read per partition). However, as long as merge-based fallback remains supported (`OFF`, `SAVE`, upgrade transition, missing saved samples, or policy-driven fallback), `stats_fm_sketch` still needs to exist for those paths. The new `stats_table_data` table also has a proper PRIMARY KEY, unlike `stats_fm_sketch` which only has a non-unique INDEX (see [#66303](https://github.com/pingcap/tidb/pull/66303)). Once merge-based fallback is retired, `stats_fm_sketch` could be removed in a follow-up change.

### Risks

- **Per-bucket NDV is zero**: The sample-based path produces zero per-bucket NDV for all histogram buckets. In practice this is not a regression — the merge-based path also zeroes per-bucket NDV after computing it, with the comment "after merging bucket NDVs have the trend to be underestimated, so for safe we don't use them." The non-partitioned index path similarly zeroes per-bucket NDV via `StandardizeForV2AnalyzeIndex` after region merge. Per-bucket NDV is therefore zero in all current V2 stats paths. Future improvement: since the sample-based path builds histograms from sorted samples, counting distinct values per bucket during construction would be straightforward and could provide per-bucket NDV where neither the merge-based nor the current non-partitioned path does.
- **Sample staleness**: If a partition's data changes significantly but is not re-analyzed, its saved samples become stale. An incremental rebuild using stale samples produces outdated distribution information for that partition. Mitigation: staleness detection via `ModifyCount` thresholds should trigger re-analyze of modified partitions.
- **Memory for very wide tables**: During partition analysis, the full sample set (~110K rows for the default Bernoulli rate) is held in memory with all analyzed column values before pruning. The global accumulator after pruning holds up to the budget (~110K rows total). For tables with hundreds of columns and large string values, this could be significant. Mitigation: existing column type exclusions (`tidb_analyze_skip_column_types`) already filter blob/json columns from sampling.

## Investigation & Alternatives

### Alternative 1: Improve the Merge-Based Path

A combined TopN+histogram merge was prototyped ([PR #66221](https://github.com/pingcap/tidb/pull/66221)) that replaces the O(P² × T × log B) TopN merge with an O(T × P) hash-map merge and extracts histogram bucket-upper-bound repeats into TopN counters during bucket collection. This eliminated the count inflation problem and achieved 5–11× speedup on the merge step itself (e.g., 4.85s → 432ms for 10,000 partitions).

**Rejected because**: Even with the improved merge, the fundamental limitation remains — bucket boundaries optimized for individual partitions cannot be perfectly combined, and the merge still uses heuristic overlap estimation. The improved merge also does not solve the single-partition re-analyze performance problem, since it still requires loading and re-merging all P partitions' histograms. The sample-based approach sidesteps both issues entirely by building histograms from raw data rather than merging pre-built approximations.

### Alternative 2: Full-Scan Global Stats

Scan all partition data in a single pass to build global stats directly, without per-partition intermediate steps.

**Rejected because**: This requires scanning the entire table for every ANALYZE, even when only one partition changed. For tables with billions of rows across thousands of partitions, this is prohibitively expensive.

### Alternative 3: Persist Full Samples (No Pruning)

Store the full sample set per partition instead of pruning. For the default Bernoulli rate this is ~110K samples per partition.

**Rejected because**: Storage cost scales with both sample count and the number and types of analyzed columns. For a table with 50 mixed-type columns and ~110K unpruned samples, the blob is roughly 50–100 MB per partition. For 1,000 partitions, this would be 50–100 GB. With progressive pruning to a ~110K total budget, total storage is ~10–20 MB regardless of partition count (see blob size estimates in the Persisting Samples section). Progressive pruning preserves statistical validity within a bounded storage budget.

### Future Extension: Binary Tree of Merged Results

Store intermediate merge results in a tree structure, enabling O(log N) incremental updates instead of O(N).

**Postponed**: The I/O cost of reading all N partitions' pruned samples is acceptable for realistic partition counts. Merging N collectors in memory takes < 100ms for 1,000 partitions. The simplicity of per-partition-only storage (no cache invalidation, trivial DDL handling) outweighs the marginal I/O savings for now. This optimization can be added later if profiling shows I/O is the bottleneck for very large partition counts.

## Unresolved Questions

1. **Per-bucket NDV**: Per-bucket NDV is currently zero in all V2 stats paths (the merge-based global path computes it then discards it; the non-partitioned index path zeroes it after region merge). The sample-based path could be the first to actually provide per-bucket NDV by counting distinct values during histogram construction from sorted samples. Should this be pursued, and would the optimizer benefit from it?

2. **Staleness policy**: When loading saved samples for incremental rebuild, should there be a staleness threshold (e.g., skip partitions whose `ModifyCount` exceeds X% of `Count`)? Currently, any saved sample is used regardless of age.

3. **Removing the variable**: The variable defaults to ON and serves as a fail-safe. Once the implementation is stable and validated, should the variable be removed entirely, making the sample-based path the only option?

4. **Saved sample budget and headroom**: If the table shrinks significantly after samples are saved (partitions dropped, data deleted), or if a later rebuild asks for denser samples than some partitions saved, `minWeight` may show that some partitions are undersampled for the new target. Storing more samples than the current budget requires (e.g., 10%, 3×) would provide headroom, but the right multiplier depends on the table's expected partition churn and how often users change `SAMPLERATE` or `N SAMPLES`. This may warrant a configurable per-table (or even per-partition for LIST partitioning) ANALYZE option that controls the saved sample density, since the appropriate value varies by table. A simple initial choice is to keep 10% extra saved samples beyond the current target, which lowers `minWeight` slightly and gives limited headroom after `DROP PARTITION` or large deletes. Possible names: `SAVED_SAMPLE_RATE`, `SAMPLE_RETENTION_RATE`, `GLOBAL_SAMPLE_BUDGET`, or `PERSIST_SAMPLE_TARGET`. A system variable could provide a cluster-wide default, with per-table overrides via `ANALYZE TABLE ... WITH <option>`. To be discussed.
