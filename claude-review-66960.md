# Review: Sample-Based Global Partition Statistics Design Doc (PR #66960)

**Reviewer**: Claude (acting as principal developer, statistics subsystem)
**Date**: 2026-03-19 (updated against local HEAD, not yet pushed)
**PR**: https://github.com/pingcap/tidb/pull/66960
**Doc**: `docs/design/2026-03-13-sample-based-global-partition-stats.md`

---

## PR Comment Status

All review comments from bots (CodeRabbit, Pantheon AI) have been **addressed** in commits.

**No human reviewer comments exist yet.** The PR has no approvals and no `lgtm`. Only bot reviews so far.

---

## Overall Assessment

The doc is well-structured, clearly motivated, and the proposed approach is sound. Reusing the existing region→table sample merge path at the partition→global level is the right architectural choice — it avoids duplicating histogram construction logic and produces demonstrably better stats. The prototype evidence (accuracy numbers, OOM avoidance) is convincing.

The doc has improved significantly since the initial version. The unified "higher-weight-wins" priority model for both Bernoulli and A-Res is cleaner than the original design. The schema compatibility fingerprint (lines 162-168) and `minWeight`-based undersampling detection (lines 157-160, 194) are good additions that address several earlier concerns. The `MaxUint64`/`MaxInt64` signedness issue, the `stats_fm_sketch` transition caveat, the async merge interaction, and the fallback condition wording are all resolved.

Remaining items are listed below — none invalidate the approach.

---

## RESOLVED since initial review (for reference)

| # | Original concern | Resolution |
|---|------------------|------------|
| 1 | `MaxUint64` vs `MaxInt64` signedness mismatch | **Fixed.** Doc now consistently uses `MaxInt64` (lines 133, 135, 137, 179). |
| 3 | Fallback condition wording inconsistency | **Fixed.** Both Section "Single-Partition Re-Analyze" (line 212) and "Gradual Transition" (lines 268-271) use the same precise condition: "no saved sample data BUT has existing TopN/histograms." New partitions without any stats don't trigger fallback. |
| 7 | `stats_fm_sketch` obsolescence needed transition caveat | **Fixed.** Section now titled "Reduced dependence on `mysql.stats_fm_sketch`" (line 342), explicitly states `stats_fm_sketch` must remain as long as merge-based fallback is supported. |
| 8 | Async merge interaction unaddressed | **Fixed.** Line 254 states `tidb_enable_async_merge_global_stats` applies only to the legacy merge-based fallback path and should be removed when fallback is retired. |

---

## OPEN items

### 1. pruneRate computation timing and source (Important, narrowed)

**Lines 177-179**:
```text
pruneRate = budget / estimatedTableRowCount
threshold = (1 - pruneRate) × MaxInt64
keep sample if Weight >= threshold
```

The `minWeight` mechanism (lines 157-160, 194) now detects when saved samples are undersampled for the current target, which addresses the single-partition re-analyze mismatch concern. However, the doc still doesn't specify:

- **During a full `ANALYZE TABLE`**: is `estimatedTableRowCount` computed once before scanning starts (from `stats_meta`/PD), or updated as partitions complete? If the pre-analyze estimate is 10× stale, the pruneRate will be off by 10× and the global accumulator will hold 10× more or fewer samples than the budget.

- **Source precedence**: Line 188 says "available from `mysql.stats_meta` ... or from PD's approximate region-based row count as a fallback." When both are available, which wins? The two can diverge significantly for tables with heavy DML.

This is less critical than before (the `minWeight` detection limits the damage), but a brief statement about computation timing would prevent implementation ambiguity.

**Optional improvement — dynamic row count refinement**: During a full `ANALYZE TABLE`, the initial `estimatedTableRowCount` (from `stats_meta` or PD) may be stale. As each partition completes analysis, TiDB learns its actual row count from the collector's `Count`. The `pruneRate` could be dynamically recomputed after each partition finishes, using `actualRowsSoFar + estimatedRowsRemaining` as the denominator. This progressively replaces stale estimates with ground truth, making the pruning threshold more accurate as the analyze progresses. By the time the last partitions are pruned, nearly all of `estimatedTableRowCount` has been replaced by observed counts. For the early partitions that were pruned with a less accurate rate, the `minWeight` mechanism already detects if they end up undersampled relative to the final target. This refinement is not required for correctness (the `minWeight` warning covers the gap), but it would reduce the frequency of undersampling warnings and produce more evenly distributed samples across partitions when the pre-analyze estimate is significantly off.

---

### 2. No discussion of concurrent ANALYZE races (Important)

Still not addressed. The doc mentions "DDL during ANALYZE" as a scenario test (line 306), but doesn't discuss concurrent ANALYZE:

- Two sessions both run `ANALYZE TABLE t PARTITION p5` concurrently. Both save samples for p5 — last writer wins (`REPLACE INTO`), fine. But both also build global stats, potentially reading a mix of old and new samples from other partitions.

- `ANALYZE TABLE t` (full) runs concurrently with `ANALYZE TABLE t PARTITION p5`. The full analyze writes samples for p1-p8000 while the single-partition analyze reads them.

**Recommendation**: Add a brief note, even if just: "Concurrent ANALYZE on the same table has the same last-writer-wins semantics as the existing merge-based path — no transactional consistency between individual partition sample writes. This is acceptable because any subsequent ANALYZE will overwrite with fresh data."

---

### 3. Column set mismatch across partitions (Moderate, narrowed from Important)

The schema fingerprint (lines 162-168) now properly handles correctness — mismatched column sets produce different fingerprints, saved samples are discarded, and the partition falls back. This is correct.

However, the doc doesn't call out that `PREDICATE COLUMNS` changes over time will **frequently invalidate saved samples** across partitions. If auto-analyze runs with evolving predicate columns, different partitions accumulate incompatible fingerprints, and the sample-based path may rarely activate until a full `ANALYZE TABLE` re-aligns them all.

**Recommendation**: Add a brief note in the Fallback section or User Interface section: "If the workload's predicate columns evolve over time, auto-analyze may produce saved samples with different column layouts across partitions. This will cause fingerprint mismatches and fallback to merge-based until a full `ANALYZE TABLE` re-aligns all partitions. Users with frequently changing predicate column sets may prefer `ALL COLUMNS` to avoid repeated fingerprint invalidation."

---

### 4. Memory accounting during full ANALYZE (Moderate)

**Line 112**: "The partition's full collector is discarded after pruning/bounding, so memory is bounded by the total budget (~110K samples)."

This describes the accumulator bound, but during a full `ANALYZE TABLE t`, multiple partitions are analyzed concurrently ("in parallel, up to concurrency limit" at line 96). Each concurrent partition holds its full sample set (~110K rows) in memory before pruning. With concurrency of 8, that's `8 × 110K` full collectors + `1 × 110K` accumulator.

Line 348 (Risks section) acknowledges per-partition memory for wide tables but doesn't mention the concurrency multiplier.

**Recommendation**: Clarify that peak memory during full ANALYZE is `concurrency × per_partition_samples + budget`, not just `budget`. This is the same as today (concurrent partitions already hold samples), but worth stating since the doc explicitly claims bounded memory.

---

### 5. FMSketch merge strategy not stated (Moderate)

The doc says the persisted collector includes per-column FMSketches (line 153), and global NDV is needed. But it doesn't state whether the sample-based path computes global NDV from:
- (a) Unioning the per-partition FMSketches (lossless — FMSketch union is exact regardless of pruning), or
- (b) Building a new FMSketch from the pruned merged samples (lossy — pruned samples miss some distinct values)

The current merge-based path uses (a) via `MergeFMSketch`. The non-partitioned path builds FMSketches during sampling and merges them across regions (also union). The sample-based global path should clearly use (a) to avoid regressing NDV accuracy.

**Recommendation**: Add one sentence, e.g., in the Proposed Approach section: "Global NDV is computed by unioning the per-partition FMSketches from the saved collectors (same as the current merge path), not by rebuilding FMSketches from the pruned sample data."

---

### 6. NITPICK: Awkward wording (line 32)

> "reusing the same histogram construction logic that already handles the region merge of stats to table and per-partition level TopN and Histograms"

Suggestion: "…that already handles merging region-level samples into table-level and per-partition-level TopN and Histograms."

---

## Summary Table

| # | Severity | Topic | Status |
|---|----------|-------|--------|
| ~~1~~ | ~~Critical~~ | ~~`MaxUint64` vs `MaxInt64` signedness~~ | **Resolved** |
| 1 | Important | `pruneRate` computation timing/source | Open (narrowed) |
| 2 | Important | No discussion of concurrent ANALYZE races | Open |
| ~~3~~ | ~~Important~~ | ~~Fallback condition wording inconsistency~~ | **Resolved** |
| 3 | Moderate | Column set mismatch / `PREDICATE COLUMNS` | Open (narrowed) |
| 4 | Moderate | Memory during concurrent partition analysis | Open |
| 5 | Moderate | FMSketch merge strategy (union vs rebuild) | Open |
| ~~7~~ | ~~Moderate~~ | ~~`stats_fm_sketch` transition plan~~ | **Resolved** |
| ~~8~~ | ~~Moderate~~ | ~~Async merge interaction~~ | **Resolved** |
| 6 | Nitpick | Awkward wording line 32 | Open |

The design is fundamentally sound and has improved materially since the first draft. The remaining items are about precision and documentation clarity — none require architectural changes.
