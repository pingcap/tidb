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

## ALL ITEMS RESOLVED

All review items have been addressed in the design doc.

## Summary Table

| # | Severity | Topic | Status |
|---|----------|-------|--------|
| ~~1~~ | ~~Critical~~ | ~~`MaxUint64` vs `MaxInt64` signedness~~ | **Resolved** |
| ~~2~~ | ~~Important~~ | ~~`pruneRate` computation timing/source~~ | **Resolved** — doc now specifies initial computation from `stats_meta` (with PD fallback), source precedence, and dynamic refinement as partitions complete |
| ~~3~~ | ~~Important~~ | ~~Fallback condition wording inconsistency~~ | **Resolved** |
| ~~4~~ | ~~Important~~ | ~~Concurrent ANALYZE races~~ | **Removed** — not a new concern; same last-writer-wins semantics as existing merge-based path |
| ~~5~~ | ~~Moderate~~ | ~~Column set mismatch / `PREDICATE COLUMNS`~~ | **Resolved** — fallback list and note added to Fallback section |
| ~~6~~ | ~~Moderate~~ | ~~Memory during concurrent partition analysis~~ | **Resolved** — Step 5 now states `concurrency × per_partition_samples + budget` |
| ~~7~~ | ~~Moderate~~ | ~~`stats_fm_sketch` transition plan~~ | **Resolved** |
| ~~8~~ | ~~Moderate~~ | ~~Async merge interaction~~ | **Resolved** |
| ~~9~~ | ~~Moderate~~ | ~~FMSketch merge strategy (union vs rebuild)~~ | **Resolved** — Step 6 now states FMSketch union explicitly |
| ~~10~~ | ~~Nitpick~~ | ~~Awkward wording line 32~~ | **Resolved** |

The design is fundamentally sound and has improved materially since the first draft.
