# Index ANALYZE pipeline: per-region histograms, TopN, CMSketch, FMSketch

Scratch note. Captures how index analyze flows from TiDB request → per-region TiKV work → TiDB merge, with a concrete example and an improvement opportunity. Written while planning the `stats_buckets → stats_data` migration; relevant because the merged result is exactly what we'll be persisting.


## Defaults

`pkg/statistics/constants.go:22`:

    DefaultHistogramBuckets = 256
    DefaultTopNValue        = 100

User overrides via `ANALYZE TABLE t WITH 64 BUCKETS, 20 TOPN` (capped at 100 000 buckets / 100 000 TopN, see `pkg/planner/core/planbuilder.go:3063`).


## Wire layout per region

For an index, each region's TiKV returns one `tipb.AnalyzeIndexResp` (`tikv/src/coprocessor/statistics/analyze.rs:863-877`):

    AnalyzeIndexResp {
        Hist:      tipb.Histogram     // up to BucketSize buckets
        Cms:       tipb.CMSketch      // also carries the per-region TopN entries
        Collector: tipb.SampleCollector { FmSketch: tipb.FMSketch }
    }

Yes — for indexes, the **per-region histogram, TopN (embedded in CMSketch), and FMSketch all travel together in a single response per region**. They are not separate roundtrips.


## Are they merged separately on TiDB?

Yes. `pkg/executor/analyze_idx.go:339-361`, per region's response:

- `Hist`  → `statistics.HistogramFromProto(resp.Hist)` then `statistics.MergeHistograms(running, this, numBuckets, statsVer)` (line 344).
- `Cms`   → `statistics.CMSketchAndTopNFromProto(resp.Cms)` returns `(cm, tmpTopN)`. Then:
  - `cms.MergeCMSketch(cm)` (line 353) — sums the matrix slot-wise.
  - `statistics.MergeTopNAndUpdateCMSketch(topn, tmpTopN, cms, numTopN)` (line 356) — combines top-n heaps, subtracting any value also in TopN from the running CMSketch so it isn't double-counted.
- `Collector.FmSketch` → `fms.MergeFMSketch(statistics.FMSketchFromProto(resp.Collector.FmSketch))` (line 360) — bitmap union.

So all four artifacts share a single wire envelope per region but follow four independent merge paths. The Histogram merge does not look at TopN; the TopN merge does not look at the Histogram (they are independent estimators).

How TopN gets embedded in CMSketch on the TiKV side: `tikv/src/coprocessor/statistics/analyze_context.rs:200-212`. The per-region top-N values are popped off `topn_heap`, subtracted from the CMSketch matrix (`c.sub` so the matrix only represents the long tail), and pushed into the proto's top-n list (`c.push_to_top_n`). TiDB later separates them again in `CMSketchAndTopNFromProto`. v1 didn't have a separate TopN proto field; v2 kept the same envelope for compatibility.

### v2 still genuinely builds and ships the CMSketch matrix — it's not just an envelope

To be precise: in v2 index analyze, **TiKV allocates and populates a real CMSketch matrix per region**. It is not a no-op or empty placeholder. Receipts:

1. **TiDB sends a non-zero matrix size.** Default v2 options at `pkg/planner/core/planbuilder.go:3074-3081`:

       ast.AnalyzeOptCMSketchWidth: 2048,
       ast.AnalyzeOptCMSketchDepth: 5,

   These flow into `pkg/executor/builder.go:3110-3113` which sets `IdxReq.CmsketchDepth = 5, CmsketchWidth = 2048` for the standard v2 index path. (The only v2 override that zeros these is `pkg/executor/analyze_col_sampling.go:560-561`, a special NDV-only virtual-column path.)

2. **TiKV allocates a 5×2048 matrix.** `tikv/src/coprocessor/statistics/cmsketch.rs:17-30`:

       pub fn new(d: usize, w: usize) -> Option<CmSketch> {
           if d == 0 || w == 0 { None } else {
               Some(CmSketch { … table: vec![vec![0; w]; d], top_n: vec![] })
           }
       }

   With 5/2048 in, this returns `Some(...)` with a fully allocated matrix.

3. **TiKV calls `cms.insert(data)` on every scanned row.** `tikv/src/coprocessor/statistics/analyze_context.rs:177-179`:

       if let Some(cms) = cms.as_mut() {
           cms.insert(&data);
       }

   `insert` does five wrapping-multiply hashes plus five saturating-add increments per row (`cmsketch.rs:40-47`).

4. **TopN is gated by the CMSketch existing.** Same file, lines 207-212:

       if let Some(c) = cms.as_mut() {
           for heap_item in topn_heap {
               c.sub(&(heap_item.0).1, (heap_item.0).0);
               c.push_to_top_n((heap_item.0).1, (heap_item.0).0 as u64);
           }
       }

   If `cms` is `None`, TopN is **silently dropped** — never serialized. So you can't get TopN over the wire today without also paying for the matrix.

5. **TiKV serializes the full matrix.** `cmsketch.rs:63-88` `From<CmSketch> for tipb::CmSketch` walks the entire `5 × 2048` `table` and emits one `CmSketchRow.counters` per row, plus the `top_n` list.

6. **TiDB merges the matrix slot-wise**, then discards it. `pkg/executor/analyze_idx.go:352-358` calls `MergeCMSketch` on every region's response. At persist time, `analyze_idx.go:78-80`:

       if statsVer != statistics.Version2 {
           idxResult.Cms = []*statistics.CMSketch{cms}
       }

   v2 omits `cms` from `idxResult` (note the negation), so the CMSketch never reaches `mysql.stats_cm_sketch`.

Net cost paid for nothing in v2: per-row matrix updates in TiKV, ~10 KB of matrix bytes per region on the wire, slot-wise sums in TiDB. The matrix exists purely as a transport envelope for TopN because of point (4). This is what the "skip CMSketch in v2" improvement opportunity below would fix — by introducing a top-level TopN field on `AnalyzeIndexResp` so the matrix can be skipped when `CmsketchDepth=0`.

### So how is TopN actually merged in v2

The same path as v1, despite the CMSketch being discarded post-merge:

1. Each region's `tipb.AnalyzeIndexResp.Cms.topn[]` carries its top-N entries (with the matrix already adjusted to exclude them).
2. `pkg/executor/analyze_idx.go:352-356`:

       cm, tmpTopN := statistics.CMSketchAndTopNFromProto(resp.Cms)
       if err := cms.MergeCMSketch(cm); err != nil { ... }
       statistics.MergeTopNAndUpdateCMSketch(topn, tmpTopN, cms, uint32(numTopN))

3. `MergeTopNAndUpdateCMSketch` keeps a running global TopN, capped at `numTopN`. When a candidate falls out of the global heap, its count is added back into the merged `cms` to keep total mass conserved.

4. After all regions are merged, `analyze_idx.go:244-246` removes the surviving TopN values from the histogram so the histogram only represents non-TopN row counts:

       if needCMS && topn.TotalCount() > 0 {
           hist.RemoveVals(topn.TopN)
       }

5. For v2, the `cms` variable is then discarded (line 78-80 above); only `topn`, `hist`, and `fms` are persisted.

Net: the TopN merge is identical between v1 and v2; v2 simply throws away the CMSketch after the merge.


## Concrete example: 100-region index, defaults

1. **Dispatch.** TiDB sends 100 cop requests, each with `IdxReq{ BucketSize: 256, TopNSize: 100, CMSketchDepth: 5, CMSketchWidth: 2048, SketchSize: <FM> }`. `SetKeepOrder(true)` ensures responses arrive in region key order.

2. **Per-region TiKV work** (`tikv/src/coprocessor/statistics/analyze_context.rs:133-218` `handle_index`):
   - Allocate `Histogram::new(256)`, `CmSketch::new(5, 2048)`, `FmSketch::new(...)`, an empty `topn_heap`.
   - `RangesScanner` walks the region's slice of the index in key order. For each row:
     - decode the key into datums,
     - `cms.insert(data)`, `fms.insert(data)`,
     - if v2, `hist.append(data, true)` and update `topn_heap` (track `cur_val` count for the current value, push to heap on value transition).
   - At end-of-scan, finalize TopN (pop heap, subtract from CMSketch, push to `cm.topn`) and emit the response.

3. **Wire result.** 100 responses, each up to ~256 buckets in the histogram + 100 TopN entries embedded in CMSketch + an FM sketch. Total wire ~25 600 buckets + 10 000 TopN-entries (with overlap) + 100 FM bitmaps.

4. **TiDB merge.** Loop in `analyze_idx.go:185-251` consumes responses in key order, calling `buildStatsFromResult` per response:

   - Histogram side, `MergeHistograms(lh, rh, 256, statsVer)` (`pkg/statistics/histogram.go:960`):
     - **Boundary stitch** (lines 974-984): if `lh`'s last upper-bound equals `rh`'s first lower-bound, merge that boundary bucket into `lh` and pop `rh[0]` so the shared point isn't double-counted.
     - **Cap each side** (lines 985-993): `lh.mergeBuckets(idx)` collapses adjacent buckets pairwise until the side has ≤ 256.
     - **Rebalance** (lines 998-1005): if one side's average bucket height is < half the other's, collapse it further so heights match across the boundary.
     - **Concatenate** (lines 1006-1012): right-side buckets appended to left, with cumulative count shifted.
     - **Final cap** (lines 1013-1015): collapse again to ≤ 256.

   - TopN side: `MergeTopNAndUpdateCMSketch` keeps a running combined heap; entries that fall out of the global top-N have their counts added back into the CMSketch.

   - CMSketch and FMSketch: trivial slot-wise sum / bitmap union.

5. **Final state after 99 merges.**
   - One histogram with ≤ 256 buckets covering the whole index in key order.
   - One CMSketch (5×2048).
   - One TopN with ≤ 100 entries.
   - One FM sketch.

   This is the tuple eventually persisted across `stats_histograms` (one row), `stats_buckets` (≤ 256 rows), `stats_top_n` (≤ 100 rows), `stats_fm_sketch` (1 row). After our migration, the histogram side becomes one row in `stats_data` (type=2 idx bucket).


## Improvement opportunity: shrink per-region BucketSize

Currently `pkg/executor/builder.go:3101,3220` sends the **same** `BucketSize` (default 256) to every region. With 100 regions, TiKV produces ~25 600 partial buckets and TiDB then collapses ~99 % of them via the cap step. The work is wasted on both sides.

A simple improvement: scale the per-region request down to roughly `ceil(M * c / N)` where `M` is the final bucket count, `N` is the number of regions for the index, and `c` is a small over-provisioning factor (e.g. 4×) to retain enough resolution near boundaries that the rebalance step still produces equal-height buckets.

For the 100-region / 256-bucket example: instead of sending `BucketSize: 256` to each region, send `BucketSize: 16` (M·4 / N = 1024 / 100 ≈ 10, round up). That would:

- Reduce TiKV CPU spent in `Histogram::append`'s splitting/merging (cost is roughly linear in target bucket size).
- Reduce wire bytes by ~16× for the histogram payload.
- Reduce TiDB merge CPU because each `MergeHistograms` cap step now collapses far fewer buckets.

Open questions before implementing:

- **Resolution loss near sparse keys**: equal-height bucketing within a region with very few buckets may misplace boundaries. The `c` over-provisioning factor needs validation against representative skewed indexes. Start at 4× and benchmark.
- **Region-count discovery**: `N` isn't known at request build time without a region cache lookup. Either:
  - approximate it from `infoschema` region count (cheap but stale), or
  - pass `BucketSize` as a function of the request's range count (each region sees a sub-range, so this only works if TiDB pre-splits — which it does for analyze).
- **Backward compatibility with TiKV**: the proto field already exists; TiKV honors whatever value TiDB sends. No TiKV change needed.
- **Interaction with `StandardizeForV2AnalyzeIndex`**: this post-merge pass assumes a certain bucket density; needs review.

Estimated benefit on a 8000-partition × 50-column / heavily-indexed table (the #66751 workload): the histogram-build phase of analyze is dominated by per-region bucket work today; shrinking per-region buckets by 16× could plausibly remove a meaningful fraction of analyze CPU on top of the storage win the `stats_buckets → stats_data` migration already delivers.

Not in scope for the current migration plan. File-and-forget here so it isn't lost.


## Improvement opportunity: skip CMSketch entirely in v2 index analyze

Today v2 builds, ships, and merges a CMSketch only to throw it away — see "v2 still computes and ships the CMSketch even though it's discarded" above. The matrix exists purely as a TopN transport.

Cleaner alternative: add a top-level `TopN` repeated field to `tipb.AnalyzeIndexResp`. When TiDB sends `CmsketchDepth = 0` and `CmsketchWidth = 0` (signaling "v2, no matrix needed"), TiKV's `handle_index` skips matrix updates entirely and serializes top-N directly into the new field. TiDB's v2 read path becomes:

- No `CMSketchAndTopNFromProto` call.
- No `cms.MergeCMSketch`.
- `MergeTopNAndUpdateCMSketch` becomes just `MergeTopN` (no matrix to keep balanced).

Saved work per region: one `cms.insert(data)` per row (5 hashes per insert at default depth 5), one `cm.sub` per popped TopN candidate, one `MergeCMSketch` slot-wise sum on TiDB. On a 100-region index with millions of rows per region, this is meaningful.

Backward compatibility: the new top-level field is additive; older TiKV ignores it. TiDB needs a probe (or a TiKV version check) to know whether to use the new field or the legacy CMSketch envelope. Same kind of negotiation as other proto rolls.

Also out of scope for the current migration. Captured here.


## Tie-back to the migration

The blob we'd persist into `stats_data.value` is the **merged final histogram**, not any per-region partial. So:

- One row per (table_id, type=1 or 2, hist_id) in `stats_data`.
- Payload size ≈ 256 buckets × (2 bound blobs + 3 int64s). For typical column types (int / short string), low single-digit kB per row; for bound-heavy types (long varbinary), could approach tens of kB.
- The proto round-trip is the same one TiKV → TiDB performs every analyze for every region — extremely well exercised on the index path. The new exposure is for v2 column histograms (built from samples locally, never proto-round-tripped before our use).
