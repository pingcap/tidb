// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cardinality

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func init() {
	statistics.GetRowCountByColumnRanges = GetRowCountByColumnRanges
	statistics.GetRowCountByIndexRanges = GetRowCountByIndexRanges
}

// colEstimateCacheKey identifies a column estimate lookup. The realtime counts
// (RealtimeCount/ModifyCount) are intentionally not part of the key: the cached
// value is the count-independent shape of the estimate (see colRangeShape), and
// scaleColumnRangesShape re-applies all count-dependent logic on every lookup,
// so an estimate stays correct even if the counts observed by a later call
// differ. This also gives repeated references to the same table in one
// statement (e.g. self-joins) identical probe results, matching the
// per-statement consistency direction discussed in #67098.
type colEstimateCacheKey struct {
	physicalID int64
	colInfoID  int64
	pkIsHandle bool
	rangesKey  string // serialized form of the range slice
}

// colEstimateCacheMap is the concrete type stored in StmtCtx.ColEstimateCache.
// Each distinct (column, ranges) tuple maps directly to its cached
// count-independent probe shape, giving O(1) lookup and storage with no
// per-key linear scan.
type colEstimateCacheMap map[colEstimateCacheKey][]colRangeShape

// colEstimateCacheRangesKeyLimit caps the serialized rangesKey length so a
// single pathological statement (e.g. an enormous IN-list under
// tidb_opt_range_max_size=0) cannot make the statement-scoped cache retain
// arbitrary memory. Calls whose serialized ranges exceed this limit bypass the
// cache entirely — the underlying histogram computation still runs, just
// without memoization. 16 KiB comfortably fits typical predicates (tens of
// ranges, each Redact()'d to a few dozen bytes).
const colEstimateCacheRangesKeyLimit = 16 * 1024

// buildColEstimateCacheKey constructs the cache key for a column estimate.
// Ranges are serialized to a string so the key is comparable without storing or
// scanning the range slice. The Collators field of each Range is intentionally
// omitted: getColumnRowCount uses the collation embedded in each Datum value,
// not the Range.Collators slice, so it does not affect the estimate result.
//
// For string/bytes datums we also append Datum.Collation() because Range.Redact
// formats only the raw value, while getColumnRowCount transforms string bounds
// via collate.GetCollator(datum.Collation()).Key(...). Two ranges with the same
// literal value but different effective collations must therefore land at
// different cache keys. Range construction normally routes string types through
// convertStringFTToBinaryCollate (forcing bin collation), so this is defensive
// rather than load-bearing today — but it removes a future-fragility footgun
// and the few extra bytes per string range are negligible.
//
// Returns ok=false when the serialized rangesKey would exceed
// colEstimateCacheRangesKeyLimit; the caller must then skip the cache.
func buildColEstimateCacheKey(physicalID, colInfoID int64, pkIsHandle bool, ranges []*ranger.Range) (colEstimateCacheKey, bool) {
	// Fast path: each range serializes to at least 4 bytes (Range.Redact always
	// emits the enclosing brackets and the low/high separator, plus the comma
	// joining consecutive ranges), so this many ranges is guaranteed to exceed
	// the byte limit. Bail before serializing so repeated estimate calls on a
	// pathological statement don't each pay the (bounded) serialization cost
	// only to fail the limit check.
	if len(ranges) > colEstimateCacheRangesKeyLimit/4 {
		return colEstimateCacheKey{}, false
	}
	var b strings.Builder
	appendDatumCollations := func(vals []types.Datum) {
		for i := range vals {
			k := vals[i].Kind()
			if k == types.KindString || k == types.KindBytes {
				b.WriteByte('|')
				b.WriteString(vals[i].Collation())
			}
		}
	}
	for i, r := range ranges {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(r.Redact(errors.RedactLogDisable))
		appendDatumCollations(r.LowVal)
		appendDatumCollations(r.HighVal)
		if b.Len() > colEstimateCacheRangesKeyLimit {
			return colEstimateCacheKey{}, false
		}
	}
	return colEstimateCacheKey{
		physicalID: physicalID,
		colInfoID:  colInfoID,
		pkIsHandle: pkIsHandle,
		rangesKey:  b.String(),
	}, true
}

// colPointShapeKind enumerates the count-independent outcomes of probing the
// column statistics for a single point value. Kinds other than
// colPointShapeFixed defer some realtimeRowCount/modifyCount-dependent
// computation to scaleColumnPointShape.
type colPointShapeKind uint8

const (
	// colPointShapeFixed is a probe that produced a final stats-space count:
	// a TopN or CM sketch hit, a histogram match that cannot be
	// underrepresented, or an empty histogram (count 0).
	colPointShapeFixed colPointShapeKind = iota
	// colPointShapeNull is a NULL point; the estimate is the column NullCount.
	colPointShapeNull
	// colPointShapeLastBucketEnd is a ver2 histogram match sitting at the last
	// bucket's end value. Whether the histogram count is trusted or replaced by
	// the uniform-distribution fallback depends on the realtime counts
	// (lastBucketEndValueUnderrepresented).
	colPointShapeLastBucketEnd
	// colPointShapeUniform found no TopN or trusted histogram match; the
	// estimate comes from estimateRowCountWithUniformDistribution, which
	// depends on the realtime counts.
	colPointShapeUniform
	// colPointShapeOutOfRangeV1 is a ver1 stats out-of-range value; the
	// estimate comes from outOfRangeEQSelectivity, which depends on
	// realtimeRowCount.
	colPointShapeOutOfRangeV1
)

// colPointShape is the count-independent probe result for a single point
// value; scaleColumnPointShape turns it into a row estimate under the current
// realtime counts.
type colPointShape struct {
	kind colPointShapeKind
	// cnt is the probed stats-space count: the final count for
	// colPointShapeFixed, or the histogram count for colPointShapeLastBucketEnd.
	// Unused for the other kinds.
	cnt float64
}

// colRangeShapeKind discriminates the variants of colRangeShape.
type colRangeShapeKind uint8

const (
	// colRangeShapePoint is a single point value estimated via colPointShape.
	colRangeShapePoint colRangeShapeKind = iota
	// colRangeShapePointPK is a point on an integer primary key handle; it
	// contributes exactly one row.
	colRangeShapePointPK
	// colRangeShapeInterval is a non-point range estimated via colIntervalShape.
	colRangeShapeInterval
)

// colRangeShape is the count-independent probe result for one contributing
// range (ver1 small ranges enumerate into multiple point entries). It is the
// unit cached in colEstimateCacheMap.
type colRangeShape struct {
	kind     colRangeShapeKind
	point    colPointShape     // colRangeShapePoint
	interval *colIntervalShape // colRangeShapeInterval
}

// colIntervalShape is the count-independent probe result for an interval
// range: the histogram/TopN between-count, the boundary point adjustments, and
// the out-of-range geometry. scaleColumnRangesShape applies the clamps, the
// increase factor, and the out-of-range scaling under the current counts.
type colIntervalShape struct {
	// betweenCnt is the stats-space row estimate for [low, high).
	betweenCnt statistics.RowEstimate
	// hasLowAdjust/lowAdjust subtract the low boundary point when the range
	// excludes it (the between-count is low-inclusive).
	hasLowAdjust bool
	lowAdjust    colPointShape
	// addNullCount adds the column NullCount when the low bound is an
	// inclusive NULL.
	addNullCount bool
	// hasHighAdjust/highAdjust add the high boundary point when the range
	// includes it (the between-count is high-exclusive).
	hasHighAdjust bool
	highAdjust    colPointShape
	// outOfRange records whether a range bound falls outside the histogram;
	// oorShape then holds the count-independent out-of-range geometry to be
	// scaled by the current counts.
	outOfRange bool
	oorShape   statistics.OutOfRangeShape
}

// GetRowCountByColumnRanges estimates the row count by a slice of Range.
// PKIsHandle indicates whether the column is the single primary key column.
// The count-independent probe shape from valid (non-pseudo) column stats is
// cached on the statement context so that subsequent calls with the same column
// and ranges — including calls from different plan candidates exploring the
// same physical table — skip the histogram probing and only re-run the cheap
// count-dependent scaling.
func GetRowCountByColumnRanges(sctx planctx.PlanContext, coll *statistics.HistColl, colUniqueID int64, colRanges []*ranger.Range, pkIsHandle bool) (statistics.RowEstimate, error) {
	sc := sctx.GetSessionVars().StmtCtx
	c := coll.GetCol(colUniqueID)
	// Two-value lookup: a non-empty UniqueID2colInfoID that happens to be
	// missing this UniqueID would otherwise silently zero out colInfoID and
	// cause collisions on cache keys and wrong-column async-load enqueues.
	colInfoID := colUniqueID
	if id, ok := coll.UniqueID2colInfoID[colUniqueID]; ok {
		colInfoID = id
	}
	recordUsedItemStatsStatus(sctx, c, coll.PhysicalID, colInfoID)
	// Pass colInfoID (metadata column ID), not colUniqueID. ColumnStatsIsInvalid
	// enqueues async histogram loads keyed by TableItemID.ID, which the loader
	// interprets as a metadata column ID (GetColumnByID). When callers pass a
	// plan-assigned UniqueID — e.g. the table-path PK case in
	// deriveTablePathStats — the loader cannot resolve it and silently bails,
	// leaving the column on pseudo estimates forever.
	if statistics.ColumnStatsIsInvalid(c, sctx, coll, colInfoID) {
		// Do not cache pseudo/invalid results — they should not be reused
		// by index estimation paths that require real column stats.
		var pseudoResult float64
		if pkIsHandle {
			if len(colRanges) == 0 {
				return statistics.DefaultRowEst(0), nil
			}
			if colRanges[0].LowVal[0].Kind() == types.KindInt64 {
				pseudoResult = getPseudoRowCountBySignedIntRanges(colRanges, float64(coll.RealtimeCount))
			} else {
				pseudoResult = getPseudoRowCountByUnsignedIntRanges(colRanges, float64(coll.RealtimeCount))
			}
		} else {
			var err error
			pseudoResult, err = getPseudoRowCountByColumnRanges(sc.TypeCtx(), float64(coll.RealtimeCount), colRanges, 0)
			if err != nil {
				return statistics.DefaultRowEst(0), err
			}
		}
		return statistics.DefaultRowEst(pseudoResult), nil
	}

	// Check the statement-scoped cache before computing. cacheable is false when
	// the serialized rangesKey would exceed the size cap; in that case we skip
	// both the lookup and the insert below, so a pathological caller cannot make
	// the cache retain arbitrarily large keys.
	key, cacheable := buildColEstimateCacheKey(coll.PhysicalID, colInfoID, pkIsHandle, colRanges)
	cache, _ := sc.ColEstimateCache.(colEstimateCacheMap)
	if cacheable && cache != nil {
		if shapes, ok := cache[key]; ok {
			return scaleColumnRangesShape(sctx, c, shapes, coll.RealtimeCount, coll.ModifyCount), nil
		}
	}

	shapes, err := getColumnRangesShape(sctx, c, colRanges, pkIsHandle)
	if err != nil {
		return statistics.DefaultRowEst(0), errors.Trace(err)
	}

	if cacheable {
		if cache == nil {
			// Pre-size for a typical multi-index sweep so a 15-ish-index
			// access-path scan does not rehash. Small enough that simple
			// queries with one or two entries waste only a few hundred bytes.
			cache = make(colEstimateCacheMap, 16)
			sc.ColEstimateCache = cache
		}
		cache[key] = shapes
	}
	return scaleColumnRangesShape(sctx, c, shapes, coll.RealtimeCount, coll.ModifyCount), nil
}

// tryColumnEstimateForSingleColRanges checks whether column statistics can be
// used instead of index statistics for the given single-column ranges. This is
// preferred over index histogram estimation because column histograms retain
// original data types, avoiding the lossy string encoding that index histograms
// use.
//
// Returns (result, true) if column stats are valid and the estimate was
// produced. Returns (zero, false) if column stats are unavailable, the ranges
// are not single-column, or the index uses a prefix length on the column, in
// which case the caller should fall back to index-based estimation.
func tryColumnEstimateForSingleColRanges(
	sctx planctx.PlanContext,
	coll *statistics.HistColl,
	idx *statistics.Index,
	indexRanges []*ranger.Range,
) (statistics.RowEstimate, bool) {
	if coll == nil || len(indexRanges) == 0 {
		return statistics.RowEstimate{}, false
	}
	// All ranges must be single-column on both bounds. Range intersection
	// (fix control 54337) can produce ranges whose LowVal and HighVal have
	// different lengths; a longer bound constrains later index columns, so
	// reading only its first value would silently drop that constraint.
	for _, r := range indexRanges {
		if len(r.LowVal) != 1 || len(r.HighVal) != 1 {
			return statistics.RowEstimate{}, false
		}
	}
	// Not applicable for prefix indexes — ranges are truncated to the prefix
	// length, so column-level ranges would not match.
	if idx.Info.Columns[0].Length != types.UnspecifiedLength {
		return statistics.RowEstimate{}, false
	}
	// Not applicable for partial indexes (ConditionExprString != "") — column
	// stats cover all rows, but a partial index only covers rows satisfying its
	// predicate, so the estimates would not match.
	// Not applicable for MV indexes — a single row can produce multiple index
	// entries, so column-level cardinality does not reflect index row counts.
	if idx.Info.ConditionExprString != "" || idx.Info.MVIndex {
		return statistics.RowEstimate{}, false
	}
	colIDs := coll.Idx2ColUniqueIDs[idx.Histogram.ID]
	if len(colIDs) == 0 {
		return statistics.RowEstimate{}, false
	}
	colID := colIDs[0]
	// Check column stats validity — do not use pseudo estimates here.
	c := coll.GetCol(colID)
	// Derive metadata column ID for the async-load path inside
	// ColumnStatsIsInvalid (the loader keys TableItemID.ID by metadata ID,
	// not by plan UniqueID). When UniqueID2colInfoID is empty (e.g. mock
	// HistColls), the IDs coincide.
	colInfoID := colID
	if len(coll.UniqueID2colInfoID) > 0 {
		if id, ok := coll.UniqueID2colInfoID[colID]; ok {
			colInfoID = id
		}
	}
	if statistics.ColumnStatsIsInvalid(c, sctx, coll, colInfoID) {
		return statistics.RowEstimate{}, false
	}
	// For a single-column unique index, the index-based path returns exactly 1
	// for every non-null point probe, which is more accurate than histogram
	// estimation (the histogram cannot know about uniqueness, and the increase
	// factor would scale the point estimate above 1 on stale stats). Bail out
	// if any range is a non-null point so those probes keep the uniqueness
	// guarantee; mixed range sets (e.g. a = 1 OR a BETWEEN 10 AND 20) give up
	// the column estimate for their interval portion as the price. Multi-column
	// unique indexes are not eligible because single-column ranges don't cover
	// the full index, so the "exactly 1" guarantee does not apply. Column
	// nullability does not matter: a unique index only enforces uniqueness on
	// non-null values, but IsPointNonNullable already filters out NULL ranges,
	// so any range it accepts is a non-null point and the constraint applies.
	if len(idx.Info.Columns) == 1 && idx.Info.Unique {
		tc := sctx.GetSessionVars().StmtCtx.TypeCtx()
		for _, r := range indexRanges {
			if r.IsPointNonNullable(tc) {
				return statistics.RowEstimate{}, false
			}
		}
	}
	// Compute or retrieve from cache.
	result, err := GetRowCountByColumnRanges(sctx, coll, colID, indexRanges, false)
	if err != nil {
		return statistics.RowEstimate{}, false
	}
	return result, true
}

// equalRowCountOnColumn probes the column statistics for a single value and
// returns the count-independent shape of the estimate. All TopN/CM sketch/
// histogram probing happens here; scaleColumnPointShape applies the
// realtimeRowCount/modifyCount-dependent logic afterwards.
func equalRowCountOnColumn(sctx planctx.PlanContext, c *statistics.Column, val types.Datum, encodedVal []byte) (colPointShape, error) {
	if val.IsNull() {
		return colPointShape{kind: colPointShapeNull}, nil
	}
	if c.StatsVer < statistics.Version2 {
		// All the values are null.
		if c.Histogram.Bounds.NumRows() == 0 {
			return colPointShape{kind: colPointShapeFixed, cnt: 0}, nil
		}
		if c.Histogram.NDV > 0 && c.OutOfRange(val) {
			return colPointShape{kind: colPointShapeOutOfRangeV1}, nil
		}
		if c.CMSketch != nil {
			count, err := statistics.QueryValue(sctx, c.CMSketch, c.TopN, val)
			return colPointShape{kind: colPointShapeFixed, cnt: float64(count)}, errors.Trace(err)
		}
		histRowCount, _ := c.Histogram.EqualRowCount(sctx, val, false)
		return colPointShape{kind: colPointShapeFixed, cnt: histRowCount}, nil
	}

	// Stats version == 2
	// All the values are null.
	if c.Histogram.Bounds.NumRows() == 0 && c.TopN.Num() == 0 {
		return colPointShape{kind: colPointShapeFixed, cnt: 0}, nil
	}
	// 1. try to find this value in TopN
	if c.TopN != nil {
		rowcount, ok := c.TopN.QueryTopN(sctx, encodedVal)
		if ok {
			return colPointShape{kind: colPointShapeFixed, cnt: float64(rowcount)}, nil
		}
	}
	// 2. try to find this value in bucket.Repeat(the last value in every bucket)
	histCnt, matched := c.Histogram.EqualRowCount(sctx, val, true)
	if matched {
		// A match at the last bucket's end value may be underrepresented by
		// stale stats, but that decision depends on the realtime counts; record
		// the position so scaleColumnPointShape can decide between the
		// histogram count and the uniform-distribution fallback.
		if isLastBucketEndValue(sctx, &c.Histogram, val) {
			return colPointShape{kind: colPointShapeLastBucketEnd, cnt: histCnt}, nil
		}
		return colPointShape{kind: colPointShapeFixed, cnt: histCnt}, nil
	}
	// 3. use uniform distribution assumption for the rest, and address special cases for out of range
	// or all values assumed to be contained within TopN.
	return colPointShape{kind: colPointShapeUniform}, nil
}

// scaleColumnPointShape turns a probed point shape into a row estimate under
// the current realtime counts. The returned value is in stats-count space;
// callers are responsible for applying GetIncreaseFactor when accumulating
// into a per-range total.
func scaleColumnPointShape(sctx planctx.PlanContext, c *statistics.Column, p colPointShape, realtimeRowCount, modifyCount int64) statistics.RowEstimate {
	switch p.kind {
	case colPointShapeNull:
		return statistics.DefaultRowEst(float64(c.NullCount))
	case colPointShapeFixed:
		return statistics.DefaultRowEst(p.cnt)
	case colPointShapeOutOfRangeV1:
		outOfRangeCnt := outOfRangeEQSelectivity(sctx, c.Histogram.NDV, realtimeRowCount, int64(c.TotalRowCount())) * c.TotalRowCount()
		return statistics.DefaultRowEst(outOfRangeCnt)
	case colPointShapeLastBucketEnd:
		histNDV := float64(c.Histogram.NDV - int64(c.TopN.Num()))
		if !lastBucketEndValueUnderrepresented(&c.Histogram, p.cnt, histNDV, realtimeRowCount, modifyCount) {
			return statistics.DefaultRowEst(p.cnt)
		}
		return estimateRowCountWithUniformDistribution(sctx, c, realtimeRowCount, modifyCount)
	case colPointShapeUniform:
		return estimateRowCountWithUniformDistribution(sctx, c, realtimeRowCount, modifyCount)
	}
	intest.Assert(false, "unknown colPointShapeKind")
	return statistics.DefaultRowEst(0)
}

// getColumnRangesShape probes the column statistics for a slice of Range and
// returns the count-independent shape of the estimate, one entry per
// contributing range (excluded points contribute nothing; ver1 small ranges
// enumerate into one point entry per value). All histogram/TopN probing, datum
// encoding, and comparison happens here so the result can be cached across
// changes in the realtime counts; scaleColumnRangesShape applies the
// count-dependent logic.
func getColumnRangesShape(sctx planctx.PlanContext, c *statistics.Column, ranges []*ranger.Range, pkIsHandle bool) ([]colRangeShape, error) {
	sc := sctx.GetSessionVars().StmtCtx
	shapes := make([]colRangeShape, 0, len(ranges))
	for _, rg := range ranges {
		highVal := *rg.HighVal[0].Clone()
		lowVal := *rg.LowVal[0].Clone()
		if highVal.Kind() == types.KindString {
			highVal.SetBytes(collate.GetCollator(highVal.Collation()).Key(highVal.GetString()))
		}
		if lowVal.Kind() == types.KindString {
			lowVal.SetBytes(collate.GetCollator(lowVal.Collation()).Key(lowVal.GetString()))
		}
		cmp, err := lowVal.Compare(sc.TypeCtx(), &highVal, collate.GetBinaryCollator())
		if err != nil {
			return nil, errors.Trace(err)
		}
		lowEncoded, err := codec.EncodeKey(sc.TimeZone(), nil, lowVal)
		err = sc.HandleError(err)
		if err != nil {
			return nil, err
		}
		highEncoded, err := codec.EncodeKey(sc.TimeZone(), nil, highVal)
		err = sc.HandleError(err)
		if err != nil {
			return nil, err
		}
		if cmp == 0 {
			// case 1: it's a point
			if !rg.LowExclude && !rg.HighExclude {
				// In this case, the row count is at most 1.
				if pkIsHandle {
					shapes = append(shapes, colRangeShape{kind: colRangeShapePointPK})
					continue
				}
				point, err := equalRowCountOnColumn(sctx, c, lowVal, lowEncoded)
				if err != nil {
					return nil, errors.Trace(err)
				}
				shapes = append(shapes, colRangeShape{kind: colRangeShapePoint, point: point})
			}
			continue
		}
		// In stats ver 1, we use CM Sketch to estimate row count for point condition, which is more accurate.
		// So for the small range, we convert it to points.
		if c.StatsVer < 2 {
			rangeVals := statistics.EnumRangeValues(lowVal, highVal, rg.LowExclude, rg.HighExclude)

			// case 2: it's a small range && using ver1 stats
			if rangeVals != nil {
				for _, val := range rangeVals {
					point, err := equalRowCountOnColumn(sctx, c, val, lowEncoded)
					if err != nil {
						return nil, err
					}
					shapes = append(shapes, colRangeShape{kind: colRangeShapePoint, point: point})
				}

				continue
			}
		}

		// case 3: it's an interval
		interval := &colIntervalShape{
			betweenCnt: betweenRowCountOnColumn(sctx, c, lowVal, highVal, lowEncoded, highEncoded),
		}
		// `betweenRowCount` returns count for [l, h) range, boundary adjustments
		// are recorded here and applied by scaleColumnRangesShape.
		// Note that, the between count does not include null values, we need specially
		//   handle cases where null is the lower bound.
		// And because we use (2, MaxValue] to represent expressions like a > 2 and use [MinNotNull, 3) to represent
		//   expressions like b < 3, we need to exclude the special values.
		if rg.LowExclude && !lowVal.IsNull() && lowVal.Kind() != types.KindMaxValue && lowVal.Kind() != types.KindMinNotNull {
			interval.lowAdjust, err = equalRowCountOnColumn(sctx, c, lowVal, lowEncoded)
			if err != nil {
				return nil, errors.Trace(err)
			}
			interval.hasLowAdjust = true
		}
		if !rg.LowExclude && lowVal.IsNull() {
			interval.addNullCount = true
		}
		if !rg.HighExclude && highVal.Kind() != types.KindMaxValue && highVal.Kind() != types.KindMinNotNull {
			interval.highAdjust, err = equalRowCountOnColumn(sctx, c, highVal, highEncoded)
			if err != nil {
				return nil, errors.Trace(err)
			}
			interval.hasHighAdjust = true
		}
		// Record the out-of-range geometry when a bound falls outside the
		// histogram. Whether it contributes depends on the count-dependent
		// at-full-range check in scaleColumnRangesShape.
		if (c.OutOfRange(lowVal) && !lowVal.IsNull()) || c.OutOfRange(highVal) {
			histNDV := c.NDV
			// Exclude the TopN
			if c.StatsVer == statistics.Version2 {
				histNDV -= int64(c.TopN.Num())
			}
			interval.outOfRange = true
			interval.oorShape = c.Histogram.OutOfRangeShape(&lowVal, &highVal, histNDV)
		}
		shapes = append(shapes, colRangeShape{kind: colRangeShapeInterval, interval: interval})
	}
	return shapes, nil
}

// scaleColumnRangesShape combines the probed range shapes with the current
// realtimeRowCount and modifyCount to produce the final row estimate. It is
// cheap — all histogram probing already happened in getColumnRangesShape — so
// it runs on every lookup, which is what allows the statement-scoped cache to
// key the shapes without the realtime counts.
// GetIncreaseFactor is applied per range, before the out-of-range contribution,
// so that histogram-based estimates are scaled to the current table size while
// ScaleOutOfRangeShape (which already operates in current-count space) is not
// double-scaled.
func scaleColumnRangesShape(sctx planctx.PlanContext, c *statistics.Column, shapes []colRangeShape, realtimeRowCount, modifyCount int64) statistics.RowEstimate {
	var totalCount statistics.RowEstimate
	// If the current table row count has changed, scale the estimates accordingly.
	increaseFactor := c.GetIncreaseFactor(realtimeRowCount)
	for i := range shapes {
		shape := &shapes[i]
		switch shape.kind {
		case colRangeShapePointPK:
			totalCount.AddAll(1)
		case colRangeShapePoint:
			cnt := scaleColumnPointShape(sctx, c, shape.point, realtimeRowCount, modifyCount)
			cnt.MultiplyAll(increaseFactor)
			totalCount.Add(cnt)
		case colRangeShapeInterval:
			interval := shape.interval
			cnt := interval.betweenCnt
			if interval.hasLowAdjust {
				cnt.Subtract(scaleColumnPointShape(sctx, c, interval.lowAdjust, realtimeRowCount, modifyCount))
				cnt.Clamp(0, c.NotNullCount())
			}
			if interval.addNullCount {
				cnt.AddAll(float64(c.NullCount))
			}
			if interval.hasHighAdjust {
				cnt.Add(scaleColumnPointShape(sctx, c, interval.highAdjust, realtimeRowCount, modifyCount))
			}
			cnt.Clamp(0, float64(realtimeRowCount))

			// Scaling to the current table size must happen before the
			// out-of-range contribution because ScaleOutOfRangeShape already
			// returns values in current-count space.
			cnt.MultiplyAll(increaseFactor)

			// Calculate if the estimate already covers the full range of realtimeRowCount.
			// Use a tolerance factor to avoid precision issues.
			atFullRange := cnt.Est >= float64(realtimeRowCount)*(1-cost.ToleranceFactor)
			// handling the out-of-range part if the estimate does not cover the full range.
			if !atFullRange && interval.outOfRange {
				cnt.Add(c.Histogram.ScaleOutOfRangeShape(sctx, interval.oorShape, realtimeRowCount, modifyCount))
			}

			totalCount.Add(cnt)
		}
	}
	totalCount.Clamp(1.0, float64(realtimeRowCount))
	return totalCount
}

// betweenRowCountOnColumn estimates the row count for interval [l, r).
func betweenRowCountOnColumn(sctx planctx.PlanContext, c *statistics.Column, l, r types.Datum, lowEncoded, highEncoded []byte) statistics.RowEstimate {
	histBetweenCnt := c.Histogram.BetweenRowCount(sctx, l, r)
	if c.StatsVer <= statistics.Version1 {
		return histBetweenCnt
	}
	topNCnt := float64(c.TopN.BetweenCount(sctx, lowEncoded, highEncoded))
	// TopN counts are exact observed frequencies, so they shift the whole
	// estimate band: add to Est, MinEst, and MaxEst alike, matching
	// betweenRowCountOnIndex. Column min/max now feeds index estimation via
	// tryColumnEstimateForSingleColRanges, so the fields must stay consistent
	// between the two paths.
	histBetweenCnt.AddAll(topNCnt)
	return histBetweenCnt
}

// getPseudoRowCountWithPartialStats calculates the row count if there are no statistics on the index, but there are column stats available.
func getPseudoRowCountWithPartialStats(sctx planctx.PlanContext, coll *statistics.HistColl, indexRanges []*ranger.Range,
	tableRowCount float64, idxCols []*expression.Column) (totalCount float64, maxCount float64, err error) {
	if tableRowCount == 0 {
		return 0, 0, nil
	}
	// If it is a single column index, directly use column estimation instead.
	if len(idxCols) == 1 {
		var countEst statistics.RowEstimate
		countEst, err = GetRowCountByColumnRanges(sctx, coll, idxCols[0].UniqueID, indexRanges, false)
		if err != nil {
			return 0, 0, err
		}
		return countEst.Est, 0, nil
	}
	tmpRan := []*ranger.Range{
		{
			LowVal:    make([]types.Datum, 1),
			HighVal:   make([]types.Datum, 1),
			Collators: make([]collate.Collator, 1),
		},
	}
	var (
		count float64
		colID int64
	)
	totalCount = float64(0)
	maxCount = float64(0)
	for _, indexRange := range indexRanges {
		selectivity := float64(1.0)
		corrSelectivity := float64(1.0)
		for i := range indexRange.LowVal {
			tmpRan[0].LowVal[0] = indexRange.LowVal[i]
			tmpRan[0].HighVal[0] = indexRange.HighVal[i]
			tmpRan[0].Collators[0] = indexRange.Collators[0]
			if i == len(indexRange.LowVal)-1 {
				tmpRan[0].LowExclude = indexRange.LowExclude
				tmpRan[0].HighExclude = indexRange.HighExclude
			}
			colID = idxCols[i].UniqueID
			// GetRowCountByColumnRanges handles invalid stats internally by using pseudo estimation
			var countEst statistics.RowEstimate
			countEst, err = GetRowCountByColumnRanges(sctx, coll, colID, tmpRan, false)
			if err != nil {
				return 0, 0, errors.Trace(err)
			}
			count = countEst.Est
			tempSelectivity := count / tableRowCount
			selectivity *= tempSelectivity
			corrSelectivity = min(corrSelectivity, tempSelectivity)
		}
		totalCount += selectivity * tableRowCount
		maxCount += corrSelectivity * tableRowCount
	}
	totalCount = mathutil.Clamp(totalCount, 1, tableRowCount)
	return totalCount, maxCount, nil
}
