// Copyright 2017 PingCAP, Inc.
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

package ranger

import (
	"bytes"
	"math"
	"regexp"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"golang.org/x/exp/slices"
)

func validInterval(sctx sessionctx.Context, low, high *point) (bool, error) {
	sc := sctx.GetSessionVars().StmtCtx
	l, err := codec.EncodeKey(sc, nil, low.value)
	if err != nil {
		return false, errors.Trace(err)
	}
	if low.excl {
		l = kv.Key(l).PrefixNext()
	}
	r, err := codec.EncodeKey(sc, nil, high.value)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !high.excl {
		r = kv.Key(r).PrefixNext()
	}
	return bytes.Compare(l, r) < 0, nil
}

// convertPoints does some preprocessing on rangePoints to make them ready to build ranges. Preprocessing includes converting
// points to the specified type, validating intervals and skipping impossible intervals.
func convertPoints(sctx sessionctx.Context, rangePoints []*point, tp *types.FieldType, skipNull bool, tableRange bool) ([]*point, error) {
	i := 0
	numPoints := len(rangePoints)
	var minValueDatum, maxValueDatum types.Datum
	if tableRange {
		// Currently, table's kv range cannot accept encoded value of MaxValueDatum. we need to convert it.
		isUnsigned := mysql.HasUnsignedFlag(tp.GetFlag())
		if isUnsigned {
			minValueDatum.SetUint64(0)
			maxValueDatum.SetUint64(math.MaxUint64)
		} else {
			minValueDatum.SetInt64(math.MinInt64)
			maxValueDatum.SetInt64(math.MaxInt64)
		}
	}
	for j := 0; j < numPoints; j += 2 {
		startPoint, err := convertPoint(sctx, rangePoints[j], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if tableRange {
			if startPoint.value.Kind() == types.KindNull {
				startPoint.value = minValueDatum
				startPoint.excl = false
			} else if startPoint.value.Kind() == types.KindMinNotNull {
				startPoint.value = minValueDatum
			}
		}
		endPoint, err := convertPoint(sctx, rangePoints[j+1], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if tableRange {
			if endPoint.value.Kind() == types.KindMaxValue {
				endPoint.value = maxValueDatum
			}
		}
		if skipNull && endPoint.value.Kind() == types.KindNull {
			continue
		}
		less, err := validInterval(sctx, startPoint, endPoint)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !less {
			continue
		}
		rangePoints[i] = startPoint
		rangePoints[i+1] = endPoint
		i += 2
	}
	return rangePoints[:i], nil
}

// estimateMemUsageForPoints2Ranges estimates the memory usage of ranges converted from points.
func estimateMemUsageForPoints2Ranges(rangePoints []*point) int64 {
	// 16 is the size of Range.Collators
	return (EmptyRangeSize+16)*int64(len(rangePoints))/2 + getPointsTotalDatumSize(rangePoints)
}

// points2Ranges build index ranges from range points.
// Only one column is built there. If there're multiple columns, use appendPoints2Ranges.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit.
// If the second return value is true, it means that the estimated memory usage of ranges exceeds rangeMaxSize and it falls back to full range.
func points2Ranges(sctx sessionctx.Context, rangePoints []*point, tp *types.FieldType, rangeMaxSize int64) (Ranges, bool, error) {
	convertedPoints, err := convertPoints(sctx, rangePoints, tp, mysql.HasNotNullFlag(tp.GetFlag()), false)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	// Estimate whether rangeMaxSize will be exceeded first before converting points to ranges.
	if rangeMaxSize > 0 && estimateMemUsageForPoints2Ranges(convertedPoints) > rangeMaxSize {
		var fullRange Ranges
		if mysql.HasNotNullFlag(tp.GetFlag()) {
			fullRange = FullNotNullRange()
		} else {
			fullRange = FullRange()
		}
		return fullRange, true, nil
	}
	ranges := make(Ranges, 0, len(convertedPoints)/2)
	for i := 0; i < len(convertedPoints); i += 2 {
		startPoint, endPoint := convertedPoints[i], convertedPoints[i+1]
		ran := &Range{
			LowVal:      []types.Datum{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []types.Datum{endPoint.value},
			HighExclude: endPoint.excl,
			Collators:   []collate.Collator{collate.GetCollator(tp.GetCollate())},
		}
		ranges = append(ranges, ran)
	}
	return ranges, false, nil
}

func convertPoint(sctx sessionctx.Context, point *point, tp *types.FieldType) (*point, error) {
	sc := sctx.GetSessionVars().StmtCtx
	switch point.value.Kind() {
	case types.KindMaxValue, types.KindMinNotNull:
		return point, nil
	}
	casted, err := point.value.ConvertTo(sc, tp)
	if err != nil {
		if sctx.GetSessionVars().StmtCtx.InPreparedPlanBuilding {
			// do not ignore these errors if in prepared plan building for safety
			return nil, errors.Trace(err)
		}
		//revive:disable:empty-block
		if tp.GetType() == mysql.TypeYear && terror.ErrorEqual(err, types.ErrWarnDataOutOfRange) {
			// see issue #20101: overflow when converting integer to year
		} else if tp.GetType() == mysql.TypeBit && terror.ErrorEqual(err, types.ErrDataTooLong) {
			// see issue #19067: we should ignore the types.ErrDataTooLong when we convert value to TypeBit value
		} else if tp.GetType() == mysql.TypeNewDecimal && terror.ErrorEqual(err, types.ErrOverflow) {
			// Ignore the types.ErrOverflow when we convert TypeNewDecimal values.
			// A trimmed valid boundary point value would be returned then. Accordingly, the `excl` of the point
			// would be adjusted. Impossible ranges would be skipped by the `validInterval` call later.
		} else if point.value.Kind() == types.KindMysqlTime && tp.GetType() == mysql.TypeTimestamp && terror.ErrorEqual(err, types.ErrWrongValue) {
			// See issue #28424: query failed after add index
			// Ignore conversion from Date[Time] to Timestamp since it must be either out of range or impossible date, which will not match a point select
		} else if tp.GetType() == mysql.TypeEnum && terror.ErrorEqual(err, types.ErrTruncated) {
			// Ignore the types.ErrorTruncated when we convert TypeEnum values.
			// We should cover Enum upper overflow, and convert to the biggest value.
			if point.value.GetInt64() > 0 {
				upperEnum, err := types.ParseEnumValue(tp.GetElems(), uint64(len(tp.GetElems())))
				if err != nil {
					return nil, err
				}
				casted.SetMysqlEnum(upperEnum, tp.GetCollate())
			}
		} else if terror.ErrorEqual(err, charset.ErrInvalidCharacterString) {
			// The invalid string can be produced by changing datum's underlying bytes directly.
			// For example, newBuildFromPatternLike calculates the end point by adding 1 to bytes.
			// We need to skip these invalid strings.
			return point, nil
		} else {
			return point, errors.Trace(err)
		}
		//revive:enable:empty-block
	}
	valCmpCasted, err := point.value.Compare(sc, &casted, collate.GetCollator(tp.GetCollate()))
	if err != nil {
		return point, errors.Trace(err)
	}
	npoint := point.Clone(casted)
	if valCmpCasted == 0 {
		return npoint, nil
	}
	if npoint.start {
		if npoint.excl {
			if valCmpCasted < 0 {
				// e.g. "a > 1.9" convert to "a >= 2".
				npoint.excl = false
			}
		} else {
			if valCmpCasted > 0 {
				// e.g. "a >= 1.1 convert to "a > 1"
				npoint.excl = true
			}
		}
	} else {
		if npoint.excl {
			if valCmpCasted > 0 {
				// e.g. "a < 1.1" convert to "a <= 1"
				npoint.excl = false
			}
		} else {
			if valCmpCasted < 0 {
				// e.g. "a <= 1.9" convert to "a < 2"
				npoint.excl = true
			}
		}
	}
	return npoint, nil
}

func getRangesTotalDatumSize(ranges Ranges) (sum int64) {
	for _, ran := range ranges {
		for _, val := range ran.LowVal {
			sum += val.MemUsage()
		}
		for _, val := range ran.HighVal {
			sum += val.MemUsage()
		}
	}
	return
}

func getPointsTotalDatumSize(points []*point) (sum int64) {
	for _, pt := range points {
		sum += pt.value.MemUsage()
	}
	return
}

// estimateMemUsageForAppendPoints2Ranges estimates the memory usage of results of appending points to ranges.
func estimateMemUsageForAppendPoints2Ranges(origin Ranges, rangePoints []*point) int64 {
	if len(origin) == 0 || len(rangePoints) == 0 {
		return 0
	}
	originDatumSize := getRangesTotalDatumSize(origin)
	pointDatumSize := getPointsTotalDatumSize(rangePoints)
	len1, len2 := int64(len(origin)), int64(len(rangePoints))/2
	// (int64(len(origin[0].LowVal))+1)*16 is the size of Range.Collators.
	return (EmptyRangeSize+(int64(len(origin[0].LowVal))+1)*16)*len1*len2 + originDatumSize*len2 + pointDatumSize*len1
}

// appendPoints2Ranges appends additional column ranges for multi-column index. The additional column ranges can only be
// appended to point ranges. For example, we have an index (a, b), if the condition is (a > 1 and b = 2), then we can not
// build a conjunctive ranges for this index.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit.
// If the second return value is true, it means that the estimated memory usage of ranges after appending points exceeds
// rangeMaxSize and the function rejects appending points to ranges.
func appendPoints2Ranges(sctx sessionctx.Context, origin Ranges, rangePoints []*point,
	ft *types.FieldType, rangeMaxSize int64) (Ranges, bool, error) {
	convertedPoints, err := convertPoints(sctx, rangePoints, ft, false, false)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	// Estimate whether rangeMaxSize will be exceeded first before appending points to ranges.
	if rangeMaxSize > 0 && estimateMemUsageForAppendPoints2Ranges(origin, convertedPoints) > rangeMaxSize {
		return origin, true, nil
	}
	var newIndexRanges Ranges
	for i := 0; i < len(origin); i++ {
		oRange := origin[i]
		if !oRange.IsPoint(sctx) {
			newIndexRanges = append(newIndexRanges, oRange)
		} else {
			newRanges, err := appendPoints2IndexRange(oRange, convertedPoints, ft)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			newIndexRanges = append(newIndexRanges, newRanges...)
		}
	}
	return newIndexRanges, false, nil
}

func appendPoints2IndexRange(origin *Range, rangePoints []*point, ft *types.FieldType) (Ranges, error) {
	newRanges := make(Ranges, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint, endPoint := rangePoints[i], rangePoints[i+1]

		lowVal := make([]types.Datum, len(origin.LowVal)+1)
		copy(lowVal, origin.LowVal)
		lowVal[len(origin.LowVal)] = startPoint.value

		highVal := make([]types.Datum, len(origin.HighVal)+1)
		copy(highVal, origin.HighVal)
		highVal[len(origin.HighVal)] = endPoint.value

		collators := make([]collate.Collator, len(origin.Collators)+1)
		copy(collators, origin.Collators)
		collators[len(origin.Collators)] = collate.GetCollator(ft.GetCollate())

		ir := &Range{
			LowVal:      lowVal,
			LowExclude:  startPoint.excl,
			HighVal:     highVal,
			HighExclude: endPoint.excl,
			Collators:   collators,
		}
		newRanges = append(newRanges, ir)
	}
	return newRanges, nil
}

// estimateMemUsageForAppendRanges2PointRanges estimates the memory usage of results of appending ranges to point ranges.
func estimateMemUsageForAppendRanges2PointRanges(pointRanges Ranges, ranges Ranges) int64 {
	len1, len2 := int64(len(pointRanges)), int64(len(ranges))
	if len1 == 0 || len2 == 0 {
		return 0
	}
	collatorSize := (int64(len(pointRanges[0].LowVal)) + int64(len(ranges[0].LowVal))) * 16
	return (EmptyRangeSize+collatorSize)*len1*len2 + getRangesTotalDatumSize(pointRanges)*len2 + getRangesTotalDatumSize(ranges)*len1
}

// appendRange2PointRange appends suffixRange to pointRange.
func appendRange2PointRange(pointRange, suffixRange *Range) *Range {
	lowVal := make([]types.Datum, 0, len(pointRange.LowVal)+len(suffixRange.LowVal))
	lowVal = append(lowVal, pointRange.LowVal...)
	lowVal = append(lowVal, suffixRange.LowVal...)

	highVal := make([]types.Datum, 0, len(pointRange.HighVal)+len(suffixRange.HighVal))
	highVal = append(highVal, pointRange.HighVal...)
	highVal = append(highVal, suffixRange.HighVal...)

	collators := make([]collate.Collator, 0, len(pointRange.Collators)+len(suffixRange.Collators))
	collators = append(collators, pointRange.Collators...)
	collators = append(collators, suffixRange.Collators...)

	return &Range{
		LowVal:      lowVal,
		LowExclude:  suffixRange.LowExclude,
		HighVal:     highVal,
		HighExclude: suffixRange.HighExclude,
		Collators:   collators,
	}
}

// AppendRanges2PointRanges appends additional ranges to point ranges.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit.
// If the second return value is true, it means that the estimated memory after appending additional ranges to point ranges
// exceeds rangeMaxSize and the function rejects appending additional ranges to point ranges.
func AppendRanges2PointRanges(pointRanges Ranges, ranges Ranges, rangeMaxSize int64) (Ranges, bool) {
	if len(ranges) == 0 {
		return pointRanges, false
	}
	// Estimate whether rangeMaxSize will be exceeded first before appending ranges to point ranges.
	if rangeMaxSize > 0 && estimateMemUsageForAppendRanges2PointRanges(pointRanges, ranges) > rangeMaxSize {
		return pointRanges, true
	}
	newRanges := make(Ranges, 0, len(pointRanges)*len(ranges))
	for _, pointRange := range pointRanges {
		for _, r := range ranges {
			newRanges = append(newRanges, appendRange2PointRange(pointRange, r))
		}
	}
	return newRanges, false
}

// points2TableRanges build ranges for table scan from range points.
// It will remove the nil and convert MinNotNull and MaxValue to MinInt64 or MinUint64 and MaxInt64 or MaxUint64.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit.
// If the second return value is true, it means that the estimated memory usage of ranges exceeds rangeMaxSize and it falls back to full range.
func points2TableRanges(sctx sessionctx.Context, rangePoints []*point, tp *types.FieldType, rangeMaxSize int64) (Ranges, bool, error) {
	convertedPoints, err := convertPoints(sctx, rangePoints, tp, true, true)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if rangeMaxSize > 0 && estimateMemUsageForPoints2Ranges(convertedPoints) > rangeMaxSize {
		return FullIntRange(mysql.HasUnsignedFlag(tp.GetFlag())), true, nil
	}
	ranges := make(Ranges, 0, len(convertedPoints)/2)
	for i := 0; i < len(convertedPoints); i += 2 {
		startPoint, endPoint := convertedPoints[i], convertedPoints[i+1]
		ran := &Range{
			LowVal:      []types.Datum{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []types.Datum{endPoint.value},
			HighExclude: endPoint.excl,
			Collators:   []collate.Collator{collate.GetCollator(tp.GetCollate())},
		}
		ranges = append(ranges, ran)
	}
	return ranges, false, nil
}

// buildColumnRange builds range from CNF conditions.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit.
// The second return value is the conditions used to build ranges and the third return value is the remained conditions.
func buildColumnRange(accessConditions []expression.Expression, sctx sessionctx.Context, tp *types.FieldType, tableRange bool,
	colLen int, rangeMaxSize int64) (Ranges, []expression.Expression, []expression.Expression, error) {
	rb := builder{sc: sctx.GetSessionVars().StmtCtx}
	rangePoints := getFullRange()
	for _, cond := range accessConditions {
		collator := collate.GetCollator(tp.GetCollate())
		rangePoints = rb.intersection(rangePoints, rb.build(cond, collator), collator)
		if rb.err != nil {
			return nil, nil, nil, errors.Trace(rb.err)
		}
	}
	var (
		ranges        Ranges
		rangeFallback bool
		err           error
	)
	newTp := newFieldType(tp)
	if tableRange {
		ranges, rangeFallback, err = points2TableRanges(sctx, rangePoints, newTp, rangeMaxSize)
	} else {
		ranges, rangeFallback, err = points2Ranges(sctx, rangePoints, newTp, rangeMaxSize)
	}
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	if rangeFallback {
		sctx.GetSessionVars().StmtCtx.RecordRangeFallback(rangeMaxSize)
		return ranges, nil, accessConditions, nil
	}
	if colLen != types.UnspecifiedLength {
		for _, ran := range ranges {
			// If the length of the last column of LowVal is equal to the prefix length, LowExclude should be set false.
			// For example, `col_varchar > 'xx'` should be converted to range [xx, +inf) when the prefix index length of
			// `col_varchar` is 2. Otherwise we would miss values like 'xxx' if we execute (xx, +inf) index range scan.
			if CutDatumByPrefixLen(&ran.LowVal[0], colLen, tp) || ReachPrefixLen(&ran.LowVal[0], colLen, tp) {
				ran.LowExclude = false
			}
			if CutDatumByPrefixLen(&ran.HighVal[0], colLen, tp) {
				ran.HighExclude = false
			}
		}
		ranges, err = UnionRanges(sctx, ranges, true)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return ranges, accessConditions, nil, nil
}

// BuildTableRange builds range of PK column for PhysicalTableScan.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit. If you ask that all conds must be used
// for building ranges, set rangeMemQuota to 0 to avoid range fallback.
// The second return value is the conditions used to build ranges and the third return value is the remained conditions.
// If you use the function to build ranges for some access path, you need to update the path's access conditions and filter
// conditions by the second and third return values respectively.
func BuildTableRange(accessConditions []expression.Expression, sctx sessionctx.Context, tp *types.FieldType,
	rangeMaxSize int64) (Ranges, []expression.Expression, []expression.Expression, error) {
	return buildColumnRange(accessConditions, sctx, tp, true, types.UnspecifiedLength, rangeMaxSize)
}

// BuildColumnRange builds range from access conditions for general columns.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit. If you ask that all conds must be used
// for building ranges, set rangeMemQuota to 0 to avoid range fallback.
// The second return value is the conditions used to build ranges and the third return value is the remained conditions.
// If you use the function to build ranges for some access path, you need to update the path's access conditions and filter
// conditions by the second and third return values respectively.
func BuildColumnRange(conds []expression.Expression, sctx sessionctx.Context, tp *types.FieldType, colLen int,
	rangeMemQuota int64) (Ranges, []expression.Expression, []expression.Expression, error) {
	if len(conds) == 0 {
		return FullRange(), nil, nil, nil
	}
	return buildColumnRange(conds, sctx, tp, false, colLen, rangeMemQuota)
}

func (d *rangeDetacher) buildRangeOnColsByCNFCond(newTp []*types.FieldType, eqAndInCount int,
	accessConds []expression.Expression) (Ranges, []expression.Expression, []expression.Expression, error) {
	rb := builder{sc: d.sctx.GetSessionVars().StmtCtx}
	var (
		ranges        Ranges
		rangeFallback bool
		err           error
	)
	for i := 0; i < eqAndInCount; i++ {
		// Build ranges for equal or in access conditions.
		point := rb.build(accessConds[i], collate.GetCollator(newTp[i].GetCollate()))
		if rb.err != nil {
			return nil, nil, nil, errors.Trace(rb.err)
		}
		if i == 0 {
			ranges, rangeFallback, err = points2Ranges(d.sctx, point, newTp[i], d.rangeMaxSize)
		} else {
			ranges, rangeFallback, err = appendPoints2Ranges(d.sctx, ranges, point, newTp[i], d.rangeMaxSize)
		}
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		if rangeFallback {
			d.sctx.GetSessionVars().StmtCtx.RecordRangeFallback(d.rangeMaxSize)
			return ranges, accessConds[:i], accessConds[i:], nil
		}
	}
	rangePoints := getFullRange()
	// Build rangePoints for non-equal access conditions.
	for i := eqAndInCount; i < len(accessConds); i++ {
		collator := collate.GetCollator(newTp[eqAndInCount].GetCollate())
		rangePoints = rb.intersection(rangePoints, rb.build(accessConds[i], collator), collator)
		if rb.err != nil {
			return nil, nil, nil, errors.Trace(rb.err)
		}
	}
	if eqAndInCount == 0 {
		ranges, rangeFallback, err = points2Ranges(d.sctx, rangePoints, newTp[0], d.rangeMaxSize)
	} else if eqAndInCount < len(accessConds) {
		ranges, rangeFallback, err = appendPoints2Ranges(d.sctx, ranges, rangePoints, newTp[eqAndInCount], d.rangeMaxSize)
	}
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	if rangeFallback {
		d.sctx.GetSessionVars().StmtCtx.RecordRangeFallback(d.rangeMaxSize)
		return ranges, accessConds[:eqAndInCount], accessConds[eqAndInCount:], nil
	}
	return ranges, accessConds, nil, nil
}

// buildCNFIndexRange builds the range for index where the top layer is CNF.
func (d *rangeDetacher) buildCNFIndexRange(newTp []*types.FieldType, eqAndInCount int,
	accessConds []expression.Expression) (Ranges, []expression.Expression, []expression.Expression, error) {
	ranges, newAccessConds, remainedConds, err := d.buildRangeOnColsByCNFCond(newTp, eqAndInCount, accessConds)
	if err != nil {
		return nil, nil, nil, err
	}

	// Take prefix index into consideration.
	if hasPrefix(d.lengths) {
		if fixPrefixColRange(ranges, d.lengths, newTp) {
			ranges, err = UnionRanges(d.sctx, ranges, d.mergeConsecutive)
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
		}
	}

	return ranges, newAccessConds, remainedConds, nil
}

type sortRange struct {
	originalValue *Range
	encodedStart  []byte
	encodedEnd    []byte
}

// UnionRanges sorts `ranges`, union adjacent ones if possible.
// For two intervals [a, b], [c, d], we have guaranteed that a <= c. If b >= c. Then two intervals are overlapped.
// And this two can be merged as [a, max(b, d)].
// Otherwise they aren't overlapped.
func UnionRanges(sctx sessionctx.Context, ranges Ranges, mergeConsecutive bool) (Ranges, error) {
	sc := sctx.GetSessionVars().StmtCtx
	if len(ranges) == 0 {
		return nil, nil
	}
	objects := make([]*sortRange, 0, len(ranges))
	for _, ran := range ranges {
		left, err := codec.EncodeKey(sc, nil, ran.LowVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ran.LowExclude {
			left = kv.Key(left).PrefixNext()
		}
		right, err := codec.EncodeKey(sc, nil, ran.HighVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ran.HighExclude {
			right = kv.Key(right).PrefixNext()
		}
		objects = append(objects, &sortRange{originalValue: ran, encodedStart: left, encodedEnd: right})
	}
	slices.SortFunc(objects, func(i, j *sortRange) bool {
		return bytes.Compare(i.encodedStart, j.encodedStart) < 0
	})
	ranges = ranges[:0]
	lastRange := objects[0]
	for i := 1; i < len(objects); i++ {
		if (mergeConsecutive && bytes.Compare(lastRange.encodedEnd, objects[i].encodedStart) >= 0) ||
			(!mergeConsecutive && bytes.Compare(lastRange.encodedEnd, objects[i].encodedStart) > 0) {
			if bytes.Compare(lastRange.encodedEnd, objects[i].encodedEnd) < 0 {
				lastRange.encodedEnd = objects[i].encodedEnd
				lastRange.originalValue.HighVal = objects[i].originalValue.HighVal
				lastRange.originalValue.HighExclude = objects[i].originalValue.HighExclude
			}
		} else {
			ranges = append(ranges, lastRange.originalValue)
			lastRange = objects[i]
		}
	}
	ranges = append(ranges, lastRange.originalValue)
	return ranges, nil
}

func hasPrefix(lengths []int) bool {
	for _, l := range lengths {
		if l != types.UnspecifiedLength {
			return true
		}
	}
	return false
}

// fixPrefixColRange checks whether the range of one column exceeds the length and needs to be cut.
// It specially handles the last column of each range point. If the last one need to be cut, it will
// change the exclude status of that point and return `true` to tell
// that we need do a range merging since that interval may have intersection.
// e.g. if the interval is (-inf -inf, a xxxxx), (a xxxxx, +inf +inf) and the length of the last column is 3,
//
//	then we'll change it to (-inf -inf, a xxx], [a xxx, +inf +inf). You can see that this two interval intersect,
//	so we need a merge operation.
//
// Q: only checking the last column to decide whether the endpoint's exclude status needs to be reset is enough?
// A: Yes, suppose that the interval is (-inf -inf, a xxxxx b) and only the second column needs to be cut.
//
//	The result would be (-inf -inf, a xxx b) if the length of it is 3. Obviously we only need to care about the data
//	whose the first two key is `a` and `xxx`. It read all data whose index value begins with `a` and `xxx` and the third
//	value less than `b`, covering the values begin with `a` and `xxxxx` and the third value less than `b` perfectly.
//	So in this case we don't need to reset its exclude status. The right endpoint case can be proved in the same way.
func fixPrefixColRange(ranges Ranges, lengths []int, tp []*types.FieldType) bool {
	var hasCut bool
	for _, ran := range ranges {
		lowTail := len(ran.LowVal) - 1
		for i := 0; i < lowTail; i++ {
			hasCut = CutDatumByPrefixLen(&ran.LowVal[i], lengths[i], tp[i]) || hasCut
		}
		lowCut := CutDatumByPrefixLen(&ran.LowVal[lowTail], lengths[lowTail], tp[lowTail])
		// If the length of the last column of LowVal is equal to the prefix length, LowExclude should be set false.
		// For example, `col_varchar > 'xx'` should be converted to range [xx, +inf) when the prefix index length of
		// `col_varchar` is 2. Otherwise we would miss values like 'xxx' if we execute (xx, +inf) index range scan.
		if lowCut || ReachPrefixLen(&ran.LowVal[lowTail], lengths[lowTail], tp[lowTail]) {
			ran.LowExclude = false
		}
		highTail := len(ran.HighVal) - 1
		for i := 0; i < highTail; i++ {
			hasCut = CutDatumByPrefixLen(&ran.HighVal[i], lengths[i], tp[i]) || hasCut
		}
		highCut := CutDatumByPrefixLen(&ran.HighVal[highTail], lengths[highTail], tp[highTail])
		if highCut {
			ran.HighExclude = false
		}
		hasCut = hasCut || lowCut || highCut
	}
	return hasCut
}

// CutDatumByPrefixLen cuts the datum according to the prefix length.
// If it's binary or ascii encoded, we will cut it by bytes rather than characters.
func CutDatumByPrefixLen(v *types.Datum, length int, tp *types.FieldType) bool {
	if (v.Kind() == types.KindString || v.Kind() == types.KindBytes) && length != types.UnspecifiedLength {
		colCharset := tp.GetCharset()
		colValue := v.GetBytes()
		if colCharset == charset.CharsetBin || colCharset == charset.CharsetASCII {
			if len(colValue) > length {
				// truncate value and limit its length
				if v.Kind() == types.KindBytes {
					v.SetBytes(colValue[:length])
				} else {
					v.SetString(v.GetString()[:length], tp.GetCollate())
				}
				return true
			}
		} else if utf8.RuneCount(colValue) > length {
			rs := bytes.Runes(colValue)
			truncateStr := string(rs[:length])
			// truncate value and limit its length
			v.SetString(truncateStr, tp.GetCollate())
			return true
		}
	}
	return false
}

// ReachPrefixLen checks whether the length of v is equal to the prefix length.
func ReachPrefixLen(v *types.Datum, length int, tp *types.FieldType) bool {
	if (v.Kind() == types.KindString || v.Kind() == types.KindBytes) && length != types.UnspecifiedLength {
		colCharset := tp.GetCharset()
		colValue := v.GetBytes()
		if colCharset == charset.CharsetBin || colCharset == charset.CharsetASCII {
			return len(colValue) == length
		}
		return utf8.RuneCount(colValue) == length
	}
	return false
}

// We cannot use the FieldType of column directly. e.g. the column a is int32 and we have a > 1111111111111111111.
// Obviously the constant is bigger than MaxInt32, so we will get overflow error if we use the FieldType of column a.
func newFieldType(tp *types.FieldType) *types.FieldType {
	switch tp.GetType() {
	// To avoid overflow error.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		newTp := types.NewFieldType(mysql.TypeLonglong)
		newTp.SetFlag(tp.GetFlag())
		newTp.SetCharset(tp.GetCharset())
		return newTp
	// To avoid data truncate error.
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		newTp := types.NewFieldTypeWithCollation(tp.GetType(), tp.GetCollate(), types.UnspecifiedLength)
		newTp.SetCharset(tp.GetCharset())
		return newTp
	default:
		return tp
	}
}

// points2EqOrInCond constructs a 'EQUAL' or 'IN' scalar function based on the
// 'points'. `col` is the target column to construct the Equal or In condition.
// NOTE:
// 1. 'points' should not be empty.
func points2EqOrInCond(ctx sessionctx.Context, points []*point, col *expression.Column) expression.Expression {
	// len(points) cannot be 0 here, since we impose early termination in ExtractEqAndInCondition
	// Constant and Column args should have same RetType, simply get from first arg
	retType := col.GetType()
	args := make([]expression.Expression, 0, len(points)/2)
	args = append(args, col)
	for i := 0; i < len(points); i = i + 2 {
		value := &expression.Constant{
			Value:   points[i].value,
			RetType: retType,
		}
		args = append(args, value)
	}
	funcName := ast.EQ
	if len(args) > 2 {
		funcName = ast.In
	}
	return expression.NewFunctionInternal(ctx, funcName, col.GetType(), args...)
}

// RangesToString print a list of Ranges into a string which can appear in an SQL as a condition.
func RangesToString(sc *stmtctx.StatementContext, rans Ranges, colNames []string) (string, error) {
	for _, ran := range rans {
		if len(ran.LowVal) != len(ran.HighVal) {
			return "", errors.New("range length mismatch")
		}
	}
	var buffer bytes.Buffer
	for i, ran := range rans {
		buffer.WriteString("(")
		for j := range ran.LowVal {
			buffer.WriteString("(")

			// The `Exclude` information is only useful for the last columns.
			// If it's not the last column, it should always be false, which means it's inclusive.
			lowExclude := false
			if ran.LowExclude && j == len(ran.LowVal)-1 {
				lowExclude = true
			}
			highExclude := false
			if ran.HighExclude && j == len(ran.LowVal)-1 {
				highExclude = true
			}

			// sanity check: only last column of the `Range` can be an interval
			if j < len(ran.LowVal)-1 {
				cmp, err := ran.LowVal[j].Compare(sc, &ran.HighVal[j], ran.Collators[j])
				if err != nil {
					return "", errors.New("comparing values error: " + err.Error())
				}
				if cmp != 0 {
					return "", errors.New("unexpected form of range")
				}
			}
			str, err := RangeSingleColToString(sc, ran.LowVal[j], ran.HighVal[j], lowExclude, highExclude, colNames[j], ran.Collators[j])
			if err != nil {
				return "false", err
			}
			buffer.WriteString(str)
			buffer.WriteString(")")
			if j < len(ran.LowVal)-1 {
				// Conditions on different columns of a range are implicitly connected with AND.
				buffer.WriteString(" and ")
			}
		}
		buffer.WriteString(")")
		if i < len(rans)-1 {
			// Conditions of different ranges are implicitly connected with OR.
			buffer.WriteString(" or ")
		}
	}
	result := buffer.String()

	// Simplify some useless conditions.
	if matched, err := regexp.MatchString(`^\(*true\)*$`, result); matched || (err != nil) {
		return "true", nil
	}
	return result, nil
}

// RangeSingleColToString prints a single column of a Range into a string which can appear in an SQL as a condition.
func RangeSingleColToString(sc *stmtctx.StatementContext, lowVal, highVal types.Datum, lowExclude, highExclude bool, colName string, collator collate.Collator) (string, error) {
	// case 1: low and high are both special values(null, min not null, max value)
	lowKind := lowVal.Kind()
	highKind := highVal.Kind()
	if (lowKind == types.KindNull || lowKind == types.KindMinNotNull || lowKind == types.KindMaxValue) &&
		(highKind == types.KindNull || highKind == types.KindMinNotNull || highKind == types.KindMaxValue) {
		if lowKind == types.KindNull && highKind == types.KindNull && !lowExclude && !highExclude {
			return colName + " is null", nil
		}
		if lowKind == types.KindNull && highKind == types.KindMaxValue && !lowExclude {
			return "true", nil
		}
		if lowKind == types.KindMinNotNull && highKind == types.KindMaxValue {
			return colName + " is not null", nil
		}
		return "false", nil
	}

	var buf bytes.Buffer
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)

	// case 2: low value and high value are the same, and low value and high value are both inclusive.
	cmp, err := lowVal.Compare(sc, &highVal, collator)
	if err != nil {
		return "false", errors.Trace(err)
	}
	if cmp == 0 && !lowExclude && !highExclude && !lowVal.IsNull() {
		buf.WriteString(colName)
		buf.WriteString(" = ")
		lowValExpr := driver.ValueExpr{Datum: lowVal}
		err := lowValExpr.Restore(restoreCtx)
		if err != nil {
			return "false", errors.Trace(err)
		}
		return buf.String(), nil
	}

	// case 3: it's an interval.
	useOR := false
	noLowerPart := false

	// Handle the low value part.
	if lowKind == types.KindNull {
		buf.WriteString(colName + " is null")
		useOR = true
	} else if lowKind == types.KindMinNotNull {
		noLowerPart = true
	} else {
		buf.WriteString(colName)
		if lowExclude {
			buf.WriteString(" > ")
		} else {
			buf.WriteString(" >= ")
		}
		lowValExpr := driver.ValueExpr{Datum: lowVal}
		err := lowValExpr.Restore(restoreCtx)
		if err != nil {
			return "false", errors.Trace(err)
		}
	}

	if !noLowerPart {
		if useOR {
			buf.WriteString(" or ")
		} else {
			buf.WriteString(" and ")
		}
	}

	// Handle the high value part
	if highKind == types.KindMaxValue {
		buf.WriteString("true")
	} else {
		buf.WriteString(colName)
		if highExclude {
			buf.WriteString(" < ")
		} else {
			buf.WriteString(" <= ")
		}
		highValExpr := driver.ValueExpr{Datum: highVal}
		err := highValExpr.Restore(restoreCtx)
		if err != nil {
			return "false", errors.Trace(err)
		}
	}

	return buf.String(), nil
}
