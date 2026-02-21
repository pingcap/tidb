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
	"fmt"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
)

func validInterval(ec errctx.Context, loc *time.Location, low, high *point) (bool, error) {
	l, err := codec.EncodeKey(loc, nil, low.value)
	err = ec.HandleError(err)
	if err != nil {
		return false, errors.Trace(err)
	}
	if low.excl {
		l = kv.Key(l).PrefixNext()
	}
	r, err := codec.EncodeKey(loc, nil, high.value)
	err = ec.HandleError(err)
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
func convertPoints(sctx *rangerctx.RangerContext, rangePoints []*point, newTp *types.FieldType, skipNull bool, tableRange bool) ([]*point, error) {
	i := 0
	numPoints := len(rangePoints)
	var minValueDatum, maxValueDatum types.Datum
	if tableRange {
		// Currently, table's kv range cannot accept encoded value of MaxValueDatum. we need to convert it.
		isUnsigned := mysql.HasUnsignedFlag(newTp.GetFlag())
		if isUnsigned {
			minValueDatum.SetUint64(0)
			maxValueDatum.SetUint64(math.MaxUint64)
		} else {
			minValueDatum.SetInt64(math.MinInt64)
			maxValueDatum.SetInt64(math.MaxInt64)
		}
	}
	for j := 0; j < numPoints; j += 2 {
		startPoint, err := convertPoint(sctx, rangePoints[j], newTp)
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
		endPoint, err := convertPoint(sctx, rangePoints[j+1], newTp)
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
		less, err := validInterval(sctx.ErrCtx, sctx.TypeCtx.Location(), startPoint, endPoint)
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
func points2Ranges(sctx *rangerctx.RangerContext, rangePoints []*point, newTp *types.FieldType, rangeMaxSize int64) (Ranges, bool, error) {
	convertedPoints, err := convertPoints(sctx, rangePoints, newTp, mysql.HasNotNullFlag(newTp.GetFlag()), false)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	// Estimate whether rangeMaxSize will be exceeded first before converting points to ranges.
	if rangeMaxSize > 0 && estimateMemUsageForPoints2Ranges(convertedPoints) > rangeMaxSize {
		var fullRange Ranges
		if mysql.HasNotNullFlag(newTp.GetFlag()) {
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
			Collators:   []collate.Collator{collate.GetCollator(newTp.GetCollate())},
		}
		ranges = append(ranges, ran)
	}
	return ranges, false, nil
}

func convertPoint(sctx *rangerctx.RangerContext, point *point, newTp *types.FieldType) (*point, error) {
	switch point.value.Kind() {
	case types.KindMaxValue, types.KindMinNotNull:
		return point, nil
	}
	casted, err := point.value.ConvertTo(sctx.TypeCtx, newTp)
	if err != nil {
		// skip plan cache in this case for safety.
		sctx.SetSkipPlanCache(fmt.Sprintf("%s when converting %v", err.Error(), point.value))

		//revive:disable:empty-block
		if newTp.GetType() == mysql.TypeYear && terror.ErrorEqual(err, types.ErrWarnDataOutOfRange) {
			// see issue #20101: overflow when converting integer to year
		} else if newTp.GetType() == mysql.TypeBit && terror.ErrorEqual(err, types.ErrDataTooLong) {
			// see issue #19067: we should ignore the types.ErrDataTooLong when we convert value to TypeBit value
		} else if (newTp.GetType() == mysql.TypeNewDecimal || mysql.IsIntegerType(newTp.GetType()) || newTp.GetType() == mysql.TypeFloat) && terror.ErrorEqual(err, types.ErrOverflow) {
			// Ignore the types.ErrOverflow when we convert TypeNewDecimal/TypeTiny/TypeShort/TypeInt24/TypeLong/TypeLonglong/TypeFloat values.
			// A trimmed valid boundary point value would be returned then. Accordingly, the `excl` of the point
			// would be adjusted. Impossible ranges would be skipped by the `validInterval` call later.
			// tests in TestIndexRange/TestIndexRangeForDecimal
		} else if point.value.Kind() == types.KindMysqlTime && newTp.GetType() == mysql.TypeTimestamp && terror.ErrorEqual(err, types.ErrWrongValue) {
			// See issue #28424: query failed after add index
			// Ignore conversion from Date[Time] to Timestamp since it must be either out of range or impossible date, which will not match a point select
		} else if newTp.GetType() == mysql.TypeEnum && terror.ErrorEqual(err, types.ErrTruncated) {
			// Ignore the types.ErrorTruncated when we convert TypeEnum values.
			// We should cover Enum upper overflow, and convert to the biggest value.
			if point.value.GetInt64() > 0 {
				upperEnum, err := types.ParseEnumValue(newTp.GetElems(), uint64(len(newTp.GetElems())))
				if err != nil {
					return nil, err
				}
				casted.SetMysqlEnum(upperEnum, newTp.GetCollate())
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
	valCmpCasted, err := point.value.Compare(sctx.TypeCtx, &casted, collate.GetCollator(newTp.GetCollate()))
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
func appendPoints2Ranges(sctx *rangerctx.RangerContext, origin Ranges, rangePoints []*point,
	newTp *types.FieldType, rangeMaxSize int64) (Ranges, bool, error) {
	convertedPoints, err := convertPoints(sctx, rangePoints, newTp, false, false)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	// Estimate whether rangeMaxSize will be exceeded first before appending points to ranges.
	if rangeMaxSize > 0 && estimateMemUsageForAppendPoints2Ranges(origin, convertedPoints) > rangeMaxSize {
		return origin, true, nil
	}
	var newIndexRanges Ranges
	for i := range origin {
		oRange := origin[i]
		if !oRange.IsPoint(sctx) {
			newIndexRanges = append(newIndexRanges, oRange)
		} else {
			newRanges, err := appendPoints2IndexRange(oRange, convertedPoints, newTp)
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
func points2TableRanges(sctx *rangerctx.RangerContext, rangePoints []*point, newTp *types.FieldType, rangeMaxSize int64) (Ranges, bool, error) {
	convertedPoints, err := convertPoints(sctx, rangePoints, newTp, true, true)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if rangeMaxSize > 0 && estimateMemUsageForPoints2Ranges(convertedPoints) > rangeMaxSize {
		return FullIntRange(mysql.HasUnsignedFlag(newTp.GetFlag())), true, nil
	}
	ranges := make(Ranges, 0, len(convertedPoints)/2)
	for i := 0; i < len(convertedPoints); i += 2 {
		startPoint, endPoint := convertedPoints[i], convertedPoints[i+1]
		ran := &Range{
			LowVal:      []types.Datum{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []types.Datum{endPoint.value},
			HighExclude: endPoint.excl,
			Collators:   []collate.Collator{collate.GetCollator(newTp.GetCollate())},
		}
		ranges = append(ranges, ran)
	}
	return ranges, false, nil
}

// buildColumnRange builds range from CNF conditions.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit.
// The second return value is the conditions used to build ranges and the third return value is the remained conditions.
func buildColumnRange(accessConditions []expression.Expression, sctx *rangerctx.RangerContext, tp *types.FieldType, tableRange bool,
	colLen int, rangeMaxSize int64) (ranges Ranges, _, _ []expression.Expression, err error) {
	rb := builder{sctx: sctx}
	newTp := newFieldType(tp)
	rangePoints := getFullRange()
	for _, cond := range accessConditions {
		collator := collate.GetCollator(charset.CollationBin)
		rangePoints = rb.intersection(rangePoints, rb.build(cond, newTp, colLen, true), collator)
		if rb.err != nil {
			return nil, nil, nil, errors.Trace(rb.err)
		}
	}
	var (
		rangeFallback bool
	)
	newTp = convertStringFTToBinaryCollate(newTp)
	if tableRange {
		ranges, rangeFallback, err = points2TableRanges(sctx, rangePoints, newTp, rangeMaxSize)
	} else {
		ranges, rangeFallback, err = points2Ranges(sctx, rangePoints, newTp, rangeMaxSize)
	}
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	if rangeFallback {
		sctx.RecordRangeFallback(rangeMaxSize)
		return ranges, nil, accessConditions, nil
	}
	if colLen != types.UnspecifiedLength {
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
func BuildTableRange(accessConditions []expression.Expression, sctx *rangerctx.RangerContext, tp *types.FieldType,
	rangeMaxSize int64) (_ Ranges, _, _ []expression.Expression, _ error) {
	return buildColumnRange(accessConditions, sctx, tp, true, types.UnspecifiedLength, rangeMaxSize)
}

// BuildColumnRange builds range from access conditions for general columns.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit. If you ask that all conds must be used
// for building ranges, set rangeMemQuota to 0 to avoid range fallback.
// The second return value is the conditions used to build ranges and the third return value is the remained conditions.
// If you use the function to build ranges for some access path, you need to update the path's access conditions and filter
// conditions by the second and third return values respectively.
func BuildColumnRange(conds []expression.Expression, sctx *rangerctx.RangerContext, tp *types.FieldType, colLen int,
	rangeMemQuota int64) (_ Ranges, _, _ []expression.Expression, _ error) {
	if len(conds) == 0 {
		return FullRange(), nil, nil, nil
	}
	return buildColumnRange(conds, sctx, tp, false, colLen, rangeMemQuota)
}

func (d *rangeDetacher) buildRangeOnColsByCNFCond(eqAndInCount int, accessConds []expression.Expression) (ranges Ranges, _, _ []expression.Expression, err error) {
	rb := builder{sctx: d.sctx}
	var (
		rangeFallback bool
	)
	for i := range eqAndInCount {
		// Build ranges for equal or in access conditions.
		point := rb.build(accessConds[i], d.newTpSlice[i], d.lengths[i], d.convertToSortKey)
		if rb.err != nil {
			return nil, nil, nil, errors.Trace(rb.err)
		}
		tmpNewTp := d.newTpSlice[i]
		if d.convertToSortKey {
			tmpNewTp = convertStringFTToBinaryCollate(tmpNewTp)
		}
		if i == 0 {
			ranges, rangeFallback, err = points2Ranges(d.sctx, point, tmpNewTp, d.rangeMaxSize)
		} else {
			ranges, rangeFallback, err = appendPoints2Ranges(d.sctx, ranges, point, tmpNewTp, d.rangeMaxSize)
		}
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		if rangeFallback {
			d.sctx.RecordRangeFallback(d.rangeMaxSize)
			return ranges, accessConds[:i], accessConds[i:], nil
		}
	}
	rangePoints := getFullRange()
	// Build rangePoints for non-equal access conditions.
	for i := eqAndInCount; i < len(accessConds); i++ {
		collator := collate.GetCollator(d.newTpSlice[eqAndInCount].GetCollate())
		if d.convertToSortKey {
			collator = collate.GetCollator(charset.CollationBin)
		}
		rangePoints = rb.intersection(rangePoints, rb.build(accessConds[i], d.newTpSlice[eqAndInCount], d.lengths[eqAndInCount], d.convertToSortKey), collator)
		if rb.err != nil {
			return nil, nil, nil, errors.Trace(rb.err)
		}
	}
	var tmpNewTp *types.FieldType
	if eqAndInCount == 0 || eqAndInCount < len(accessConds) {
		if d.convertToSortKey {
			tmpNewTp = convertStringFTToBinaryCollate(d.newTpSlice[eqAndInCount])
		} else {
			tmpNewTp = d.newTpSlice[eqAndInCount]
		}
	}
	if eqAndInCount == 0 {
		ranges, rangeFallback, err = points2Ranges(d.sctx, rangePoints, tmpNewTp, d.rangeMaxSize)
	} else if eqAndInCount < len(accessConds) {
		ranges, rangeFallback, err = appendPoints2Ranges(d.sctx, ranges, rangePoints, tmpNewTp, d.rangeMaxSize)
	}
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	if rangeFallback {
		d.sctx.RecordRangeFallback(d.rangeMaxSize)
		return ranges, accessConds[:eqAndInCount], accessConds[eqAndInCount:], nil
	}
	return ranges, accessConds, nil, nil
}

func convertStringFTToBinaryCollate(ft *types.FieldType) *types.FieldType {
	if ft.EvalType() != types.ETString ||
		ft.GetType() == mysql.TypeEnum ||
		ft.GetType() == mysql.TypeSet {
		return ft
	}
	newTp := ft.Clone()
	newTp.SetCharset(charset.CharsetBin)
	newTp.SetCollate(charset.CollationBin)
	return newTp
}

// buildCNFIndexRange builds the range for index where the top layer is CNF.
func (d *rangeDetacher) buildCNFIndexRange(eqAndInCount int, accessConds []expression.Expression) (ranges Ranges, newAccessConds, remainedConds []expression.Expression, err error) {
	ranges, newAccessConds, remainedConds, err = d.buildRangeOnColsByCNFCond(eqAndInCount, accessConds)
	if err != nil {
		return nil, nil, nil, err
	}

	// Take prefix index into consideration.
	if hasPrefix(d.lengths) {
		ranges, err = UnionRanges(d.sctx, ranges, d.mergeConsecutive)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
	}

	return ranges, newAccessConds, remainedConds, nil
}

