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
	"regexp"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
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
		if sctx.InPreparedPlanBuilding {
			// skip plan cache in this case for safety.
			sctx.SetSkipPlanCache(fmt.Sprintf("%s when converting %v", err.Error(), point.value))
		}
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
	for i := 0; i < len(origin); i++ {
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
	colLen int, rangeMaxSize int64) (Ranges, []expression.Expression, []expression.Expression, error) {
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
		ranges        Ranges
		rangeFallback bool
		err           error
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
	rangeMaxSize int64) (Ranges, []expression.Expression, []expression.Expression, error) {
	return buildColumnRange(accessConditions, sctx, tp, true, types.UnspecifiedLength, rangeMaxSize)
}

// BuildColumnRange builds range from access conditions for general columns.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit. If you ask that all conds must be used
// for building ranges, set rangeMemQuota to 0 to avoid range fallback.
// The second return value is the conditions used to build ranges and the third return value is the remained conditions.
// If you use the function to build ranges for some access path, you need to update the path's access conditions and filter
// conditions by the second and third return values respectively.
func BuildColumnRange(conds []expression.Expression, sctx *rangerctx.RangerContext, tp *types.FieldType, colLen int,
	rangeMemQuota int64) (Ranges, []expression.Expression, []expression.Expression, error) {
	if len(conds) == 0 {
		return FullRange(), nil, nil, nil
	}
	return buildColumnRange(conds, sctx, tp, false, colLen, rangeMemQuota)
}

func (d *rangeDetacher) buildRangeOnColsByCNFCond(newTp []*types.FieldType, eqAndInCount int,
	accessConds []expression.Expression) (Ranges, []expression.Expression, []expression.Expression, error) {
	rb := builder{sctx: d.sctx}
	var (
		ranges        Ranges
		rangeFallback bool
		err           error
	)
	for i := 0; i < eqAndInCount; i++ {
		// Build ranges for equal or in access conditions.
		point := rb.build(accessConds[i], newTp[i], d.lengths[i], d.convertToSortKey)
		if rb.err != nil {
			return nil, nil, nil, errors.Trace(rb.err)
		}
		tmpNewTp := newTp[i]
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
		collator := collate.GetCollator(newTp[eqAndInCount].GetCollate())
		if d.convertToSortKey {
			collator = collate.GetCollator(charset.CollationBin)
		}
		rangePoints = rb.intersection(rangePoints, rb.build(accessConds[i], newTp[eqAndInCount], d.lengths[eqAndInCount], d.convertToSortKey), collator)
		if rb.err != nil {
			return nil, nil, nil, errors.Trace(rb.err)
		}
	}
	var tmpNewTp *types.FieldType
	if eqAndInCount == 0 || eqAndInCount < len(accessConds) {
		if d.convertToSortKey {
			tmpNewTp = convertStringFTToBinaryCollate(newTp[eqAndInCount])
		} else {
			tmpNewTp = newTp[eqAndInCount]
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
func (d *rangeDetacher) buildCNFIndexRange(newTp []*types.FieldType, eqAndInCount int,
	accessConds []expression.Expression) (Ranges, []expression.Expression, []expression.Expression, error) {
	ranges, newAccessConds, remainedConds, err := d.buildRangeOnColsByCNFCond(newTp, eqAndInCount, accessConds)
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

type sortRange struct {
	originalValue *Range
	encodedStart  []byte
	encodedEnd    []byte
}

// UnionRanges sorts `ranges`, union adjacent ones if possible.
// For two intervals [a, b], [c, d], we have guaranteed that a <= c. If b >= c. Then two intervals are overlapped.
// And this two can be merged as [a, max(b, d)].
// Otherwise they aren't overlapped.
func UnionRanges(sctx *rangerctx.RangerContext, ranges Ranges, mergeConsecutive bool) (Ranges, error) {
	if len(ranges) == 0 {
		return nil, nil
	}
	objects := make([]*sortRange, 0, len(ranges))
	for _, ran := range ranges {
		left, err := codec.EncodeKey(sctx.TypeCtx.Location(), nil, ran.LowVal...)
		err = sctx.ErrCtx.HandleError(err)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ran.LowExclude {
			left = kv.Key(left).PrefixNext()
		}
		right, err := codec.EncodeKey(sctx.TypeCtx.Location(), nil, ran.HighVal...)
		err = sctx.ErrCtx.HandleError(err)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ran.HighExclude {
			right = kv.Key(right).PrefixNext()
		}
		objects = append(objects, &sortRange{originalValue: ran, encodedStart: left, encodedEnd: right})
	}
	slices.SortFunc(objects, func(i, j *sortRange) int {
		return bytes.Compare(i.encodedStart, j.encodedStart)
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

// cutPrefixForPoints cuts the prefix of points according to the prefix length of the prefix index.
// It may modify the point.value and point.excl. The modification is in-place.
// This function doesn't require the start and end points to be paired in the input.
func cutPrefixForPoints(points []*point, length int, tp *types.FieldType) {
	if length == types.UnspecifiedLength {
		return
	}
	for _, p := range points {
		if p == nil {
			continue
		}
		cut := CutDatumByPrefixLen(&p.value, length, tp)
		// In two cases, we need to convert the exclusive point to an inclusive point.
		// case 1: we actually cut the value to accommodate the prefix index.
		if cut ||
			// case 2: the value is already equal to the prefix index.
			// For example, col_varchar > 'xx' should be converted to range [xx, +inf) when the prefix index length of
			// `col_varchar` is 2. Otherwise, we would miss values like 'xxx' if we execute (xx, +inf) index range scan.
			(p.start && ReachPrefixLen(&p.value, length, tp)) {
			p.excl = false
		}
	}
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

// In util/ranger, for each datum that is used in the Range, we will convert data type for them.
// But we cannot use the FieldType of column directly. e.g. the column a is int32 and we have a > 1111111111111111111.
// Obviously the constant is bigger than MaxInt32, so we will get overflow error if we use the FieldType of column a.
// In util/ranger here, we usually use "newTp" to emphasize its difference from the original FieldType of the column.
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
func points2EqOrInCond(ctx expression.BuildContext, points []*point, col *expression.Column) expression.Expression {
	// len(points) cannot be 0 here, since we impose early termination in ExtractEqAndInCondition
	// Constant and Column args should have same RetType, simply get from first arg
	retType := col.GetType(ctx.GetEvalCtx())
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
	return expression.NewFunctionInternal(ctx, funcName, col.GetType(ctx.GetEvalCtx()), args...)
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
				cmp, err := ran.LowVal[j].Compare(sc.TypeCtx(), &ran.HighVal[j], ran.Collators[j])
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
	cmp, err := lowVal.Compare(sc.TypeCtx(), &highVal, collator)
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
