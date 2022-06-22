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
	"sort"
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

// points2Ranges build index ranges from range points.
// Only one column is built there. If there're multiple columns, use appendPoints2Ranges.
func points2Ranges(sctx sessionctx.Context, rangePoints []*point, tp *types.FieldType) ([]*Range, error) {
	ranges := make([]*Range, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint, err := convertPoint(sctx, rangePoints[i], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		endPoint, err := convertPoint(sctx, rangePoints[i+1], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		less, err := validInterval(sctx, startPoint, endPoint)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !less {
			continue
		}
		// If column has not null flag, [null, null] should be removed.
		if mysql.HasNotNullFlag(tp.GetFlag()) && endPoint.value.Kind() == types.KindNull {
			continue
		}

		ran := &Range{
			LowVal:      []types.Datum{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []types.Datum{endPoint.value},
			HighExclude: endPoint.excl,
			Collators:   []collate.Collator{collate.GetCollator(tp.GetCollate())},
		}
		ranges = append(ranges, ran)
	}
	return ranges, nil
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

// appendPoints2Ranges appends additional column ranges for multi-column index.
// The additional column ranges can only be appended to point ranges.
// for example we have an index (a, b), if the condition is (a > 1 and b = 2)
// then we can not build a conjunctive ranges for this index.
func appendPoints2Ranges(sctx sessionctx.Context, origin []*Range, rangePoints []*point,
	ft *types.FieldType) ([]*Range, error) {
	var newIndexRanges []*Range
	for i := 0; i < len(origin); i++ {
		oRange := origin[i]
		if !oRange.IsPoint(sctx) {
			newIndexRanges = append(newIndexRanges, oRange)
		} else {
			newRanges, err := appendPoints2IndexRange(sctx, oRange, rangePoints, ft)
			if err != nil {
				return nil, errors.Trace(err)
			}
			newIndexRanges = append(newIndexRanges, newRanges...)
		}
	}
	return newIndexRanges, nil
}

func appendPoints2IndexRange(sctx sessionctx.Context, origin *Range, rangePoints []*point,
	ft *types.FieldType) ([]*Range, error) {
	newRanges := make([]*Range, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint, err := convertPoint(sctx, rangePoints[i], ft)
		if err != nil {
			return nil, errors.Trace(err)
		}
		endPoint, err := convertPoint(sctx, rangePoints[i+1], ft)
		if err != nil {
			return nil, errors.Trace(err)
		}
		less, err := validInterval(sctx, startPoint, endPoint)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !less {
			continue
		}

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

func appendRanges2PointRanges(pointRanges []*Range, ranges []*Range) []*Range {
	if len(ranges) == 0 {
		return pointRanges
	}
	newRanges := make([]*Range, 0, len(pointRanges)*len(ranges))
	for _, pointRange := range pointRanges {
		for _, r := range ranges {
			lowVal := append(pointRange.LowVal, r.LowVal...)
			highVal := append(pointRange.HighVal, r.HighVal...)
			collators := append(pointRange.Collators, r.Collators...)
			newRange := &Range{
				LowVal:      lowVal,
				LowExclude:  r.LowExclude,
				HighVal:     highVal,
				HighExclude: r.HighExclude,
				Collators:   collators,
			}
			newRanges = append(newRanges, newRange)
		}
	}
	return newRanges
}

// points2TableRanges build ranges for table scan from range points.
// It will remove the nil and convert MinNotNull and MaxValue to MinInt64 or MinUint64 and MaxInt64 or MaxUint64.
func points2TableRanges(sctx sessionctx.Context, rangePoints []*point, tp *types.FieldType) ([]*Range, error) {
	ranges := make([]*Range, 0, len(rangePoints)/2)
	var minValueDatum, maxValueDatum types.Datum
	// Currently, table's kv range cannot accept encoded value of MaxValueDatum. we need to convert it.
	if mysql.HasUnsignedFlag(tp.GetFlag()) {
		minValueDatum.SetUint64(0)
		maxValueDatum.SetUint64(math.MaxUint64)
	} else {
		minValueDatum.SetInt64(math.MinInt64)
		maxValueDatum.SetInt64(math.MaxInt64)
	}
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint, err := convertPoint(sctx, rangePoints[i], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if startPoint.value.Kind() == types.KindNull {
			startPoint.value = minValueDatum
			startPoint.excl = false
		} else if startPoint.value.Kind() == types.KindMinNotNull {
			startPoint.value = minValueDatum
		}
		endPoint, err := convertPoint(sctx, rangePoints[i+1], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if endPoint.value.Kind() == types.KindMaxValue {
			endPoint.value = maxValueDatum
		} else if endPoint.value.Kind() == types.KindNull {
			continue
		}
		less, err := validInterval(sctx, startPoint, endPoint)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !less {
			continue
		}
		ran := &Range{
			LowVal:      []types.Datum{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []types.Datum{endPoint.value},
			HighExclude: endPoint.excl,
			Collators:   []collate.Collator{collate.GetCollator(tp.GetCollate())},
		}
		ranges = append(ranges, ran)
	}
	return ranges, nil
}

// buildColumnRange builds range from CNF conditions.
func buildColumnRange(accessConditions []expression.Expression, sctx sessionctx.Context, tp *types.FieldType, tableRange bool, colLen int) (ranges []*Range, err error) {
	rb := builder{sc: sctx.GetSessionVars().StmtCtx}
	rangePoints := getFullRange()
	for _, cond := range accessConditions {
		collator := collate.GetCollator(tp.GetCollate())
		rangePoints = rb.intersection(rangePoints, rb.build(cond, collator), collator)
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
	}
	newTp := newFieldType(tp)
	if tableRange {
		ranges, err = points2TableRanges(sctx, rangePoints, newTp)
	} else {
		ranges, err = points2Ranges(sctx, rangePoints, newTp)
	}
	if err != nil {
		return nil, errors.Trace(err)
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
			return nil, err
		}
	}
	return ranges, nil
}

// BuildTableRange builds range of PK column for PhysicalTableScan.
func BuildTableRange(accessConditions []expression.Expression, sctx sessionctx.Context, tp *types.FieldType) ([]*Range, error) {
	return buildColumnRange(accessConditions, sctx, tp, true, types.UnspecifiedLength)
}

// BuildColumnRange builds range from access conditions for general columns.
func BuildColumnRange(conds []expression.Expression, sctx sessionctx.Context, tp *types.FieldType, colLen int) ([]*Range, error) {
	if len(conds) == 0 {
		return FullRange(), nil
	}
	return buildColumnRange(conds, sctx, tp, false, colLen)
}

// buildCNFIndexRange builds the range for index where the top layer is CNF.
func (d *rangeDetacher) buildCNFIndexRange(newTp []*types.FieldType,
	eqAndInCount int, accessCondition []expression.Expression) ([]*Range, error) {
	rb := builder{sc: d.sctx.GetSessionVars().StmtCtx}
	var (
		ranges []*Range
		err    error
	)
	for _, col := range d.cols {
		newTp = append(newTp, newFieldType(col.RetType))
	}
	for i := 0; i < eqAndInCount; i++ {
		// Build ranges for equal or in access conditions.
		point := rb.build(accessCondition[i], collate.GetCollator(newTp[i].GetCollate()))
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
		if i == 0 {
			ranges, err = points2Ranges(d.sctx, point, newTp[i])
		} else {
			ranges, err = appendPoints2Ranges(d.sctx, ranges, point, newTp[i])
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	rangePoints := getFullRange()
	// Build rangePoints for non-equal access conditions.
	for i := eqAndInCount; i < len(accessCondition); i++ {
		collator := collate.GetCollator(newTp[eqAndInCount].GetCollate())
		rangePoints = rb.intersection(rangePoints, rb.build(accessCondition[i], collator), collator)
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
	}
	if eqAndInCount == 0 {
		ranges, err = points2Ranges(d.sctx, rangePoints, newTp[0])
	} else if eqAndInCount < len(accessCondition) {
		ranges, err = appendPoints2Ranges(d.sctx, ranges, rangePoints, newTp[eqAndInCount])
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Take prefix index into consideration.
	if hasPrefix(d.lengths) {
		if fixPrefixColRange(ranges, d.lengths, newTp) {
			ranges, err = UnionRanges(d.sctx, ranges, d.mergeConsecutive)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	return ranges, nil
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
func UnionRanges(sctx sessionctx.Context, ranges []*Range, mergeConsecutive bool) ([]*Range, error) {
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
	sort.Slice(objects, func(i, j int) bool {
		return bytes.Compare(objects[i].encodedStart, objects[j].encodedStart) < 0
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
//      then we'll change it to (-inf -inf, a xxx], [a xxx, +inf +inf). You can see that this two interval intersect,
//      so we need a merge operation.
// Q: only checking the last column to decide whether the endpoint's exclude status needs to be reset is enough?
// A: Yes, suppose that the interval is (-inf -inf, a xxxxx b) and only the second column needs to be cut.
//    The result would be (-inf -inf, a xxx b) if the length of it is 3. Obviously we only need to care about the data
//    whose the first two key is `a` and `xxx`. It read all data whose index value begins with `a` and `xxx` and the third
//    value less than `b`, covering the values begin with `a` and `xxxxx` and the third value less than `b` perfectly.
//    So in this case we don't need to reset its exclude status. The right endpoint case can be proved in the same way.
func fixPrefixColRange(ranges []*Range, lengths []int, tp []*types.FieldType) bool {
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

// DetachCondAndBuildRangeForPartition will detach the index filters from table filters.
// The returned values are encapsulated into a struct DetachRangeResult, see its comments for explanation.
func DetachCondAndBuildRangeForPartition(sctx sessionctx.Context, conditions []expression.Expression, cols []*expression.Column,
	lengths []int) (*DetachRangeResult, error) {
	d := &rangeDetacher{
		sctx:             sctx,
		allConds:         conditions,
		cols:             cols,
		lengths:          lengths,
		mergeConsecutive: false,
	}
	return d.detachCondAndBuildRangeForCols()
}

// RangesToString print a list of Ranges into a string which can appear in an SQL as a condition.
func RangesToString(sc *stmtctx.StatementContext, rans []*Range, colNames []string) (string, error) {
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
