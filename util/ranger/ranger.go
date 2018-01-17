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
// See the License for the specific language governing permissions and
// limitations under the License.

package ranger

import (
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

// points2NewRanges build index ranges from range points.
// Only one column is built there. If there're multiple columns, use appendPoints2NewRanges.
func points2NewRanges(sc *stmtctx.StatementContext, rangePoints []point, tp *types.FieldType) ([]*NewRange, error) {
	ranges := make([]*NewRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint, err := convertPoint(sc, rangePoints[i], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		endPoint, err := convertPoint(sc, rangePoints[i+1], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		less, err := rangePointLess(sc, startPoint, endPoint)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !less {
			continue
		}
		// If column has not null flag, [null, null] should be removed.
		if mysql.HasNotNullFlag(tp.Flag) && endPoint.value.Kind() == types.KindNull {
			continue
		}
		ran := &NewRange{
			LowVal:      []types.Datum{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []types.Datum{endPoint.value},
			HighExclude: endPoint.excl,
		}
		ranges = append(ranges, ran)
	}
	return ranges, nil
}

func convertPoint(sc *stmtctx.StatementContext, point point, tp *types.FieldType) (point, error) {
	switch point.value.Kind() {
	case types.KindMaxValue, types.KindMinNotNull:
		return point, nil
	}
	casted, err := point.value.ConvertTo(sc, tp)
	if err != nil {
		return point, errors.Trace(err)
	}
	valCmpCasted, err := point.value.CompareDatum(sc, &casted)
	if err != nil {
		return point, errors.Trace(err)
	}
	point.value = casted
	if valCmpCasted == 0 {
		return point, nil
	}
	if point.start {
		if point.excl {
			if valCmpCasted < 0 {
				// e.g. "a > 1.9" convert to "a >= 2".
				point.excl = false
			}
		} else {
			if valCmpCasted > 0 {
				// e.g. "a >= 1.1 convert to "a > 1"
				point.excl = true
			}
		}
	} else {
		if point.excl {
			if valCmpCasted > 0 {
				// e.g. "a < 1.1" convert to "a <= 1"
				point.excl = false
			}
		} else {
			if valCmpCasted < 0 {
				// e.g. "a <= 1.9" convert to "a < 2"
				point.excl = true
			}
		}
	}
	return point, nil
}

// appendPoints2NewRanges appends additional column ranges for multi-column index.
// The additional column ranges can only be appended to point ranges.
// for example we have an index (a, b), if the condition is (a > 1 and b = 2)
// then we can not build a conjunctive ranges for this index.
func appendPoints2NewRanges(sc *stmtctx.StatementContext, origin []*NewRange, rangePoints []point,
	ft *types.FieldType) ([]*NewRange, error) {
	var newIndexRanges []*NewRange
	for i := 0; i < len(origin); i++ {
		oRange := origin[i]
		if !oRange.IsPoint(sc) {
			newIndexRanges = append(newIndexRanges, oRange)
		} else {
			newRanges, err := appendPoints2IndexRange(sc, oRange, rangePoints, ft)
			if err != nil {
				return nil, errors.Trace(err)
			}
			newIndexRanges = append(newIndexRanges, newRanges...)
		}
	}
	return newIndexRanges, nil
}

func appendPoints2IndexRange(sc *stmtctx.StatementContext, origin *NewRange, rangePoints []point,
	ft *types.FieldType) ([]*NewRange, error) {
	newRanges := make([]*NewRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint, err := convertPoint(sc, rangePoints[i], ft)
		if err != nil {
			return nil, errors.Trace(err)
		}
		endPoint, err := convertPoint(sc, rangePoints[i+1], ft)
		if err != nil {
			return nil, errors.Trace(err)
		}
		less, err := rangePointLess(sc, startPoint, endPoint)
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

		ir := &NewRange{
			LowVal:      lowVal,
			LowExclude:  startPoint.excl,
			HighVal:     highVal,
			HighExclude: endPoint.excl,
		}
		newRanges = append(newRanges, ir)
	}
	return newRanges, nil
}

// points2TableRanges build ranges for table scan from range points.
// It will remove the nil and convert MinNotNull and MaxValue to MinInt64 or MinUint64 and MaxInt64 or MaxUint64.
func points2TableRanges(sc *stmtctx.StatementContext, rangePoints []point, tp *types.FieldType) ([]*NewRange, error) {
	ranges := make([]*NewRange, 0, len(rangePoints)/2)
	var minValueDatum, maxValueDatum types.Datum
	// Currently, table's kv range cannot accept encoded value of MaxValueDatum. we need to convert it.
	if mysql.HasUnsignedFlag(tp.Flag) {
		minValueDatum.SetUint64(0)
		maxValueDatum.SetUint64(math.MaxUint64)
	} else {
		minValueDatum.SetInt64(math.MinInt64)
		maxValueDatum.SetInt64(math.MaxInt64)
	}
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint, err := convertPoint(sc, rangePoints[i], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if startPoint.value.Kind() == types.KindNull {
			startPoint.value = minValueDatum
			startPoint.excl = false
		} else if startPoint.value.Kind() == types.KindMinNotNull {
			startPoint.value = minValueDatum
		}
		endPoint, err := convertPoint(sc, rangePoints[i+1], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if endPoint.value.Kind() == types.KindMaxValue {
			endPoint.value = maxValueDatum
		} else if endPoint.value.Kind() == types.KindNull {
			continue
		}
		less, err := rangePointLess(sc, startPoint, endPoint)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !less {
			continue
		}
		ran := &NewRange{
			LowVal:      []types.Datum{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []types.Datum{endPoint.value},
			HighExclude: endPoint.excl,
		}
		ranges = append(ranges, ran)
	}
	return ranges, nil
}

// BuildTableRange will build range of pk for PhysicalTableScan
func BuildTableRange(accessConditions []expression.Expression, sc *stmtctx.StatementContext, tp *types.FieldType) ([]*NewRange, error) {
	rb := builder{sc: sc}
	rangePoints := fullRange
	for _, cond := range accessConditions {
		rangePoints = rb.intersection(rangePoints, rb.build(cond))
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
	}
	newTp := newFieldType(tp)
	ranges, err := points2TableRanges(sc, rangePoints, newTp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ranges, nil
}

// BuildColumnRange builds the range for sampling histogram to calculate the row count.
func BuildColumnRange(conds []expression.Expression, sc *stmtctx.StatementContext, tp *types.FieldType) ([]*NewRange, error) {
	if len(conds) == 0 {
		return []*NewRange{{LowVal: []types.Datum{{}}, HighVal: []types.Datum{types.MaxValueDatum()}}}, nil
	}

	rb := builder{sc: sc}
	rangePoints := fullRange
	for _, cond := range conds {
		rangePoints = rb.intersection(rangePoints, rb.build(cond))
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
	}
	newTp := newFieldType(tp)
	ranges, err := points2NewRanges(sc, rangePoints, newTp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ranges, nil
}

// BuildIndexRange builds the range for index.
func BuildIndexRange(sc *stmtctx.StatementContext, cols []*expression.Column, lengths []int,
	accessCondition []expression.Expression) ([]*NewRange, error) {
	rb := builder{sc: sc}
	var (
		ranges       []*NewRange
		eqAndInCount int
		err          error
	)
	newTp := make([]*types.FieldType, 0, len(cols))
	for _, col := range cols {
		newTp = append(newTp, newFieldType(col.RetType))
	}
	for eqAndInCount = 0; eqAndInCount < len(accessCondition) && eqAndInCount < len(cols); eqAndInCount++ {
		if sf, ok := accessCondition[eqAndInCount].(*expression.ScalarFunction); !ok || (sf.FuncName.L != ast.EQ && sf.FuncName.L != ast.In) {
			break
		}
		// Build ranges for equal or in access conditions.
		point := rb.build(accessCondition[eqAndInCount])
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
		if eqAndInCount == 0 {
			ranges, err = points2NewRanges(sc, point, newTp[eqAndInCount])
		} else {
			ranges, err = appendPoints2NewRanges(sc, ranges, point, newTp[eqAndInCount])
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	rangePoints := fullRange
	// Build rangePoints for non-equal access conditions.
	for i := eqAndInCount; i < len(accessCondition); i++ {
		rangePoints = rb.intersection(rangePoints, rb.build(accessCondition[i]))
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
	}
	if eqAndInCount == 0 {
		ranges, err = points2NewRanges(sc, rangePoints, newTp[0])
	} else if eqAndInCount < len(accessCondition) {
		ranges, err = appendPoints2NewRanges(sc, ranges, rangePoints, newTp[eqAndInCount])
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Take prefix index into consideration.
	if hasPrefix(lengths) {
		fixPrefixColRange(ranges, lengths)
	}

	if len(ranges) > 0 && len(ranges[0].LowVal) < len(cols) {
		for _, ran := range ranges {
			if ran.HighExclude || ran.LowExclude {
				if ran.HighExclude {
					ran.HighVal = append(ran.HighVal, types.NewDatum(nil))
				} else {
					ran.HighVal = append(ran.HighVal, types.MaxValueDatum())
				}
				if ran.LowExclude {
					ran.LowVal = append(ran.LowVal, types.MaxValueDatum())
				} else {
					ran.LowVal = append(ran.LowVal, types.NewDatum(nil))
				}
			}
		}
	}
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

func fixPrefixColRange(ranges []*NewRange, lengths []int) {
	for _, ran := range ranges {
		for i := 0; i < len(ran.LowVal); i++ {
			fixRangeDatum(&ran.LowVal[i], lengths[i])
		}
		ran.LowExclude = false
		for i := 0; i < len(ran.HighVal); i++ {
			fixRangeDatum(&ran.HighVal[i], lengths[i])
		}
		ran.HighExclude = false
	}
}

func fixRangeDatum(v *types.Datum, length int) {
	// If this column is prefix and the prefix length is smaller than the range, cut it.
	if length != types.UnspecifiedLength && length < len(v.GetBytes()) {
		v.SetBytes(v.GetBytes()[:length])
	}
}

// We cannot use the FieldType of column directly. e.g. the column a is int32 and we have a > 1111111111111111111.
// Obviously the constant is bigger than MaxInt32, so we will get overflow error if we use the FieldType of column a.
func newFieldType(tp *types.FieldType) *types.FieldType {
	switch tp.Tp {
	// To avoid overflow error.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		newTp := types.NewFieldType(mysql.TypeLonglong)
		newTp.Flag = tp.Flag
		return newTp
	// To avoid data truncate error.
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		return types.NewFieldType(tp.Tp)
	default:
		return tp
	}
}
