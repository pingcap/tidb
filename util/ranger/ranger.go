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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

// points2IndexRanges build index ranges from range points.
// Only the first column in the index is built, extra column ranges will be appended by
// appendPoints2IndexRanges.
func points2IndexRanges(sc *stmtctx.StatementContext, rangePoints []point, tp *types.FieldType) ([]*IndexRange, error) {
	indexRanges := make([]*IndexRange, 0, len(rangePoints)/2)
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
		ir := &IndexRange{
			LowVal:      []types.Datum{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []types.Datum{endPoint.value},
			HighExclude: endPoint.excl,
		}
		indexRanges = append(indexRanges, ir)
	}
	return indexRanges, nil
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

// appendPoints2IndexRanges appends additional column ranges for multi-column index.
// The additional column ranges can only be appended to point ranges.
// for example we have an index (a, b), if the condition is (a > 1 and b = 2)
// then we can not build a conjunctive ranges for this index.
func appendPoints2IndexRanges(sc *stmtctx.StatementContext, origin []*IndexRange, rangePoints []point,
	ft *types.FieldType) ([]*IndexRange, error) {
	var newIndexRanges []*IndexRange
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

func appendPoints2IndexRange(sc *stmtctx.StatementContext, origin *IndexRange, rangePoints []point,
	ft *types.FieldType) ([]*IndexRange, error) {
	newRanges := make([]*IndexRange, 0, len(rangePoints)/2)
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

		ir := &IndexRange{
			LowVal:      lowVal,
			LowExclude:  startPoint.excl,
			HighVal:     highVal,
			HighExclude: endPoint.excl,
		}
		newRanges = append(newRanges, ir)
	}
	return newRanges, nil
}

// points2TableRanges will construct the range slice with the given range points
func points2TableRanges(sc *stmtctx.StatementContext, rangePoints []point) ([]IntColumnRange, error) {
	tableRanges := make([]IntColumnRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := rangePoints[i]
		if startPoint.value.IsNull() || startPoint.value.Kind() == types.KindMinNotNull {
			if startPoint.value.IsNull() {
				startPoint.excl = false
			}
			startPoint.value.SetInt64(math.MinInt64)
		}
		startInt, err := startPoint.value.ToInt64(sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		startDatum := types.NewDatum(startInt)
		cmp, err := startDatum.CompareDatum(sc, &startPoint.value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if cmp < 0 || (cmp == 0 && startPoint.excl) {
			if startInt == math.MaxInt64 {
				continue
			}
			startInt++
		}
		endPoint := rangePoints[i+1]
		if endPoint.value.IsNull() {
			continue
		} else if endPoint.value.Kind() == types.KindMaxValue {
			endPoint.value.SetInt64(math.MaxInt64)
		}
		endInt, err := endPoint.value.ToInt64(sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		endDatum := types.NewDatum(endInt)
		cmp, err = endDatum.CompareDatum(sc, &endPoint.value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if cmp > 0 || (cmp == 0 && endPoint.excl) {
			if endInt == math.MinInt64 {
				continue
			}
			endInt--
		}
		if startInt > endInt {
			continue
		}
		tableRanges = append(tableRanges, IntColumnRange{LowVal: startInt, HighVal: endInt})
	}
	return tableRanges, nil
}

func points2ColumnRanges(sc *stmtctx.StatementContext, points []point, tp *types.FieldType) ([]*ColumnRange, error) {
	columnRanges := make([]*ColumnRange, 0, len(points)/2)
	for i := 0; i < len(points); i += 2 {
		startPoint, err := convertPoint(sc, points[i], tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		endPoint, err := convertPoint(sc, points[i+1], tp)
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
		cr := &ColumnRange{
			Low:      startPoint.value,
			LowExcl:  startPoint.excl,
			High:     endPoint.value,
			HighExcl: endPoint.excl,
		}
		columnRanges = append(columnRanges, cr)
	}
	return columnRanges, nil
}

// buildTableRange will build range of pk for PhysicalTableScan
func buildTableRange(accessConditions []expression.Expression, sc *stmtctx.StatementContext) ([]IntColumnRange, error) {
	if len(accessConditions) == 0 {
		return FullIntRange(), nil
	}

	rb := builder{sc: sc}
	rangePoints := fullRange
	for _, cond := range accessConditions {
		rangePoints = rb.intersection(rangePoints, rb.build(cond))
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
	}
	ranges, err := points2TableRanges(sc, rangePoints)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ranges, nil
}

// buildColumnRange builds the range for sampling histogram to calculate the row count.
func buildColumnRange(conds []expression.Expression, sc *stmtctx.StatementContext, tp *types.FieldType) ([]*ColumnRange, error) {
	if len(conds) == 0 {
		return []*ColumnRange{{Low: types.Datum{}, High: types.MaxValueDatum()}}, nil
	}

	rb := builder{sc: sc}
	rangePoints := fullRange
	for _, cond := range conds {
		rangePoints = rb.intersection(rangePoints, rb.build(cond))
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
	}
	ranges, err := points2ColumnRanges(sc, rangePoints, tp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ranges, nil
}

func buildIndexRange(sc *stmtctx.StatementContext, cols []*expression.Column, lengths []int,
	accessCondition []expression.Expression) ([]*IndexRange, error) {
	rb := builder{sc: sc}
	var (
		ranges       []*IndexRange
		eqAndInCount int
		err          error
	)
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
			ranges, err = points2IndexRanges(sc, point, cols[eqAndInCount].RetType)
		} else {
			ranges, err = appendPoints2IndexRanges(sc, ranges, point, cols[eqAndInCount].RetType)
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
		ranges, err = points2IndexRanges(sc, rangePoints, cols[0].RetType)
	} else if eqAndInCount < len(accessCondition) {
		ranges, err = appendPoints2IndexRanges(sc, ranges, rangePoints, cols[eqAndInCount].RetType)
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

func fixPrefixColRange(ranges []*IndexRange, lengths []int) {
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

// BuildRange is a method which can calculate IntColumnRange, ColumnRange, IndexRange.
func BuildRange(sc *stmtctx.StatementContext, conds []expression.Expression, rangeType int, cols []*expression.Column,
	lengths []int) (retRanges []Range, _ error) {
	if rangeType == IntRangeType {
		ranges, err := buildTableRange(conds, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		retRanges = make([]Range, 0, len(ranges))
		for _, ran := range ranges {
			retRanges = append(retRanges, ran)
		}
	} else if rangeType == ColumnRangeType {
		ranges, err := buildColumnRange(conds, sc, cols[0].RetType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		retRanges = make([]Range, 0, len(ranges))
		for _, ran := range ranges {
			retRanges = append(retRanges, ran)
		}
	} else if rangeType == IndexRangeType {
		ranges, err := buildIndexRange(sc, cols, lengths, conds)
		if err != nil {
			return nil, errors.Trace(err)
		}
		retRanges = make([]Range, 0, len(ranges))
		for _, ran := range ranges {
			retRanges = append(retRanges, ran)
		}
	}
	return
}

// Ranges2IntRanges changes []Range to []IntColumnRange
func Ranges2IntRanges(ranges []Range) []IntColumnRange {
	retRanges := make([]IntColumnRange, 0, len(ranges))
	for _, ran := range ranges {
		retRanges = append(retRanges, ran.Convert2IntRange())
	}
	return retRanges
}

// Ranges2ColumnRanges changes []Range to []*ColumnRange
func Ranges2ColumnRanges(ranges []Range) []*ColumnRange {
	retRanges := make([]*ColumnRange, 0, len(ranges))
	for _, ran := range ranges {
		retRanges = append(retRanges, ran.Convert2ColumnRange())
	}
	return retRanges
}

// Ranges2IndexRanges changes []Range to []*IndexRange
func Ranges2IndexRanges(ranges []Range) []*IndexRange {
	retRanges := make([]*IndexRange, 0, len(ranges))
	for _, ran := range ranges {
		retRanges = append(retRanges, ran.Convert2IndexRange())
	}
	return retRanges
}
