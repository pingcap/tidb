// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"math"
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

// Error instances.
var (
	ErrUnsupportedType = terror.ClassOptimizerPlan.New(CodeUnsupportedType, "Unsupported type")
)

// Error codes.
const (
	CodeUnsupportedType terror.ErrCode = 1
)

// Range type.
const (
	IntRangeType = iota
	ColumnRangeType
	IndexRangeType
)

// Point is the end point of range interval.
type point struct {
	value types.Datum
	excl  bool // exclude
	start bool
}

func (rp point) String() string {
	val := rp.value.GetValue()
	if rp.value.Kind() == types.KindMinNotNull {
		val = "-inf"
	} else if rp.value.Kind() == types.KindMaxValue {
		val = "+inf"
	}
	if rp.start {
		symbol := "["
		if rp.excl {
			symbol = "("
		}
		return fmt.Sprintf("%s%v", symbol, val)
	}
	symbol := "]"
	if rp.excl {
		symbol = ")"
	}
	return fmt.Sprintf("%v%s", val, symbol)
}

type pointSorter struct {
	points []point
	err    error
	sc     *variable.StatementContext
}

func (r *pointSorter) Len() int {
	return len(r.points)
}

func (r *pointSorter) Less(i, j int) bool {
	a := r.points[i]
	b := r.points[j]
	less, err := rangePointLess(r.sc, a, b)
	if err != nil {
		r.err = err
	}
	return less
}

func rangePointLess(sc *variable.StatementContext, a, b point) (bool, error) {
	cmp, err := a.value.CompareDatum(sc, b.value)
	if cmp != 0 {
		return cmp < 0, nil
	}
	return rangePointEqualValueLess(a, b), errors.Trace(err)
}

func rangePointEqualValueLess(a, b point) bool {
	if a.start && b.start {
		return !a.excl && b.excl
	} else if a.start {
		return !a.excl && !b.excl
	} else if b.start {
		return a.excl || b.excl
	}
	return a.excl && !b.excl
}

func (r *pointSorter) Swap(i, j int) {
	r.points[i], r.points[j] = r.points[j], r.points[i]
}

// builder is the range builder struct.
type builder struct {
	err error
	sc  *variable.StatementContext
}

func (r *builder) build(expr expression.Expression) []point {
	switch x := expr.(type) {
	case *expression.Column:
		return r.buildFromColumn(x)
	case *expression.ScalarFunction:
		return r.buildFromScalarFunc(x)
	case *expression.Constant:
		return r.buildFromConstant(x)
	}

	return fullRange
}

func (r *builder) buildFromConstant(expr *expression.Constant) []point {
	if expr.Value.IsNull() {
		return nil
	}

	val, err := expr.Value.ToBool(r.sc)
	if err != nil {
		r.err = err
		return nil
	}

	if val == 0 {
		return nil
	}
	return fullRange
}

func (r *builder) buildFromColumn(expr *expression.Column) []point {
	// column name expression is equivalent to column name is true.
	startPoint1 := point{value: types.MinNotNullDatum(), start: true}
	endPoint1 := point{excl: true}
	endPoint1.value.SetInt64(0)
	startPoint2 := point{excl: true, start: true}
	startPoint2.value.SetInt64(0)
	endPoint2 := point{value: types.MaxValueDatum()}
	return []point{startPoint1, endPoint1, startPoint2, endPoint2}
}

func (r *builder) buildFormBinOp(expr *expression.ScalarFunction) []point {
	// This has been checked that the binary operation is comparison operation, and one of
	// the operand is column name expression.
	var value types.Datum
	var op string
	if v, ok := expr.GetArgs()[0].(*expression.Constant); ok {
		value = v.Value
		switch expr.FuncName.L {
		case ast.GE:
			op = ast.LE
		case ast.GT:
			op = ast.LT
		case ast.LT:
			op = ast.GT
		case ast.LE:
			op = ast.GE
		default:
			op = expr.FuncName.L
		}
	} else {
		value = expr.GetArgs()[1].(*expression.Constant).Value
		op = expr.FuncName.L
	}
	if value.IsNull() {
		return nil
	}

	switch op {
	case ast.EQ:
		startPoint := point{value: value, start: true}
		endPoint := point{value: value}
		return []point{startPoint, endPoint}
	case ast.NE:
		startPoint1 := point{value: types.MinNotNullDatum(), start: true}
		endPoint1 := point{value: value, excl: true}
		startPoint2 := point{value: value, start: true, excl: true}
		endPoint2 := point{value: types.MaxValueDatum()}
		return []point{startPoint1, endPoint1, startPoint2, endPoint2}
	case ast.LT:
		startPoint := point{value: types.MinNotNullDatum(), start: true}
		endPoint := point{value: value, excl: true}
		return []point{startPoint, endPoint}
	case ast.LE:
		startPoint := point{value: types.MinNotNullDatum(), start: true}
		endPoint := point{value: value}
		return []point{startPoint, endPoint}
	case ast.GT:
		startPoint := point{value: value, start: true, excl: true}
		endPoint := point{value: types.MaxValueDatum()}
		return []point{startPoint, endPoint}
	case ast.GE:
		startPoint := point{value: value, start: true}
		endPoint := point{value: types.MaxValueDatum()}
		return []point{startPoint, endPoint}
	}
	return nil
}

func (r *builder) buildFromIsTrue(expr *expression.ScalarFunction, isNot int) []point {
	if isNot == 1 {
		// NOT TRUE range is {[null null] [0, 0]}
		startPoint1 := point{start: true}
		endPoint1 := point{}
		startPoint2 := point{start: true}
		startPoint2.value.SetInt64(0)
		endPoint2 := point{}
		endPoint2.value.SetInt64(0)
		return []point{startPoint1, endPoint1, startPoint2, endPoint2}
	}
	// TRUE range is {[-inf 0) (0 +inf]}
	startPoint1 := point{value: types.MinNotNullDatum(), start: true}
	endPoint1 := point{excl: true}
	endPoint1.value.SetInt64(0)
	startPoint2 := point{excl: true, start: true}
	startPoint2.value.SetInt64(0)
	endPoint2 := point{value: types.MaxValueDatum()}
	return []point{startPoint1, endPoint1, startPoint2, endPoint2}
}

func (r *builder) buildFromIsFalse(expr *expression.ScalarFunction, isNot int) []point {
	if isNot == 1 {
		// NOT FALSE range is {[-inf, 0), (0, +inf], [null, null]}
		startPoint1 := point{start: true}
		endPoint1 := point{excl: true}
		endPoint1.value.SetInt64(0)
		startPoint2 := point{start: true, excl: true}
		startPoint2.value.SetInt64(0)
		endPoint2 := point{value: types.MaxValueDatum()}
		return []point{startPoint1, endPoint1, startPoint2, endPoint2}
	}
	// FALSE range is {[0, 0]}
	startPoint := point{start: true}
	startPoint.value.SetInt64(0)
	endPoint := point{}
	endPoint.value.SetInt64(0)
	return []point{startPoint, endPoint}
}

func (r *builder) newBuildFromIn(expr *expression.ScalarFunction) []point {
	var rangePoints []point
	list := expr.GetArgs()[1:]
	for _, e := range list {
		v, ok := e.(*expression.Constant)
		if !ok {
			r.err = ErrUnsupportedType.Gen("expr:%v is not constant", e)
			return fullRange
		}
		startPoint := point{value: types.NewDatum(v.Value.GetValue()), start: true}
		endPoint := point{value: types.NewDatum(v.Value.GetValue())}
		rangePoints = append(rangePoints, startPoint, endPoint)
	}
	sorter := pointSorter{points: rangePoints, sc: r.sc}
	sort.Sort(&sorter)
	if sorter.err != nil {
		r.err = sorter.err
	}
	// check duplicates
	hasDuplicate := false
	isStart := false
	for _, v := range rangePoints {
		if isStart == v.start {
			hasDuplicate = true
			break
		}
		isStart = v.start
	}
	if !hasDuplicate {
		return rangePoints
	}
	// remove duplicates
	distinctRangePoints := make([]point, 0, len(rangePoints))
	isStart = false
	for i := 0; i < len(rangePoints); i++ {
		current := rangePoints[i]
		if isStart == current.start {
			continue
		}
		distinctRangePoints = append(distinctRangePoints, current)
		isStart = current.start
	}
	return distinctRangePoints
}

func (r *builder) newBuildFromPatternLike(expr *expression.ScalarFunction) []point {
	pattern, err := expr.GetArgs()[1].(*expression.Constant).Value.ToString()
	if err != nil {
		r.err = errors.Trace(err)
		return fullRange
	}
	if pattern == "" {
		startPoint := point{value: types.NewStringDatum(""), start: true}
		endPoint := point{value: types.NewStringDatum("")}
		return []point{startPoint, endPoint}
	}
	lowValue := make([]byte, 0, len(pattern))
	escape := byte(expr.GetArgs()[2].(*expression.Constant).Value.GetInt64())
	var exclude bool
	isExactMatch := true
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == escape {
			i++
			if i < len(pattern) {
				lowValue = append(lowValue, pattern[i])
			} else {
				lowValue = append(lowValue, escape)
			}
			continue
		}
		if pattern[i] == '%' {
			// Get the prefix.
			isExactMatch = false
			break
		} else if pattern[i] == '_' {
			// Get the prefix, but exclude the prefix.
			// e.g., "abc_x", the start point exclude "abc",
			// because the string length is more than 3.
			exclude = true
			isExactMatch = false
			break
		}
		lowValue = append(lowValue, pattern[i])
	}
	if len(lowValue) == 0 {
		return []point{{value: types.MinNotNullDatum(), start: true}, {value: types.MaxValueDatum()}}
	}
	if isExactMatch {
		val := types.NewStringDatum(string(lowValue))
		return []point{{value: val, start: true}, {value: val}}
	}
	startPoint := point{start: true, excl: exclude}
	startPoint.value.SetBytesAsString(lowValue)
	highValue := make([]byte, len(lowValue))
	copy(highValue, lowValue)
	endPoint := point{excl: true}
	for i := len(highValue) - 1; i >= 0; i-- {
		// Make the end point value more than the start point value,
		// and the length of the end point value is the same as the length of the start point value.
		// e.g., the start point value is "abc", so the end point value is "abd".
		highValue[i]++
		if highValue[i] != 0 {
			endPoint.value.SetBytesAsString(highValue)
			break
		}
		// If highValue[i] is 255 and highValue[i]++ is 0, then the end point value is max value.
		if i == 0 {
			endPoint.value = types.MaxValueDatum()
		}
	}
	return []point{startPoint, endPoint}
}

func (r *builder) buildFromNot(expr *expression.ScalarFunction) []point {
	switch n := expr.FuncName.L; n {
	case ast.IsTruth:
		return r.buildFromIsTrue(expr, 1)
	case ast.IsFalsity:
		return r.buildFromIsFalse(expr, 1)
	case ast.In:
		// Pattern not in is not supported.
		r.err = ErrUnsupportedType.Gen("NOT IN is not supported")
		return fullRange
	case ast.Like:
		// Pattern not like is not supported.
		r.err = ErrUnsupportedType.Gen("NOT LIKE is not supported.")
		return fullRange
	case ast.IsNull:
		startPoint := point{value: types.MinNotNullDatum(), start: true}
		endPoint := point{value: types.MaxValueDatum()}
		return []point{startPoint, endPoint}
	}
	return nil
}

func (r *builder) buildFromScalarFunc(expr *expression.ScalarFunction) []point {
	switch op := expr.FuncName.L; op {
	case ast.GE, ast.GT, ast.LT, ast.LE, ast.EQ, ast.NE:
		return r.buildFormBinOp(expr)
	case ast.LogicAnd:
		return r.intersection(r.build(expr.GetArgs()[0]), r.build(expr.GetArgs()[1]))
	case ast.LogicOr:
		return r.union(r.build(expr.GetArgs()[0]), r.build(expr.GetArgs()[1]))
	case ast.IsTruth:
		return r.buildFromIsTrue(expr, 0)
	case ast.IsFalsity:
		return r.buildFromIsFalse(expr, 0)
	case ast.In:
		return r.newBuildFromIn(expr)
	case ast.Like:
		return r.newBuildFromPatternLike(expr)
	case ast.IsNull:
		startPoint := point{start: true}
		endPoint := point{}
		return []point{startPoint, endPoint}
	case ast.UnaryNot:
		return r.buildFromNot(expr.GetArgs()[0].(*expression.ScalarFunction))
	}

	return nil
}

func (r *builder) intersection(a, b []point) []point {
	return r.merge(a, b, false)
}

func (r *builder) union(a, b []point) []point {
	return r.merge(a, b, true)
}

func (r *builder) merge(a, b []point, union bool) []point {
	sorter := pointSorter{points: append(a, b...), sc: r.sc}
	sort.Sort(&sorter)
	if sorter.err != nil {
		r.err = sorter.err
		return nil
	}
	var (
		merged               []point
		inRangeCount         int
		requiredInRangeCount int
	)
	if union {
		requiredInRangeCount = 1
	} else {
		requiredInRangeCount = 2
	}
	for _, val := range sorter.points {
		if val.start {
			inRangeCount++
			if inRangeCount == requiredInRangeCount {
				// just reached the required in range count, a new range started.
				merged = append(merged, val)
			}
		} else {
			if inRangeCount == requiredInRangeCount {
				// just about to leave the required in range count, the range is ended.
				merged = append(merged, val)
			}
			inRangeCount--
		}
	}
	return merged
}

// buildIndexRanges build index ranges from range points.
// Only the first column in the index is built, extra column ranges will be appended by
// appendIndexRanges.
func (r *builder) buildIndexRanges(rangePoints []point, tp *types.FieldType) []*types.IndexRange {
	indexRanges := make([]*types.IndexRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := r.convertPoint(rangePoints[i], tp)
		endPoint := r.convertPoint(rangePoints[i+1], tp)
		less, err := rangePointLess(r.sc, startPoint, endPoint)
		if err != nil {
			r.err = errors.Trace(err)
		}
		if !less {
			continue
		}
		ir := &types.IndexRange{
			LowVal:      []types.Datum{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []types.Datum{endPoint.value},
			HighExclude: endPoint.excl,
		}
		indexRanges = append(indexRanges, ir)
	}
	return indexRanges
}

func (r *builder) convertPoint(point point, tp *types.FieldType) point {
	switch point.value.Kind() {
	case types.KindMaxValue, types.KindMinNotNull:
		return point
	}
	casted, err := point.value.ConvertTo(r.sc, tp)
	if err != nil {
		r.err = errors.Trace(err)
	}
	valCmpCasted, err := point.value.CompareDatum(r.sc, casted)
	if err != nil {
		r.err = errors.Trace(err)
	}
	point.value = casted
	if valCmpCasted == 0 {
		return point
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
	return point
}

// appendIndexRanges appends additional column ranges for multi-column index.
// The additional column ranges can only be appended to point ranges.
// for example we have an index (a, b), if the condition is (a > 1 and b = 2)
// then we can not build a conjunctive ranges for this index.
func (r *builder) appendIndexRanges(origin []*types.IndexRange, rangePoints []point, ft *types.FieldType) []*types.IndexRange {
	var newIndexRanges []*types.IndexRange
	for i := 0; i < len(origin); i++ {
		oRange := origin[i]
		if !oRange.IsPoint(r.sc) {
			newIndexRanges = append(newIndexRanges, oRange)
		} else {
			newIndexRanges = append(newIndexRanges, r.appendIndexRange(oRange, rangePoints, ft)...)
		}
	}
	return newIndexRanges
}

func (r *builder) appendIndexRange(origin *types.IndexRange, rangePoints []point, ft *types.FieldType) []*types.IndexRange {
	newRanges := make([]*types.IndexRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := r.convertPoint(rangePoints[i], ft)
		endPoint := r.convertPoint(rangePoints[i+1], ft)
		less, err := rangePointLess(r.sc, startPoint, endPoint)
		if err != nil {
			r.err = errors.Trace(err)
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

		ir := &types.IndexRange{
			LowVal:      lowVal,
			LowExclude:  startPoint.excl,
			HighVal:     highVal,
			HighExclude: endPoint.excl,
		}
		newRanges = append(newRanges, ir)
	}
	return newRanges
}

// buildTableRanges will construct the range slice with the given range points
func (r *builder) buildTableRanges(rangePoints []point) []types.IntColumnRange {
	tableRanges := make([]types.IntColumnRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := rangePoints[i]
		if startPoint.value.IsNull() || startPoint.value.Kind() == types.KindMinNotNull {
			startPoint.value.SetInt64(math.MinInt64)
		}
		startInt, err := startPoint.value.ToInt64(r.sc)
		if err != nil {
			r.err = errors.Trace(err)
			return tableRanges
		}
		startDatum := types.NewDatum(startInt)
		cmp, err := startDatum.CompareDatum(r.sc, startPoint.value)
		if err != nil {
			r.err = errors.Trace(err)
			return tableRanges
		}
		if cmp < 0 || (cmp == 0 && startPoint.excl) {
			startInt++
		}
		endPoint := rangePoints[i+1]
		if endPoint.value.IsNull() {
			endPoint.value.SetInt64(math.MinInt64)
		} else if endPoint.value.Kind() == types.KindMaxValue {
			endPoint.value.SetInt64(math.MaxInt64)
		}
		endInt, err := endPoint.value.ToInt64(r.sc)
		if err != nil {
			r.err = errors.Trace(err)
			return tableRanges
		}
		endDatum := types.NewDatum(endInt)
		cmp, err = endDatum.CompareDatum(r.sc, endPoint.value)
		if err != nil {
			r.err = errors.Trace(err)
			return tableRanges
		}
		if cmp > 0 || (cmp == 0 && endPoint.excl) {
			endInt--
		}
		if startInt > endInt {
			continue
		}
		tableRanges = append(tableRanges, types.IntColumnRange{LowVal: startInt, HighVal: endInt})
	}
	return tableRanges
}

func (r *builder) buildColumnRanges(points []point, tp *types.FieldType) []*types.ColumnRange {
	columnRanges := make([]*types.ColumnRange, 0, len(points)/2)
	for i := 0; i < len(points); i += 2 {
		startPoint := r.convertPoint(points[i], tp)
		endPoint := r.convertPoint(points[i+1], tp)
		less, err := rangePointLess(r.sc, startPoint, endPoint)
		if err != nil {
			r.err = errors.Trace(err)
			return nil
		}
		if !less {
			continue
		}
		cr := &types.ColumnRange{
			Low:      startPoint.value,
			LowExcl:  startPoint.excl,
			High:     endPoint.value,
			HighExcl: endPoint.excl,
		}
		columnRanges = append(columnRanges, cr)
	}
	return columnRanges
}
