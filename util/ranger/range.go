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

// Point is the end point of range interval.
type Point struct {
	value types.Datum
	excl  bool // exclude
	start bool
}

func (rp Point) String() string {
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
	points []Point
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

func rangePointLess(sc *variable.StatementContext, a, b Point) (bool, error) {
	cmp, err := a.value.CompareDatum(sc, b.value)
	if cmp != 0 {
		return cmp < 0, nil
	}
	return rangePointEqualValueLess(a, b), errors.Trace(err)
}

func rangePointEqualValueLess(a, b Point) bool {
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

// Builder is the range builder struct.
type Builder struct {
	err error
	Sc  *variable.StatementContext
}

// BuildFromConds is used for test.
func (r *Builder) BuildFromConds(conds []expression.Expression) ([]Point, error) {
	result := FullRange
	for _, cond := range conds {
		result = r.intersection(result, r.build(expression.PushDownNot(cond, false, nil)))
	}
	return result, r.err
}

func (r *Builder) build(expr expression.Expression) []Point {
	switch x := expr.(type) {
	case *expression.Column:
		return r.buildFromColumn(x)
	case *expression.ScalarFunction:
		return r.buildFromScalarFunc(x)
	case *expression.Constant:
		return r.buildFromConstant(x)
	}

	return FullRange
}

func (r *Builder) buildFromConstant(expr *expression.Constant) []Point {
	if expr.Value.IsNull() {
		return nil
	}

	val, err := expr.Value.ToBool(r.Sc)
	if err != nil {
		r.err = err
		return nil
	}

	if val == 0 {
		return nil
	}
	return FullRange
}

func (r *Builder) buildFromColumn(expr *expression.Column) []Point {
	// column name expression is equivalent to column name is true.
	startPoint1 := Point{value: types.MinNotNullDatum(), start: true}
	endPoint1 := Point{excl: true}
	endPoint1.value.SetInt64(0)
	startPoint2 := Point{excl: true, start: true}
	startPoint2.value.SetInt64(0)
	endPoint2 := Point{value: types.MaxValueDatum()}
	return []Point{startPoint1, endPoint1, startPoint2, endPoint2}
}

func (r *Builder) buildFormBinOp(expr *expression.ScalarFunction) []Point {
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
		startPoint := Point{value: value, start: true}
		endPoint := Point{value: value}
		return []Point{startPoint, endPoint}
	case ast.NE:
		startPoint1 := Point{value: types.MinNotNullDatum(), start: true}
		endPoint1 := Point{value: value, excl: true}
		startPoint2 := Point{value: value, start: true, excl: true}
		endPoint2 := Point{value: types.MaxValueDatum()}
		return []Point{startPoint1, endPoint1, startPoint2, endPoint2}
	case ast.LT:
		startPoint := Point{value: types.MinNotNullDatum(), start: true}
		endPoint := Point{value: value, excl: true}
		return []Point{startPoint, endPoint}
	case ast.LE:
		startPoint := Point{value: types.MinNotNullDatum(), start: true}
		endPoint := Point{value: value}
		return []Point{startPoint, endPoint}
	case ast.GT:
		startPoint := Point{value: value, start: true, excl: true}
		endPoint := Point{value: types.MaxValueDatum()}
		return []Point{startPoint, endPoint}
	case ast.GE:
		startPoint := Point{value: value, start: true}
		endPoint := Point{value: types.MaxValueDatum()}
		return []Point{startPoint, endPoint}
	}
	return nil
}

func (r *Builder) buildFromIsTrue(expr *expression.ScalarFunction, isNot int) []Point {
	if isNot == 1 {
		// NOT TRUE range is {[null null] [0, 0]}
		startPoint1 := Point{start: true}
		endPoint1 := Point{}
		startPoint2 := Point{start: true}
		startPoint2.value.SetInt64(0)
		endPoint2 := Point{}
		endPoint2.value.SetInt64(0)
		return []Point{startPoint1, endPoint1, startPoint2, endPoint2}
	}
	// TRUE range is {[-inf 0) (0 +inf]}
	startPoint1 := Point{value: types.MinNotNullDatum(), start: true}
	endPoint1 := Point{excl: true}
	endPoint1.value.SetInt64(0)
	startPoint2 := Point{excl: true, start: true}
	startPoint2.value.SetInt64(0)
	endPoint2 := Point{value: types.MaxValueDatum()}
	return []Point{startPoint1, endPoint1, startPoint2, endPoint2}
}

func (r *Builder) buildFromIsFalse(expr *expression.ScalarFunction, isNot int) []Point {
	if isNot == 1 {
		// NOT FALSE range is {[-inf, 0), (0, +inf], [null, null]}
		startPoint1 := Point{start: true}
		endPoint1 := Point{excl: true}
		endPoint1.value.SetInt64(0)
		startPoint2 := Point{start: true, excl: true}
		startPoint2.value.SetInt64(0)
		endPoint2 := Point{value: types.MaxValueDatum()}
		return []Point{startPoint1, endPoint1, startPoint2, endPoint2}
	}
	// FALSE range is {[0, 0]}
	startPoint := Point{start: true}
	startPoint.value.SetInt64(0)
	endPoint := Point{}
	endPoint.value.SetInt64(0)
	return []Point{startPoint, endPoint}
}

func (r *Builder) newBuildFromIn(expr *expression.ScalarFunction) []Point {
	var rangePoints []Point
	list := expr.GetArgs()[1:]
	for _, e := range list {
		v, ok := e.(*expression.Constant)
		if !ok {
			r.err = ErrUnsupportedType.Gen("expr:%v is not constant", e)
			return FullRange
		}
		startPoint := Point{value: types.NewDatum(v.Value.GetValue()), start: true}
		endPoint := Point{value: types.NewDatum(v.Value.GetValue())}
		rangePoints = append(rangePoints, startPoint, endPoint)
	}
	sorter := pointSorter{points: rangePoints, sc: r.Sc}
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
	distinctRangePoints := make([]Point, 0, len(rangePoints))
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

func (r *Builder) newBuildFromPatternLike(expr *expression.ScalarFunction) []Point {
	pattern, err := expr.GetArgs()[1].(*expression.Constant).Value.ToString()
	if err != nil {
		r.err = errors.Trace(err)
		return FullRange
	}
	if pattern == "" {
		startPoint := Point{value: types.NewStringDatum(""), start: true}
		endPoint := Point{value: types.NewStringDatum("")}
		return []Point{startPoint, endPoint}
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
		return []Point{{value: types.MinNotNullDatum(), start: true}, {value: types.MaxValueDatum()}}
	}
	if isExactMatch {
		val := types.NewStringDatum(string(lowValue))
		return []Point{{value: val, start: true}, {value: val}}
	}
	startPoint := Point{start: true, excl: exclude}
	startPoint.value.SetBytesAsString(lowValue)
	highValue := make([]byte, len(lowValue))
	copy(highValue, lowValue)
	endPoint := Point{excl: true}
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
	return []Point{startPoint, endPoint}
}

func (r *Builder) buildFromNot(expr *expression.ScalarFunction) []Point {
	switch n := expr.FuncName.L; n {
	case ast.IsTruth:
		return r.buildFromIsTrue(expr, 1)
	case ast.IsFalsity:
		return r.buildFromIsFalse(expr, 1)
	case ast.In:
		// Pattern not in is not supported.
		r.err = ErrUnsupportedType.Gen("NOT IN is not supported")
		return FullRange
	case ast.Like:
		// Pattern not like is not supported.
		r.err = ErrUnsupportedType.Gen("NOT LIKE is not supported.")
		return FullRange
	case ast.IsNull:
		startPoint := Point{value: types.MinNotNullDatum(), start: true}
		endPoint := Point{value: types.MaxValueDatum()}
		return []Point{startPoint, endPoint}
	}
	return nil
}

func (r *Builder) buildFromScalarFunc(expr *expression.ScalarFunction) []Point {
	switch op := expr.FuncName.L; op {
	case ast.GE, ast.GT, ast.LT, ast.LE, ast.EQ, ast.NE:
		return r.buildFormBinOp(expr)
	case ast.AndAnd:
		return r.intersection(r.build(expr.GetArgs()[0]), r.build(expr.GetArgs()[1]))
	case ast.OrOr:
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
		startPoint := Point{start: true}
		endPoint := Point{}
		return []Point{startPoint, endPoint}
	case ast.UnaryNot:
		return r.buildFromNot(expr.GetArgs()[0].(*expression.ScalarFunction))
	}

	return nil
}

func (r *Builder) intersection(a, b []Point) []Point {
	return r.merge(a, b, false)
}

func (r *Builder) union(a, b []Point) []Point {
	return r.merge(a, b, true)
}

func (r *Builder) merge(a, b []Point, union bool) []Point {
	sorter := pointSorter{points: append(a, b...), sc: r.Sc}
	sort.Sort(&sorter)
	if sorter.err != nil {
		r.err = sorter.err
		return nil
	}
	var (
		merged               []Point
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

// BuildIndexRanges build index ranges from range points.
// Only the first column in the index is built, extra column ranges will be appended by
// appendIndexRanges.
func (r *Builder) BuildIndexRanges(rangePoints []Point, tp *types.FieldType) []*types.IndexRange {
	indexRanges := make([]*types.IndexRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := r.convertPoint(rangePoints[i], tp)
		endPoint := r.convertPoint(rangePoints[i+1], tp)
		less, err := rangePointLess(r.Sc, startPoint, endPoint)
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

func (r *Builder) convertPoint(point Point, tp *types.FieldType) Point {
	switch point.value.Kind() {
	case types.KindMaxValue, types.KindMinNotNull:
		return point
	}
	casted, err := point.value.ConvertTo(r.Sc, tp)
	if err != nil {
		r.err = errors.Trace(err)
	}
	valCmpCasted, err := point.value.CompareDatum(r.Sc, casted)
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
func (r *Builder) appendIndexRanges(origin []*types.IndexRange, rangePoints []Point, ft *types.FieldType) []*types.IndexRange {
	var newIndexRanges []*types.IndexRange
	for i := 0; i < len(origin); i++ {
		oRange := origin[i]
		if !oRange.IsPoint(r.Sc) {
			newIndexRanges = append(newIndexRanges, oRange)
		} else {
			newIndexRanges = append(newIndexRanges, r.appendIndexRange(oRange, rangePoints, ft)...)
		}
	}
	return newIndexRanges
}

func (r *Builder) appendIndexRange(origin *types.IndexRange, rangePoints []Point, ft *types.FieldType) []*types.IndexRange {
	newRanges := make([]*types.IndexRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := r.convertPoint(rangePoints[i], ft)
		endPoint := r.convertPoint(rangePoints[i+1], ft)
		less, err := rangePointLess(r.Sc, startPoint, endPoint)
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

// BuildTableRanges will construct the range slice with the given range points
func (r *Builder) BuildTableRanges(rangePoints []Point) []types.IntColumnRange {
	tableRanges := make([]types.IntColumnRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := rangePoints[i]
		if startPoint.value.IsNull() || startPoint.value.Kind() == types.KindMinNotNull {
			startPoint.value.SetInt64(math.MinInt64)
		}
		startInt, err := startPoint.value.ToInt64(r.Sc)
		if err != nil {
			r.err = errors.Trace(err)
			return tableRanges
		}
		startDatum := types.NewDatum(startInt)
		cmp, err := startDatum.CompareDatum(r.Sc, startPoint.value)
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
		endInt, err := endPoint.value.ToInt64(r.Sc)
		if err != nil {
			r.err = errors.Trace(err)
			return tableRanges
		}
		endDatum := types.NewDatum(endInt)
		cmp, err = endDatum.CompareDatum(r.Sc, endPoint.value)
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
