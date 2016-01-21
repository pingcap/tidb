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

package plan

import (
	"fmt"
	"math"
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

type rangePoint struct {
	value interface{}
	excl  bool // exclude
	start bool
}

func (rp rangePoint) String() string {
	if rp.start {
		symbol := "["
		if rp.excl {
			symbol = "("
		}
		return fmt.Sprintf("%s%v", symbol, rp.value)
	}
	symbol := "]"
	if rp.excl {
		symbol = ")"
	}
	return fmt.Sprintf("%v%s", rp.value, symbol)
}

type rangePointSorter struct {
	points []rangePoint
	err    error
}

func (r *rangePointSorter) Len() int {
	return len(r.points)
}

func (r *rangePointSorter) Less(i, j int) bool {
	a := r.points[i]
	b := r.points[j]
	if a.value == nil && b.value == nil {
		return r.equalValueLess(a, b)
	} else if b.value == nil {
		return false
	} else if a.value == nil {
		return true
	}

	// a and b both not nil
	if a.value == MinNotNullVal && b.value == MinNotNullVal {
		return r.equalValueLess(a, b)
	} else if b.value == MinNotNullVal {
		return false
	} else if a.value == MinNotNullVal {
		return true
	}

	// a and b both not min value
	if a.value == MaxVal && b.value == MaxVal {
		return r.equalValueLess(a, b)
	} else if a.value == MaxVal {
		return false
	} else if b.value == MaxVal {
		return true
	}

	n, err := types.Compare(a.value, b.value)
	if err != nil {
		r.err = err
		return true
	}
	if n == 0 {
		return r.equalValueLess(a, b)
	}
	return n < 0
}

func (r *rangePointSorter) equalValueLess(a, b rangePoint) bool {
	if a.start && b.start {
		return !a.excl && b.excl
	} else if a.start {
		return !b.excl
	} else if b.start {
		return a.excl || b.excl
	}
	return a.excl && !b.excl
}

func (r *rangePointSorter) Swap(i, j int) {
	r.points[i], r.points[j] = r.points[j], r.points[i]
}

type rangeBuilder struct {
	err error
}

func (r *rangeBuilder) build(expr ast.ExprNode) []rangePoint {
	switch x := expr.(type) {
	case *ast.BinaryOperationExpr:
		return r.buildFromBinop(x)
	case *ast.PatternInExpr:
		return r.buildFromIn(x)
	case *ast.ParenthesesExpr:
		return r.build(x.Expr)
	case *ast.BetweenExpr:
		return r.buildFromBetween(x)
	case *ast.IsNullExpr:
		return r.buildFromIsNull(x)
	case *ast.IsTruthExpr:
		return r.buildFromIsTruth(x)
	case *ast.PatternLikeExpr:
		return r.buildFromPatternLike(x)
	case *ast.ColumnNameExpr:
		return r.buildFromColumnName(x)
	}
	return fullRange
}

func (r *rangeBuilder) buildFromBinop(x *ast.BinaryOperationExpr) []rangePoint {
	if x.Op == opcode.OrOr {
		return r.union(r.build(x.L), r.build(x.R))
	} else if x.Op == opcode.AndAnd {
		return r.intersection(r.build(x.L), r.build(x.R))
	}
	// This has been checked that the binary operation is comparison operation, and one of
	// the operand is column name expression.
	var value interface{}
	var op opcode.Op
	if _, ok := x.L.(*ast.ValueExpr); ok {
		value = x.L.GetValue()
		switch x.Op {
		case opcode.GE:
			op = opcode.LE
		case opcode.GT:
			op = opcode.LT
		case opcode.LT:
			op = opcode.GT
		case opcode.LE:
			op = opcode.GE
		default:
			op = x.Op
		}
	} else {
		value = x.R.GetValue()
		op = x.Op
	}
	if value == nil {
		return nil
	}
	switch op {
	case opcode.EQ:
		startPoint := rangePoint{value: value, start: true}
		endPoint := rangePoint{value: value}
		return []rangePoint{startPoint, endPoint}
	case opcode.NE:
		startPoint1 := rangePoint{value: MinNotNullVal, start: true}
		endPoint1 := rangePoint{value: value, excl: true}
		startPoint2 := rangePoint{value: value, start: true, excl: true}
		endPoint2 := rangePoint{value: MaxVal}
		return []rangePoint{startPoint1, endPoint1, startPoint2, endPoint2}
	case opcode.LT:
		startPoint := rangePoint{value: MinNotNullVal, start: true}
		endPoint := rangePoint{value: value, excl: true}
		return []rangePoint{startPoint, endPoint}
	case opcode.LE:
		startPoint := rangePoint{value: MinNotNullVal, start: true}
		endPoint := rangePoint{value: value}
		return []rangePoint{startPoint, endPoint}
	case opcode.GT:
		startPoint := rangePoint{value: value, start: true, excl: true}
		endPoint := rangePoint{value: MaxVal}
		return []rangePoint{startPoint, endPoint}
	case opcode.GE:
		startPoint := rangePoint{value: value, start: true}
		endPoint := rangePoint{value: MaxVal}
		return []rangePoint{startPoint, endPoint}
	}
	return nil
}

func (r *rangeBuilder) buildFromIn(x *ast.PatternInExpr) []rangePoint {
	if x.Not {
		r.err = ErrUnsupportedType.Gen("NOT IN is not supported")
		return fullRange
	}
	var rangePoints []rangePoint
	for _, v := range x.List {
		startPoint := rangePoint{value: v.GetValue(), start: true}
		endPoint := rangePoint{value: v.GetValue()}
		rangePoints = append(rangePoints, startPoint, endPoint)
	}
	sorter := rangePointSorter{points: rangePoints}
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
	distinctRangePoints := make([]rangePoint, 0, len(rangePoints))
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

func (r *rangeBuilder) buildFromBetween(x *ast.BetweenExpr) []rangePoint {
	if x.Not {
		binop1 := &ast.BinaryOperationExpr{Op: opcode.LT, L: x.Expr, R: x.Left}
		binop2 := &ast.BinaryOperationExpr{Op: opcode.GT, L: x.Expr, R: x.Right}
		range1 := r.buildFromBinop(binop1)
		range2 := r.buildFromBinop(binop2)
		return r.union(range1, range2)
	}
	binop1 := &ast.BinaryOperationExpr{Op: opcode.GE, L: x.Expr, R: x.Left}
	binop2 := &ast.BinaryOperationExpr{Op: opcode.LE, L: x.Expr, R: x.Right}
	range1 := r.buildFromBinop(binop1)
	range2 := r.buildFromBinop(binop2)
	return r.intersection(range1, range2)
}

func (r *rangeBuilder) buildFromIsNull(x *ast.IsNullExpr) []rangePoint {
	if x.Not {
		startPoint := rangePoint{value: MinNotNullVal, start: true}
		endPoint := rangePoint{value: MaxVal}
		return []rangePoint{startPoint, endPoint}
	}
	startPoint := rangePoint{start: true}
	endPoint := rangePoint{}
	return []rangePoint{startPoint, endPoint}
}

func (r *rangeBuilder) buildFromIsTruth(x *ast.IsTruthExpr) []rangePoint {
	if x.True != 0 {
		if x.Not {
			startPoint1 := rangePoint{start: true}
			endPoint1 := rangePoint{}
			startPoint2 := rangePoint{value: int64(0), start: true}
			endPoint2 := rangePoint{value: int64(0)}
			return []rangePoint{startPoint1, endPoint1, startPoint2, endPoint2}
		}
		startPoint1 := rangePoint{value: MinNotNullVal, start: true}
		endPoint1 := rangePoint{value: int64(0), excl: true}
		startPoint2 := rangePoint{value: int64(0), excl: true, start: true}
		endPoint2 := rangePoint{value: MaxVal}
		return []rangePoint{startPoint1, endPoint1, startPoint2, endPoint2}
	}
	if x.Not {
		startPoint1 := rangePoint{start: true}
		endPoint1 := rangePoint{value: int64(0), excl: true}
		startPoint2 := rangePoint{value: int64(0), start: true, excl: true}
		endPoint2 := rangePoint{value: MaxVal}
		return []rangePoint{startPoint1, endPoint1, startPoint2, endPoint2}
	}
	startPoint := rangePoint{value: int64(0), start: true}
	endPoint := rangePoint{value: int64(0)}
	return []rangePoint{startPoint, endPoint}
}

func (r *rangeBuilder) buildFromPatternLike(x *ast.PatternLikeExpr) []rangePoint {
	if x.Not {
		// Pattern not like is not supported.
		r.err = ErrUnsupportedType.Gen("NOT LIKE is not supported.")
		return fullRange
	}
	pattern, err := types.ToString(x.Pattern.GetValue())
	if err != nil {
		r.err = errors.Trace(err)
		return fullRange
	}
	lowValue := make([]byte, 0, len(pattern))
	// unscape the pattern
	var exclude bool
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == x.Escape {
			i++
			if i < len(pattern) {
				lowValue = append(lowValue, pattern[i])
			} else {
				lowValue = append(lowValue, x.Escape)
			}
			continue
		}
		if pattern[i] == '%' {
			break
		} else if pattern[i] == '_' {
			exclude = true
			break
		}
		lowValue = append(lowValue, pattern[i])
	}
	if len(lowValue) == 0 {
		return []rangePoint{{value: MinNotNullVal, start: true}, {value: MaxVal}}
	}
	startPoint := rangePoint{value: string(lowValue), start: true, excl: exclude}
	highValue := make([]byte, len(lowValue))
	copy(highValue, lowValue)

	endPoint := rangePoint{excl: true}
	for i := len(highValue) - 1; i >= 0; i-- {
		highValue[i]++
		if highValue[i] != 0 {
			endPoint.value = string(highValue)
			break
		}
		if i == 0 {
			endPoint.value = MaxVal
			break
		}
	}
	return []rangePoint{startPoint, endPoint}
}

func (r *rangeBuilder) buildFromColumnName(x *ast.ColumnNameExpr) []rangePoint {
	// column name expression is equivalent to column name is true.
	startPoint1 := rangePoint{value: MinNotNullVal, start: true}
	endPoint1 := rangePoint{value: int64(0), excl: true}
	startPoint2 := rangePoint{value: int64(0), excl: true, start: true}
	endPoint2 := rangePoint{value: MaxVal}
	return []rangePoint{startPoint1, endPoint1, startPoint2, endPoint2}
}

func (r *rangeBuilder) intersection(a, b []rangePoint) []rangePoint {
	return r.merge(a, b, false)
}

func (r *rangeBuilder) union(a, b []rangePoint) []rangePoint {
	return r.merge(a, b, true)
}

func (r *rangeBuilder) merge(a, b []rangePoint, union bool) []rangePoint {
	sorter := rangePointSorter{points: append(a, b...)}
	sort.Sort(&sorter)
	if sorter.err != nil {
		r.err = sorter.err
		return nil
	}
	var (
		merged               []rangePoint
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
func (r *rangeBuilder) buildIndexRanges(rangePoints []rangePoint) []*IndexRange {
	indexRanges := make([]*IndexRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := rangePoints[i]
		endPoint := rangePoints[i+1]
		ir := &IndexRange{
			LowVal:      []interface{}{startPoint.value},
			LowExclude:  startPoint.excl,
			HighVal:     []interface{}{endPoint.value},
			HighExclude: endPoint.excl,
		}
		indexRanges = append(indexRanges, ir)
	}
	return indexRanges
}

// appendIndexRanges appends additional column ranges for multi-column index.
// The additional column ranges can only be appended to point ranges.
// for example we have an index (a, b), if the condition is (a > 1 and b = 2)
// then we can not build a conjunctive ranges for this index.
func (r *rangeBuilder) appendIndexRanges(origin []*IndexRange, rangePoints []rangePoint) []*IndexRange {
	var newIndexRanges []*IndexRange
	for i := 0; i < len(origin); i++ {
		oRange := origin[i]
		if !oRange.IsPoint() {
			newIndexRanges = append(newIndexRanges, oRange)
		} else {
			newIndexRanges = append(newIndexRanges, r.appendIndexRange(oRange, rangePoints)...)
		}
	}
	return newIndexRanges
}

func (r *rangeBuilder) appendIndexRange(origin *IndexRange, rangePoints []rangePoint) []*IndexRange {
	newRanges := make([]*IndexRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := rangePoints[i]
		lowVal := make([]interface{}, len(origin.LowVal)+1)
		copy(lowVal, origin.LowVal)
		lowVal[len(origin.LowVal)] = startPoint.value

		endPoint := rangePoints[i+1]
		highVal := make([]interface{}, len(origin.HighVal)+1)
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
	return newRanges
}

func (r *rangeBuilder) buildTableRanges(rangePoints []rangePoint) []TableRange {
	tableRanges := make([]TableRange, 0, len(rangePoints)/2)
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := rangePoints[i]
		if startPoint.value == nil || startPoint.value == MinNotNullVal {
			startPoint.value = math.MinInt64
		}
		startInt, err := types.ToInt64(startPoint.value)
		if err != nil {
			r.err = errors.Trace(err)
			return tableRanges
		}
		cmp, err := types.Compare(startInt, startPoint.value)
		if err != nil {
			r.err = errors.Trace(err)
			return tableRanges
		}
		if cmp < 0 || (cmp == 0 && startPoint.excl) {
			startInt++
		}
		endPoint := rangePoints[i+1]
		if endPoint.value == nil {
			endPoint.value = math.MinInt64
		} else if endPoint.value == MaxVal {
			endPoint.value = math.MaxInt64
		}
		endInt, err := types.ToInt64(endPoint.value)
		if err != nil {
			r.err = errors.Trace(err)
			return tableRanges
		}
		cmp, err = types.Compare(endInt, endPoint.value)
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
		tableRanges = append(tableRanges, TableRange{LowVal: startInt, HighVal: endInt})
	}
	return tableRanges
}
