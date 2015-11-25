package optimizer

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
	"sort"
)

type rangePoint struct {
	value interface{}
	excl  bool
	start bool
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
	if a.value == plan.MinNotNullVal && b.value == plan.MinNotNullVal {
		return r.equalValueLess(a, b)
	} else if b.value == plan.MinNotNullVal {
		return false
	} else if a.value == plan.MinNotNullVal {
		return true
	}

	// a and b both not min value
	if a.value == plan.MaxVal && b.value == plan.MaxVal {
		return r.equalValueLess(a, b)
	} else if a.value == plan.MaxVal {
		return false
	} else if b.value == plan.MaxVal {
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
	ctx context.Context
	err error
}

func (r *rangeBuilder) build(expr ast.ExprNode) []rangePoint {
	switch x := expr.(type) {
	case *ast.BinaryOperationExpr:
		return r.buildFromBinop(x)
	case *ast.PatternInExpr:
		return r.buildFromIn(x)
	case *ast.BetweenExpr:
		return r.buildFromBetween(x)
	}
	return nil
}

func (r *rangeBuilder) buildFromBinop(x *ast.BinaryOperationExpr) []rangePoint {
	if x.Op == opcode.OrOr {
		return r.merge(r.build(x.L), r.build(x.R), true)
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
	switch op {
	case opcode.EQ:
		startPoint := rangePoint{value: value, start: true}
		endPoint := rangePoint{value: value}
		return []rangePoint{startPoint, endPoint}
	case opcode.NE:
		startPoint1 := rangePoint{value: plan.MinNotNullVal, start: true}
		endPoint1 := rangePoint{value: value, excl: true}
		startPoint2 := rangePoint{value: value, start: true, excl: true}
		endPoint2 := rangePoint{value: plan.MaxVal}
		return []rangePoint{startPoint1, endPoint1, startPoint2, endPoint2}
	case opcode.LT:
		startPoint := rangePoint{value: plan.MinNotNullVal, start: true}
		endPoint := rangePoint{value: value, excl: true}
		return []rangePoint{startPoint, endPoint}
	case opcode.LE:
		startPoint := rangePoint{value: plan.MinNotNullVal, start: true}
		endPoint := rangePoint{value: value}
		return []rangePoint{startPoint, endPoint}
	case opcode.GT:
		startPoint := rangePoint{value: value, start: true, excl: true}
		endPoint := rangePoint{value: plan.MaxVal}
		return []rangePoint{startPoint, endPoint}
	case opcode.GE:
		startPoint := rangePoint{value: value, start: true}
		endPoint := rangePoint{value: plan.MaxVal}
		return []rangePoint{startPoint, endPoint}
	}
	return nil
}

func (r *rangeBuilder) buildFromIn(x *ast.PatternInExpr) []rangePoint {
	var rangePoints []rangePoint
	for _, v := range x.List {
		startPoint := rangePoint{value: v.GetValue(), start: true}
		endPoint := rangePoint{value: v.GetValue()}
		rangePoints = append(rangePoints, startPoint, endPoint)
	}
	return rangePoints
}

func (r *rangeBuilder) buildFromBetween(x *ast.BetweenExpr) []rangePoint {
	startPoint := rangePoint{value: x.Left.GetValue(), start: true}
	endPoint := rangePoint{value: x.Right.GetValue()}
	return []rangePoint{startPoint, endPoint}
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
func (r *rangeBuilder) buildIndexRanges(rangePoints []rangePoint) []*plan.IndexRange {
	var indexRanges []*plan.IndexRange
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := rangePoints[i]
		endPoint := rangePoints[i+1]
		ir := &plan.IndexRange{
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
func (r *rangeBuilder) appendIndexRanges(origin []*plan.IndexRange, rangePoints []rangePoint) []*plan.IndexRange {
	var newIndexRanges []*plan.IndexRange
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

func (r *rangeBuilder) appendIndexRange(origin *plan.IndexRange, rangePoints []rangePoint) []*plan.IndexRange {
	var newRanges []*plan.IndexRange
	for i := 0; i < len(rangePoints); i += 2 {
		startPoint := rangePoints[i]
		lowVal := make([]interface{}, len(origin.LowVal)+1)
		copy(lowVal, origin.LowVal)
		lowVal[len(origin.LowVal)] = startPoint.value

		endPoint := rangePoints[i+1]
		highVal := make([]interface{}, len(origin.HighVal)+1)
		copy(highVal, origin.HighVal)
		highVal[len(origin.HighVal)] = endPoint.value

		ir := &plan.IndexRange{
			LowVal:      lowVal,
			LowExclude:  startPoint.excl,
			HighVal:     highVal,
			HighExclude: endPoint.excl,
		}
		newRanges = append(newRanges, ir)
	}
	return newRanges
}
