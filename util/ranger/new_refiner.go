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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

func buildIndexRange(sc *variable.StatementContext, cols []*expression.Column, lengths []int, inAndEqCount int,
	accessCondition []expression.Expression) ([]*types.IndexRange, error) {
	rb := builder{sc: sc}
	var ranges []*types.IndexRange
	for i := 0; i < inAndEqCount; i++ {
		// Build ranges for equal or in access conditions.
		point := rb.build(accessCondition[i])
		if i == 0 {
			ranges = rb.buildIndexRanges(point, cols[i].RetType)
		} else {
			ranges = rb.appendIndexRanges(ranges, point, cols[i].RetType)
		}
	}
	rangePoints := fullRange
	// Build rangePoints for non-equal access conditions.
	for i := inAndEqCount; i < len(accessCondition); i++ {
		rangePoints = rb.intersection(rangePoints, rb.build(accessCondition[i]))
	}
	if inAndEqCount == 0 {
		ranges = rb.buildIndexRanges(rangePoints, cols[0].RetType)
	} else if inAndEqCount < len(accessCondition) {
		ranges = rb.appendIndexRanges(ranges, rangePoints, cols[inAndEqCount].RetType)
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
	return ranges, errors.Trace(rb.err)
}

func hasPrefix(lengths []int) bool {
	for _, l := range lengths {
		if l != types.UnspecifiedLength {
			return true
		}
	}
	return false
}

func fixPrefixColRange(ranges []*types.IndexRange, lengths []int) {
	for _, ran := range ranges {
		for i := 0; i < len(ran.LowVal); i++ {
			fixRangeDatum(&ran.LowVal[i], lengths[i])
		}
		ran.LowExclude = false
		for i := 0; i < len(ran.HighVal); i++ {
			fixRangeDatum(&ran.HighVal[i], lengths[i])
		}
	}
}

func fixRangeDatum(v *types.Datum, length int) {
	// If this column is prefix and the prefix length is smaller than the range, cut it.
	if length != types.UnspecifiedLength && length < len(v.GetBytes()) {
		v.SetBytes(v.GetBytes()[:length])
	}
}

// getEQColOffset judge if the expression is a eq function that one side is constant and another is column.
// If so, it will return the offset of this column in the slice.
func getEQColOffset(expr expression.Expression, cols []*expression.Column) int {
	f, ok := expr.(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.EQ {
		return -1
	}
	if c, ok := f.GetArgs()[0].(*expression.Column); ok {
		if _, ok := f.GetArgs()[1].(*expression.Constant); ok {
			for i, col := range cols {
				if col.Equal(c, nil) {
					return i
				}
			}
		}
	}
	if c, ok := f.GetArgs()[1].(*expression.Column); ok {
		if _, ok := f.GetArgs()[0].(*expression.Constant); ok {
			for i, col := range cols {
				if col.Equal(c, nil) {
					return i
				}
			}
		}
	}
	return -1
}

// detachIndexScanConditions will detach the index filters from table filters.
func detachIndexScanConditions(conditions []expression.Expression, cols []*expression.Column, lengths []int) (accessConds []expression.Expression,
	filterConds []expression.Expression, accessEqualCount int, accessInAndEqCount int) {
	accessConds = make([]expression.Expression, len(cols))
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, cond := range conditions {
		conditions[i] = expression.PushDownNot(cond, false, nil)
	}
	for _, cond := range conditions {
		offset := getEQColOffset(cond, cols)
		if offset != -1 {
			accessConds[offset] = cond
		}
	}
	for i, cond := range accessConds {
		if cond == nil {
			accessConds = accessConds[:i]
			accessEqualCount = i
			break
		}
		if lengths[i] != types.UnspecifiedLength {
			filterConds = append(filterConds, cond)
		}
		if i == len(accessConds)-1 {
			accessEqualCount = len(accessConds)
		}
	}
	accessInAndEqCount = accessEqualCount
	// We should remove all accessConds, so that they will not be added to filter conditions.
	conditions = removeAccessConditions(conditions, accessConds)
	var curIndex int
	for curIndex = accessEqualCount; curIndex < len(cols); curIndex++ {
		checker := &conditionChecker{
			cols:         cols,
			columnOffset: curIndex,
			length:       lengths[curIndex],
		}
		// First of all, we should extract all of in/eq expressions from rest conditions for every continuous index column.
		// e.g. For index (a,b,c) and conditions a in (1,2) and b < 1 and c in (3,4), we should only extract column a in (1,2).
		accessIdx := checker.findEqOrInFunc(conditions)
		// If we fail to find any in or eq expression, we should consider all of other conditions for the next column.
		if accessIdx == -1 {
			accessConds, filterConds = checker.extractAccessAndFilterConds(conditions, accessConds, filterConds)
			break
		}
		accessInAndEqCount++
		accessConds = append(accessConds, conditions[accessIdx])
		if lengths[curIndex] != types.UnspecifiedLength {
			filterConds = append(filterConds, conditions[accessIdx])
		}
		conditions = append(conditions[:accessIdx], conditions[accessIdx+1:]...)
	}
	// If curIndex equals to len of index columns, it means the rest conditions haven't been appended to filter conditions.
	if curIndex == len(cols) {
		filterConds = append(filterConds, conditions...)
	}
	return accessConds, filterConds, accessEqualCount, accessInAndEqCount
}

// buildColumnRange builds the range for sampling histogram to calculate the row count.
func buildColumnRange(conds []expression.Expression, sc *variable.StatementContext, tp *types.FieldType) ([]*types.ColumnRange, error) {
	if len(conds) == 0 {
		return []*types.ColumnRange{{Low: types.Datum{}, High: types.MaxValueDatum()}}, nil
	}

	rb := builder{sc: sc}
	rangePoints := fullRange
	for _, cond := range conds {
		rangePoints = rb.intersection(rangePoints, rb.build(cond))
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
	}
	ranges := rb.buildColumnRanges(rangePoints, tp)
	if rb.err != nil {
		return nil, errors.Trace(rb.err)
	}
	return ranges, nil
}

// BuildRange is a method which can calculate IntColumnRange, ColumnRange, IndexRange.
func BuildRange(sc *variable.StatementContext, conds []expression.Expression, rangeType int, cols []*expression.Column, lengths []int) (retRanges []types.Range,
	accessConditions, otherConditions []expression.Expression, _ error) {
	if rangeType == IntRangeType {
		accessConditions, otherConditions = DetachColumnConditions(conds, cols[0].ColName)
		ranges, err := BuildTableRange(accessConditions, sc)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		retRanges = make([]types.Range, 0, len(ranges))
		for _, ran := range ranges {
			retRanges = append(retRanges, ran)
		}
	} else if rangeType == ColumnRangeType {
		accessConditions, otherConditions = DetachColumnConditions(conds, cols[0].ColName)
		ranges, err := buildColumnRange(accessConditions, sc, cols[0].RetType)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		retRanges = make([]types.Range, 0, len(ranges))
		for _, ran := range ranges {
			retRanges = append(retRanges, ran)
		}
	} else if rangeType == IndexRangeType {
		var eqAndInCount int
		accessConditions, otherConditions, _, eqAndInCount = detachIndexScanConditions(conds, cols, lengths)
		ranges, err := buildIndexRange(sc, cols, lengths, eqAndInCount, accessConditions)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		retRanges = make([]types.Range, 0, len(ranges))
		for _, ran := range ranges {
			retRanges = append(retRanges, ran)
		}
	}
	return
}

// Ranges2IntRanges changes []types.Range to []types.IntColumnRange
func Ranges2IntRanges(ranges []types.Range) []types.IntColumnRange {
	retRanges := make([]types.IntColumnRange, 0, len(ranges))
	for _, ran := range ranges {
		retRanges = append(retRanges, ran.Convert2IntRange())
	}
	return retRanges
}

// Ranges2ColumnRanges changes []types.Range to []*types.ColumnRange
func Ranges2ColumnRanges(ranges []types.Range) []*types.ColumnRange {
	retRanges := make([]*types.ColumnRange, 0, len(ranges))
	for _, ran := range ranges {
		retRanges = append(retRanges, ran.Convert2ColumnRange())
	}
	return retRanges
}

// Ranges2IndexRanges changes []types.Range to []*types.IndexRange
func Ranges2IndexRanges(ranges []types.Range) []*types.IndexRange {
	retRanges := make([]*types.IndexRange, 0, len(ranges))
	for _, ran := range ranges {
		retRanges = append(retRanges, ran.Convert2IndexRange())
	}
	return retRanges
}
