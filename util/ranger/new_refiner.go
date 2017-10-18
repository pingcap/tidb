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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

func buildIndexRange(sc *variable.StatementContext, cols []*expression.Column, lengths []int,
	accessCondition []expression.Expression) ([]*types.IndexRange, error) {
	rb := builder{sc: sc}
	var (
		ranges  []*types.IndexRange
		eqCount int
	)
	for eqCount = 0; eqCount < len(accessCondition) && eqCount < len(cols); eqCount++ {
		if sf, ok := accessCondition[eqCount].(*expression.ScalarFunction); !ok || sf.FuncName.L != ast.EQ {
			break
		}
		// Build ranges for equal or in access conditions.
		point := rb.build(accessCondition[eqCount])
		if eqCount == 0 {
			ranges = rb.buildIndexRanges(point, cols[eqCount].RetType)
		} else {
			ranges = rb.appendIndexRanges(ranges, point, cols[eqCount].RetType)
		}
	}
	rangePoints := fullRange
	// Build rangePoints for non-equal access conditions.
	for i := eqCount; i < len(accessCondition); i++ {
		rangePoints = rb.intersection(rangePoints, rb.build(accessCondition[i]))
	}
	if eqCount == 0 {
		ranges = rb.buildIndexRanges(rangePoints, cols[0].RetType)
	} else if eqCount < len(accessCondition) {
		ranges = rb.appendIndexRanges(ranges, rangePoints, cols[eqCount].RetType)
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
		ran.HighExclude = false
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

// detachColumnCNFConditions detaches the condition for calculating range from the other conditions.
// Please make sure that the top level is CNF form.
func detachColumnCNFConditions(conditions []expression.Expression, checker *conditionChecker) ([]expression.Expression, []expression.Expression) {
	var accessConditions, filterConditions []expression.Expression
	for _, cond := range conditions {
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			dnfItems := expression.FlattenDNFConditions(sf)
			colulmnDNFItems, hasResidual := detachColumnDNFConditions(dnfItems, checker)
			// If this CNF has expression that cannot be resolved as access condition, then the total DNF expression
			// should be also appended into filter condition.
			if hasResidual {
				filterConditions = append(filterConditions, cond)
			}
			if len(colulmnDNFItems) == 0 {
				continue
			}
			rebuildDNF := expression.ComposeDNFCondition(nil, colulmnDNFItems...)
			accessConditions = append(accessConditions, rebuildDNF)
			continue
		}
		if !checker.check(cond) {
			filterConditions = append(filterConditions, cond)
			continue
		}
		accessConditions = append(accessConditions, cond)
		if checker.shouldReserve {
			filterConditions = append(filterConditions, cond)
			checker.shouldReserve = false
		}
	}
	return accessConditions, filterConditions
}

// detachColumnDNFConditions detaches the condition for calculating range from the other conditions.
// Please make sure that the top level is DNF form.
func detachColumnDNFConditions(conditions []expression.Expression, checker *conditionChecker) ([]expression.Expression, bool) {
	var (
		hasResidualConditions bool
		accessConditions      []expression.Expression
	)
	for _, cond := range conditions {
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			cnfItems := expression.FlattenCNFConditions(sf)
			columnCNFItems, others := detachColumnCNFConditions(cnfItems, checker)
			if len(others) > 0 {
				hasResidualConditions = true
			}
			if len(columnCNFItems) == 0 {
				continue
			}
			rebuildCNF := expression.ComposeCNFCondition(nil, columnCNFItems...)
			accessConditions = append(accessConditions, rebuildCNF)
		} else if checker.check(cond) {
			accessConditions = append(accessConditions, cond)
			if checker.shouldReserve {
				hasResidualConditions = true
				checker.shouldReserve = false
			}
		} else {
			return nil, true
		}
	}
	return accessConditions, hasResidualConditions
}

// DetachIndexConditions will detach the index filters from table filters.
func DetachIndexConditions(conditions []expression.Expression, cols []*expression.Column,
	lengths []int) (accessConds []expression.Expression, filterConds []expression.Expression) {
	accessConds = make([]expression.Expression, len(cols))
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, cond := range conditions {
		conditions[i] = expression.PushDownNot(cond, false, nil)
	}
	var accessEqualCount int
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
	return accessConds, filterConds
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

// DetachCondsForSelectivity detaches the conditions used for range calculation from other useless conditions.
func DetachCondsForSelectivity(conds []expression.Expression, rangeType int, cols []*expression.Column,
	lengths []int) (accessConditions, otherConditions []expression.Expression) {
	if rangeType == IntRangeType || rangeType == ColumnRangeType {
		return DetachColumnConditions(conds, cols[0].ColName)
	} else if rangeType == IndexRangeType {
		return DetachIndexConditions(conds, cols, lengths)
	}
	return nil, conds
}

// DetachCondsForTableRange detaches the conditions used for range calculation form other useless conditions for
// calculating the table range.
func DetachCondsForTableRange(ctx context.Context, conds []expression.Expression, col *expression.Column) (accessContditions, otherConditions []expression.Expression) {
	checker := &conditionChecker{
		colName: col.ColName,
		length:  types.UnspecifiedLength,
	}
	for i, cond := range conds {
		conds[i] = expression.PushDownNot(cond, false, ctx)
	}
	return detachColumnCNFConditions(conds, checker)
}

// BuildRange is a method which can calculate IntColumnRange, ColumnRange, IndexRange.
func BuildRange(sc *variable.StatementContext, conds []expression.Expression, rangeType int, cols []*expression.Column,
	lengths []int) (retRanges []types.Range, _ error) {
	if rangeType == IntRangeType {
		ranges, err := BuildTableRange(conds, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		retRanges = make([]types.Range, 0, len(ranges))
		for _, ran := range ranges {
			retRanges = append(retRanges, ran)
		}
	} else if rangeType == ColumnRangeType {
		ranges, err := buildColumnRange(conds, sc, cols[0].RetType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		retRanges = make([]types.Range, 0, len(ranges))
		for _, ran := range ranges {
			retRanges = append(retRanges, ran)
		}
	} else if rangeType == IndexRangeType {
		ranges, err := buildIndexRange(sc, cols, lengths, conds)
		if err != nil {
			return nil, errors.Trace(err)
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
