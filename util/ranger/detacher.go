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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// detachColumnCNFConditions detaches the condition for calculating range from the other conditions.
// Please make sure that the top level is CNF form.
func detachColumnCNFConditions(sctx sessionctx.Context, conditions []expression.Expression, checker *conditionChecker) ([]expression.Expression, []expression.Expression) {
	var accessConditions, filterConditions []expression.Expression
	for _, cond := range conditions {
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			dnfItems := expression.FlattenDNFConditions(sf)
			colulmnDNFItems, hasResidual := detachColumnDNFConditions(sctx, dnfItems, checker)
			// If this CNF has expression that cannot be resolved as access condition, then the total DNF expression
			// should be also appended into filter condition.
			if hasResidual {
				filterConditions = append(filterConditions, cond)
			}
			if len(colulmnDNFItems) == 0 {
				continue
			}
			rebuildDNF := expression.ComposeDNFCondition(sctx, colulmnDNFItems...)
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
			checker.shouldReserve = checker.length != types.UnspecifiedLength
		}
	}
	return accessConditions, filterConditions
}

// detachColumnDNFConditions detaches the condition for calculating range from the other conditions.
// Please make sure that the top level is DNF form.
func detachColumnDNFConditions(sctx sessionctx.Context, conditions []expression.Expression, checker *conditionChecker) ([]expression.Expression, bool) {
	var (
		hasResidualConditions bool
		accessConditions      []expression.Expression
	)
	for _, cond := range conditions {
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			cnfItems := expression.FlattenCNFConditions(sf)
			columnCNFItems, others := detachColumnCNFConditions(sctx, cnfItems, checker)
			if len(others) > 0 {
				hasResidualConditions = true
			}
			// If one part of DNF has no access condition. Then this DNF cannot get range.
			if len(columnCNFItems) == 0 {
				return nil, true
			}
			rebuildCNF := expression.ComposeCNFCondition(sctx, columnCNFItems...)
			accessConditions = append(accessConditions, rebuildCNF)
		} else if checker.check(cond) {
			accessConditions = append(accessConditions, cond)
			if checker.shouldReserve {
				hasResidualConditions = true
				checker.shouldReserve = checker.length != types.UnspecifiedLength
			}
		} else {
			return nil, true
		}
	}
	return accessConditions, hasResidualConditions
}

// getEqOrInColOffset checks if the expression is a eq function that one side is constant and another is column or an
// in function which is `column in (constant list)`.
// If so, it will return the offset of this column in the slice, otherwise return -1 for not found.
func getEqOrInColOffset(expr expression.Expression, cols []*expression.Column) int {
	f, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return -1
	}
	if f.FuncName.L == ast.EQ {
		if c, ok := f.GetArgs()[0].(*expression.Column); ok {
			if _, ok := f.GetArgs()[1].(*expression.Constant); ok {
				for i, col := range cols {
					if col.Equal(nil, c) {
						return i
					}
				}
			}
		}
		if c, ok := f.GetArgs()[1].(*expression.Column); ok {
			if _, ok := f.GetArgs()[0].(*expression.Constant); ok {
				for i, col := range cols {
					if col.Equal(nil, c) {
						return i
					}
				}
			}
		}
	}
	if f.FuncName.L == ast.In {
		c, ok := f.GetArgs()[0].(*expression.Column)
		if !ok {
			return -1
		}
		for _, arg := range f.GetArgs()[1:] {
			if _, ok := arg.(*expression.Constant); !ok {
				return -1
			}
		}
		for i, col := range cols {
			if col.Equal(nil, c) {
				return i
			}
		}
	}
	return -1
}

// detachCNFCondAndBuildRangeForIndex will detach the index filters from table filters. These conditions are connected with `and`
// It will first find the point query column and then extract the range query column.
// considerDNF is true means it will try to extract access conditions from the DNF expressions.
func detachCNFCondAndBuildRangeForIndex(sctx sessionctx.Context, conditions []expression.Expression, cols []*expression.Column,
	tpSlice []*types.FieldType, lengths []int, considerDNF bool) (*DetachRangeResult, error) {
	var (
		eqCount int
		ranges  []*Range
		err     error
	)
	res := &DetachRangeResult{}

	accessConds, filterConds, newConditions, emptyRange := ExtractEqAndInCondition(sctx, conditions, cols, lengths)
	if emptyRange {
		return res, nil
	}

	for ; eqCount < len(accessConds); eqCount++ {
		if accessConds[eqCount].(*expression.ScalarFunction).FuncName.L != ast.EQ {
			break
		}
	}
	eqOrInCount := len(accessConds)
	res.EqCondCount = eqCount
	res.EqOrInCount = eqOrInCount
	if eqOrInCount == len(cols) {
		filterConds = append(filterConds, newConditions...)
		ranges, err = buildCNFIndexRange(sctx.GetSessionVars().StmtCtx, cols, tpSlice, lengths, eqOrInCount, accessConds)
		if err != nil {
			return res, err
		}
		res.Ranges = ranges
		res.AccessConds = accessConds
		res.RemainedConds = filterConds
		return res, nil
	}
	checker := &conditionChecker{
		colUniqueID:   cols[eqOrInCount].UniqueID,
		length:        lengths[eqOrInCount],
		shouldReserve: lengths[eqOrInCount] != types.UnspecifiedLength,
	}
	if considerDNF {
		accesses, filters := detachColumnCNFConditions(sctx, newConditions, checker)
		accessConds = append(accessConds, accesses...)
		filterConds = append(filterConds, filters...)
	} else {
		for _, cond := range newConditions {
			if !checker.check(cond) {
				filterConds = append(filterConds, cond)
				continue
			}
			accessConds = append(accessConds, cond)
		}
	}
	ranges, err = buildCNFIndexRange(sctx.GetSessionVars().StmtCtx, cols, tpSlice, lengths, eqOrInCount, accessConds)
	res.Ranges = ranges
	res.AccessConds = accessConds
	res.RemainedConds = filterConds
	return res, err
}

// ExtractEqAndInCondition will split the given condition into three parts by the information of index columns and their lengths.
// accesses: The condition will be used to build range.
// filters: filters is the part that some access conditions need to be evaluate again since it's only the prefix part of char column.
// newConditions: We'll simplify the given conditions if there're multiple in conditions or eq conditions on the same column.
//   e.g. if there're a in (1, 2, 3) and a in (2, 3, 4). This two will be combined to a in (2, 3) and pushed to newConditions.
// bool: indicate whether there's nil range when merging eq and in conditions.
func ExtractEqAndInCondition(sctx sessionctx.Context, conditions []expression.Expression,
	cols []*expression.Column, lengths []int) ([]expression.Expression, []expression.Expression, []expression.Expression, bool) {
	var filters []expression.Expression
	rb := builder{sc: sctx.GetSessionVars().StmtCtx}
	accesses := make([]expression.Expression, len(cols))
	points := make([][]point, len(cols))
	mergedAccesses := make([]expression.Expression, len(cols))
	newConditions := make([]expression.Expression, 0, len(conditions))
	for _, cond := range conditions {
		offset := getEqOrInColOffset(cond, cols)
		if offset == -1 {
			newConditions = append(newConditions, cond)
			continue
		}
		if accesses[offset] == nil {
			accesses[offset] = cond
			continue
		}
		// Multiple Eq/In conditions for one column in CNF, apply intersection on them
		// Lazily compute the points for the previously visited Eq/In
		if mergedAccesses[offset] == nil {
			mergedAccesses[offset] = accesses[offset]
			points[offset] = rb.build(accesses[offset])
		}
		points[offset] = rb.intersection(points[offset], rb.build(cond))
		// Early termination if false expression found
		if len(points[offset]) == 0 {
			return nil, nil, nil, true
		}
	}
	for i, ma := range mergedAccesses {
		if ma == nil {
			if accesses[i] != nil {
				newConditions = append(newConditions, accesses[i])
			}
			continue
		}
		accesses[i] = points2EqOrInCond(sctx, points[i], mergedAccesses[i])
		newConditions = append(newConditions, accesses[i])
	}
	for i, cond := range accesses {
		if cond == nil {
			accesses = accesses[:i]
			break
		}
		if lengths[i] != types.UnspecifiedLength {
			filters = append(filters, cond)
		}
	}
	// We should remove all accessConds, so that they will not be added to filter conditions.
	newConditions = removeAccessConditions(newConditions, accesses)
	return accesses, filters, newConditions, false
}

// detachDNFCondAndBuildRangeForIndex will detach the index filters from table filters when it's a DNF.
// We will detach the conditions of every DNF items, then compose them to a DNF.
func detachDNFCondAndBuildRangeForIndex(sctx sessionctx.Context, condition *expression.ScalarFunction,
	cols []*expression.Column, newTpSlice []*types.FieldType, lengths []int) ([]*Range, []expression.Expression, bool, error) {
	sc := sctx.GetSessionVars().StmtCtx
	firstColumnChecker := &conditionChecker{
		colUniqueID:   cols[0].UniqueID,
		shouldReserve: lengths[0] != types.UnspecifiedLength,
		length:        lengths[0],
	}
	rb := builder{sc: sc}
	dnfItems := expression.FlattenDNFConditions(condition)
	newAccessItems := make([]expression.Expression, 0, len(dnfItems))
	var totalRanges []*Range
	hasResidual := false
	for _, item := range dnfItems {
		if sf, ok := item.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			cnfItems := expression.FlattenCNFConditions(sf)
			var accesses, filters []expression.Expression
			res, err := detachCNFCondAndBuildRangeForIndex(sctx, cnfItems, cols, newTpSlice, lengths, true)
			if err != nil {
				return nil, nil, false, nil
			}
			ranges := res.Ranges
			accesses = res.AccessConds
			filters = res.RemainedConds
			if len(accesses) == 0 {
				return FullRange(), nil, true, nil
			}
			if len(filters) > 0 {
				hasResidual = true
			}
			totalRanges = append(totalRanges, ranges...)
			newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(sctx, accesses...))
		} else if firstColumnChecker.check(item) {
			if firstColumnChecker.shouldReserve {
				hasResidual = true
				firstColumnChecker.shouldReserve = lengths[0] != types.UnspecifiedLength
			}
			points := rb.build(item)
			ranges, err := points2Ranges(sc, points, newTpSlice[0])
			if err != nil {
				return nil, nil, false, errors.Trace(err)
			}
			totalRanges = append(totalRanges, ranges...)
			newAccessItems = append(newAccessItems, item)
		} else {
			return FullRange(), nil, true, nil
		}
	}

	totalRanges, err := unionRanges(sc, totalRanges)
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}

	return totalRanges, []expression.Expression{expression.ComposeDNFCondition(sctx, newAccessItems...)}, hasResidual, nil
}

// DetachRangeResult wraps up results when detaching conditions and builing ranges.
type DetachRangeResult struct {
	// Ranges is the ranges extracted and built from conditions.
	Ranges []*Range
	// AccessConds is the extracted conditions for access.
	AccessConds []expression.Expression
	// RemainedConds is the filter conditions which should be kept after access.
	RemainedConds []expression.Expression
	// EqCondCount is the number of equal conditions extracted.
	EqCondCount int
	// EqOrInCount is the number of equal/in conditions extracted.
	EqOrInCount int
	// IsDNFCond indicates if the top layer of conditions are in DNF.
	IsDNFCond bool
}

// DetachCondAndBuildRangeForIndex will detach the index filters from table filters.
// The returned values are encapsulated into a struct DetachRangeResult, see its comments for explanation.
func DetachCondAndBuildRangeForIndex(sctx sessionctx.Context, conditions []expression.Expression, cols []*expression.Column,
	lengths []int) (*DetachRangeResult, error) {
	res := &DetachRangeResult{}
	newTpSlice := make([]*types.FieldType, 0, len(cols))
	for _, col := range cols {
		newTpSlice = append(newTpSlice, newFieldType(col.RetType))
	}
	if len(conditions) == 1 {
		if sf, ok := conditions[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			ranges, accesses, hasResidual, err := detachDNFCondAndBuildRangeForIndex(sctx, sf, cols, newTpSlice, lengths)
			if err != nil {
				return res, errors.Trace(err)
			}
			res.Ranges = ranges
			res.AccessConds = accesses
			res.IsDNFCond = true
			// If this DNF have something cannot be to calculate range, then all this DNF should be pushed as filter condition.
			if hasResidual {
				res.RemainedConds = conditions
				return res, nil
			}
			return res, nil
		}
	}
	return detachCNFCondAndBuildRangeForIndex(sctx, conditions, cols, newTpSlice, lengths, true)
}

// DetachSimpleCondAndBuildRangeForIndex will detach the index filters from table filters.
// It will find the point query column firstly and then extract the range query column.
func DetachSimpleCondAndBuildRangeForIndex(sctx sessionctx.Context, conditions []expression.Expression,
	cols []*expression.Column, lengths []int) ([]*Range, []expression.Expression, error) {
	newTpSlice := make([]*types.FieldType, 0, len(cols))
	for _, col := range cols {
		newTpSlice = append(newTpSlice, newFieldType(col.RetType))
	}
	res, err := detachCNFCondAndBuildRangeForIndex(sctx, conditions, cols, newTpSlice, lengths, false)
	return res.Ranges, res.AccessConds, err
}

func removeAccessConditions(conditions, accessConds []expression.Expression) []expression.Expression {
	filterConds := make([]expression.Expression, 0, len(conditions))
	for _, cond := range conditions {
		if !expression.Contains(accessConds, cond) {
			filterConds = append(filterConds, cond)
		}
	}
	return filterConds
}

// ExtractAccessConditionsForColumn extracts the access conditions used for range calculation. Since
// we don't need to return the remained filter conditions, it is much simpler than DetachCondsForColumn.
func ExtractAccessConditionsForColumn(conds []expression.Expression, uniqueID int64) []expression.Expression {
	checker := conditionChecker{
		colUniqueID: uniqueID,
		length:      types.UnspecifiedLength,
	}
	accessConds := make([]expression.Expression, 0, 8)
	return expression.Filter(accessConds, conds, checker.check)
}

// DetachCondsForColumn detaches access conditions for specified column from other filter conditions.
func DetachCondsForColumn(sctx sessionctx.Context, conds []expression.Expression, col *expression.Column) (accessConditions, otherConditions []expression.Expression) {
	checker := &conditionChecker{
		colUniqueID: col.UniqueID,
		length:      types.UnspecifiedLength,
	}
	return detachColumnCNFConditions(sctx, conds, checker)
}
