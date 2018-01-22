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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/types"
)

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
			checker.shouldReserve = checker.length != types.UnspecifiedLength
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
			// If one part of DNF has no access condition. Then this DNF cannot get range.
			if len(columnCNFItems) == 0 {
				return nil, true
			}
			rebuildCNF := expression.ComposeCNFCondition(nil, columnCNFItems...)
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
			if col.Equal(c, nil) {
				return i
			}
		}
	}
	return -1
}

func extractAccessAndFilterConds(conditions, accessConds, filterConds []expression.Expression,
	col *expression.Column, length int, simple bool) ([]expression.Expression, []expression.Expression) {
	checker := &conditionChecker{
		colName:       col.ColName,
		length:        length,
		shouldReserve: length != types.UnspecifiedLength,
	}
	if simple {
		for _, cond := range conditions {
			if !checker.check(cond) {
				filterConds = append(filterConds, cond)
				continue
			}
			accessConds = append(accessConds, cond)
			// TODO: It will lead to repeated computation cost.
			if checker.shouldReserve {
				filterConds = append(filterConds, cond)
				checker.shouldReserve = checker.length != types.UnspecifiedLength
			}
		}
		return accessConds, filterConds
	}
	accesses, filters := detachColumnCNFConditions(conditions, checker)
	accessConds = append(accessConds, accesses...)
	filterConds = append(filterConds, filters...)
	return accessConds, filterConds
}

// DetachCNFIndexConditions will detach the index filters from table filters. These conditions are connected with `and`
// It will first find the point query column and then extract the range query column.
// Simple is true means it will not take a deep look into the DNF conditions.
func DetachCNFIndexConditions(conditions []expression.Expression, cols []*expression.Column,
	lengths []int, simple bool) (accessConds, filterConds []expression.Expression, eqOrInCount int) {
	accessConds = make([]expression.Expression, len(cols))
	for _, cond := range conditions {
		offset := getEqOrInColOffset(cond, cols)
		if offset != -1 {
			accessConds[offset] = cond
		}
	}
	for i, cond := range accessConds {
		if cond == nil {
			accessConds = accessConds[:i]
			eqOrInCount = i
			break
		}
		if lengths[i] != types.UnspecifiedLength {
			filterConds = append(filterConds, cond)
		}
		if i == len(accessConds)-1 {
			eqOrInCount = len(accessConds)
		}
	}
	// We should remove all accessConds, so that they will not be added to filter conditions.
	conditions = removeAccessConditions(conditions, accessConds)
	if eqOrInCount == len(cols) {
		// If curIndex equals to len of index columns, it means the rest conditions haven't been appended to filter conditions.
		filterConds = append(filterConds, conditions...)
		return accessConds, filterConds, eqOrInCount
	}
	accessConds, filterConds = extractAccessAndFilterConds(conditions, accessConds, filterConds, cols[eqOrInCount], lengths[eqOrInCount], simple)
	return accessConds, filterConds, eqOrInCount
}

// detachDNFIndexConditions will detach the index filters from table filters when it's a DNF.
// We will detach the conditions of every DNF items, then compose them to a DNF.
func detachDNFIndexConditions(condition *expression.ScalarFunction, cols []*expression.Column,
	lengths []int) (accessConds []expression.Expression, hasResidual bool, eqAndInCounts []int) {
	newAccessItems := make([]expression.Expression, 0, len(condition.GetArgs()))
	firstColumnChecker := &conditionChecker{
		colName:       cols[0].ColName,
		shouldReserve: lengths[0] != types.UnspecifiedLength,
		length:        lengths[0],
	}
	dnfItems := expression.FlattenDNFConditions(condition)
	eqAndInCounts = make([]int, 0, len(dnfItems))
	for _, item := range dnfItems {
		if sf, ok := item.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			cnfItems := expression.FlattenCNFConditions(sf)
			accesses, filters, eqAndInCount := DetachCNFIndexConditions(cnfItems, cols, lengths, false)
			eqAndInCounts = append(eqAndInCounts, eqAndInCount)
			// If one part of DNF has no access condition. Then this DNF cannot get range.
			if len(accesses) == 0 {
				return nil, true, nil
			}
			if len(filters) > 0 {
				hasResidual = true
			}
			newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(nil, accesses...))
		} else if ok := firstColumnChecker.check(item); ok {
			newAccessItems = append(newAccessItems, item)
			eqAndInCounts = append(eqAndInCounts, 0)
			if firstColumnChecker.shouldReserve {
				hasResidual = true
				firstColumnChecker.shouldReserve = lengths[0] != types.UnspecifiedLength
			}
		} else {
			return nil, true, nil
		}
	}

	return newAccessItems, hasResidual, eqAndInCounts
}

// DetachIndexConditions will detach the index filters from table filters.
// If the top layer is CNF, then we will return the eqAndInCount, otherwise we just return -1.
func DetachIndexConditions(conditions []expression.Expression, cols []*expression.Column,
	lengths []int) (accessConds, filterConds []expression.Expression, eqAndInCounts []int, isCNF bool) {
	if len(conditions) == 1 {
		if sf, ok := conditions[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			var hasResidual bool
			accessConds, hasResidual, eqAndInCounts = detachDNFIndexConditions(sf, cols, lengths)
			// If this DNF have something cannot be to calculate range, then all this DNF should be pushed as filter condition.
			if hasResidual {
				return accessConds, conditions, eqAndInCounts, false
			}
			return accessConds, nil, eqAndInCounts, false
		}
	}
	accessConds, filterConds, eqAndInCount := DetachCNFIndexConditions(conditions, cols, lengths, false)
	return accessConds, filterConds, []int{eqAndInCount}, true
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

// ExtractAccessConditions detaches the access conditions used for range calculation.
func ExtractAccessConditions(conds []expression.Expression, rangeType RangeType, cols []*expression.Column,
	lengths []int) (accessConds []expression.Expression, eqAndInCount int) {
	switch rangeType {
	case IntRangeType, ColumnRangeType:
		return extractColumnConditions(conds, cols[0].ColName), 0
	case IndexRangeType:
		accessConds, _, eqAndInCount = DetachCNFIndexConditions(conds, cols, lengths, true)
		return accessConds, eqAndInCount
	}
	return nil, 0
}

func extractColumnConditions(conds []expression.Expression, colName model.CIStr) []expression.Expression {
	if colName.L == "" {
		return nil
	}
	checker := conditionChecker{
		colName: colName,
		length:  types.UnspecifiedLength,
	}
	accessConds := make([]expression.Expression, 0, 8)
	return expression.Filter(accessConds, conds, checker.check)
}

// DetachCondsForTableRange detaches the conditions used for range calculation form other useless conditions for
// calculating the table range.
func DetachCondsForTableRange(ctx context.Context, conds []expression.Expression, col *expression.Column) (accessContditions, otherConditions []expression.Expression) {
	checker := &conditionChecker{
		colName: col.ColName,
		length:  types.UnspecifiedLength,
	}
	return detachColumnCNFConditions(conds, checker)
}
