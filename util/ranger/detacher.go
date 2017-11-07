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

func removeAccessConditions(conditions, accessConds []expression.Expression) []expression.Expression {
	for i := len(conditions) - 1; i >= 0; i-- {
		for _, cond := range accessConds {
			if cond == conditions[i] {
				conditions = append(conditions[:i], conditions[i+1:]...)
				break
			}
		}
	}
	return conditions
}

// DetachCondsForSelectivity detaches the conditions used for range calculation from other useless conditions.
func DetachCondsForSelectivity(conds []expression.Expression, rangeType int, cols []*expression.Column,
	lengths []int) (accessConditions, otherConditions []expression.Expression) {
	if rangeType == IntRangeType || rangeType == ColumnRangeType {
		return detachColumnConditions(conds, cols[0].ColName)
	} else if rangeType == IndexRangeType {
		return DetachIndexConditions(conds, cols, lengths)
	}
	return nil, conds
}

// detachColumnConditions distinguishes between access conditions and filter conditions from conditions.
func detachColumnConditions(conditions []expression.Expression, colName model.CIStr) ([]expression.Expression, []expression.Expression) {
	if colName.L == "" {
		return nil, conditions
	}

	var accessConditions, filterConditions []expression.Expression
	checker := conditionChecker{
		colName: colName,
		length:  types.UnspecifiedLength,
	}
	for _, cond := range conditions {
		cond = expression.PushDownNot(cond, false, nil)
		if !checker.check(cond) {
			filterConditions = append(filterConditions, cond)
			continue
		}
		accessConditions = append(accessConditions, cond)
		// TODO: it will lead to repeated computation cost.
		if checker.shouldReserve {
			filterConditions = append(filterConditions, cond)
			checker.shouldReserve = false
		}
	}

	return accessConditions, filterConditions
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
