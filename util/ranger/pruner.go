// Copyright 2020 PingCAP, Inc.
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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

// DetachCondAndBuildRangeForPartition will detach the index filters from table filters.
// The returned values are encapsulated into a struct DetachRangeResult, see its comments for explanation.
func DetachCondAndBuildRangeForPartition(sctx sessionctx.Context, conditions []expression.Expression, cols []*expression.Column,
	lengths []int) (*DetachRangeResult, error) {
	res := &DetachRangeResult{}
	newTpSlice := make([]*types.FieldType, 0, len(cols))
	for _, col := range cols {
		newTpSlice = append(newTpSlice, newFieldType(col.RetType))
	}
	if len(conditions) == 1 {
		if sf, ok := conditions[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			ranges, accesses, hasResidual, err := detachDNFCondAndBuildRangeForPartition(sctx, sf, cols, newTpSlice, lengths)
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
	return detachCNFCondAndBuildRangeForPartition(sctx, conditions, cols, newTpSlice, lengths, true)
}

// buildCNFIndexRange builds the range for index where the top layer is CNF.
func buildCNFPartitionRange(sc *stmtctx.StatementContext, cols []*expression.Column, newTp []*types.FieldType, lengths []int,
	eqAndInCount int, accessCondition []expression.Expression) ([]*Range, error) {
	rb := builder{sc: sc}
	var (
		ranges []*Range
		err    error
	)
	for _, col := range cols {
		newTp = append(newTp, newFieldType(col.RetType))
	}
	for i := 0; i < eqAndInCount; i++ {
		if sf, ok := accessCondition[i].(*expression.ScalarFunction); !ok || (sf.FuncName.L != ast.EQ && sf.FuncName.L != ast.In) {
			break
		}
		// Build ranges for equal or in access conditions.
		point := rb.build(accessCondition[i])
		if rb.err != nil {
			return nil, errors.Trace(rb.err)
		}
		if i == 0 {
			ranges, err = points2Ranges(sc, point, newTp[i])
		} else {
			ranges, err = appendPoints2Ranges(sc, ranges, point, newTp[i])
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
		ranges, err = points2Ranges(sc, rangePoints, newTp[0])
	} else if eqAndInCount < len(accessCondition) {
		ranges, err = appendPoints2Ranges(sc, ranges, rangePoints, newTp[eqAndInCount])
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Take prefix index into consideration.
	if hasPrefix(lengths) {
		if fixPrefixColRange(ranges, lengths, newTp) {
			ranges, err = UnionPartitionRanges(sc, ranges)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	return ranges, nil
}

// detachDNFCondAndBuildRangeForPartition will detach the partition columns filters from table filters when it's a DNF.
// We will detach the conditions of every DNF items, then compose them to a DNF.
func detachDNFCondAndBuildRangeForPartition(sctx sessionctx.Context, condition *expression.ScalarFunction,
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
			res, err := detachCNFCondAndBuildRangeForPartition(sctx, cnfItems, cols, newTpSlice, lengths, true)
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
	totalRanges, err := UnionPartitionRanges(sc, totalRanges)
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}

	return totalRanges, []expression.Expression{expression.ComposeDNFCondition(sctx, newAccessItems...)}, hasResidual, nil
}

// detachCNFCondAndBuildRangeForPartition will detach the partition filters from table filters. These conditions are connected with `and`
// It will first find the point query column and then extract the range query column.
// considerDNF is true means it will try to extract access conditions from the DNF expressions.
func detachCNFCondAndBuildRangeForPartition(sctx sessionctx.Context, conditions []expression.Expression, cols []*expression.Column,
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
		ranges, err = buildCNFPartitionRange(sctx.GetSessionVars().StmtCtx, cols, tpSlice, lengths, eqOrInCount, accessConds)
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
	ranges, err = buildCNFPartitionRange(sctx.GetSessionVars().StmtCtx, cols, tpSlice, lengths, eqOrInCount, accessConds)
	res.Ranges = ranges
	res.AccessConds = accessConds
	res.RemainedConds = filterConds
	return res, err
}
