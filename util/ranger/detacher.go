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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ranger

import (
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
)

// detachColumnCNFConditions detaches the condition for calculating range from the other conditions.
// Please make sure that the top level is CNF form.
func detachColumnCNFConditions(sctx sessionctx.Context, conditions []expression.Expression, checker *conditionChecker) ([]expression.Expression, []expression.Expression) {
	var accessConditions, filterConditions []expression.Expression
	for _, cond := range conditions {
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			dnfItems := expression.FlattenDNFConditions(sf)
			columnDNFItems, hasResidual := detachColumnDNFConditions(sctx, dnfItems, checker)
			// If this CNF has expression that cannot be resolved as access condition, then the total DNF expression
			// should be also appended into filter condition.
			if hasResidual {
				filterConditions = append(filterConditions, cond)
			}
			if len(columnDNFItems) == 0 {
				continue
			}
			rebuildDNF := expression.ComposeDNFCondition(sctx, columnDNFItems...)
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

// getPotentialEqOrInColOffset checks if the expression is a eq/le/ge/lt/gt function that one side is constant and another is column or an
// in function which is `column in (constant list)`.
// If so, it will return the offset of this column in the slice, otherwise return -1 for not found.
// Since combining `x >= 2` and `x <= 2` can lead to an eq condition `x = 2`, we take le/ge/lt/gt into consideration.
func getPotentialEqOrInColOffset(expr expression.Expression, cols []*expression.Column) int {
	f, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return -1
	}
	_, collation := expr.CharsetAndCollation(f.GetCtx())
	switch f.FuncName.L {
	case ast.LogicOr:
		dnfItems := expression.FlattenDNFConditions(f)
		offset := int(-1)
		for _, dnfItem := range dnfItems {
			curOffset := getPotentialEqOrInColOffset(dnfItem, cols)
			if curOffset == -1 {
				return -1
			}
			if offset != -1 && curOffset != offset {
				return -1
			}
			offset = curOffset
		}
		return offset
	case ast.EQ, ast.NullEQ, ast.LE, ast.GE, ast.LT, ast.GT:
		if c, ok := f.GetArgs()[0].(*expression.Column); ok {
			if c.RetType.EvalType() == types.ETString && !collate.CompatibleCollate(c.RetType.Collate, collation) {
				return -1
			}
			if (f.FuncName.L == ast.LT || f.FuncName.L == ast.GT) && c.RetType.EvalType() != types.ETInt {
				return -1
			}
			if constVal, ok := f.GetArgs()[1].(*expression.Constant); ok {
				val, err := constVal.Eval(chunk.Row{})
				if err != nil || val.IsNull() {
					// treat col<=>null as range scan instead of point get to avoid incorrect results
					// when nullable unique index has multiple matches for filter x is null
					return -1
				}
				for i, col := range cols {
					// When cols are a generated expression col, compare them in terms of virtual expr.
					if col.EqualByExprAndID(nil, c) {
						return i
					}
				}
			}
		}
		if c, ok := f.GetArgs()[1].(*expression.Column); ok {
			if c.RetType.EvalType() == types.ETString && !collate.CompatibleCollate(c.RetType.Collate, collation) {
				return -1
			}
			if (f.FuncName.L == ast.LT || f.FuncName.L == ast.GT) && c.RetType.EvalType() != types.ETInt {
				return -1
			}
			if constVal, ok := f.GetArgs()[0].(*expression.Constant); ok {
				val, err := constVal.Eval(chunk.Row{})
				if err != nil || val.IsNull() {
					return -1
				}
				for i, col := range cols {
					if col.Equal(nil, c) {
						return i
					}
				}
			}
		}
	case ast.In:
		c, ok := f.GetArgs()[0].(*expression.Column)
		if !ok {
			return -1
		}
		if c.RetType.EvalType() == types.ETString && !collate.CompatibleCollate(c.RetType.Collate, collation) {
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

// extractIndexPointRangesForCNF extracts a CNF item from the input CNF expressions, such that the CNF item
// is totally composed of point range filters.
// e.g, for input CNF expressions ((a,b) in ((1,1),(2,2))) and a > 1 and ((a,b,c) in (1,1,1),(2,2,2))
// ((a,b,c) in (1,1,1),(2,2,2)) would be extracted.
func extractIndexPointRangesForCNF(sctx sessionctx.Context, conds []expression.Expression, cols []*expression.Column, lengths []int) (*DetachRangeResult, int, []*valueInfo, error) {
	if len(conds) < 2 {
		return nil, -1, nil, nil
	}
	var r *DetachRangeResult
	columnValues := make([]*valueInfo, len(cols))
	maxNumCols := int(0)
	offset := int(-1)
	for i, cond := range conds {
		tmpConds := []expression.Expression{cond}
		colSets := expression.ExtractColumnSet(cond)
		if colSets.Len() == 0 {
			continue
		}
		res, err := DetachCondAndBuildRangeForIndex(sctx, tmpConds, cols, lengths)
		if err != nil {
			return nil, -1, nil, err
		}
		if len(res.Ranges) == 0 {
			return &DetachRangeResult{}, -1, nil, nil
		}
		// take the union of the two columnValues
		columnValues = unionColumnValues(columnValues, res.ColumnValues)
		if len(res.AccessConds) == 0 || len(res.RemainedConds) > 0 {
			continue
		}
		sameLens, allPoints := true, true
		numCols := int(0)
		for j, ran := range res.Ranges {
			if !ran.IsPoint(sctx.GetSessionVars().StmtCtx) {
				allPoints = false
				break
			}
			if j == 0 {
				numCols = len(ran.LowVal)
			} else if numCols != len(ran.LowVal) {
				sameLens = false
				break
			}
		}
		if !allPoints || !sameLens {
			continue
		}
		if numCols > maxNumCols {
			r = res
			offset = i
			maxNumCols = numCols
		}
	}
	if r != nil {
		r.IsDNFCond = false
	}
	return r, offset, columnValues, nil
}

func unionColumnValues(lhs, rhs []*valueInfo) []*valueInfo {
	if lhs == nil {
		return rhs
	}
	if rhs != nil {
		for i, valInfo := range lhs {
			if i >= len(rhs) {
				break
			}
			if valInfo == nil && rhs[i] != nil {
				lhs[i] = rhs[i]
			}
		}
	}
	return lhs
}

// detachCNFCondAndBuildRangeForIndex will detach the index filters from table filters. These conditions are connected with `and`
// It will first find the point query column and then extract the range query column.
// considerDNF is true means it will try to extract access conditions from the DNF expressions.
func (d *rangeDetacher) detachCNFCondAndBuildRangeForIndex(conditions []expression.Expression, tpSlice []*types.FieldType, considerDNF bool) (*DetachRangeResult, error) {
	var (
		eqCount int
		ranges  []*Range
		err     error
	)
	res := &DetachRangeResult{}

	accessConds, filterConds, newConditions, columnValues, emptyRange := ExtractEqAndInCondition(d.sctx, conditions, d.cols, d.lengths)
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
	ranges, err = d.buildCNFIndexRange(tpSlice, eqOrInCount, accessConds)
	if err != nil {
		return nil, err
	}

	// Though ranges are built from equal/in conditions, some range may not be a single point after UnionRanges in buildCNFIndexRange.
	// In order to prepare for the following appendRanges2PointRanges, we set d.mergeConsecutive to false and call buildCNFIndexRange
	// again to get pointRanges, in which each range must be a single point. If we use ranges rather than pointRanges when calling
	// appendRanges2PointRanges, wrong ranges would be calculated as issue https://github.com/pingcap/tidb/issues/26029 describes.
	mergeConsecutive := d.mergeConsecutive
	d.mergeConsecutive = false
	pointRanges, err := d.buildCNFIndexRange(tpSlice, eqOrInCount, accessConds)
	if err != nil {
		return nil, err
	}
	d.mergeConsecutive = mergeConsecutive

	res.Ranges = ranges
	res.AccessConds = accessConds
	res.RemainedConds = filterConds
	res.ColumnValues = columnValues
	if eqOrInCount == len(d.cols) || len(newConditions) == 0 {
		res.RemainedConds = append(res.RemainedConds, newConditions...)
		return res, nil
	}
	checker := &conditionChecker{
		checkerCol:    d.cols[eqOrInCount],
		length:        d.lengths[eqOrInCount],
		shouldReserve: d.lengths[eqOrInCount] != types.UnspecifiedLength,
	}
	if considerDNF {
		pointRes, offset, columnValues, err := extractIndexPointRangesForCNF(d.sctx, conditions, d.cols, d.lengths)
		if err != nil {
			return nil, err
		}
		res.ColumnValues = unionColumnValues(res.ColumnValues, columnValues)
		if pointRes != nil {
			if len(pointRes.Ranges) == 0 {
				return &DetachRangeResult{}, nil
			}
			if len(pointRes.Ranges[0].LowVal) > eqOrInCount {
				pointRes.ColumnValues = res.ColumnValues
				res = pointRes
				pointRanges = pointRes.Ranges
				eqOrInCount = len(res.Ranges[0].LowVal)
				newConditions = newConditions[:0]
				newConditions = append(newConditions, conditions[:offset]...)
				newConditions = append(newConditions, conditions[offset+1:]...)
				if eqOrInCount == len(d.cols) || len(newConditions) == 0 {
					res.RemainedConds = append(res.RemainedConds, newConditions...)
					return res, nil
				}
			}
		}
		if eqOrInCount > 0 {
			newCols := d.cols[eqOrInCount:]
			newLengths := d.lengths[eqOrInCount:]
			tailRes, err := DetachCondAndBuildRangeForIndex(d.sctx, newConditions, newCols, newLengths)
			if err != nil {
				return nil, err
			}
			if len(tailRes.Ranges) == 0 {
				return &DetachRangeResult{}, nil
			}
			if len(tailRes.AccessConds) > 0 {
				res.Ranges = appendRanges2PointRanges(pointRanges, tailRes.Ranges)
				res.AccessConds = append(res.AccessConds, tailRes.AccessConds...)
			}
			res.RemainedConds = append(res.RemainedConds, tailRes.RemainedConds...)
			// For cases like `((a = 1 and b = 1) or (a = 2 and b = 2)) and c = 1` on index (a,b,c), eqOrInCount is 2,
			// res.EqOrInCount is 0, and tailRes.EqOrInCount is 1. We should not set res.EqOrInCount to 1, otherwise,
			// `b = CorrelatedColumn` would be extracted as access conditions as well, which is not as expected at least for now.
			if res.EqOrInCount > 0 {
				if res.EqOrInCount == res.EqCondCount {
					res.EqCondCount = res.EqCondCount + tailRes.EqCondCount
				}
				res.EqOrInCount = res.EqOrInCount + tailRes.EqOrInCount
			}
			return res, nil
		}
		// `eqOrInCount` must be 0 when coming here.
		res.AccessConds, res.RemainedConds = detachColumnCNFConditions(d.sctx, newConditions, checker)
		ranges, err = d.buildCNFIndexRange(tpSlice, 0, res.AccessConds)
		if err != nil {
			return nil, err
		}
		res.Ranges = ranges
		return res, nil
	}
	for _, cond := range newConditions {
		if !checker.check(cond) {
			filterConds = append(filterConds, cond)
			continue
		}
		accessConds = append(accessConds, cond)
	}
	ranges, err = d.buildCNFIndexRange(tpSlice, eqOrInCount, accessConds)
	if err != nil {
		return nil, err
	}
	res.Ranges = ranges
	res.AccessConds = accessConds
	res.RemainedConds = filterConds
	return res, nil
}

// excludeToIncludeForIntPoint converts `(i` to `[i+1` and `i)` to `i-1]` if `i` is integer.
// For example, if p is `(3`, i.e., point { value: int(3), excl: true, start: true }, it is equal to `[4`, i.e., point { value: int(4), excl: false, start: true }.
// Similarly, if p is `8)`, i.e., point { value: int(8), excl: true, start: false}, it is equal to `7]`, i.e., point { value: int(7), excl: false, start: false }.
// If return value is nil, it means p is unsatisfiable. For example, `(MaxInt64` is unsatisfiable.
func excludeToIncludeForIntPoint(p *point) *point {
	if !p.excl {
		return p
	}
	if p.value.Kind() == types.KindInt64 {
		val := p.value.GetInt64()
		if p.start {
			if val == math.MaxInt64 {
				return nil
			}
			p.value.SetInt64(val + 1)
			p.excl = false
		} else {
			if val == math.MinInt64 {
				return nil
			}
			p.value.SetInt64(val - 1)
			p.excl = false
		}
	} else if p.value.Kind() == types.KindUint64 {
		val := p.value.GetUint64()
		if p.start {
			if val == math.MaxUint64 {
				return nil
			}
			p.value.SetUint64(val + 1)
			p.excl = false
		} else {
			if val == 0 {
				return nil
			}
			p.value.SetUint64(val - 1)
			p.excl = false
		}
	}
	return p
}

// If there exists an interval whose length is large than 0, return nil. Otherwise remove all unsatisfiable intervals
// and return array of single point intervals.
func allSinglePoints(sc *stmtctx.StatementContext, points []*point) []*point {
	pos := 0
	for i := 0; i < len(points); i += 2 {
		// Remove unsatisfiable interval. For example, (MaxInt64, +inf) and (-inf, MinInt64) is unsatisfiable.
		left := excludeToIncludeForIntPoint(points[i])
		if left == nil {
			continue
		}
		right := excludeToIncludeForIntPoint(points[i+1])
		if right == nil {
			continue
		}
		// If interval is not a single point, just return nil.
		if !left.start || right.start || left.excl || right.excl {
			return nil
		}
		cmp, err := left.value.CompareDatum(sc, &right.value)
		if err != nil || cmp != 0 {
			return nil
		}
		// If interval is a single point, add it back to array.
		points[pos] = left
		points[pos+1] = right
		pos += 2
	}
	return points[:pos]
}

func allEqOrIn(expr expression.Expression) bool {
	f, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	switch f.FuncName.L {
	case ast.LogicOr:
		for _, arg := range f.GetArgs() {
			if !allEqOrIn(arg) {
				return false
			}
		}
		return true
	case ast.EQ, ast.NullEQ, ast.In:
		return true
	}
	return false
}

func extractValueInfo(expr expression.Expression) *valueInfo {
	if f, ok := expr.(*expression.ScalarFunction); ok && (f.FuncName.L == ast.EQ || f.FuncName.L == ast.NullEQ) {
		getValueInfo := func(c *expression.Constant) *valueInfo {
			mutable := c.ParamMarker != nil || c.DeferredExpr != nil
			var value *types.Datum
			if !mutable {
				value = &c.Value
			}
			return &valueInfo{mutable, value}
		}
		if c, ok := f.GetArgs()[0].(*expression.Constant); ok {
			return getValueInfo(c)
		}
		if c, ok := f.GetArgs()[1].(*expression.Constant); ok {
			return getValueInfo(c)
		}
	}
	return nil
}

// ExtractEqAndInCondition will split the given condition into three parts by the information of index columns and their lengths.
// accesses: The condition will be used to build range.
// filters: filters is the part that some access conditions need to be evaluate again since it's only the prefix part of char column.
// newConditions: We'll simplify the given conditions if there're multiple in conditions or eq conditions on the same column.
//   e.g. if there're a in (1, 2, 3) and a in (2, 3, 4). This two will be combined to a in (2, 3) and pushed to newConditions.
// columnValues: the constant column values for all index columns. columnValues[i] is nil if cols[i] is not constant.
// bool: indicate whether there's nil range when merging eq and in conditions.
func ExtractEqAndInCondition(sctx sessionctx.Context, conditions []expression.Expression, cols []*expression.Column,
	lengths []int) ([]expression.Expression, []expression.Expression, []expression.Expression, []*valueInfo, bool) {
	var filters []expression.Expression
	rb := builder{sc: sctx.GetSessionVars().StmtCtx}
	accesses := make([]expression.Expression, len(cols))
	points := make([][]*point, len(cols))
	mergedAccesses := make([]expression.Expression, len(cols))
	newConditions := make([]expression.Expression, 0, len(conditions))
	columnValues := make([]*valueInfo, len(cols))
	offsets := make([]int, len(conditions))
	for i, cond := range conditions {
		offset := getPotentialEqOrInColOffset(cond, cols)
		offsets[i] = offset
		if offset == -1 {
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
		if len(points[offset]) == 0 { // Early termination if false expression found
			if expression.MaybeOverOptimized4PlanCache(sctx, conditions) {
				// cannot return an empty-range for plan-cache since the range may become non-empty as parameters change
				// for safety, return the whole conditions in this case
				return nil, conditions, nil, nil, false
			}
			return nil, nil, nil, nil, true
		}
	}
	for i, ma := range mergedAccesses {
		if ma == nil {
			if accesses[i] != nil {
				if allEqOrIn(accesses[i]) {
					newConditions = append(newConditions, accesses[i])
					columnValues[i] = extractValueInfo(accesses[i])
				} else {
					accesses[i] = nil
				}
			}
			continue
		}
		points[i] = allSinglePoints(sctx.GetSessionVars().StmtCtx, points[i])
		if points[i] == nil {
			// There exists an interval whose length is larger than 0
			accesses[i] = nil
		} else if len(points[i]) == 0 { // Early termination if false expression found
			if expression.MaybeOverOptimized4PlanCache(sctx, conditions) {
				// cannot return an empty-range for plan-cache since the range may become non-empty as parameters change
				// for safety, return the whole conditions in this case
				return nil, conditions, nil, nil, false
			}
			return nil, nil, nil, nil, true
		} else {
			// All Intervals are single points
			accesses[i] = points2EqOrInCond(sctx, points[i], cols[i])
			newConditions = append(newConditions, accesses[i])
			if f, ok := accesses[i].(*expression.ScalarFunction); ok && f.FuncName.L == ast.EQ {
				// Actually the constant column value may not be mutable. Here we assume it is mutable to keep it simple.
				// Maybe we can improve it later.
				columnValues[i] = &valueInfo{mutable: true}
			}
			if expression.MaybeOverOptimized4PlanCache(sctx, conditions) {
				// TODO: optimize it more elaborately, e.g. return [2 3, 2 3] as accesses for 'where a = 2 and b = 3 and c >= ? and c <= ?'
				return nil, conditions, nil, nil, false
			}
		}
	}
	for i, offset := range offsets {
		if offset == -1 || accesses[offset] == nil {
			newConditions = append(newConditions, conditions[i])
		}
	}
	for i, cond := range accesses {
		if cond == nil {
			accesses = accesses[:i]
			break
		}

		// Currently, if the access cond is on a prefix index, we will also add this cond to table filters.
		// A possible optimization is that, if the value in the cond is shorter than the length of the prefix index, we don't
		// need to add this cond to table filters.
		// e.g. CREATE TABLE t(a varchar(10), index i(a(5)));  SELECT * FROM t USE INDEX i WHERE a > 'aaa';
		// However, please notice that if you're implementing this, please (1) set StatementContext.OptimDependOnMutableConst to true,
		// or (2) don't do this optimization when StatementContext.UseCache is true. That's because this plan is affected by
		// flen of user variable, we cannot cache this plan.
		if lengths[i] != types.UnspecifiedLength {
			filters = append(filters, cond)
		}
	}
	// We should remove all accessConds, so that they will not be added to filter conditions.
	newConditions = removeAccessConditions(newConditions, accesses)
	return accesses, filters, newConditions, columnValues, false
}

// detachDNFCondAndBuildRangeForIndex will detach the index filters from table filters when it's a DNF.
// We will detach the conditions of every DNF items, then compose them to a DNF.
func (d *rangeDetacher) detachDNFCondAndBuildRangeForIndex(condition *expression.ScalarFunction, newTpSlice []*types.FieldType) ([]*Range, []expression.Expression, []*valueInfo, bool, error) {
	sc := d.sctx.GetSessionVars().StmtCtx
	firstColumnChecker := &conditionChecker{
		checkerCol:    d.cols[0],
		shouldReserve: d.lengths[0] != types.UnspecifiedLength,
		length:        d.lengths[0],
	}
	rb := builder{sc: sc}
	dnfItems := expression.FlattenDNFConditions(condition)
	newAccessItems := make([]expression.Expression, 0, len(dnfItems))
	var totalRanges []*Range
	columnValues := make([]*valueInfo, len(d.cols))
	hasResidual := false
	for i, item := range dnfItems {
		if sf, ok := item.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			cnfItems := expression.FlattenCNFConditions(sf)
			var accesses, filters []expression.Expression
			res, err := d.detachCNFCondAndBuildRangeForIndex(cnfItems, newTpSlice, true)
			if err != nil {
				return nil, nil, nil, false, err
			}
			ranges := res.Ranges
			accesses = res.AccessConds
			filters = res.RemainedConds
			if len(accesses) == 0 {
				return FullRange(), nil, nil, true, nil
			}
			if len(filters) > 0 {
				hasResidual = true
			}
			totalRanges = append(totalRanges, ranges...)
			newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(d.sctx, accesses...))
			if res.ColumnValues != nil {
				if i == 0 {
					columnValues = res.ColumnValues
				} else {
					// take the intersection of the two columnValues
					for j, valInfo := range columnValues {
						if valInfo == nil {
							continue
						}
						sameValue, err := isSameValue(d.sctx.GetSessionVars().StmtCtx, valInfo, res.ColumnValues[j])
						if err != nil {
							return nil, nil, nil, false, errors.Trace(err)
						}
						if !sameValue {
							columnValues[j] = nil
						}
					}
				}
			}
		} else if firstColumnChecker.check(item) {
			if firstColumnChecker.shouldReserve {
				hasResidual = true
				firstColumnChecker.shouldReserve = d.lengths[0] != types.UnspecifiedLength
			}
			points := rb.build(item)
			ranges, err := points2Ranges(sc, points, newTpSlice[0])
			if err != nil {
				return nil, nil, nil, false, errors.Trace(err)
			}
			totalRanges = append(totalRanges, ranges...)
			newAccessItems = append(newAccessItems, item)
			if i == 0 {
				columnValues[0] = extractValueInfo(item)
			} else if columnValues[0] != nil {
				valInfo := extractValueInfo(item)
				sameValue, err := isSameValue(d.sctx.GetSessionVars().StmtCtx, columnValues[0], valInfo)
				if err != nil {
					return nil, nil, nil, false, errors.Trace(err)
				}
				if !sameValue {
					columnValues[0] = nil
				}
			}
		} else {
			return FullRange(), nil, nil, true, nil
		}
	}

	// Take prefix index into consideration.
	if hasPrefix(d.lengths) {
		fixPrefixColRange(totalRanges, d.lengths, newTpSlice)
	}
	totalRanges, err := UnionRanges(sc, totalRanges, d.mergeConsecutive)
	if err != nil {
		return nil, nil, nil, false, errors.Trace(err)
	}

	return totalRanges, []expression.Expression{expression.ComposeDNFCondition(d.sctx, newAccessItems...)}, columnValues, hasResidual, nil
}

// valueInfo is used for recording the constant column value in DetachCondAndBuildRangeForIndex.
type valueInfo struct {
	mutable bool         // If true, the constant column value depends on mutable constant.
	value   *types.Datum // If not mutable, value is the constant column value. Otherwise value is nil.
}

func isSameValue(sc *stmtctx.StatementContext, lhs, rhs *valueInfo) (bool, error) {
	// We assume `lhs` and `rhs` are not the same when either `lhs` or `rhs` is mutable to keep it simple. If we consider
	// mutable valueInfo, we need to set `sc.OptimDependOnMutableConst = true`, which makes the plan not able to be cached.
	// On the other hand, the equal condition may not be used for optimization. Hence we simply regard mutable valueInfos different
	// from others. Maybe we can improve it later.
	// TODO: is `lhs.value.Kind() != rhs.value.Kind()` necessary?
	if lhs == nil || rhs == nil || lhs.mutable || rhs.mutable || lhs.value.Kind() != rhs.value.Kind() {
		return false, nil
	}
	collation := lhs.value.Collation()
	// binary collator may not the best choice, but it can make sure the result is correct.
	lhs.value.SetCollation(charset.CollationBin)
	cmp, err := lhs.value.CompareDatum(sc, rhs.value)
	lhs.value.SetCollation(collation)
	if err != nil {
		return false, err
	}
	return cmp == 0, nil
}

// DetachRangeResult wraps up results when detaching conditions and builing ranges.
type DetachRangeResult struct {
	// Ranges is the ranges extracted and built from conditions.
	Ranges []*Range
	// AccessConds is the extracted conditions for access.
	AccessConds []expression.Expression
	// RemainedConds is the filter conditions which should be kept after access.
	RemainedConds []expression.Expression
	// ColumnValues records the constant column values for all index columns.
	// For the ith column, if it is evaluated as constant, ColumnValues[i] is its value. Otherwise ColumnValues[i] is nil.
	ColumnValues []*valueInfo
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
	d := &rangeDetacher{
		sctx:             sctx,
		allConds:         conditions,
		cols:             cols,
		lengths:          lengths,
		mergeConsecutive: true,
	}
	return d.detachCondAndBuildRangeForCols()
}

type rangeDetacher struct {
	sctx             sessionctx.Context
	allConds         []expression.Expression
	cols             []*expression.Column
	lengths          []int
	mergeConsecutive bool
}

func (d *rangeDetacher) detachCondAndBuildRangeForCols() (*DetachRangeResult, error) {
	res := &DetachRangeResult{}
	newTpSlice := make([]*types.FieldType, 0, len(d.cols))
	for _, col := range d.cols {
		newTpSlice = append(newTpSlice, newFieldType(col.RetType))
	}
	if len(d.allConds) == 1 {
		if sf, ok := d.allConds[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			ranges, accesses, columnValues, hasResidual, err := d.detachDNFCondAndBuildRangeForIndex(sf, newTpSlice)
			if err != nil {
				return res, errors.Trace(err)
			}
			res.Ranges = ranges
			res.AccessConds = accesses
			res.ColumnValues = columnValues
			res.IsDNFCond = true
			// If this DNF have something cannot be to calculate range, then all this DNF should be pushed as filter condition.
			if hasResidual {
				res.RemainedConds = d.allConds
				return res, nil
			}
			return res, nil
		}
	}
	return d.detachCNFCondAndBuildRangeForIndex(d.allConds, newTpSlice, true)
}

// DetachSimpleCondAndBuildRangeForIndex will detach the index filters from table filters.
// It will find the point query column firstly and then extract the range query column.
func DetachSimpleCondAndBuildRangeForIndex(sctx sessionctx.Context, conditions []expression.Expression,
	cols []*expression.Column, lengths []int) ([]*Range, []expression.Expression, error) {
	newTpSlice := make([]*types.FieldType, 0, len(cols))
	for _, col := range cols {
		newTpSlice = append(newTpSlice, newFieldType(col.RetType))
	}
	d := &rangeDetacher{
		sctx:             sctx,
		allConds:         conditions,
		cols:             cols,
		lengths:          lengths,
		mergeConsecutive: true,
	}
	res, err := d.detachCNFCondAndBuildRangeForIndex(conditions, newTpSlice, false)
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
func ExtractAccessConditionsForColumn(conds []expression.Expression, col *expression.Column) []expression.Expression {
	checker := conditionChecker{
		checkerCol: col,
		length:     types.UnspecifiedLength,
	}
	accessConds := make([]expression.Expression, 0, 8)
	return expression.Filter(accessConds, conds, checker.check)
}

// DetachCondsForColumn detaches access conditions for specified column from other filter conditions.
func DetachCondsForColumn(sctx sessionctx.Context, conds []expression.Expression, col *expression.Column) (accessConditions, otherConditions []expression.Expression) {
	checker := &conditionChecker{
		checkerCol: col,
		length:     types.UnspecifiedLength,
	}
	return detachColumnCNFConditions(sctx, conds, checker)
}

// MergeDNFItems4Col receives a slice of DNF conditions, merges some of them which can be built into ranges on a single column, then returns.
// For example, [a > 5, b > 6, c > 7, a = 1, b > 3] will become [a > 5 or a = 1, b > 6 or b > 3, c > 7].
func MergeDNFItems4Col(ctx sessionctx.Context, dnfItems []expression.Expression) []expression.Expression {
	mergedDNFItems := make([]expression.Expression, 0, len(dnfItems))
	col2DNFItems := make(map[int64][]expression.Expression)
	for _, dnfItem := range dnfItems {
		cols := expression.ExtractColumns(dnfItem)
		// If this condition contains multiple columns, we can't merge it.
		// If this column is _tidb_rowid, we also can't merge it since Selectivity() doesn't handle it, or infinite recursion will happen.
		if len(cols) != 1 || cols[0].ID == model.ExtraHandleID {
			mergedDNFItems = append(mergedDNFItems, dnfItem)
			continue
		}

		uniqueID := cols[0].UniqueID
		checker := &conditionChecker{
			checkerCol: cols[0],
			length:     types.UnspecifiedLength,
		}
		// If we can't use this condition to build range, we can't merge it.
		// Currently, we assume if every condition in a DNF expression can pass this check, then `Selectivity` must be able to
		// cover this entire DNF directly without recursively call `Selectivity`. If this doesn't hold in the future, this logic
		// may cause infinite recursion in `Selectivity`.
		if !checker.check(dnfItem) {
			mergedDNFItems = append(mergedDNFItems, dnfItem)
			continue
		}

		col2DNFItems[uniqueID] = append(col2DNFItems[uniqueID], dnfItem)
	}
	for _, items := range col2DNFItems {
		mergedDNFItems = append(mergedDNFItems, expression.ComposeDNFCondition(ctx, items...))
	}
	return mergedDNFItems
}
