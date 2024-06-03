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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
)

// detachColumnCNFConditions detaches the condition for calculating range from the other conditions.
// Please make sure that the top level is CNF form.
func detachColumnCNFConditions(sctx expression.BuildContext, conditions []expression.Expression, checker *conditionChecker) ([]expression.Expression, []expression.Expression) {
	var accessConditions, filterConditions []expression.Expression //nolint: prealloc
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
		isAccessCond, shouldReserve := checker.check(cond)
		if !isAccessCond {
			filterConditions = append(filterConditions, cond)
			continue
		}
		accessConditions = append(accessConditions, cond)
		if shouldReserve {
			filterConditions = append(filterConditions, cond)
		}
	}
	return accessConditions, filterConditions
}

// detachColumnDNFConditions detaches the condition for calculating range from the other conditions.
// Please make sure that the top level is DNF form.
func detachColumnDNFConditions(sctx expression.BuildContext, conditions []expression.Expression, checker *conditionChecker) ([]expression.Expression, bool) {
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
		} else {
			isAccessCond, shouldReserve := checker.check(cond)
			if !isAccessCond {
				return nil, true
			}
			accessConditions = append(accessConditions, cond)
			if shouldReserve {
				hasResidualConditions = true
			}
		}
	}
	return accessConditions, hasResidualConditions
}

// getPotentialEqOrInColOffset checks if the expression is a eq/le/ge/lt/gt function that one side is constant and another is column or an
// in function which is `column in (constant list)`.
// If so, it will return the offset of this column in the slice, otherwise return -1 for not found.
// Since combining `x >= 2` and `x <= 2` can lead to an eq condition `x = 2`, we take le/ge/lt/gt into consideration.
func getPotentialEqOrInColOffset(sctx *rangerctx.RangerContext, expr expression.Expression, cols []*expression.Column) int {
	evalCtx := sctx.ExprCtx.GetEvalCtx()
	f, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return -1
	}
	_, collation := expr.CharsetAndCollation()
	switch f.FuncName.L {
	case ast.LogicOr:
		dnfItems := expression.FlattenDNFConditions(f)
		offset := int(-1)
		for _, dnfItem := range dnfItems {
			curOffset := getPotentialEqOrInColOffset(sctx, dnfItem, cols)
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
			if c.RetType.EvalType() == types.ETString && !collate.CompatibleCollate(c.RetType.GetCollate(), collation) {
				return -1
			}
			if (f.FuncName.L == ast.LT || f.FuncName.L == ast.GT) && c.RetType.EvalType() != types.ETInt {
				return -1
			}
			if constVal, ok := f.GetArgs()[1].(*expression.Constant); ok {
				val, err := constVal.Eval(evalCtx, chunk.Row{})
				if err != nil || (!sctx.RegardNULLAsPoint && val.IsNull()) {
					// treat col<=>null as range scan instead of point get to avoid incorrect results
					// when nullable unique index has multiple matches for filter x is null
					return -1
				}
				for i, col := range cols {
					// When cols are a generated expression col, compare them in terms of virtual expr.
					if col.EqualByExprAndID(evalCtx, c) {
						return i
					}
				}
			}
		}
		if c, ok := f.GetArgs()[1].(*expression.Column); ok {
			if c.RetType.EvalType() == types.ETString && !collate.CompatibleCollate(c.RetType.GetCollate(), collation) {
				return -1
			}
			if (f.FuncName.L == ast.LT || f.FuncName.L == ast.GT) && c.RetType.EvalType() != types.ETInt {
				return -1
			}
			if constVal, ok := f.GetArgs()[0].(*expression.Constant); ok {
				val, err := constVal.Eval(evalCtx, chunk.Row{})
				if err != nil || (!sctx.RegardNULLAsPoint && val.IsNull()) {
					return -1
				}
				for i, col := range cols {
					if col.EqualColumn(c) {
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
		if c.RetType.EvalType() == types.ETString && !collate.CompatibleCollate(c.RetType.GetCollate(), collation) {
			return -1
		}
		for _, arg := range f.GetArgs()[1:] {
			if _, ok := arg.(*expression.Constant); !ok {
				return -1
			}
		}
		for i, col := range cols {
			if col.EqualColumn(c) {
				return i
			}
		}
	}
	return -1
}

type cnfItemRangeResult struct {
	rangeResult *DetachRangeResult
	offset      int
	// sameLenPointRanges means that each range is point range and all of them have the same column numbers(i.e., maxColNum = minColNum).
	sameLenPointRanges bool
	maxColNum          int
	minColNum          int
}

func getCNFItemRangeResult(sctx *rangerctx.RangerContext, rangeResult *DetachRangeResult, offset int) *cnfItemRangeResult {
	sameLenPointRanges := true
	var maxColNum, minColNum int
	for i, ran := range rangeResult.Ranges {
		if !ran.IsPoint(sctx) {
			sameLenPointRanges = false
		}
		if i == 0 {
			maxColNum = len(ran.LowVal)
			minColNum = len(ran.LowVal)
		} else {
			maxColNum = max(maxColNum, len(ran.LowVal))
			minColNum = min(minColNum, len(ran.LowVal))
		}
	}
	if minColNum != maxColNum {
		sameLenPointRanges = false
	}
	return &cnfItemRangeResult{
		rangeResult:        rangeResult,
		offset:             offset,
		sameLenPointRanges: sameLenPointRanges,
		maxColNum:          maxColNum,
		minColNum:          minColNum,
	}
}

func compareCNFItemRangeResult(curResult, bestResult *cnfItemRangeResult) (curIsBetter bool) {
	if curResult.sameLenPointRanges && bestResult.sameLenPointRanges {
		return curResult.minColNum > bestResult.minColNum
	}
	if !curResult.sameLenPointRanges && !bestResult.sameLenPointRanges {
		if curResult.minColNum == bestResult.minColNum {
			return curResult.maxColNum > bestResult.maxColNum
		}
		return curResult.minColNum > bestResult.minColNum
	}
	// Point ranges is better than non-point ranges since we can append subsequent column ranges to point ranges.
	return curResult.sameLenPointRanges
}

// extractBestCNFItemRanges builds ranges for each CNF item from the input CNF expressions and returns the best CNF
// item ranges.
// e.g, for input CNF expressions ((a,b) in ((1,1),(2,2))) and a > 1 and ((a,b,c) in (1,1,1),(2,2,2))
// ((a,b,c) in (1,1,1),(2,2,2)) would be extracted.
func extractBestCNFItemRanges(sctx *rangerctx.RangerContext, conds []expression.Expression, cols []*expression.Column,
	lengths []int, rangeMaxSize int64, convertToSortKey bool) (*cnfItemRangeResult, []*valueInfo, error) {
	if len(conds) < 2 {
		return nil, nil, nil
	}
	var bestRes *cnfItemRangeResult
	columnValues := make([]*valueInfo, len(cols))
	for i, cond := range conds {
		tmpConds := []expression.Expression{cond}
		colSets := expression.ExtractColumnSet(cond)
		if colSets.Len() == 0 {
			continue
		}
		// When we build ranges for the CNF item, we choose not to merge consecutive ranges because we hope to get point
		// ranges here. See https://github.com/pingcap/tidb/issues/41572 for more details.
		//
		// Here is an example. Assume that the index is `idx(a,b,c)` and the condition is `((a,b) in ((1,1),(1,2)) and c = 1`.
		// We build ranges for `(a,b) in ((1,1),(1,2))` and get `[1 1, 1 1] [1 2, 1 2]`, which are point ranges and we can
		// append `c = 1` to the point ranges. However, if we choose to merge consecutive ranges here, we get `[1 1, 1 2]`,
		// which are not point ranges, and we cannot append `c = 1` anymore.
		res, err := detachCondAndBuildRangeWithoutMerging(sctx, tmpConds, cols, lengths, rangeMaxSize, convertToSortKey)
		if err != nil {
			return nil, nil, err
		}
		if len(res.Ranges) == 0 {
			return &cnfItemRangeResult{rangeResult: res, offset: i}, nil, nil
		}
		// take the union of the two columnValues
		columnValues = unionColumnValues(columnValues, res.ColumnValues)
		if len(res.AccessConds) == 0 || len(res.RemainedConds) > 0 {
			continue
		}
		curRes := getCNFItemRangeResult(sctx, res, i)
		if bestRes == nil || compareCNFItemRangeResult(curRes, bestRes) {
			bestRes = curRes
		}
	}
	if bestRes != nil && bestRes.rangeResult != nil {
		bestRes.rangeResult.IsDNFCond = false
	}
	return bestRes, columnValues, nil
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
func (d *rangeDetacher) detachCNFCondAndBuildRangeForIndex(conditions []expression.Expression, newTpSlice []*types.FieldType, considerDNF bool) (*DetachRangeResult, error) {
	var (
		eqCount int
		ranges  Ranges
		err     error
	)
	res := &DetachRangeResult{}

	accessConds, filterConds, newConditions, columnValues, emptyRange := ExtractEqAndInCondition(d.sctx, conditions, d.cols, d.lengths)
	if emptyRange {
		return res, nil
	}
	var remainedConds []expression.Expression
	ranges, accessConds, remainedConds, err = d.buildRangeOnColsByCNFCond(newTpSlice, len(accessConds), accessConds)
	if err != nil {
		return nil, err
	}
	if len(remainedConds) > 0 {
		filterConds = removeConditions(filterConds, remainedConds)
		newConditions = append(newConditions, remainedConds...)
	}
	for ; eqCount < len(accessConds); eqCount++ {
		if accessConds[eqCount].(*expression.ScalarFunction).FuncName.L != ast.EQ {
			break
		}
	}
	eqOrInCount := len(accessConds)
	res.EqCondCount = eqCount
	res.EqOrInCount = eqOrInCount
	// If index has prefix column and d.mergeConsecutive is true, ranges may not be point ranges anymore after UnionRanges.
	// Therefore, we need to calculate pointRanges separately so that it can be used to append tail ranges in considerDNF branch.
	// See https://github.com/pingcap/tidb/issues/26029 for details.
	var pointRanges Ranges
	if hasPrefix(d.lengths) {
		if d.mergeConsecutive {
			pointRanges = make(Ranges, 0, len(ranges))
			for _, ran := range ranges {
				pointRanges = append(pointRanges, ran.Clone())
			}
			ranges, err = UnionRanges(d.sctx, ranges, d.mergeConsecutive)
			if err != nil {
				return nil, errors.Trace(err)
			}
			pointRanges, err = UnionRanges(d.sctx, pointRanges, false)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			ranges, err = UnionRanges(d.sctx, ranges, d.mergeConsecutive)
			if err != nil {
				return nil, errors.Trace(err)
			}
			pointRanges = ranges
		}
	} else {
		pointRanges = ranges
	}

	res.Ranges = ranges
	res.AccessConds = accessConds
	res.RemainedConds = filterConds
	res.ColumnValues = columnValues
	if eqOrInCount == len(d.cols) || len(newConditions) == 0 {
		res.RemainedConds = append(res.RemainedConds, newConditions...)
		return res, nil
	}
	checker := &conditionChecker{
		checkerCol:               d.cols[eqOrInCount],
		length:                   d.lengths[eqOrInCount],
		optPrefixIndexSingleScan: d.sctx.OptPrefixIndexSingleScan,
		ctx:                      d.sctx.ExprCtx.GetEvalCtx(),
	}
	if considerDNF {
		bestCNFItemRes, columnValues, err := extractBestCNFItemRanges(d.sctx, conditions, d.cols, d.lengths, d.rangeMaxSize, d.convertToSortKey)
		if err != nil {
			return nil, err
		}
		res.ColumnValues = unionColumnValues(res.ColumnValues, columnValues)
		if bestCNFItemRes != nil && bestCNFItemRes.rangeResult != nil {
			if len(bestCNFItemRes.rangeResult.Ranges) == 0 {
				return &DetachRangeResult{}, nil
			}
			if bestCNFItemRes.sameLenPointRanges && bestCNFItemRes.minColNum > eqOrInCount {
				bestCNFItemRes.rangeResult.ColumnValues = res.ColumnValues
				res = bestCNFItemRes.rangeResult
				pointRanges = bestCNFItemRes.rangeResult.Ranges
				eqOrInCount = len(res.Ranges[0].LowVal)
				newConditions = newConditions[:0]
				newConditions = append(newConditions, conditions[:bestCNFItemRes.offset]...)
				newConditions = append(newConditions, conditions[bestCNFItemRes.offset+1:]...)
				if eqOrInCount == len(d.cols) || len(newConditions) == 0 {
					res.RemainedConds = append(res.RemainedConds, newConditions...)
					return res, nil
				}
			} else {
				considerCNFItemNonPointRanges := fixcontrol.GetBoolWithDefault(d.sctx.OptimizerFixControl, fixcontrol.Fix44389, false)
				if considerCNFItemNonPointRanges && !bestCNFItemRes.sameLenPointRanges && eqOrInCount == 0 && bestCNFItemRes.minColNum > 0 && bestCNFItemRes.maxColNum > 1 {
					// When eqOrInCount is 0, if we don't enter the IF branch, we would use detachColumnCNFConditions to build
					// ranges on the first index column.
					// Considering minColNum > 0 and maxColNum > 1, bestCNFItemRes is better than the ranges built by detachColumnCNFConditions
					// in most cases.
					bestCNFItemRes.rangeResult.ColumnValues = res.ColumnValues
					res = bestCNFItemRes.rangeResult
					newConditions = newConditions[:0]
					newConditions = append(newConditions, conditions[:bestCNFItemRes.offset]...)
					newConditions = append(newConditions, conditions[bestCNFItemRes.offset+1:]...)
					res.RemainedConds = append(res.RemainedConds, newConditions...)
					return res, nil
				}
			}
		}
		if eqOrInCount > 0 {
			newCols := d.cols[eqOrInCount:]
			newLengths := d.lengths[eqOrInCount:]
			tailRes, err := DetachCondAndBuildRangeForIndex(d.sctx, newConditions, newCols, newLengths, d.rangeMaxSize)
			if err != nil {
				return nil, err
			}
			if len(tailRes.Ranges) == 0 {
				return &DetachRangeResult{}, nil
			}
			if len(tailRes.AccessConds) > 0 {
				newRanges, rangeFallback := AppendRanges2PointRanges(pointRanges, tailRes.Ranges, d.rangeMaxSize)
				if rangeFallback {
					d.sctx.RecordRangeFallback(d.rangeMaxSize)
					res.RemainedConds = append(res.RemainedConds, tailRes.AccessConds...)
					// Some conditions may be in both tailRes.AccessConds and tailRes.RemainedConds so we call AppendConditionsIfNotExist here.
					res.RemainedConds = AppendConditionsIfNotExist(res.RemainedConds, tailRes.RemainedConds)
					return res, nil
				}
				res.Ranges = newRanges
				res.AccessConds = append(res.AccessConds, tailRes.AccessConds...)
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
			res.RemainedConds = append(res.RemainedConds, tailRes.RemainedConds...)
			return res, nil
		}
		// `eqOrInCount` must be 0 when coming here.
		res.AccessConds, res.RemainedConds = detachColumnCNFConditions(d.sctx.ExprCtx, newConditions, checker)
		ranges, res.AccessConds, remainedConds, err = d.buildCNFIndexRange(newTpSlice, 0, res.AccessConds)
		if err != nil {
			return nil, err
		}
		// detachColumnCNFConditions extracts `a = 10 or a = 30` from `(a = 10 and b = 20) or (a = 30 and b = 40)`. If
		// [10, 10] [30, 30] exceeds range mem limit, we add `a = 10 or a = 30` back to RemainedConds, which is actually
		// unnecessary because `(a = 10 and b = 20) or (a = 30 and b = 40)` is already in RemainedConds.
		// TODO: we will optimize it later.
		res.RemainedConds = AppendConditionsIfNotExist(res.RemainedConds, remainedConds)
		res.Ranges = ranges
		return res, nil
	}
	for _, cond := range newConditions {
		isAccessCond, _ := checker.check(cond)
		if !isAccessCond {
			filterConds = append(filterConds, cond)
			continue
		}
		accessConds = append(accessConds, cond)
		// TODO: if it's prefix column, we need to add cond to filterConds?
	}
	ranges, accessConds, remainedConds, err = d.buildCNFIndexRange(newTpSlice, eqOrInCount, accessConds)
	if err != nil {
		return nil, err
	}
	filterConds = append(filterConds, remainedConds...)
	res.Ranges = ranges
	res.AccessConds = accessConds
	res.RemainedConds = filterConds
	return res, nil
}

// excludeToIncludeForIntPoint converts `(i` to `[i+1` and `i)` to `i-1]` if `i` is integer.
// For example, if p is `(3`, i.e., point { value: int(3), excl: true, start: true }, it is equal to `[4`, i.e., point { value: int(4), excl: false, start: true }.
// Similarly, if p is `8)`, i.e., point { value: int(8), excl: true, start: false}, it is equal to `7]`, i.e., point { value: int(7), excl: false, start: false }.
// If return value is nil, it means p is unsatisfiable. For example, `(MaxUint64` is unsatisfiable.
// The boundary value will be treated as the bigger type: For example, `(MaxInt64` of type KindInt64 will become `[MaxInt64+1` of type KindUint64,
// and vice versa for `0)` of type KindUint64 will become `-1]` of type KindInt64.
func excludeToIncludeForIntPoint(p *point) *point {
	if !p.excl {
		return p
	}
	if p.value.Kind() == types.KindInt64 {
		val := p.value.GetInt64()
		if p.start {
			if val == math.MaxInt64 {
				p.value.SetUint64(uint64(val + 1))
			} else {
				p.value.SetInt64(val + 1)
			}
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
				p.value.SetInt64(int64(val - 1))
			} else {
				p.value.SetUint64(val - 1)
			}
			p.excl = false
		}
	}
	return p
}

// If there exists an interval whose length is large than 0, return nil. Otherwise remove all unsatisfiable intervals
// and return array of single point intervals.
func allSinglePoints(typeCtx types.Context, points []*point) []*point {
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
		// Since the point's collations are equal to the column's collation, we can use any of them.
		cmp, err := left.value.Compare(typeCtx, &right.value, collate.GetCollator(left.value.Collation()))
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
			return &valueInfo{value, mutable}
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
// filters: filters is the part that some access conditions need to be evaluated again since it's only the prefix part of char column.
// newConditions: We'll simplify the given conditions if there're multiple in conditions or eq conditions on the same column.
//
//	e.g. if there're a in (1, 2, 3) and a in (2, 3, 4). This two will be combined to a in (2, 3) and pushed to newConditions.
//
// columnValues: the constant column values for all index columns. columnValues[i] is nil if cols[i] is not constant.
// bool: indicate whether there's nil range when merging eq and in conditions.
func ExtractEqAndInCondition(sctx *rangerctx.RangerContext, conditions []expression.Expression, cols []*expression.Column,
	lengths []int) ([]expression.Expression, []expression.Expression, []expression.Expression, []*valueInfo, bool) {
	var filters []expression.Expression
	rb := builder{sctx: sctx}
	accesses := make([]expression.Expression, len(cols))
	points := make([][]*point, len(cols))
	mergedAccesses := make([]expression.Expression, len(cols))
	newConditions := make([]expression.Expression, 0, len(conditions))
	columnValues := make([]*valueInfo, len(cols))
	offsets := make([]int, len(conditions))
	for i, cond := range conditions {
		offset := getPotentialEqOrInColOffset(sctx, cond, cols)
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
		newTp := newFieldType(cols[offset].GetType(sctx.ExprCtx.GetEvalCtx()))
		collator := collate.GetCollator(cols[offset].GetType(sctx.ExprCtx.GetEvalCtx()).GetCollate())
		if mergedAccesses[offset] == nil {
			mergedAccesses[offset] = accesses[offset]
			// Note that this is a relatively special usage of build(). We will restore the points back to Expression for
			// later use and may build the Expression to points again.
			// We need to keep the original value here, which means we neither cut prefix nor convert to sort key.
			points[offset] = rb.build(accesses[offset], newTp, types.UnspecifiedLength, false)
		}
		points[offset] = rb.intersection(points[offset], rb.build(cond, newTp, types.UnspecifiedLength, false), collator)
		if len(points[offset]) == 0 { // Early termination if false expression found
			if expression.MaybeOverOptimized4PlanCache(sctx.ExprCtx, conditions) {
				// `a>@x and a<@y` --> `invalid-range if @x>=@y`
				sctx.SetSkipPlanCache("some parameters may be overwritten")
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
		points[i] = allSinglePoints(sctx.TypeCtx, points[i])
		if points[i] == nil {
			// There exists an interval whose length is larger than 0
			accesses[i] = nil
		} else if len(points[i]) == 0 { // Early termination if false expression found
			if expression.MaybeOverOptimized4PlanCache(sctx.ExprCtx, conditions) {
				// `a>@x and a<@y` --> `invalid-range if @x>=@y`
				sctx.SetSkipPlanCache("some parameters may be overwritten")
			}
			return nil, nil, nil, nil, true
		} else {
			// All Intervals are single points
			accesses[i] = points2EqOrInCond(sctx.ExprCtx, points[i], cols[i])
			newConditions = append(newConditions, accesses[i])
			if f, ok := accesses[i].(*expression.ScalarFunction); ok && f.FuncName.L == ast.EQ {
				// Actually the constant column value may not be mutable. Here we assume it is mutable to keep it simple.
				// Maybe we can improve it later.
				columnValues[i] = &valueInfo{mutable: true}
			}
			if expression.MaybeOverOptimized4PlanCache(sctx.ExprCtx, conditions) {
				// `a=@x and a=@y` --> `a=@x if @x==@y`
				sctx.SetSkipPlanCache("some parameters may be overwritten")
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
		isFullLength := lengths[i] == types.UnspecifiedLength || lengths[i] == cols[i].GetType(sctx.ExprCtx.GetEvalCtx()).GetFlen()
		if !isFullLength {
			filters = append(filters, cond)
		}
	}
	// We should remove all accessConds, so that they will not be added to filter conditions.
	newConditions = removeConditions(newConditions, accesses)
	return accesses, filters, newConditions, columnValues, false
}

// detachDNFCondAndBuildRangeForIndex will detach the index filters from table filters when it's a DNF(Disjunctive Normal Form).
// We will detach the conditions of every DNF items, then compose them to a DNF.
func (d *rangeDetacher) detachDNFCondAndBuildRangeForIndex(condition *expression.ScalarFunction, newTpSlice []*types.FieldType) (Ranges, []expression.Expression, []*valueInfo, bool, error) {
	firstColumnChecker := &conditionChecker{
		checkerCol:               d.cols[0],
		length:                   d.lengths[0],
		optPrefixIndexSingleScan: d.sctx.OptPrefixIndexSingleScan,
		ctx:                      d.sctx.ExprCtx.GetEvalCtx(),
	}
	rb := builder{sctx: d.sctx}
	dnfItems := expression.FlattenDNFConditions(condition)
	newAccessItems := make([]expression.Expression, 0, len(dnfItems))
	var (
		totalRanges         Ranges
		totalRangesMemUsage int64
	)
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
			// If DNF item always false, we can return ignore this DNF item.
			if len(ranges) == 0 {
				continue
			}
			accesses = res.AccessConds
			filters = res.RemainedConds
			if len(accesses) == 0 {
				return FullRange(), nil, nil, true, nil
			}
			if len(filters) > 0 {
				hasResidual = true
			}
			totalRanges = append(totalRanges, ranges...)
			totalRangesMemUsage += ranges.MemUsage()
			if d.rangeMaxSize > 0 && totalRangesMemUsage > d.rangeMaxSize {
				d.sctx.RecordRangeFallback(d.rangeMaxSize)
				return FullRange(), nil, nil, true, nil
			}
			newAccessItems = append(newAccessItems, expression.ComposeCNFCondition(d.sctx.ExprCtx, accesses...))
			if res.ColumnValues != nil {
				if i == 0 {
					columnValues = res.ColumnValues
				} else {
					// take the intersection of the two columnValues
					for j, valInfo := range columnValues {
						if valInfo == nil {
							continue
						}
						sameValue, err := isSameValue(d.sctx.TypeCtx, valInfo, res.ColumnValues[j])
						if err != nil {
							return nil, nil, nil, false, errors.Trace(err)
						}
						if !sameValue {
							columnValues[j] = nil
						}
					}
				}
			}
		} else {
			isAccessCond, shouldReserve := firstColumnChecker.check(item)
			if !isAccessCond {
				return FullRange(), nil, nil, true, nil
			}
			if shouldReserve {
				hasResidual = true
			}
			points := rb.build(item, newTpSlice[0], d.lengths[0], d.convertToSortKey)
			tmpNewTp := newTpSlice[0]
			if d.convertToSortKey {
				tmpNewTp = convertStringFTToBinaryCollate(tmpNewTp)
			}
			// TODO: restrict the mem usage of ranges
			ranges, rangeFallback, err := points2Ranges(d.sctx, points, tmpNewTp, d.rangeMaxSize)
			if err != nil {
				return nil, nil, nil, false, errors.Trace(err)
			}
			if rangeFallback {
				d.sctx.RecordRangeFallback(d.rangeMaxSize)
				return FullRange(), nil, nil, true, nil
			}
			totalRanges = append(totalRanges, ranges...)
			totalRangesMemUsage += ranges.MemUsage()
			if d.rangeMaxSize > 0 && totalRangesMemUsage > d.rangeMaxSize {
				d.sctx.RecordRangeFallback(d.rangeMaxSize)
				return FullRange(), nil, nil, true, nil
			}
			newAccessItems = append(newAccessItems, item)
			if i == 0 {
				columnValues[0] = extractValueInfo(item)
			} else if columnValues[0] != nil {
				valInfo := extractValueInfo(item)
				sameValue, err := isSameValue(d.sctx.TypeCtx, columnValues[0], valInfo)
				if err != nil {
					return nil, nil, nil, false, errors.Trace(err)
				}
				if !sameValue {
					columnValues[0] = nil
				}
			}
		}
	}

	totalRanges, err := UnionRanges(d.sctx, totalRanges, d.mergeConsecutive)
	if err != nil {
		return nil, nil, nil, false, errors.Trace(err)
	}

	return totalRanges, []expression.Expression{expression.ComposeDNFCondition(d.sctx.ExprCtx, newAccessItems...)}, columnValues, hasResidual, nil
}

// valueInfo is used for recording the constant column value in DetachCondAndBuildRangeForIndex.
type valueInfo struct {
	value   *types.Datum // If not mutable, value is the constant column value. Otherwise value is nil.
	mutable bool         // If true, the constant column value depends on mutable constant.
}

func isSameValue(typeCtx types.Context, lhs, rhs *valueInfo) (bool, error) {
	// We assume `lhs` and `rhs` are not the same when either `lhs` or `rhs` is mutable to keep it simple. If we consider
	// mutable valueInfo, we need to set `sc.OptimDependOnMutableConst = true`, which makes the plan not able to be cached.
	// On the other hand, the equal condition may not be used for optimization. Hence we simply regard mutable valueInfos different
	// from others. Maybe we can improve it later.
	// TODO: is `lhs.value.Kind() != rhs.value.Kind()` necessary?
	if lhs == nil || rhs == nil || lhs.mutable || rhs.mutable || lhs.value.Kind() != rhs.value.Kind() {
		return false, nil
	}
	// binary collator may not the best choice, but it can make sure the result is correct.
	cmp, err := lhs.value.Compare(typeCtx, rhs.value, collate.GetBinaryCollator())
	if err != nil {
		return false, err
	}
	return cmp == 0, nil
}

// DetachRangeResult wraps up results when detaching conditions and builing ranges.
type DetachRangeResult struct {
	// Ranges is the ranges extracted and built from conditions.
	Ranges Ranges
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
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit. If you ask that all conditions must be used
// for building ranges, set rangeMemQuota to 0 to avoid range fallback.
// The returned values are encapsulated into a struct DetachRangeResult, see its comments for explanation.
func DetachCondAndBuildRangeForIndex(sctx *rangerctx.RangerContext, conditions []expression.Expression, cols []*expression.Column,
	lengths []int, rangeMaxSize int64) (*DetachRangeResult, error) {
	d := &rangeDetacher{
		sctx:             sctx,
		allConds:         conditions,
		cols:             cols,
		lengths:          lengths,
		mergeConsecutive: true,
		convertToSortKey: true,
		rangeMaxSize:     rangeMaxSize,
	}
	return d.detachCondAndBuildRangeForCols()
}

// detachCondAndBuildRangeWithoutMerging detaches the index filters from table filters and uses them to build ranges.
// When building ranges, it doesn't merge consecutive ranges.
func detachCondAndBuildRangeWithoutMerging(sctx *rangerctx.RangerContext, conditions []expression.Expression, cols []*expression.Column,
	lengths []int, rangeMaxSize int64, convertToSortKey bool) (*DetachRangeResult, error) {
	d := &rangeDetacher{
		sctx:             sctx,
		allConds:         conditions,
		cols:             cols,
		lengths:          lengths,
		mergeConsecutive: false,
		convertToSortKey: convertToSortKey,
		rangeMaxSize:     rangeMaxSize,
	}
	return d.detachCondAndBuildRangeForCols()
}

// DetachCondAndBuildRangeForPartition will detach the index filters from table filters.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit. If you ask that all conditions must be used
// for building ranges, set rangeMemQuota to 0 to avoid range fallback.
// The returned values are encapsulated into a struct DetachRangeResult, see its comments for explanation.
func DetachCondAndBuildRangeForPartition(sctx *rangerctx.RangerContext, conditions []expression.Expression, cols []*expression.Column,
	lengths []int, rangeMaxSize int64) (*DetachRangeResult, error) {
	return detachCondAndBuildRangeWithoutMerging(sctx, conditions, cols, lengths, rangeMaxSize, false)
}

type rangeDetacher struct {
	sctx             *rangerctx.RangerContext
	allConds         []expression.Expression
	cols             []*expression.Column
	lengths          []int
	mergeConsecutive bool
	convertToSortKey bool
	rangeMaxSize     int64
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
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit. If you ask that all conditions must be used
// for building ranges, set rangeMemQuota to 0 to avoid range fallback.
func DetachSimpleCondAndBuildRangeForIndex(sctx *rangerctx.RangerContext, conditions []expression.Expression,
	cols []*expression.Column, lengths []int, rangeMaxSize int64) (Ranges, []expression.Expression, error) {
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
		convertToSortKey: true,
		rangeMaxSize:     rangeMaxSize,
	}
	res, err := d.detachCNFCondAndBuildRangeForIndex(conditions, newTpSlice, false)
	return res.Ranges, res.AccessConds, err
}

func removeConditions(conditions, condsToRemove []expression.Expression) []expression.Expression {
	filterConds := make([]expression.Expression, 0, len(conditions))
	for _, cond := range conditions {
		if !expression.Contains(condsToRemove, cond) {
			filterConds = append(filterConds, cond)
		}
	}
	return filterConds
}

// AppendConditionsIfNotExist appends conditions if they are absent.
func AppendConditionsIfNotExist(conditions, condsToAppend []expression.Expression) []expression.Expression {
	shouldAppend := make([]expression.Expression, 0, len(condsToAppend))
	for _, cond := range condsToAppend {
		if !expression.Contains(conditions, cond) {
			shouldAppend = append(shouldAppend, cond)
		}
	}
	return append(conditions, shouldAppend...)
}

// ExtractAccessConditionsForColumn extracts the access conditions used for range calculation. Since
// we don't need to return the remained filter conditions, it is much simpler than DetachCondsForColumn.
func ExtractAccessConditionsForColumn(ctx *rangerctx.RangerContext, conds []expression.Expression, col *expression.Column) []expression.Expression {
	checker := conditionChecker{
		checkerCol:               col,
		length:                   types.UnspecifiedLength,
		optPrefixIndexSingleScan: ctx.OptPrefixIndexSingleScan,
		ctx:                      ctx.ExprCtx.GetEvalCtx(),
	}
	accessConds := make([]expression.Expression, 0, 8)
	filter := func(expr expression.Expression) bool {
		isAccessCond, _ := checker.check(expr)
		return isAccessCond
	}
	return expression.Filter(accessConds, conds, filter)
}

// DetachCondsForColumn detaches access conditions for specified column from other filter conditions.
func DetachCondsForColumn(sctx *rangerctx.RangerContext, conds []expression.Expression, col *expression.Column) (accessConditions, otherConditions []expression.Expression) {
	checker := &conditionChecker{
		checkerCol:               col,
		length:                   types.UnspecifiedLength,
		optPrefixIndexSingleScan: sctx.OptPrefixIndexSingleScan,
		ctx:                      sctx.ExprCtx.GetEvalCtx(),
	}
	return detachColumnCNFConditions(sctx.ExprCtx, conds, checker)
}

// MergeDNFItems4Col receives a slice of DNF conditions, merges some of them which can be built into ranges on a single column, then returns.
// For example, [a > 5, b > 6, c > 7, a = 1, b > 3] will become [a > 5 or a = 1, b > 6 or b > 3, c > 7].
func MergeDNFItems4Col(ctx *rangerctx.RangerContext, dnfItems []expression.Expression) []expression.Expression {
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
			checkerCol:               cols[0],
			length:                   types.UnspecifiedLength,
			optPrefixIndexSingleScan: ctx.OptPrefixIndexSingleScan,
			ctx:                      ctx.ExprCtx.GetEvalCtx(),
		}
		// If we can't use this condition to build range, we can't merge it.
		// Currently, we assume if every condition in a DNF expression can pass this check, then `Selectivity` must be able to
		// cover this entire DNF directly without recursively call `Selectivity`. If this doesn't hold in the future, this logic
		// may cause infinite recursion in `Selectivity`.
		isAccessCond, _ := checker.check(dnfItem)
		if !isAccessCond {
			mergedDNFItems = append(mergedDNFItems, dnfItem)
			continue
		}

		col2DNFItems[uniqueID] = append(col2DNFItems[uniqueID], dnfItem)
	}
	for _, items := range col2DNFItems {
		mergedDNFItems = append(mergedDNFItems, expression.ComposeDNFCondition(ctx.ExprCtx, items...))
	}
	return mergedDNFItems
}

// AddGcColumnCond add the `tidb_shard(x) = xxx` to the condition
// @param[in] cols          the columns of shard index, such as [tidb_shard(a), a, ...]
// @param[in] accessCond    the conditions relative to the index and arranged by the index column order.
//
//	e.g. the index is uk(tidb_shard(a), a, b) and the where clause is
//	`WHERE b = 1 AND a = 2 AND c = 3`, the param accessCond is {a = 2, b = 1} that is
//	only relative to uk's columns.
//
// @param[in] columnValues  the values of index columns in param accessCond. if accessCond is {a = 2, b = 1},
//
//	columnValues is {2, 1}. if accessCond the "IN" function like `a IN (1, 2)`, columnValues
//	is empty.
//
// @retval -  []expression.Expression   the new conditions after adding `tidb_shard() = xxx` prefix
//
//	error                     if error gernerated, return error
func AddGcColumnCond(sctx *rangerctx.RangerContext,
	cols []*expression.Column,
	accessesCond []expression.Expression,
	columnValues []*valueInfo) ([]expression.Expression, error) {
	if cond := accessesCond[1]; cond != nil {
		if f, ok := cond.(*expression.ScalarFunction); ok {
			switch f.FuncName.L {
			case ast.EQ:
				return AddGcColumn4EqCond(sctx, cols, accessesCond, columnValues)
			case ast.In:
				return AddGcColumn4InCond(sctx, cols, accessesCond)
			}
		}
	}

	return accessesCond, nil
}

// AddGcColumn4InCond add the `tidb_shard(x) = xxx` for `IN` condition
// For param explanation, please refer to the function `AddGcColumnCond`.
// @retval -  []expression.Expression   the new conditions after adding `tidb_shard() = xxx` prefix
//
//	error                     if error gernerated, return error
func AddGcColumn4InCond(sctx *rangerctx.RangerContext,
	cols []*expression.Column,
	accessesCond []expression.Expression) ([]expression.Expression, error) {
	var errRes error
	var newAccessCond []expression.Expression
	record := make([]types.Datum, 1)

	expr := cols[0].VirtualExpr.Clone()
	andType := types.NewFieldType(mysql.TypeTiny)

	sf := accessesCond[1].(*expression.ScalarFunction)
	c := sf.GetArgs()[0].(*expression.Column)
	var andOrExpr expression.Expression
	evalCtx := sctx.ExprCtx.GetEvalCtx()
	for i, arg := range sf.GetArgs()[1:] {
		// get every const value and calculate tidb_shard(val)
		con := arg.(*expression.Constant)
		conVal, err := con.Eval(evalCtx, chunk.Row{})
		if err != nil {
			return accessesCond, err
		}

		record[0] = conVal
		mutRow := chunk.MutRowFromDatums(record)
		exprVal, err := expr.Eval(evalCtx, mutRow.ToRow())
		if err != nil {
			return accessesCond, err
		}

		// tmpArg1 is like `tidb_shard(a) = 8`, tmpArg2 is like `a = 100`
		exprCon := &expression.Constant{Value: exprVal, RetType: cols[0].RetType}
		tmpArg1, err := expression.NewFunction(sctx.ExprCtx, ast.EQ, cols[0].RetType, cols[0], exprCon)
		if err != nil {
			return accessesCond, err
		}
		tmpArg2, err := expression.NewFunction(sctx.ExprCtx, ast.EQ, c.RetType, c.Clone(), arg)
		if err != nil {
			return accessesCond, err
		}

		// make a LogicAnd, e.g. `tidb_shard(a) = 8 AND a = 100`
		andExpr, err := expression.NewFunction(sctx.ExprCtx, ast.LogicAnd, andType, tmpArg1, tmpArg2)
		if err != nil {
			return accessesCond, err
		}

		if i == 0 {
			andOrExpr = andExpr
		} else {
			// if the LogicAnd more than one, make a LogicOr,
			// e.g. `(tidb_shard(a) = 8 AND a = 100) OR (tidb_shard(a) = 161 AND a = 200)`
			andOrExpr, errRes = expression.NewFunction(sctx.ExprCtx, ast.LogicOr, andType, andOrExpr, andExpr)
			if errRes != nil {
				return accessesCond, errRes
			}
		}
	}

	newAccessCond = append(newAccessCond, andOrExpr)

	return newAccessCond, nil
}

// AddGcColumn4EqCond add the `tidb_shard(x) = xxx` prefix for equal condition
// For param explanation, please refer to the function `AddGcColumnCond`.
// @retval -  []expression.Expression   the new conditions after adding `tidb_shard() = xxx` prefix
//
//	[]*valueInfo              the values of every columns in the returned new conditions
//	error                     if error gernerated, return error
func AddGcColumn4EqCond(sctx *rangerctx.RangerContext,
	cols []*expression.Column,
	accessesCond []expression.Expression,
	columnValues []*valueInfo) ([]expression.Expression, error) {
	expr := cols[0].VirtualExpr.Clone()
	record := make([]types.Datum, len(columnValues)-1)

	for i := 1; i < len(columnValues); i++ {
		cv := columnValues[i]
		if cv == nil {
			break
		}
		record[i-1] = *cv.value
	}

	mutRow := chunk.MutRowFromDatums(record)
	exprCtx := sctx.ExprCtx
	evaluated, err := expr.Eval(exprCtx.GetEvalCtx(), mutRow.ToRow())
	if err != nil {
		return accessesCond, err
	}
	vi := &valueInfo{&evaluated, false}
	con := &expression.Constant{Value: evaluated, RetType: cols[0].RetType}
	// make a tidb_shard() function, e.g. `tidb_shard(a) = 8`
	cond, err := expression.NewFunction(exprCtx, ast.EQ, cols[0].RetType, cols[0], con)
	if err != nil {
		return accessesCond, err
	}

	accessesCond[0] = cond
	columnValues[0] = vi
	return accessesCond, nil
}

// AddExpr4EqAndInCondition add the `tidb_shard(x) = xxx` prefix
// Add tidb_shard() for EQ and IN function. e.g. input condition is `WHERE a = 1`,
// output condition is `WHERE tidb_shard(a) = 214 AND a = 1`. e.g. input condition
// is `WHERE a IN (1, 2 ,3)`, output condition is `WHERE (tidb_shard(a) = 214 AND a = 1)
// OR (tidb_shard(a) = 143 AND a = 2) OR (tidb_shard(a) = 156 AND a = 3)`
// @param[in] conditions  the original condition to be processed
// @param[in] cols        the columns of shard index, such as [tidb_shard(a), a, ...]
// @param[in] lengths     the length for every column of shard index
// @retval - the new condition after adding tidb_shard() prefix
func AddExpr4EqAndInCondition(sctx *rangerctx.RangerContext, conditions []expression.Expression,
	cols []*expression.Column) ([]expression.Expression, error) {
	accesses := make([]expression.Expression, len(cols))
	columnValues := make([]*valueInfo, len(cols))
	offsets := make([]int, len(conditions))
	addGcCond := true

	// the array accesses stores conditions of every column in the index in the definition order
	// e.g. the original condition is `WHERE b = 100 AND a = 200 AND c = 300`, the definition of
	// index is (tidb_shard(a), a, b), then accesses is "[a = 200, b = 100]"
	for i, cond := range conditions {
		offset := getPotentialEqOrInColOffset(sctx, cond, cols)
		offsets[i] = offset
		if offset == -1 {
			continue
		}
		if accesses[offset] == nil {
			accesses[offset] = cond
			continue
		}
		// if the same field appear twice or more, don't add tidb_shard()
		// e.g. `WHERE a > 100 and a < 200`
		addGcCond = false
	}

	for i, cond := range accesses {
		if cond == nil {
			continue
		}
		if !allEqOrIn(cond) {
			addGcCond = false
			break
		}
		columnValues[i] = extractValueInfo(cond)
	}

	if !addGcCond || !NeedAddGcColumn4ShardIndex(cols, accesses, columnValues) {
		return conditions, nil
	}

	// remove the accesses from newConditions
	newConditions := make([]expression.Expression, 0, len(conditions))
	newConditions = append(newConditions, conditions...)
	newConditions = removeConditions(newConditions, accesses)

	// add Gc condition for accesses and return new condition to newAccesses
	newAccesses, err := AddGcColumnCond(sctx, cols, accesses, columnValues)
	if err != nil {
		return conditions, err
	}

	// merge newAccesses and original condition execept accesses
	newConditions = append(newConditions, newAccesses...)

	return newConditions, nil
}

// NeedAddGcColumn4ShardIndex check whether to add `tidb_shard(x) = xxx`
// @param[in] cols          the columns of shard index, such as [tidb_shard(a), a, ...]
// @param[in] accessCond    the conditions relative to the index and arranged by the index column order.
//
//	e.g. the index is uk(tidb_shard(a), a, b) and the where clause is
//	`WHERE b = 1 AND a = 2 AND c = 3`, the param accessCond is {a = 2, b = 1} that is
//	only relative to uk's columns.
//
// @param[in] columnValues  the values of index columns in param accessCond. if accessCond is {a = 2, b = 1},
//
//	columnValues is {2, 1}. if accessCond the "IN" function like `a IN (1, 2)`, columnValues
//	is empty.
//
// @retval -  return true if it needs to addr tidb_shard() prefix, ohterwise return false
func NeedAddGcColumn4ShardIndex(cols []*expression.Column, accessCond []expression.Expression, columnValues []*valueInfo) bool {
	// the columns of shard index shoude be more than 2, like (tidb_shard(a),a,...)
	// check cols and columnValues in the sub call function
	if len(accessCond) < 2 || len(cols) < 2 {
		return false
	}

	if !IsValidShardIndex(cols) {
		return false
	}

	// accessCond[0] shoudle be nil, because it has no access condition for
	// the prefix tidb_shard() of the shard index
	if cond := accessCond[1]; cond != nil {
		if f, ok := cond.(*expression.ScalarFunction); ok {
			switch f.FuncName.L {
			case ast.EQ:
				return NeedAddColumn4EqCond(cols, accessCond, columnValues)
			case ast.In:
				return NeedAddColumn4InCond(cols, accessCond, f)
			}
		}
	}

	return false
}

// NeedAddColumn4EqCond `tidb_shard(x) = xxx`
// For param explanation, please refer to the function `NeedAddGcColumn4ShardIndex`.
// It checks whether EQ conditions need to be added tidb_shard() prefix.
// (1) columns in accessCond are all columns of the index except the first.
// (2) every column in accessCond has a constan value
func NeedAddColumn4EqCond(cols []*expression.Column,
	accessCond []expression.Expression, columnValues []*valueInfo) bool {
	valCnt := 0
	matchedKeyFldCnt := 0

	// the columns of shard index shoude be more than 2, like (tidb_shard(a),a,...)
	if len(columnValues) < 2 {
		return false
	}

	for _, cond := range accessCond[1:] {
		if cond == nil {
			break
		}

		f, ok := cond.(*expression.ScalarFunction)
		if !ok || f.FuncName.L != ast.EQ {
			return false
		}

		matchedKeyFldCnt++
	}
	for _, val := range columnValues[1:] {
		if val == nil {
			break
		}
		valCnt++
	}

	if matchedKeyFldCnt != len(cols)-1 ||
		valCnt != len(cols)-1 ||
		accessCond[0] != nil ||
		columnValues[0] != nil {
		return false
	}

	return true
}

// NeedAddColumn4InCond `tidb_shard(x) = xxx`
// For param explanation, please refer to the function `NeedAddGcColumn4ShardIndex`.
// It checks whether "IN" conditions need to be added tidb_shard() prefix.
// (1) columns in accessCond are all columns of the index except the first.
// (2) the first param of "IN" function should be a column not a expression like `a + b`
// (3) the rest params of "IN" function all should be constant
// (4) the first param of "IN" function should be the column in the expression of first index field.
//
//	e.g. uk(tidb_shard(a), a). If the conditions is `WHERE b in (1, 2, 3)`, the first param of "IN" function
//	is `b` that's not the column in `tidb_shard(a)`.
//
// @param  sf	"IN" function, e.g. `a IN (1, 2, 3)`
func NeedAddColumn4InCond(cols []*expression.Column, accessCond []expression.Expression, sf *expression.ScalarFunction) bool {
	if len(cols) == 0 || len(accessCond) == 0 || sf == nil {
		return false
	}

	if accessCond[0] != nil {
		return false
	}

	fields := ExtractColumnsFromExpr(cols[0].VirtualExpr.(*expression.ScalarFunction))

	c, ok := sf.GetArgs()[0].(*expression.Column)
	if !ok {
		return false
	}

	for _, arg := range sf.GetArgs()[1:] {
		if _, ok := arg.(*expression.Constant); !ok {
			return false
		}
	}

	if len(fields) != 1 ||
		!fields[0].EqualColumn(c) {
		return false
	}

	return true
}

// ExtractColumnsFromExpr get all fields from input expression virtaulExpr
func ExtractColumnsFromExpr(virtaulExpr *expression.ScalarFunction) []*expression.Column {
	var fields []*expression.Column

	if virtaulExpr == nil {
		return fields
	}

	for _, arg := range virtaulExpr.GetArgs() {
		if sf, ok := arg.(*expression.ScalarFunction); ok {
			fields = append(fields, ExtractColumnsFromExpr(sf)...)
		} else if c, ok := arg.(*expression.Column); ok {
			if !c.InColumnArray(fields) {
				fields = append(fields, c)
			}
		}
	}

	return fields
}

// IsValidShardIndex Check whether the definition of shard index is valid. The form of index
// should like `index(tidb_shard(a), a, ....)`.
// 1) the column count shoudle be >= 2
// 2) the first column should be tidb_shard(xxx)
// 3) the parameter of tidb_shard shoudle be a column that is the second column of index
// @param[in] cols        the columns of shard index, such as [tidb_shard(a), a, ...]
// @retval - if the shard index is valid return true, otherwise return false
func IsValidShardIndex(cols []*expression.Column) bool {
	// definition of index should like the form: index(tidb_shard(a), a, ....)
	if len(cols) < 2 {
		return false
	}

	// the first coulmn of index must be GC column and the expr must be tidb_shard
	if !expression.GcColumnExprIsTidbShard(cols[0].VirtualExpr) {
		return false
	}

	shardFunc, _ := cols[0].VirtualExpr.(*expression.ScalarFunction)

	argCount := len(shardFunc.GetArgs())
	if argCount != 1 {
		return false
	}

	// parameter of tidb_shard must be the second column of the input index columns
	col, ok := shardFunc.GetArgs()[0].(*expression.Column)
	if !ok || !col.EqualColumn(cols[1]) {
		return false
	}

	return true
}
