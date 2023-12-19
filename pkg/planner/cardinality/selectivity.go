// Copyright 2023 PingCAP, Inc.
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

package cardinality

import (
	"cmp"
	"math"
	"math/bits"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	planutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

var (
	outOfRangeBetweenRate int64 = 100
)

// Selectivity is a function calculate the selectivity of the expressions on the specified HistColl.
// The definition of selectivity is (row count after filter / row count before filter).
// And exprs must be CNF now, in other words, `exprs[0] and exprs[1] and ... and exprs[len - 1]`
// should be held when you call this.
// Currently the time complexity is o(n^2).
func Selectivity(
	ctx sessionctx.Context,
	coll *statistics.HistColl,
	exprs []expression.Expression,
	filledPaths []*planutil.AccessPath,
) (
	result float64,
	retStatsNodes []*StatsNode,
	err error,
) {
	var exprStrs []string
	if ctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(ctx)
		exprStrs = expression.ExprsToStringsForDisplay(exprs)
		debugtrace.RecordAnyValuesWithNames(ctx, "Input Expressions", exprStrs)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(ctx, "Result", result)
			debugtrace.LeaveContextCommon(ctx)
		}()
	}
	// If table's count is zero or conditions are empty, we should return 100% selectivity.
	if coll.RealtimeCount == 0 || len(exprs) == 0 {
		return 1, nil, nil
	}
	ret := 1.0
	sc := ctx.GetSessionVars().StmtCtx
	tableID := coll.PhysicalID
	// TODO: If len(exprs) is bigger than 63, we could use bitset structure to replace the int64.
	// This will simplify some code and speed up if we use this rather than a boolean slice.
	if len(exprs) > 63 || (len(coll.Columns) == 0 && len(coll.Indices) == 0) {
		ret = pseudoSelectivity(coll, exprs)
		if sc.EnableOptimizerCETrace {
			ceTraceExpr(ctx, tableID, "Table Stats-Pseudo-Expression",
				expression.ComposeCNFCondition(ctx, exprs...), ret*float64(coll.RealtimeCount))
		}
		return ret, nil, nil
	}

	var nodes []*StatsNode

	var remainedExprStrs []string
	remainedExprs := make([]expression.Expression, 0, len(exprs))

	// Deal with the correlated column.
	for i, expr := range exprs {
		c := isColEqCorCol(expr)
		if c == nil {
			remainedExprs = append(remainedExprs, expr)
			if sc.EnableOptimizerDebugTrace {
				remainedExprStrs = append(remainedExprStrs, exprStrs[i])
			}
			continue
		}

		colHist := coll.Columns[c.UniqueID]
		var sel float64
		if colHist == nil || colHist.IsInvalid(ctx, coll.Pseudo) {
			sel = 1.0 / pseudoEqualRate
		} else if colHist.Histogram.NDV > 0 {
			sel = 1 / float64(colHist.Histogram.NDV)
		} else {
			sel = 1.0 / pseudoEqualRate
		}
		if sc.EnableOptimizerDebugTrace {
			debugtrace.RecordAnyValuesWithNames(ctx, "Expression", expr.String(), "Selectivity", sel)
		}
		ret *= sel
	}

	extractedCols := make([]*expression.Column, 0, len(coll.Columns))
	extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, remainedExprs, nil)
	colIDs := maps.Keys(coll.Columns)
	slices.Sort(colIDs)
	for _, id := range colIDs {
		colStats := coll.Columns[id]
		col := expression.ColInfo2Col(extractedCols, colStats.Info)
		if col != nil {
			maskCovered, ranges, _, err := getMaskAndRanges(ctx, remainedExprs, ranger.ColumnRangeType, nil, nil, col)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			nodes = append(nodes, &StatsNode{Tp: ColType, ID: id, mask: maskCovered, Ranges: ranges, numCols: 1})
			if colStats.IsHandle {
				nodes[len(nodes)-1].Tp = PkType
				var cnt float64
				cnt, err = GetRowCountByIntColumnRanges(ctx, coll, id, ranges)
				if err != nil {
					return 0, nil, errors.Trace(err)
				}
				nodes[len(nodes)-1].Selectivity = cnt / float64(coll.RealtimeCount)
				continue
			}
			cnt, err := GetRowCountByColumnRanges(ctx, coll, id, ranges)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			nodes[len(nodes)-1].Selectivity = cnt / float64(coll.RealtimeCount)
		}
	}
	id2Paths := make(map[int64]*planutil.AccessPath)
	for _, path := range filledPaths {
		// Index merge path and table path don't have index.
		if path.Index == nil {
			continue
		}
		id2Paths[path.Index.ID] = path
	}
	idxIDs := maps.Keys(coll.Indices)
	slices.Sort(idxIDs)
	for _, id := range idxIDs {
		idxStats := coll.Indices[id]
		idxCols := findPrefixOfIndexByCol(ctx, extractedCols, coll.Idx2ColumnIDs[id], id2Paths[idxStats.ID])
		if len(idxCols) > 0 {
			lengths := make([]int, 0, len(idxCols))
			for i := 0; i < len(idxCols) && i < len(idxStats.Info.Columns); i++ {
				lengths = append(lengths, idxStats.Info.Columns[i].Length)
			}
			// If the found columns are more than the columns held by the index. We are appending the int pk to the tail of it.
			// When storing index data to key-value store, we use (idx_col1, ...., idx_coln, handle_col) as its key.
			if len(idxCols) > len(idxStats.Info.Columns) {
				lengths = append(lengths, types.UnspecifiedLength)
			}
			maskCovered, ranges, partCover, err := getMaskAndRanges(ctx, remainedExprs,
				ranger.IndexRangeType, lengths, id2Paths[idxStats.ID], idxCols...)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			cnt, err := GetRowCountByIndexRanges(ctx, coll, id, ranges)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			selectivity := cnt / float64(coll.RealtimeCount)
			nodes = append(nodes, &StatsNode{
				Tp:          IndexType,
				ID:          id,
				mask:        maskCovered,
				Ranges:      ranges,
				numCols:     len(idxStats.Info.Columns),
				Selectivity: selectivity,
				partCover:   partCover,
			})
		}
	}
	usedSets := GetUsableSetsByGreedy(nodes)
	// Initialize the mask with the full set.
	mask := (int64(1) << uint(len(remainedExprs))) - 1
	// curExpr records covered expressions by now. It's for cardinality estimation tracing.
	var curExpr []expression.Expression

	for _, set := range usedSets {
		mask &^= set.mask
		ret *= set.Selectivity
		// If `partCover` is true, it means that the conditions are in DNF form, and only part
		// of the DNF expressions are extracted as access conditions, so besides from the selectivity
		// of the extracted access conditions, we multiply another selectionFactor for the residual
		// conditions.
		if set.partCover {
			ret *= selectionFactor
		}
		if sc.EnableOptimizerCETrace {
			// Tracing for the expression estimation results after applying this StatsNode.
			for i := range remainedExprs {
				if set.mask&(1<<uint64(i)) > 0 {
					curExpr = append(curExpr, remainedExprs[i])
				}
			}
			expr := expression.ComposeCNFCondition(ctx, curExpr...)
			ceTraceExpr(ctx, tableID, "Table Stats-Expression-CNF", expr, ret*float64(coll.RealtimeCount))
		} else if sc.EnableOptimizerDebugTrace {
			var strs []string
			for i := range remainedExprs {
				if set.mask&(1<<uint64(i)) > 0 {
					strs = append(strs, remainedExprStrs[i])
				}
			}
			debugtrace.RecordAnyValuesWithNames(ctx,
				"Expressions", strs,
				"Selectivity", set.Selectivity,
				"partial cover", set.partCover,
			)
		}
	}

	notCoveredConstants := make(map[int]*expression.Constant)
	notCoveredDNF := make(map[int]*expression.ScalarFunction)
	notCoveredStrMatch := make(map[int]*expression.ScalarFunction)
	notCoveredNegateStrMatch := make(map[int]*expression.ScalarFunction)
	notCoveredOtherExpr := make(map[int]expression.Expression)
	if mask > 0 {
		for i, expr := range remainedExprs {
			if mask&(1<<uint64(i)) == 0 {
				continue
			}
			switch x := expr.(type) {
			case *expression.Constant:
				notCoveredConstants[i] = x
				continue
			case *expression.ScalarFunction:
				switch x.FuncName.L {
				case ast.LogicOr:
					notCoveredDNF[i] = x
					continue
				case ast.Like, ast.Ilike, ast.Regexp, ast.RegexpLike:
					notCoveredStrMatch[i] = x
					continue
				case ast.UnaryNot:
					inner := expression.GetExprInsideIsTruth(x.GetArgs()[0])
					innerSF, ok := inner.(*expression.ScalarFunction)
					if ok {
						switch innerSF.FuncName.L {
						case ast.Like, ast.Ilike, ast.Regexp, ast.RegexpLike:
							notCoveredNegateStrMatch[i] = x
							continue
						}
					}
				}
			}
			notCoveredOtherExpr[i] = expr
		}
	}

	// Try to cover remaining Constants
	for i, c := range notCoveredConstants {
		if expression.MaybeOverOptimized4PlanCache(ctx, []expression.Expression{c}) {
			continue
		}
		if c.Value.IsNull() {
			// c is null
			ret *= 0
			mask &^= 1 << uint64(i)
			delete(notCoveredConstants, i)
		} else if isTrue, err := c.Value.ToBool(sc.TypeCtx()); err == nil {
			if isTrue == 0 {
				// c is false
				ret *= 0
			}
			// c is true, no need to change ret
			mask &^= 1 << uint64(i)
			delete(notCoveredConstants, i)
		}
		// Not expected to come here:
		// err != nil, no need to do anything.
	}

	// Try to cover remaining DNF conditions using independence assumption,
	// i.e., sel(condA or condB) = sel(condA) + sel(condB) - sel(condA) * sel(condB)
OUTER:
	for i, scalarCond := range notCoveredDNF {
		// If there are columns not in stats, we won't handle them. This case might happen after DDL operations.
		cols := expression.ExtractColumns(scalarCond)
		for i := range cols {
			if _, ok := coll.Columns[cols[i].UniqueID]; !ok {
				continue OUTER
			}
		}

		dnfItems := expression.FlattenDNFConditions(scalarCond)
		dnfItems = ranger.MergeDNFItems4Col(ctx, dnfItems)
		// If the conditions only contain a single column, we won't handle them.
		if len(dnfItems) <= 1 {
			continue
		}

		selectivity := 0.0
		for _, cond := range dnfItems {
			// In selectivity calculation, we don't handle CorrelatedColumn, so we directly skip over it.
			// Other kinds of `Expression`, i.e., Constant, Column and ScalarFunction all can possibly be built into
			// ranges and used to calculation selectivity, so we accept them all.
			_, ok := cond.(*expression.CorrelatedColumn)
			if ok {
				continue
			}

			var cnfItems []expression.Expression
			if scalar, ok := cond.(*expression.ScalarFunction); ok && scalar.FuncName.L == ast.LogicAnd {
				cnfItems = expression.FlattenCNFConditions(scalar)
			} else {
				cnfItems = append(cnfItems, cond)
			}

			curSelectivity, _, err := Selectivity(ctx, coll, cnfItems, nil)
			if err != nil {
				logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
				curSelectivity = selectionFactor
			}

			selectivity = selectivity + curSelectivity - selectivity*curSelectivity
		}
		if sc.EnableOptimizerCETrace {
			// Tracing for the expression estimation results of this DNF.
			ceTraceExpr(ctx, tableID, "Table Stats-Expression-DNF", scalarCond, selectivity*float64(coll.RealtimeCount))
		} else if sc.EnableOptimizerDebugTrace {
			debugtrace.RecordAnyValuesWithNames(ctx, "Expression", remainedExprStrs[i], "Selectivity", selectivity)
		}

		if selectivity != 0 {
			ret *= selectivity
			mask &^= 1 << uint64(i)
			delete(notCoveredDNF, i)
		}
		if sc.EnableOptimizerCETrace {
			// Tracing for the expression estimation results after applying the DNF estimation result.
			curExpr = append(curExpr, remainedExprs[i])
			expr := expression.ComposeCNFCondition(ctx, curExpr...)
			ceTraceExpr(ctx, tableID, "Table Stats-Expression-CNF", expr, ret*float64(coll.RealtimeCount))
		}
	}

	// Try to cover remaining string matching functions by evaluating the expressions with TopN to estimate.
	if ctx.GetSessionVars().EnableEvalTopNEstimationForStrMatch() {
		for i, scalarCond := range notCoveredStrMatch {
			ok, sel, err := GetSelectivityByFilter(ctx, coll, []expression.Expression{scalarCond})
			if err != nil {
				sc.AppendWarning(errors.NewNoStackError("Error when using TopN-assisted estimation: " + err.Error()))
			}
			if !ok {
				continue
			}
			ret *= sel
			mask &^= 1 << uint64(i)
			delete(notCoveredStrMatch, i)
			if sc.EnableOptimizerDebugTrace {
				debugtrace.RecordAnyValuesWithNames(ctx, "Expression", remainedExprStrs[i], "Selectivity", sel)
			}
		}
		for i, scalarCond := range notCoveredNegateStrMatch {
			ok, sel, err := GetSelectivityByFilter(ctx, coll, []expression.Expression{scalarCond})
			if err != nil {
				sc.AppendWarning(errors.NewNoStackError("Error when using TopN-assisted estimation: " + err.Error()))
			}
			if !ok {
				continue
			}
			ret *= sel
			mask &^= 1 << uint64(i)
			delete(notCoveredNegateStrMatch, i)
			if sc.EnableOptimizerDebugTrace {
				debugtrace.RecordAnyValuesWithNames(ctx, "Expression", remainedExprStrs[i], "Selectivity", sel)
			}
		}
	}

	// At last, if there are still conditions which cannot be estimated, we multiply the selectivity with
	// the minimal default selectivity of the remaining conditions.
	// Currently, only string matching functions (like and regexp) may have a different default selectivity,
	// other expressions' default selectivity is selectionFactor.
	if mask > 0 {
		minSelectivity := 1.0
		if len(notCoveredConstants) > 0 || len(notCoveredDNF) > 0 || len(notCoveredOtherExpr) > 0 {
			minSelectivity = math.Min(minSelectivity, selectionFactor)
		}
		if len(notCoveredStrMatch) > 0 {
			minSelectivity = math.Min(minSelectivity, ctx.GetSessionVars().GetStrMatchDefaultSelectivity())
		}
		if len(notCoveredNegateStrMatch) > 0 {
			minSelectivity = math.Min(minSelectivity, ctx.GetSessionVars().GetNegateStrMatchDefaultSelectivity())
		}
		ret *= minSelectivity
		if sc.EnableOptimizerDebugTrace {
			debugtrace.RecordAnyValuesWithNames(ctx, "Default Selectivity", minSelectivity)
		}
	}

	if sc.EnableOptimizerCETrace {
		// Tracing for the expression estimation results after applying the default selectivity.
		totalExpr := expression.ComposeCNFCondition(ctx, remainedExprs...)
		ceTraceExpr(ctx, tableID, "Table Stats-Expression-CNF", totalExpr, ret*float64(coll.RealtimeCount))
	}
	return ret, nodes, nil
}

// StatsNode is used for calculating selectivity.
type StatsNode struct {
	// Ranges contains all the Ranges we got.
	Ranges []*ranger.Range
	Tp     int
	ID     int64
	// mask is a bit pattern whose ith bit will indicate whether the ith expression is covered by this index/column.
	mask int64
	// Selectivity indicates the Selectivity of this column/index.
	Selectivity float64
	// numCols is the number of columns contained in the index or column(which is always 1).
	numCols int
	// partCover indicates whether the bit in the mask is for a full cover or partial cover. It is only true
	// when the condition is a DNF expression on index, and the expression is not totally extracted as access condition.
	partCover bool
}

// The type of the StatsNode.
const (
	IndexType = iota
	PkType
	ColType
)

func compareType(l, r int) int {
	if l == r {
		return 0
	}
	if l == ColType {
		return -1
	}
	if l == PkType {
		return 1
	}
	if r == ColType {
		return 1
	}
	return -1
}

const unknownColumnID = math.MinInt64

// getConstantColumnID receives two expressions and if one of them is column and another is constant, it returns the
// ID of the column.
func getConstantColumnID(e []expression.Expression) int64 {
	if len(e) != 2 {
		return unknownColumnID
	}
	col, ok1 := e[0].(*expression.Column)
	_, ok2 := e[1].(*expression.Constant)
	if ok1 && ok2 {
		return col.ID
	}
	col, ok1 = e[1].(*expression.Column)
	_, ok2 = e[0].(*expression.Constant)
	if ok1 && ok2 {
		return col.ID
	}
	return unknownColumnID
}

// GetUsableSetsByGreedy will select the indices and pk used for calculate selectivity by greedy algorithm.
func GetUsableSetsByGreedy(nodes []*StatsNode) (newBlocks []*StatsNode) {
	slices.SortFunc(nodes, func(i, j *StatsNode) int {
		if r := compareType(i.Tp, j.Tp); r != 0 {
			return r
		}
		return cmp.Compare(i.ID, j.ID)
	})
	marked := make([]bool, len(nodes))
	mask := int64(math.MaxInt64)
	for {
		// Choose the index that covers most.
		bestID, bestCount, bestTp, bestNumCols, bestMask, bestSel := -1, 0, ColType, 0, int64(0), float64(0)
		for i, set := range nodes {
			if marked[i] {
				continue
			}
			curMask := set.mask & mask
			if curMask != set.mask {
				marked[i] = true
				continue
			}
			bits := bits.OnesCount64(uint64(curMask))
			// This set cannot cover any thing, just skip it.
			if bits == 0 {
				marked[i] = true
				continue
			}
			// We greedy select the stats info based on:
			// (1): The stats type, always prefer the primary key or index.
			// (2): The number of expression that it covers, the more the better.
			// (3): The number of columns that it contains, the less the better.
			// (4): The selectivity of the covered conditions, the less the better.
			//      The rationale behind is that lower selectivity tends to reflect more functional dependencies
			//      between columns. It's hard to decide the priority of this rule against rule 2 and 3, in order
			//      to avoid massive plan changes between tidb-server versions, I adopt this conservative strategy
			//      to impose this rule after rule 2 and 3.
			if (bestTp == ColType && set.Tp != ColType) ||
				bestCount < bits ||
				(bestCount == bits && bestNumCols > set.numCols) ||
				(bestCount == bits && bestNumCols == set.numCols && bestSel > set.Selectivity) {
				bestID, bestCount, bestTp, bestNumCols, bestMask, bestSel = i, bits, set.Tp, set.numCols, curMask, set.Selectivity
			}
		}
		if bestCount == 0 {
			break
		}

		// Update the mask, remove the bit that nodes[bestID].mask has.
		mask &^= bestMask

		newBlocks = append(newBlocks, nodes[bestID])
		marked[bestID] = true
	}
	return
}

// isColEqCorCol checks if the expression is a eq function that one side is correlated column and another is column.
// If so, it will return the column's reference. Otherwise return nil instead.
func isColEqCorCol(filter expression.Expression) *expression.Column {
	f, ok := filter.(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.EQ {
		return nil
	}
	if c, ok := f.GetArgs()[0].(*expression.Column); ok {
		if _, ok := f.GetArgs()[1].(*expression.CorrelatedColumn); ok {
			return c
		}
	}
	if c, ok := f.GetArgs()[1].(*expression.Column); ok {
		if _, ok := f.GetArgs()[0].(*expression.CorrelatedColumn); ok {
			return c
		}
	}
	return nil
}

// findPrefixOfIndexByCol will find columns in index by checking the unique id or the virtual expression.
// So it will return at once no matching column is found.
func findPrefixOfIndexByCol(ctx sessionctx.Context, cols []*expression.Column, idxColIDs []int64,
	cachedPath *planutil.AccessPath) []*expression.Column {
	if cachedPath != nil {
		idxCols := cachedPath.IdxCols
		retCols := make([]*expression.Column, 0, len(idxCols))
	idLoop:
		for _, idCol := range idxCols {
			for _, col := range cols {
				if col.EqualByExprAndID(ctx, idCol) {
					retCols = append(retCols, col)
					continue idLoop
				}
			}
			// If no matching column is found, just return.
			return retCols
		}
		return retCols
	}
	return expression.FindPrefixOfIndex(cols, idxColIDs)
}

func getMaskAndRanges(ctx sessionctx.Context, exprs []expression.Expression, rangeType ranger.RangeType,
	lengths []int, cachedPath *planutil.AccessPath, cols ...*expression.Column) (
	mask int64, ranges []*ranger.Range, partCover bool, err error) {
	isDNF := false
	var accessConds, remainedConds []expression.Expression
	switch rangeType {
	case ranger.ColumnRangeType:
		accessConds = ranger.ExtractAccessConditionsForColumn(ctx, exprs, cols[0])
		ranges, accessConds, _, err = ranger.BuildColumnRange(accessConds, ctx, cols[0].RetType,
			types.UnspecifiedLength, ctx.GetSessionVars().RangeMaxSize)
	case ranger.IndexRangeType:
		if cachedPath != nil {
			ranges, accessConds, remainedConds, isDNF = cachedPath.Ranges,
				cachedPath.AccessConds, cachedPath.TableFilters, cachedPath.IsDNFCond
			break
		}
		var res *ranger.DetachRangeResult
		res, err = ranger.DetachCondAndBuildRangeForIndex(ctx, exprs, cols, lengths, ctx.GetSessionVars().RangeMaxSize)
		if err != nil {
			return 0, nil, false, err
		}
		ranges, accessConds, remainedConds, isDNF = res.Ranges, res.AccessConds, res.RemainedConds, res.IsDNFCond
	default:
		panic("should never be here")
	}
	if err != nil {
		return 0, nil, false, err
	}
	if isDNF && len(accessConds) > 0 {
		mask |= 1
		return mask, ranges, len(remainedConds) > 0, nil
	}
	for i := range exprs {
		for j := range accessConds {
			if exprs[i].Equal(ctx, accessConds[j]) {
				mask |= 1 << uint64(i)
				break
			}
		}
	}
	return mask, ranges, false, nil
}

// GetSelectivityByFilter try to estimate selectivity of expressions by evaluate the expressions using TopN, Histogram buckets boundaries and NULL.
// Currently, this method can only handle expressions involving a single column.
func GetSelectivityByFilter(sctx sessionctx.Context, coll *statistics.HistColl, filters []expression.Expression) (ok bool, selectivity float64, err error) {
	// 1. Make sure the expressions
	//   (1) are safe to be evaluated here,
	//   (2) involve only one column,
	//   (3) and this column is not a "new collation" string column so that we're able to restore values from the stats.
	for _, filter := range filters {
		if expression.IsMutableEffectsExpr(filter) {
			return false, 0, nil
		}
	}
	if expression.ContainCorrelatedColumn(filters) {
		return false, 0, nil
	}
	cols := expression.ExtractColumnsFromExpressions(nil, filters, nil)
	if len(cols) != 1 {
		return false, 0, nil
	}
	col := cols[0]
	tp := col.RetType
	if types.IsString(tp.GetType()) && collate.NewCollationEnabled() && !collate.IsBinCollation(tp.GetCollate()) {
		return false, 0, nil
	}

	// 2. Get the available stats, make sure it's a ver2 stats and get the needed data structure from it.
	isIndex, i := findAvailableStatsForCol(sctx, coll, col.UniqueID)
	if i < 0 {
		return false, 0, nil
	}
	var statsVer, nullCnt int64
	var histTotalCnt, totalCnt float64
	var topnTotalCnt uint64
	var hist *statistics.Histogram
	var topn *statistics.TopN
	if isIndex {
		stats := coll.Indices[i]
		statsVer = stats.StatsVer
		hist = &stats.Histogram
		nullCnt = hist.NullCount
		topn = stats.TopN
	} else {
		stats := coll.Columns[i]
		statsVer = stats.StatsVer
		hist = &stats.Histogram
		nullCnt = hist.NullCount
		topn = stats.TopN
	}
	// Only in stats ver2, we can assume that: TopN + Histogram + NULL == All data
	if statsVer != statistics.Version2 {
		return false, 0, nil
	}
	topnTotalCnt = topn.TotalCount()
	histTotalCnt = hist.NotNullCount()
	totalCnt = float64(topnTotalCnt) + histTotalCnt + float64(nullCnt)

	var topNSel, histSel, nullSel float64

	// Prepare for evaluation.

	// For execution, we use Column.Index instead of Column.UniqueID to locate a column.
	// We have only one column here, so we set it to 0.
	originalIndex := col.Index
	col.Index = 0
	defer func() {
		// Restore the original Index to avoid unexpected situation.
		col.Index = originalIndex
	}()
	topNLen := 0
	histBucketsLen := hist.Len()
	if topn != nil {
		topNLen = len(topn.TopN)
	}
	c := chunk.NewChunkWithCapacity([]*types.FieldType{tp}, max(1, topNLen))
	selected := make([]bool, 0, max(histBucketsLen, topNLen))

	// 3. Calculate the TopN part selectivity.
	// This stage is considered as the core functionality of this method, errors in this stage would make this entire method fail.
	var topNSelectedCnt uint64
	if topn != nil {
		for _, item := range topn.TopN {
			_, val, err := codec.DecodeOne(item.Encoded)
			if err != nil {
				return false, 0, err
			}
			c.AppendDatum(0, &val)
		}
		selected, err = expression.VectorizedFilter(sctx, filters, chunk.NewIterator4Chunk(c), selected)
		if err != nil {
			return false, 0, err
		}
		for i, isTrue := range selected {
			if isTrue {
				topNSelectedCnt += topn.TopN[i].Count
			}
		}
	}
	topNSel = float64(topNSelectedCnt) / totalCnt

	// 4. Calculate the Histogram part selectivity.
	// The buckets upper bounds and the Bucket.Repeat are used like the TopN above.
	// The buckets lower bounds are used as random samples and are regarded equally.
	if hist != nil && histTotalCnt > 0 {
		selected = selected[:0]
		selected, err = expression.VectorizedFilter(sctx, filters, chunk.NewIterator4Chunk(hist.Bounds), selected)
		if err != nil {
			return false, 0, err
		}
		var bucketRepeatTotalCnt, bucketRepeatSelectedCnt, lowerBoundMatchCnt int64
		for i := range hist.Buckets {
			bucketRepeatTotalCnt += hist.Buckets[i].Repeat
			if len(selected) < 2*i {
				// This should not happen, but we add this check for safety.
				break
			}
			if selected[2*i] {
				lowerBoundMatchCnt++
			}
			if selected[2*i+1] {
				bucketRepeatSelectedCnt += hist.Buckets[i].Repeat
			}
		}
		var lowerBoundsRatio, upperBoundsRatio, lowerBoundsSel, upperBoundsSel float64
		upperBoundsRatio = min(float64(bucketRepeatTotalCnt)/histTotalCnt, 1)
		lowerBoundsRatio = 1 - upperBoundsRatio
		if bucketRepeatTotalCnt > 0 {
			upperBoundsSel = float64(bucketRepeatSelectedCnt) / float64(bucketRepeatTotalCnt)
		}
		lowerBoundsSel = float64(lowerBoundMatchCnt) / float64(histBucketsLen)
		histSel = lowerBoundsSel*lowerBoundsRatio + upperBoundsSel*upperBoundsRatio
		histSel *= histTotalCnt / totalCnt
	}

	// 5. Calculate the NULL part selectivity.
	// Errors in this staged would be returned, but would not make this entire method fail.
	c.Reset()
	c.AppendNull(0)
	selected = selected[:0]
	selected, err = expression.VectorizedFilter(sctx, filters, chunk.NewIterator4Chunk(c), selected)
	if err != nil || len(selected) != 1 || !selected[0] {
		nullSel = 0
	} else {
		nullSel = float64(nullCnt) / totalCnt
	}

	// 6. Get the final result.
	res := topNSel + histSel + nullSel
	return true, res, err
}

func findAvailableStatsForCol(sctx sessionctx.Context, coll *statistics.HistColl, uniqueID int64) (isIndex bool, idx int64) {
	// try to find available stats in column stats
	if colStats, ok := coll.Columns[uniqueID]; ok && colStats != nil && !colStats.IsInvalid(sctx, coll.Pseudo) && colStats.IsFullLoad() {
		return false, uniqueID
	}
	// try to find available stats in single column index stats (except for prefix index)
	for idxStatsIdx, cols := range coll.Idx2ColumnIDs {
		if len(cols) == 1 && cols[0] == uniqueID {
			idxStats, ok := coll.Indices[idxStatsIdx]
			if ok &&
				idxStats.Info.Columns[0].Length == types.UnspecifiedLength &&
				!idxStats.IsInvalid(sctx, coll.Pseudo) &&
				idxStats.IsFullLoad() {
				return true, idxStatsIdx
			}
		}
	}
	return false, -1
}

// getEqualCondSelectivity gets the selectivity of the equal conditions.
func getEqualCondSelectivity(sctx sessionctx.Context, coll *statistics.HistColl, idx *statistics.Index, bytes []byte,
	usedColsLen int, idxPointRange *ranger.Range) (result float64, err error) {
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			var idxName string
			if idx != nil && idx.Info != nil {
				idxName = idx.Info.Name.O
			}
			debugtrace.RecordAnyValuesWithNames(
				sctx,
				"Index Name", idxName,
				"Encoded", bytes,
				"UsedColLen", usedColsLen,
				"Range", idxPointRange.String(),
				"Result", result,
				"error", err,
			)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	coverAll := len(idx.Info.Columns) == usedColsLen
	// In this case, the row count is at most 1.
	if idx.Info.Unique && coverAll {
		return 1.0 / idx.TotalRowCount(), nil
	}
	val := types.NewBytesDatum(bytes)
	if outOfRangeOnIndex(idx, val) {
		// When the value is out of range, we could not found this value in the CM Sketch,
		// so we use heuristic methods to estimate the selectivity.
		if idx.NDV > 0 && coverAll {
			return outOfRangeEQSelectivity(sctx, idx.NDV, coll.RealtimeCount, int64(idx.TotalRowCount())), nil
		}
		// The equal condition only uses prefix columns of the index.
		colIDs := coll.Idx2ColumnIDs[idx.ID]
		var ndv int64
		for i, colID := range colIDs {
			if i >= usedColsLen {
				break
			}
			if col, ok := coll.Columns[colID]; ok {
				ndv = max(ndv, col.Histogram.NDV)
			}
		}
		return outOfRangeEQSelectivity(sctx, ndv, coll.RealtimeCount, int64(idx.TotalRowCount())), nil
	}

	minRowCount, crossValidSelectivity, err := crossValidationSelectivity(sctx, coll, idx, usedColsLen, idxPointRange)
	if err != nil {
		return 0, err
	}

	idxCount := float64(idx.QueryBytes(sctx, bytes))
	if minRowCount < idxCount {
		return crossValidSelectivity, nil
	}
	return idxCount / idx.TotalRowCount(), nil
}

// outOfRangeEQSelectivity estimates selectivities for out-of-range values.
// It assumes all modifications are insertions and all new-inserted rows are uniformly distributed
// and has the same distribution with analyzed rows, which means each unique value should have the
// same number of rows(Tot/NDV) of it.
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func outOfRangeEQSelectivity(sctx sessionctx.Context, ndv, realtimeRowCount, columnRowCount int64) (result float64) {
	if sctx != nil && sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Result", result)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	increaseRowCount := realtimeRowCount - columnRowCount
	if increaseRowCount <= 0 {
		return 0 // it must be 0 since the histogram contains the whole data
	}
	if ndv < outOfRangeBetweenRate {
		ndv = outOfRangeBetweenRate // avoid inaccurate selectivity caused by small NDV
	}
	selectivity := 1 / float64(ndv)
	if selectivity*float64(columnRowCount) > float64(increaseRowCount) {
		selectivity = float64(increaseRowCount) / float64(columnRowCount)
	}
	return selectivity
}

// crossValidationSelectivity gets the selectivity of multi-column equal conditions by cross validation.
func crossValidationSelectivity(
	sctx sessionctx.Context,
	coll *statistics.HistColl,
	idx *statistics.Index,
	usedColsLen int,
	idxPointRange *ranger.Range,
) (
	minRowCount float64,
	crossValidationSelectivity float64,
	err error,
) {
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			var idxName string
			if idx != nil && idx.Info != nil {
				idxName = idx.Info.Name.O
			}
			debugtrace.RecordAnyValuesWithNames(
				sctx,
				"Index Name", idxName,
				"minRowCount", minRowCount,
				"crossValidationSelectivity", crossValidationSelectivity,
				"error", err,
			)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	minRowCount = math.MaxFloat64
	cols := coll.Idx2ColumnIDs[idx.ID]
	crossValidationSelectivity = 1.0
	totalRowCount := idx.TotalRowCount()
	for i, colID := range cols {
		if i >= usedColsLen {
			break
		}
		if col, ok := coll.Columns[colID]; ok {
			if col.IsInvalid(sctx, coll.Pseudo) {
				continue
			}
			// Since the column range is point range(LowVal is equal to HighVal), we need to set both LowExclude and HighExclude to false.
			// Otherwise we would get 0.0 estRow from GetColumnRowCount.
			rang := ranger.Range{
				LowVal:      []types.Datum{idxPointRange.LowVal[i]},
				LowExclude:  false,
				HighVal:     []types.Datum{idxPointRange.HighVal[i]},
				HighExclude: false,
				Collators:   []collate.Collator{idxPointRange.Collators[i]},
			}

			rowCount, err := GetColumnRowCount(sctx, col, []*ranger.Range{&rang}, coll.RealtimeCount, coll.ModifyCount, col.IsHandle)
			if err != nil {
				return 0, 0, err
			}
			crossValidationSelectivity = crossValidationSelectivity * (rowCount / totalRowCount)

			if rowCount < minRowCount {
				minRowCount = rowCount
			}
		}
	}
	return minRowCount, crossValidationSelectivity, nil
}
