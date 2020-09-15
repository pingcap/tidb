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

package statistics

import (
	"math"
	"math/bits"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	planutil "github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

// If one condition can't be calculated, we will assume that the selectivity of this condition is 0.8.
const selectionFactor = 0.8

// StatsNode is used for calculating selectivity.
type StatsNode struct {
	Tp int
	ID int64
	// mask is a bit pattern whose ith bit will indicate whether the ith expression is covered by this index/column.
	mask int64
	// Ranges contains all the Ranges we got.
	Ranges []*ranger.Range
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

// MockStatsNode is only used for test.
func MockStatsNode(id int64, m int64, num int) *StatsNode {
	return &StatsNode{ID: id, mask: m, numCols: num}
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

func pseudoSelectivity(coll *HistColl, exprs []expression.Expression) float64 {
	minFactor := selectionFactor
	colExists := make(map[string]bool)
	for _, expr := range exprs {
		fun, ok := expr.(*expression.ScalarFunction)
		if !ok {
			continue
		}
		colID := getConstantColumnID(fun.GetArgs())
		if colID == unknownColumnID {
			continue
		}
		switch fun.FuncName.L {
		case ast.EQ, ast.NullEQ, ast.In:
			minFactor = math.Min(minFactor, 1.0/pseudoEqualRate)
			col, ok := coll.Columns[colID]
			if !ok {
				continue
			}
			colExists[col.Info.Name.L] = true
			if mysql.HasUniKeyFlag(col.Info.Flag) {
				return 1.0 / float64(coll.Count)
			}
		case ast.GE, ast.GT, ast.LE, ast.LT:
			minFactor = math.Min(minFactor, 1.0/pseudoLessRate)
			// FIXME: To resolve the between case.
		}
	}
	if len(colExists) == 0 {
		return minFactor
	}
	// use the unique key info
	for _, idx := range coll.Indices {
		if !idx.Info.Unique {
			continue
		}
		unique := true
		for _, col := range idx.Info.Columns {
			if !colExists[col.Name.L] {
				unique = false
				break
			}
		}
		if unique {
			return 1.0 / float64(coll.Count)
		}
	}
	return minFactor
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

// Selectivity is a function calculate the selectivity of the expressions.
// The definition of selectivity is (row count after filter / row count before filter).
// And exprs must be CNF now, in other words, `exprs[0] and exprs[1] and ... and exprs[len - 1]` should be held when you call this.
// Currently the time complexity is o(n^2).
func (coll *HistColl) Selectivity(ctx sessionctx.Context, exprs []expression.Expression, filledPaths []*planutil.AccessPath) (float64, []*StatsNode, error) {
	// If table's count is zero or conditions are empty, we should return 100% selectivity.
	if coll.Count == 0 || len(exprs) == 0 {
		return 1, nil, nil
	}
	// TODO: If len(exprs) is bigger than 63, we could use bitset structure to replace the int64.
	// This will simplify some code and speed up if we use this rather than a boolean slice.
	if len(exprs) > 63 || (len(coll.Columns) == 0 && len(coll.Indices) == 0) {
		return pseudoSelectivity(coll, exprs), nil, nil
	}
	ret := 1.0
	var nodes []*StatsNode
	sc := ctx.GetSessionVars().StmtCtx

	remainedExprs := make([]expression.Expression, 0, len(exprs))

	// Deal with the correlated column.
	for _, expr := range exprs {
		c := isColEqCorCol(expr)
		if c == nil {
			remainedExprs = append(remainedExprs, expr)
			continue
		}

		if colHist := coll.Columns[c.UniqueID]; colHist == nil || colHist.IsInvalid(sc, coll.Pseudo) {
			ret *= 1.0 / pseudoEqualRate
			continue
		}

		colHist := coll.Columns[c.UniqueID]
		if colHist.NDV > 0 {
			ret *= 1 / float64(colHist.NDV)
		} else {
			ret *= 1.0 / pseudoEqualRate
		}
	}

	extractedCols := make([]*expression.Column, 0, len(coll.Columns))
	extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, remainedExprs, nil)
	for id, colInfo := range coll.Columns {
		col := expression.ColInfo2Col(extractedCols, colInfo.Info)
		if col != nil {
			maskCovered, ranges, _, err := getMaskAndRanges(ctx, remainedExprs, ranger.ColumnRangeType, nil, nil, col)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			nodes = append(nodes, &StatsNode{Tp: ColType, ID: id, mask: maskCovered, Ranges: ranges, numCols: 1})
			if colInfo.IsHandle {
				nodes[len(nodes)-1].Tp = PkType
				var cnt float64
				cnt, err = coll.GetRowCountByIntColumnRanges(sc, id, ranges)
				if err != nil {
					return 0, nil, errors.Trace(err)
				}
				nodes[len(nodes)-1].Selectivity = cnt / float64(coll.Count)
				continue
			}
			cnt, err := coll.GetRowCountByColumnRanges(sc, id, ranges)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			nodes[len(nodes)-1].Selectivity = cnt / float64(coll.Count)
		}
	}
	id2Paths := make(map[int64]*planutil.AccessPath)
	for _, path := range filledPaths {
		if path.IsIntHandlePath {
			continue
		}
		id2Paths[path.Index.ID] = path
	}
	for id, idxInfo := range coll.Indices {
		idxCols := expression.FindPrefixOfIndex(extractedCols, coll.Idx2ColumnIDs[id])
		if len(idxCols) > 0 {
			lengths := make([]int, 0, len(idxCols))
			for i := 0; i < len(idxCols); i++ {
				lengths = append(lengths, idxInfo.Info.Columns[i].Length)
			}
			maskCovered, ranges, partCover, err := getMaskAndRanges(ctx, remainedExprs, ranger.IndexRangeType, lengths, id2Paths[idxInfo.ID], idxCols...)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			cnt, err := coll.GetRowCountByIndexRanges(sc, id, ranges)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			selectivity := cnt / float64(coll.Count)
			nodes = append(nodes, &StatsNode{
				Tp:          IndexType,
				ID:          id,
				mask:        maskCovered,
				Ranges:      ranges,
				numCols:     len(idxInfo.Info.Columns),
				Selectivity: selectivity,
				partCover:   partCover,
			})
		}
	}
	usedSets := GetUsableSetsByGreedy(nodes)
	// Initialize the mask with the full set.
	mask := (int64(1) << uint(len(remainedExprs))) - 1
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
	}

	// Now we try to cover those still not covered DNF conditions using independence assumption,
	// i.e., sel(condA or condB) = sel(condA) + sel(condB) - sel(condA) * sel(condB)
	if mask > 0 {
		for i, expr := range remainedExprs {
			if mask&(1<<uint64(i)) == 0 {
				continue
			}
			scalarCond, ok := expr.(*expression.ScalarFunction)
			// Make sure we only handle DNF condition.
			if !ok || scalarCond.FuncName.L != ast.LogicOr {
				continue
			}
			dnfItems := expression.FlattenDNFConditions(scalarCond)
			dnfItems = ranger.MergeDNFItems4Col(ctx, dnfItems)

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

				curSelectivity, _, err := coll.Selectivity(ctx, cnfItems, nil)
				if err != nil {
					logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
					selectivity = selectionFactor
				}

				selectivity = selectivity + curSelectivity - selectivity*curSelectivity
			}

			if selectivity != 0 {
				ret *= selectivity
				mask &^= 1 << uint64(i)
			}
		}
	}

	// If there's still conditions which cannot be calculated, we will multiply a selectionFactor.
	if mask > 0 {
		ret *= selectionFactor
	}
	return ret, nodes, nil
}

func getMaskAndRanges(ctx sessionctx.Context, exprs []expression.Expression, rangeType ranger.RangeType, lengths []int, cachedPath *planutil.AccessPath, cols ...*expression.Column) (mask int64, ranges []*ranger.Range, partCover bool, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	isDNF := false
	var accessConds, remainedConds []expression.Expression
	switch rangeType {
	case ranger.ColumnRangeType:
		accessConds = ranger.ExtractAccessConditionsForColumn(exprs, cols[0].UniqueID)
		ranges, err = ranger.BuildColumnRange(accessConds, sc, cols[0].RetType, types.UnspecifiedLength)
	case ranger.IndexRangeType:
		if cachedPath != nil {
			ranges, accessConds, remainedConds, isDNF = cachedPath.Ranges, cachedPath.AccessConds, cachedPath.TableFilters, cachedPath.IsDNFCond
			break
		}
		var res *ranger.DetachRangeResult
		res, err = ranger.DetachCondAndBuildRangeForIndex(ctx, exprs, cols, lengths)
		ranges, accessConds, remainedConds, isDNF = res.Ranges, res.AccessConds, res.RemainedConds, res.IsDNFCond
		if err != nil {
			return 0, nil, false, err
		}
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

// GetUsableSetsByGreedy will select the indices and pk used for calculate selectivity by greedy algorithm.
func GetUsableSetsByGreedy(nodes []*StatsNode) (newBlocks []*StatsNode) {
	sort.Slice(nodes, func(i int, j int) bool {
		if r := compareType(nodes[i].Tp, nodes[j].Tp); r != 0 {
			return r < 0
		}
		return nodes[i].ID < nodes[j].ID
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
