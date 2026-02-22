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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rule

import (
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func newListPartitionPruner(ctx base.PlanContext, tbl table.Table, partitionNames []ast.CIStr, s *PartitionProcessor, pruneList *tables.ForListPruning, columns []*expression.Column) *listPartitionPruner {
	pruneList = pruneList.Clone()
	for i := range pruneList.PruneExprCols {
		for j := range columns {
			if columns[j].ID == pruneList.PruneExprCols[i].ID {
				pruneList.PruneExprCols[i].UniqueID = columns[j].UniqueID
				break
			}
		}
	}
	for i := range pruneList.ColPrunes {
		for j := range columns {
			if columns[j].ID == pruneList.ColPrunes[i].ExprCol.ID {
				pruneList.ColPrunes[i].ExprCol.UniqueID = columns[j].UniqueID
				break
			}
		}
	}
	fullRange := make(map[int]struct{})
	fullRange[FullRange] = struct{}{}
	return &listPartitionPruner{
		PartitionProcessor: s,
		ctx:                ctx,
		pi:                 tbl.Meta().Partition,
		partitionNames:     partitionNames,
		fullRange:          fullRange,
		listPrune:          pruneList,
	}
}

func (l *listPartitionPruner) locatePartition(cond expression.Expression) (tables.ListPartitionLocation, bool, error) {
	switch sf := cond.(type) {
	case *expression.Constant:
		b, err := sf.Value.ToBool(l.ctx.GetSessionVars().StmtCtx.TypeCtx())
		if err == nil && b == 0 {
			// A constant false expression.
			return nil, false, nil
		}
	case *expression.ScalarFunction:
		switch sf.FuncName.L {
		case ast.LogicOr:
			dnfItems := expression.FlattenDNFConditions(sf)
			return l.locatePartitionByDNFCondition(dnfItems)
		case ast.LogicAnd:
			cnfItems := expression.FlattenCNFConditions(sf)
			return l.locatePartitionByCNFCondition(cnfItems)
		}
		return l.locatePartitionByColumn(sf)
	}
	return nil, true, nil
}

func (l *listPartitionPruner) locatePartitionByCNFCondition(conds []expression.Expression) (tables.ListPartitionLocation, bool, error) {
	if len(conds) == 0 {
		return nil, true, nil
	}
	countFull := 0
	helper := tables.NewListPartitionLocationHelper()
	for _, cond := range conds {
		cnfLoc, isFull, err := l.locatePartition(cond)
		if err != nil {
			return nil, false, err
		}
		if isFull {
			countFull++
			continue
		}
		if cnfLoc.IsEmpty() {
			// No partition for intersection, just return no partitions.
			return nil, false, nil
		}
		if !helper.Intersect(cnfLoc) {
			return nil, false, nil
		}
	}
	if countFull == len(conds) {
		return nil, true, nil
	}
	return helper.GetLocation(), false, nil
}

func (l *listPartitionPruner) locatePartitionByDNFCondition(conds []expression.Expression) (tables.ListPartitionLocation, bool, error) {
	if len(conds) == 0 {
		return nil, true, nil
	}
	helper := tables.NewListPartitionLocationHelper()
	for _, cond := range conds {
		dnfLoc, isFull, err := l.locatePartition(cond)
		if err != nil || isFull {
			return nil, isFull, err
		}
		helper.Union(dnfLoc)
	}
	return helper.GetLocation(), false, nil
}

// locatePartitionByColumn uses to locate partition by the one of the list columns value.
// Such as: partition by list columns(a,b) (partition p0 values in ((1,1),(2,2)), partition p1 values in ((6,6),(7,7)));
// and if the condition is `a=1`, then we can use `a=1` and the expression `(a in (1,2))` to locate partition `p0`.
func (l *listPartitionPruner) locatePartitionByColumn(cond *expression.ScalarFunction) (tables.ListPartitionLocation, bool, error) {
	condCols := expression.ExtractColumns(cond)
	if len(condCols) != 1 {
		return nil, true, nil
	}
	var colPrune *tables.ForListColumnPruning
	for _, cp := range l.listPrune.ColPrunes {
		if cp.ExprCol.ID == condCols[0].ID {
			colPrune = cp
			break
		}
	}
	if colPrune == nil {
		return nil, true, nil
	}
	return l.locateColumnPartitionsByCondition(cond, colPrune)
}

func (l *listPartitionPruner) locateColumnPartitionsByCondition(cond expression.Expression, colPrune *tables.ForListColumnPruning) (tables.ListPartitionLocation, bool, error) {
	ranges, err := l.detachCondAndBuildRange([]expression.Expression{cond}, colPrune.ExprCol)
	if err != nil {
		return nil, false, err
	}

	sc := l.ctx.GetSessionVars().StmtCtx
	tc, ec := sc.TypeCtx(), sc.ErrCtx()
	helper := tables.NewListPartitionLocationHelper()
	for _, r := range ranges {
		if len(r.LowVal) != 1 || len(r.HighVal) != 1 {
			return nil, true, nil
		}
		var locations []tables.ListPartitionLocation
		if r.IsPointNullable(tc) {
			location, err := colPrune.LocatePartition(tc, ec, r.HighVal[0])
			if types.ErrOverflow.Equal(err) {
				return nil, true, nil // return full-scan if over-flow
			}
			if err != nil {
				return nil, false, err
			}
			if colPrune.HasDefault() {
				if location == nil || len(l.listPrune.ColPrunes) > 1 {
					if location != nil {
						locations = append(locations, location)
					}
					location = tables.ListPartitionLocation{
						tables.ListPartitionGroup{
							PartIdx:   l.listPrune.GetDefaultIdx(),
							GroupIdxs: []int{-1}, // Special group!
						},
					}
				}
			}
			locations = append(locations, location)
		} else {
			locations, err = colPrune.LocateRanges(tc, ec, r, l.listPrune.GetDefaultIdx())
			if types.ErrOverflow.Equal(err) {
				return nil, true, nil // return full-scan if over-flow
			}
			if err != nil {
				return nil, false, err
			}
			if colPrune.HasDefault() /* && len(l.listPrune.ColPrunes) > 1 */ {
				locations = append(locations,
					tables.ListPartitionLocation{
						tables.ListPartitionGroup{
							PartIdx:   l.listPrune.GetDefaultIdx(),
							GroupIdxs: []int{-1}, // Special group!
						},
					})
			}
		}
		for _, location := range locations {
			for _, pg := range location {
				idx := l.pi.GetOverlappingDroppingPartitionIdx(pg.PartIdx)
				if idx == -1 {
					// Skip dropping partitions
					continue
				}
				if idx != pg.PartIdx {
					pg = tables.ListPartitionGroup{
						PartIdx: idx,
						// TODO: Test this!!!
						// How does it work with intersection for example?
						GroupIdxs: []int{-1}, // Special group!
					}
				}
				if len(l.partitionNames) > 0 {
					if l.FindByName(l.partitionNames, l.pi.Definitions[pg.PartIdx].Name.L) {
						helper.UnionPartitionGroup(pg)
					}
				} else {
					helper.UnionPartitionGroup(pg)
				}
			}
		}
	}
	return helper.GetLocation(), false, nil
}

func (l *listPartitionPruner) detachCondAndBuildRange(conds []expression.Expression, exprCols ...*expression.Column) ([]*ranger.Range, error) {
	cols := make([]*expression.Column, 0, len(exprCols))
	colLen := make([]int, 0, len(exprCols))
	for _, c := range exprCols {
		c = c.Clone().(*expression.Column)
		cols = append(cols, c)
		colLen = append(colLen, types.UnspecifiedLength)
	}

	detachedResult, err := ranger.DetachCondAndBuildRangeForPartition(l.ctx.GetRangerCtx(), conds, cols, colLen, l.ctx.GetSessionVars().RangeMaxSize)
	if err != nil {
		return nil, err
	}
	return detachedResult.Ranges, nil
}

func (l *listPartitionPruner) findUsedListColumnsPartitions(conds []expression.Expression) (map[int]struct{}, error) {
	if len(conds) == 0 {
		return l.fullRange, nil
	}
	location, isFull, err := l.locatePartitionByCNFCondition(conds)
	if err != nil {
		return nil, err
	}
	if isFull {
		return l.fullRange, nil
	}
	used := make(map[int]struct{}, len(location))
	for _, pg := range location {
		used[pg.PartIdx] = struct{}{}
	}
	return used, nil
}

func (l *listPartitionPruner) findUsedListPartitions(conds []expression.Expression) (map[int]struct{}, error) {
	if len(conds) == 0 {
		return l.fullRange, nil
	}
	exprCols := l.listPrune.PruneExprCols
	pruneExpr := l.listPrune.PruneExpr
	ranges, err := l.detachCondAndBuildRange(conds, exprCols...)
	if err != nil {
		return nil, err
	}
	used := make(map[int]struct{}, len(ranges))
	tc := l.ctx.GetSessionVars().StmtCtx.TypeCtx()
	for _, r := range ranges {
		if len(r.HighVal) != len(exprCols) || r.IsFullRange(false) {
			return l.fullRange, nil
		}
		var idxs map[int]struct{}
		if !r.IsPointNullable(tc) {
			// Only support `pruneExpr` is a Column
			if _, ok := pruneExpr.(*expression.Column); !ok {
				return l.fullRange, nil
			}
			idxs, err = l.listPrune.LocatePartitionByRange(l.ctx.GetExprCtx().GetEvalCtx(), r)
			if err != nil {
				return nil, err
			}
		} else {
			value, isNull, err := pruneExpr.EvalInt(l.ctx.GetExprCtx().GetEvalCtx(), chunk.MutRowFromDatums(r.HighVal).ToRow())
			if err != nil {
				return nil, err
			}
			idxs = make(map[int]struct{})
			idxs[l.listPrune.LocatePartition(l.ctx.GetExprCtx().GetEvalCtx(), value, isNull)] = struct{}{}
		}
		for idx := range idxs {
			idx = l.pi.GetOverlappingDroppingPartitionIdx(idx)
			if idx == -1 {
				continue
			}
			if len(l.partitionNames) > 0 && !l.FindByName(l.partitionNames, l.pi.Definitions[idx].Name.L) {
				continue
			}
			used[idx] = struct{}{}
		}
	}
	return used, nil
}

func (s *PartitionProcessor) findUsedListPartitions(ctx base.PlanContext, tbl table.Table, partitionNames []ast.CIStr,
	conds []expression.Expression, columns []*expression.Column) ([]int, error) {
	pi := tbl.Meta().Partition
	partExpr := tbl.(base.PartitionTable).PartitionExpr()

	listPruner := newListPartitionPruner(ctx, tbl, partitionNames, s, partExpr.ForListPruning, columns)
	var used map[int]struct{}
	var err error
	if partExpr.ForListPruning.ColPrunes == nil {
		used, err = listPruner.findUsedListPartitions(conds)
	} else {
		used, err = listPruner.findUsedListColumnsPartitions(conds)
	}
	if err != nil {
		return nil, err
	}
	if _, ok := used[FullRange]; ok {
		ret := make([]int, 0, len(pi.Definitions))
		for i := range pi.Definitions {
			if len(partitionNames) > 0 && !listPruner.FindByName(partitionNames, pi.Definitions[i].Name.L) {
				continue
			}
			if i != pi.GetOverlappingDroppingPartitionIdx(i) {
				continue
			}
			ret = append(ret, i)
		}
		if len(ret) == len(pi.Definitions) {
			return []int{FullRange}, nil
		}
		return ret, nil
	}
	if len(used) == len(pi.Definitions) {
		return []int{FullRange}, nil
	}
	ret := make([]int, 0, len(used))
	for k := range used {
		ret = append(ret, k)
	}
	slices.Sort(ret)
	return ret, nil
}

// PruneListPartition prune list partitions.
func (s *PartitionProcessor) PruneListPartition(ctx base.PlanContext, tbl table.Table, partitionNames []ast.CIStr,
	conds []expression.Expression, columns []*expression.Column) ([]int, error) {
	used, err := s.findUsedListPartitions(ctx, tbl, partitionNames, conds, columns)
	if err != nil {
		return nil, err
	}
	return used, nil
}
