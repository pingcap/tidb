// Copyright 2018 PingCAP, Inc.
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
	"fmt"
	"math"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func generateHashPartitionExpr(ctx base.PlanContext, pi *model.PartitionInfo, columns []*expression.Column, names types.NameSlice) (expression.Expression, error) {
	schema := expression.NewSchema(columns...)
	// Increase the PlanID to make sure some tests will pass. The old implementation to rewrite AST builds a `TableDual`
	// that causes the `PlanID` increases, and many test cases hardcoded the output plan ID in the expected result.
	// Considering the new `ParseSimpleExpr` does not do the same thing and to make the test pass,
	// we have to increase the `PlanID` here. But it is safe to remove this line without introducing any bug.
	// TODO: remove this line after fixing the test cases.
	ctx.GetSessionVars().PlanID.Add(1)
	expr, err := expression.ParseSimpleExpr(ctx.GetExprCtx(), pi.Expr, expression.WithInputSchemaAndNames(schema, names, nil))
	if err != nil {
		return nil, err
	}
	expr.HashCode()
	return expr, nil
}

func getPartColumnsForHashPartition(hashExpr expression.Expression) ([]*expression.Column, []int) {
	partCols := expression.ExtractColumns(hashExpr)
	colLen := make([]int, 0, len(partCols))
	retCols := make([]*expression.Column, 0, len(partCols))
	filled := make(map[int64]struct{})
	for i := range partCols {
		// Deal with same columns.
		if _, done := filled[partCols[i].UniqueID]; !done {
			partCols[i].Index = len(filled)
			filled[partCols[i].UniqueID] = struct{}{}
			colLen = append(colLen, types.UnspecifiedLength)
			retCols = append(retCols, partCols[i])
		}
	}
	return retCols, colLen
}

func (s *PartitionProcessor) getUsedHashPartitions(ctx base.PlanContext,
	tbl table.Table, partitionNames []ast.CIStr, columns []*expression.Column,
	conds []expression.Expression, names types.NameSlice) ([]int, error) {
	pi := tbl.Meta().Partition
	hashExpr, err := generateHashPartitionExpr(ctx, pi, columns, names)
	if err != nil {
		return nil, err
	}
	partCols, colLen := getPartColumnsForHashPartition(hashExpr)
	detachedResult, err := ranger.DetachCondAndBuildRangeForPartition(ctx.GetRangerCtx(), conds, partCols, colLen, ctx.GetSessionVars().RangeMaxSize)
	if err != nil {
		return nil, err
	}
	ranges := detachedResult.Ranges
	used := make([]int, 0, len(ranges))
	tc := ctx.GetSessionVars().StmtCtx.TypeCtx()
	for _, r := range ranges {
		if !r.IsPointNullable(tc) {
			// processing hash partition pruning. eg:
			// create table t2 (a int, b bigint, index (a), index (b)) partition by hash(a) partitions 10;
			// desc select * from t2 where t2.a between 10 and 15;
			// determine whether the partition key is int
			if col, ok := hashExpr.(*expression.Column); ok && col.RetType.EvalType() == types.ETInt {
				numPartitions := len(pi.Definitions)

				posHigh, highIsNull, err := hashExpr.EvalInt(ctx.GetExprCtx().GetEvalCtx(), chunk.MutRowFromDatums(r.HighVal).ToRow())
				if err != nil {
					return nil, err
				}

				posLow, lowIsNull, err := hashExpr.EvalInt(ctx.GetExprCtx().GetEvalCtx(), chunk.MutRowFromDatums(r.LowVal).ToRow())
				if err != nil {
					return nil, err
				}

				// consider whether the range is closed or open
				if r.LowExclude {
					posLow++
				}
				if r.HighExclude {
					posHigh--
				}

				var rangeScalar uint64
				if mysql.HasUnsignedFlag(col.RetType.GetFlag()) {
					// Avoid integer overflow
					if uint64(posHigh) < uint64(posLow) {
						rangeScalar = 0
					} else {
						rangeScalar = uint64(posHigh) - uint64(posLow)
					}
				} else {
					// Avoid integer overflow
					if posHigh < posLow {
						rangeScalar = 0
					} else {
						rangeScalar = uint64(posHigh - posLow)
					}
				}

				// if range is less than the number of partitions, there will be unused partitions we can prune out.
				if rangeScalar < uint64(numPartitions) && !highIsNull && !lowIsNull {
					for i := range rangeScalar + 1 {
						idx := mathutil.Abs((posLow + int64(i)) % int64(numPartitions))
						if len(partitionNames) > 0 && !s.FindByName(partitionNames, pi.Definitions[idx].Name.L) {
							continue
						}
						used = append(used, int(idx))
					}
					continue
				}

				// issue:#22619
				if col.RetType.GetType() == mysql.TypeBit {
					// maximum number of partitions is 8192
					if col.RetType.GetFlen() > 0 && col.RetType.GetFlen() < int(math.Log2(mysql.PartitionCountLimit)) {
						// all possible hash values
						maxUsedPartitions := 1 << col.RetType.GetFlen()
						if maxUsedPartitions < numPartitions {
							for i := range maxUsedPartitions {
								used = append(used, i)
							}
							continue
						}
					}
				}
			}

			used = []int{FullRange}
			break
		}

		// The code below is for the range `r` is a point.
		if len(r.HighVal) != len(partCols) {
			used = []int{FullRange}
			break
		}
		vals := make([]types.Datum, 0, len(partCols))
		vals = append(vals, r.HighVal...)
		pos, isNull, err := hashExpr.EvalInt(ctx.GetExprCtx().GetEvalCtx(), chunk.MutRowFromDatums(vals).ToRow())
		if err != nil {
			// If we failed to get the point position, we can just skip and ignore it.
			continue
		}
		if isNull {
			pos = 0
		}
		idx := mathutil.Abs(pos % int64(pi.Num))
		if len(partitionNames) > 0 && !s.FindByName(partitionNames, pi.Definitions[idx].Name.L) {
			continue
		}
		used = append(used, int(idx))
	}
	return used, nil
}

func (s *PartitionProcessor) getUsedKeyPartitions(ctx base.PlanContext,
	tbl table.Table, partitionNames []ast.CIStr, columns []*expression.Column,
	conds []expression.Expression, _ types.NameSlice) ([]int, error) {
	pi := tbl.Meta().Partition
	partExpr := tbl.(base.PartitionTable).PartitionExpr()
	partCols, colLen := partExpr.GetPartColumnsForKeyPartition(columns)
	pe := &tables.ForKeyPruning{KeyPartCols: partCols}
	detachedResult, err := ranger.DetachCondAndBuildRangeForPartition(ctx.GetRangerCtx(), conds, partCols, colLen, ctx.GetSessionVars().RangeMaxSize)
	if err != nil {
		return nil, err
	}
	ranges := detachedResult.Ranges
	used := make([]int, 0, len(ranges))

	tc := ctx.GetSessionVars().StmtCtx.TypeCtx()
	for _, r := range ranges {
		if !r.IsPointNullable(tc) {
			if len(partCols) == 1 && partCols[0].RetType.EvalType() == types.ETInt {
				col := partCols[0]
				posHigh, highIsNull, err := col.EvalInt(ctx.GetExprCtx().GetEvalCtx(), chunk.MutRowFromDatums(r.HighVal).ToRow())
				if err != nil {
					return nil, err
				}

				posLow, lowIsNull, err := col.EvalInt(ctx.GetExprCtx().GetEvalCtx(), chunk.MutRowFromDatums(r.LowVal).ToRow())
				if err != nil {
					return nil, err
				}

				// consider whether the range is closed or open
				if r.LowExclude {
					posLow++
				}
				if r.HighExclude {
					posHigh--
				}

				var rangeScalar uint64
				if mysql.HasUnsignedFlag(col.RetType.GetFlag()) {
					// Avoid integer overflow
					if uint64(posHigh) < uint64(posLow) {
						rangeScalar = 0
					} else {
						rangeScalar = uint64(posHigh) - uint64(posLow)
					}
				} else {
					// Avoid integer overflow
					if posHigh < posLow {
						rangeScalar = 0
					} else {
						rangeScalar = uint64(posHigh - posLow)
					}
				}

				// if range is less than the number of partitions, there will be unused partitions we can prune out.
				if rangeScalar < pi.Num && !highIsNull && !lowIsNull {
					m := make(map[int]struct{})
					for i := 0; i <= int(rangeScalar); i++ {
						var d types.Datum
						if mysql.HasUnsignedFlag(col.RetType.GetFlag()) {
							d = types.NewUintDatum(uint64(posLow) + uint64(i))
						} else {
							d = types.NewIntDatum(posLow + int64(i))
						}
						idx, err := pe.LocateKeyPartition(pi.Num, []types.Datum{d})
						if err != nil {
							// If we failed to get the point position, we can just skip and ignore it.
							continue
						}
						if _, ok := m[idx]; ok {
							// Keys maybe in a same partition, we should skip.
							continue
						}
						if len(partitionNames) > 0 && !s.FindByName(partitionNames, pi.Definitions[idx].Name.L) {
							continue
						}
						used = append(used, idx)
						m[idx] = struct{}{}
					}
					continue
				}
			}
			used = []int{FullRange}
			break
		}
		if len(r.HighVal) != len(partCols) {
			used = []int{FullRange}
			break
		}

		colVals := make([]types.Datum, 0, len(r.HighVal))
		colVals = append(colVals, r.HighVal...)
		idx, err := pe.LocateKeyPartition(pi.Num, colVals)
		if err != nil {
			// If we failed to get the point position, we can just skip and ignore it.
			continue
		}

		if len(partitionNames) > 0 && !s.FindByName(partitionNames, pi.Definitions[idx].Name.L) {
			continue
		}
		// TODO: Also return an array of the column values, to avoid doing it again for static prune mode
		used = append(used, idx)
	}
	return used, nil
}

// getUsedPartitions is used to get used partitions for hash or key partition tables
func (s *PartitionProcessor) getUsedPartitions(ctx base.PlanContext, tbl table.Table,
	partitionNames []ast.CIStr, columns []*expression.Column, conds []expression.Expression,
	names types.NameSlice, partType ast.PartitionType) ([]int, error) {
	if partType == ast.PartitionTypeHash {
		return s.getUsedHashPartitions(ctx, tbl, partitionNames, columns, conds, names)
	}
	return s.getUsedKeyPartitions(ctx, tbl, partitionNames, columns, conds, names)
}

// findUsedPartitions is used to get used partitions for hash or key partition tables.
// The first returning is the used partition index set pruned by `conds`.
func (s *PartitionProcessor) findUsedPartitions(ctx base.PlanContext,
	tbl table.Table, partitionNames []ast.CIStr, conds []expression.Expression,
	columns []*expression.Column, names types.NameSlice) ([]int, error) {
	pi := tbl.Meta().Partition
	used, err := s.getUsedPartitions(ctx, tbl, partitionNames, columns, conds, names, pi.Type)
	if err != nil {
		return nil, err
	}

	if len(partitionNames) > 0 && len(used) == 1 && used[0] == FullRange {
		or := PartitionRangeOR{PartitionRange{0, len(pi.Definitions)}}
		return s.ConvertToIntSlice(or, pi, partitionNames), nil
	}
	slices.Sort(used)
	used = slices.Compact(used)
	return used, nil
}

// ConvertToIntSlice convert partition requirement to int slices.
func (s *PartitionProcessor) ConvertToIntSlice(or PartitionRangeOR, pi *model.PartitionInfo, partitionNames []ast.CIStr) []int {
	if len(or) == 1 && or[0].Start == 0 && or[0].End == len(pi.Definitions) {
		if len(partitionNames) == 0 {
			if len(pi.Definitions) == 1 {
				// Return as singe partition, instead of full range!
				return []int{0}
			}
			return []int{FullRange}
		}
	}
	ret := make([]int, 0, len(or))
	for i := range or {
		for pos := or[i].Start; pos < or[i].End; pos++ {
			if len(partitionNames) > 0 && !s.FindByName(partitionNames, pi.Definitions[pos].Name.L) {
				continue
			}
			ret = append(ret, pos)
		}
	}
	return ret
}

func convertToRangeOr(used []int, pi *model.PartitionInfo) PartitionRangeOR {
	if len(used) == 1 && used[0] == -1 {
		return GetFullRange(len(pi.Definitions))
	}
	ret := make(PartitionRangeOR, 0, len(used))
	for _, i := range used {
		ret = append(ret, PartitionRange{i, i + 1})
	}
	return ret
}

// PruneHashOrKeyPartition is used to prune hash or key partition tables
func (s *PartitionProcessor) PruneHashOrKeyPartition(ctx base.PlanContext, tbl table.Table, partitionNames []ast.CIStr,
	conds []expression.Expression, columns []*expression.Column, names types.NameSlice) ([]int, error) {
	used, err := s.findUsedPartitions(ctx, tbl, partitionNames, conds, columns, names)
	if err != nil {
		return nil, err
	}
	return used, nil
}

// reconstructTableColNames reconstructs FieldsNames according to ds.TblCols.
// ds.names may not match ds.TblCols since ds.names is pruned while ds.TblCols contains all original columns.
// please see https://github.com/pingcap/tidb/issues/22635 for more details.
func (*PartitionProcessor) reconstructTableColNames(ds *logicalop.DataSource) ([]*types.FieldName, error) {
	names := make([]*types.FieldName, 0, len(ds.TblCols))
	// Use DeletableCols to get all the columns.
	colsInfo := ds.Table.DeletableCols()
	colsInfoMap := make(map[int64]*table.Column, len(colsInfo))
	for _, c := range colsInfo {
		colsInfoMap[c.ID] = c
	}
	for _, colExpr := range ds.TblCols {
		if colExpr.ID == model.ExtraHandleID {
			names = append(names, &types.FieldName{
				DBName:      ds.DBName,
				TblName:     ds.TableInfo.Name,
				ColName:     model.ExtraHandleName,
				OrigColName: model.ExtraHandleName,
			})
			continue
		}
		if colExpr.ID == model.ExtraPhysTblID {
			names = append(names, &types.FieldName{
				DBName:      ds.DBName,
				TblName:     ds.TableInfo.Name,
				ColName:     model.ExtraPhysTblIDName,
				OrigColName: model.ExtraPhysTblIDName,
			})
			continue
		}
		if colExpr.ID == model.ExtraCommitTSID {
			names = append(names, &types.FieldName{
				DBName:      ds.DBName,
				TblName:     ds.TableInfo.Name,
				ColName:     model.ExtraCommitTSName,
				OrigColName: model.ExtraCommitTSName,
			})
			continue
		}
		if colInfo, found := colsInfoMap[colExpr.ID]; found {
			names = append(names, &types.FieldName{
				DBName:      ds.DBName,
				TblName:     ds.TableInfo.Name,
				ColName:     colInfo.Name,
				OrigTblName: ds.TableInfo.Name,
				OrigColName: colInfo.Name,
			})
			continue
		}

		ectx := ds.SCtx().GetExprCtx().GetEvalCtx()
		return nil, errors.Trace(fmt.Errorf("information of column %v is not found", colExpr.StringWithCtx(ectx, errors.RedactLogDisable)))
	}
	return names, nil
}

func (s *PartitionProcessor) processHashOrKeyPartition(ds *logicalop.DataSource, pi *model.PartitionInfo) (base.LogicalPlan, error) {
	names, err := s.reconstructTableColNames(ds)
	if err != nil {
		return nil, err
	}

	used, err := s.PruneHashOrKeyPartition(ds.SCtx(), ds.Table, ds.PartitionNames, ds.AllConds, ds.TblCols, names)
	if err != nil {
		return nil, err
	}
	if used != nil {
		return s.makeUnionAllChildren(ds, pi, convertToRangeOr(used, pi))
	}
	tableDual := logicalop.LogicalTableDual{RowCount: 0}.Init(ds.SCtx(), ds.QueryBlockOffset())
	tableDual.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("TableDual/Static partition pruning mode")
	tableDual.SetSchema(ds.Schema())
	return tableDual, nil
}

// listPartitionPruner uses to prune partition for list partition.
type listPartitionPruner struct {
	*PartitionProcessor
	ctx            base.PlanContext
	pi             *model.PartitionInfo
	partitionNames []ast.CIStr
	fullRange      map[int]struct{}
	listPrune      *tables.ForListPruning
}

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
