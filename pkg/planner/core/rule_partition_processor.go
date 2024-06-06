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

package core

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/set"
)

// FullRange represent used all partitions.
const FullRange = -1

// partitionProcessor rewrites the ast for table partition.
// Used by static partition prune mode.
/*
// create table t (id int) partition by range (id)
//   (partition p1 values less than (10),
//    partition p2 values less than (20),
//    partition p3 values less than (30))
//
// select * from t is equal to
// select * from (union all
//      select * from p1 where id < 10
//      select * from p2 where id < 20
//      select * from p3 where id < 30)
*/
// partitionProcessor is here because it's easier to prune partition after predicate push down.
type partitionProcessor struct{}

func (s *partitionProcessor) optimize(_ context.Context, lp base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	p, err := s.rewriteDataSource(lp, opt)
	return p, planChanged, err
}

func (s *partitionProcessor) rewriteDataSource(lp base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	// Assert there will not be sel -> sel in the ast.
	switch p := lp.(type) {
	case *DataSource:
		return s.prune(p, opt)
	case *LogicalUnionScan:
		ds := p.Children()[0]
		ds, err := s.prune(ds.(*DataSource), opt)
		if err != nil {
			return nil, err
		}
		if ua, ok := ds.(*LogicalPartitionUnionAll); ok {
			// Adjust the UnionScan->Union->DataSource1, DataSource2 ... to
			// Union->(UnionScan->DataSource1), (UnionScan->DataSource2)
			children := make([]base.LogicalPlan, 0, len(ua.Children()))
			for _, child := range ua.Children() {
				us := LogicalUnionScan{
					conditions: p.conditions,
					handleCols: p.handleCols,
				}.Init(ua.SCtx(), ua.QueryBlockOffset())
				us.SetChildren(child)
				children = append(children, us)
			}
			ua.SetChildren(children...)
			ua.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("Static partition pruning mode")
			return ua, nil
		}
		// Only one partition, no union all.
		p.SetChildren(ds)
		return p, nil
	case *LogicalCTE:
		return lp, nil
	default:
		children := lp.Children()
		for i, child := range children {
			newChild, err := s.rewriteDataSource(child, opt)
			if err != nil {
				return nil, err
			}
			children[i] = newChild
		}
	}

	return lp, nil
}

// partitionTable is for those tables which implement partition.
type partitionTable interface {
	PartitionExpr() *tables.PartitionExpr
}

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
	for i := 0; i < len(partCols); i++ {
		partCols[i].Index = i
		colLen = append(colLen, types.UnspecifiedLength)
	}
	return partCols, colLen
}

func (s *partitionProcessor) getUsedHashPartitions(ctx base.PlanContext,
	tbl table.Table, partitionNames []model.CIStr, columns []*expression.Column,
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
					var i int64
					for i = 0; i <= int64(rangeScalar); i++ {
						idx := mathutil.Abs((posLow + i) % int64(numPartitions))
						if len(partitionNames) > 0 && !s.findByName(partitionNames, pi.Definitions[idx].Name.L) {
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
							for i := 0; i < maxUsedPartitions; i++ {
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
		if !r.HighVal[0].IsNull() {
			if len(r.HighVal) != len(partCols) {
				used = []int{-1}
				break
			}
		}
		highLowVals := make([]types.Datum, 0, len(r.HighVal)+len(r.LowVal))
		highLowVals = append(highLowVals, r.HighVal...)
		highLowVals = append(highLowVals, r.LowVal...)
		pos, isNull, err := hashExpr.EvalInt(ctx.GetExprCtx().GetEvalCtx(), chunk.MutRowFromDatums(highLowVals).ToRow())
		if err != nil {
			// If we failed to get the point position, we can just skip and ignore it.
			continue
		}
		if isNull {
			pos = 0
		}
		idx := mathutil.Abs(pos % int64(pi.Num))
		if len(partitionNames) > 0 && !s.findByName(partitionNames, pi.Definitions[idx].Name.L) {
			continue
		}
		used = append(used, int(idx))
	}
	return used, nil
}

func (s *partitionProcessor) getUsedKeyPartitions(ctx base.PlanContext,
	tbl table.Table, partitionNames []model.CIStr, columns []*expression.Column,
	conds []expression.Expression, _ types.NameSlice) ([]int, error) {
	pi := tbl.Meta().Partition
	partExpr := tbl.(partitionTable).PartitionExpr()
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
						if len(partitionNames) > 0 && !s.findByName(partitionNames, pi.Definitions[idx].Name.L) {
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

		if len(partitionNames) > 0 && !s.findByName(partitionNames, pi.Definitions[idx].Name.L) {
			continue
		}
		// TODO: Also return an array of the column values, to avoid doing it again for static prune mode
		used = append(used, idx)
	}
	return used, nil
}

// getUsedPartitions is used to get used partitions for hash or key partition tables
func (s *partitionProcessor) getUsedPartitions(ctx base.PlanContext, tbl table.Table,
	partitionNames []model.CIStr, columns []*expression.Column, conds []expression.Expression,
	names types.NameSlice, partType model.PartitionType) ([]int, error) {
	if partType == model.PartitionTypeHash {
		return s.getUsedHashPartitions(ctx, tbl, partitionNames, columns, conds, names)
	}
	return s.getUsedKeyPartitions(ctx, tbl, partitionNames, columns, conds, names)
}

// findUsedPartitions is used to get used partitions for hash or key partition tables.
// The first returning is the used partition index set pruned by `conds`.
func (s *partitionProcessor) findUsedPartitions(ctx base.PlanContext,
	tbl table.Table, partitionNames []model.CIStr, conds []expression.Expression,
	columns []*expression.Column, names types.NameSlice) ([]int, error) {
	pi := tbl.Meta().Partition
	used, err := s.getUsedPartitions(ctx, tbl, partitionNames, columns, conds, names, pi.Type)
	if err != nil {
		return nil, err
	}

	if len(partitionNames) > 0 && len(used) == 1 && used[0] == FullRange {
		or := partitionRangeOR{partitionRange{0, len(pi.Definitions)}}
		return s.convertToIntSlice(or, pi, partitionNames), nil
	}
	slices.Sort(used)
	ret := used[:0]
	for i := 0; i < len(used); i++ {
		if i == 0 || used[i] != used[i-1] {
			ret = append(ret, used[i])
		}
	}
	return ret, nil
}

func (s *partitionProcessor) convertToIntSlice(or partitionRangeOR, pi *model.PartitionInfo, partitionNames []model.CIStr) []int {
	if len(or) == 1 && or[0].start == 0 && or[0].end == len(pi.Definitions) {
		if len(partitionNames) == 0 {
			if len(pi.Definitions) == 1 {
				// Return as singe partition, instead of full range!
				return []int{0}
			}
			return []int{FullRange}
		}
	}
	ret := make([]int, 0, len(or))
	for i := 0; i < len(or); i++ {
		for pos := or[i].start; pos < or[i].end; pos++ {
			if len(partitionNames) > 0 && !s.findByName(partitionNames, pi.Definitions[pos].Name.L) {
				continue
			}
			ret = append(ret, pos)
		}
	}
	return ret
}

func convertToRangeOr(used []int, pi *model.PartitionInfo) partitionRangeOR {
	if len(used) == 1 && used[0] == -1 {
		return fullRange(len(pi.Definitions))
	}
	ret := make(partitionRangeOR, 0, len(used))
	for _, i := range used {
		ret = append(ret, partitionRange{i, i + 1})
	}
	return ret
}

// pruneHashOrKeyPartition is used to prune hash or key partition tables
func (s *partitionProcessor) pruneHashOrKeyPartition(ctx base.PlanContext, tbl table.Table, partitionNames []model.CIStr,
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
func (*partitionProcessor) reconstructTableColNames(ds *DataSource) ([]*types.FieldName, error) {
	names := make([]*types.FieldName, 0, len(ds.TblCols))
	// Use DeletableCols to get all the columns.
	colsInfo := ds.table.DeletableCols()
	colsInfoMap := make(map[int64]*table.Column, len(colsInfo))
	for _, c := range colsInfo {
		colsInfoMap[c.ID] = c
	}
	for _, colExpr := range ds.TblCols {
		if colExpr.ID == model.ExtraHandleID {
			names = append(names, &types.FieldName{
				DBName:      ds.DBName,
				TblName:     ds.tableInfo.Name,
				ColName:     model.ExtraHandleName,
				OrigColName: model.ExtraHandleName,
			})
			continue
		}
		if colExpr.ID == model.ExtraPidColID {
			names = append(names, &types.FieldName{
				DBName:      ds.DBName,
				TblName:     ds.tableInfo.Name,
				ColName:     model.ExtraPartitionIdName,
				OrigColName: model.ExtraPartitionIdName,
			})
			continue
		}
		if colExpr.ID == model.ExtraPhysTblID {
			names = append(names, &types.FieldName{
				DBName:      ds.DBName,
				TblName:     ds.tableInfo.Name,
				ColName:     model.ExtraPhysTblIdName,
				OrigColName: model.ExtraPhysTblIdName,
			})
			continue
		}
		if colInfo, found := colsInfoMap[colExpr.ID]; found {
			names = append(names, &types.FieldName{
				DBName:      ds.DBName,
				TblName:     ds.tableInfo.Name,
				ColName:     colInfo.Name,
				OrigTblName: ds.tableInfo.Name,
				OrigColName: colInfo.Name,
			})
			continue
		}
		return nil, errors.Trace(fmt.Errorf("information of column %v is not found", colExpr.String()))
	}
	return names, nil
}

func (s *partitionProcessor) processHashOrKeyPartition(ds *DataSource, pi *model.PartitionInfo, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	names, err := s.reconstructTableColNames(ds)
	if err != nil {
		return nil, err
	}

	used, err := s.pruneHashOrKeyPartition(ds.SCtx(), ds.table, ds.partitionNames, ds.allConds, ds.TblCols, names)
	if err != nil {
		return nil, err
	}
	if used != nil {
		return s.makeUnionAllChildren(ds, pi, convertToRangeOr(used, pi), opt)
	}
	tableDual := LogicalTableDual{RowCount: 0}.Init(ds.SCtx(), ds.QueryBlockOffset())
	tableDual.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("TableDual/Static partition pruning mode")
	tableDual.schema = ds.Schema()
	appendNoPartitionChildTraceStep(ds, tableDual, opt)
	return tableDual, nil
}

// listPartitionPruner uses to prune partition for list partition.
type listPartitionPruner struct {
	*partitionProcessor
	ctx            base.PlanContext
	pi             *model.PartitionInfo
	partitionNames []model.CIStr
	fullRange      map[int]struct{}
	listPrune      *tables.ForListPruning
}

func newListPartitionPruner(ctx base.PlanContext, tbl table.Table, partitionNames []model.CIStr, s *partitionProcessor, pruneList *tables.ForListPruning, columns []*expression.Column) *listPartitionPruner {
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
		partitionProcessor: s,
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
			if len(l.partitionNames) > 0 {
				for _, pg := range location {
					if l.findByName(l.partitionNames, l.pi.Definitions[pg.PartIdx].Name.L) {
						helper.UnionPartitionGroup(pg)
					}
				}
			} else {
				helper.Union(location)
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
		if !r.IsPointNullable(tc) {
			return l.fullRange, nil
		}
		if len(r.HighVal) != len(exprCols) {
			return l.fullRange, nil
		}
		value, isNull, err := pruneExpr.EvalInt(l.ctx.GetExprCtx().GetEvalCtx(), chunk.MutRowFromDatums(r.HighVal).ToRow())
		if err != nil {
			return nil, err
		}
		partitionIdx := l.listPrune.LocatePartition(value, isNull)
		if partitionIdx == -1 {
			continue
		}
		if len(l.partitionNames) > 0 && !l.findByName(l.partitionNames, l.pi.Definitions[partitionIdx].Name.L) {
			continue
		}
		used[partitionIdx] = struct{}{}
	}
	return used, nil
}

func (s *partitionProcessor) findUsedListPartitions(ctx base.PlanContext, tbl table.Table, partitionNames []model.CIStr,
	conds []expression.Expression, columns []*expression.Column) ([]int, error) {
	pi := tbl.Meta().Partition
	partExpr := tbl.(partitionTable).PartitionExpr()

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
		or := partitionRangeOR{partitionRange{0, len(pi.Definitions)}}
		return s.convertToIntSlice(or, pi, partitionNames), nil
	}
	ret := make([]int, 0, len(used))
	for k := range used {
		ret = append(ret, k)
	}
	slices.Sort(ret)
	return ret, nil
}

func (s *partitionProcessor) pruneListPartition(ctx base.PlanContext, tbl table.Table, partitionNames []model.CIStr,
	conds []expression.Expression, columns []*expression.Column) ([]int, error) {
	used, err := s.findUsedListPartitions(ctx, tbl, partitionNames, conds, columns)
	if err != nil {
		return nil, err
	}
	return used, nil
}

func (s *partitionProcessor) prune(ds *DataSource, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	pi := ds.tableInfo.GetPartitionInfo()
	if pi == nil {
		return ds, nil
	}
	// PushDownNot here can convert condition 'not (a != 1)' to 'a = 1'. When we build range from ds.allConds, the condition
	// like 'not (a != 1)' would not be handled so we need to convert it to 'a = 1', which can be handled when building range.
	// TODO: there may be a better way to push down Not once for all.
	for i, cond := range ds.allConds {
		ds.allConds[i] = expression.PushDownNot(ds.SCtx().GetExprCtx(), cond)
	}
	// Try to locate partition directly for hash partition.
	// TODO: See if there is a way to remove conditions that does not
	// apply for some partitions like:
	// a = 1 OR a = 2 => for p1 only "a = 1" and for p2 only "a = 2"
	// since a cannot be 2 in p1 and a cannot be 1 in p2
	switch pi.Type {
	case model.PartitionTypeRange:
		return s.processRangePartition(ds, pi, opt)
	case model.PartitionTypeHash, model.PartitionTypeKey:
		return s.processHashOrKeyPartition(ds, pi, opt)
	case model.PartitionTypeList:
		return s.processListPartition(ds, pi, opt)
	}

	return s.makeUnionAllChildren(ds, pi, fullRange(len(pi.Definitions)), opt)
}

// findByName checks whether object name exists in list.
func (*partitionProcessor) findByName(partitionNames []model.CIStr, partitionName string) bool {
	for _, s := range partitionNames {
		if s.L == partitionName {
			return true
		}
	}
	return false
}

func (*partitionProcessor) name() string {
	return "partition_processor"
}

type lessThanDataInt struct {
	data     []int64
	unsigned bool
	maxvalue bool
}

func (lt *lessThanDataInt) length() int {
	return len(lt.data)
}

func (lt *lessThanDataInt) compare(ith int, v int64, unsigned bool) int {
	// TODO: get an extra partition when `v` bigger than `lt.maxvalue``, but the result still correct.
	if ith == lt.length()-1 && lt.maxvalue {
		return 1
	}

	return types.CompareInt(lt.data[ith], lt.unsigned, v, unsigned)
}

// partitionRange represents [start, end)
type partitionRange struct {
	start int
	end   int
}

// partitionRangeOR represents OR(range1, range2, ...)
type partitionRangeOR []partitionRange

func fullRange(end int) partitionRangeOR {
	var reduceAllocation [3]partitionRange
	reduceAllocation[0] = partitionRange{0, end}
	return reduceAllocation[:1]
}

func (or partitionRangeOR) intersectionRange(start, end int) partitionRangeOR {
	// Let M = intersection, U = union, then
	// a M (b U c) == (a M b) U (a M c)
	ret := or[:0]
	for _, r1 := range or {
		newStart, newEnd := intersectionRange(r1.start, r1.end, start, end)
		// Exclude the empty one.
		if newEnd > newStart {
			ret = append(ret, partitionRange{newStart, newEnd})
		}
	}
	return ret
}

func (or partitionRangeOR) Len() int {
	return len(or)
}

func (or partitionRangeOR) Less(i, j int) bool {
	return or[i].start < or[j].start
}

func (or partitionRangeOR) Swap(i, j int) {
	or[i], or[j] = or[j], or[i]
}

func (or partitionRangeOR) union(x partitionRangeOR) partitionRangeOR {
	or = append(or, x...)
	return or.simplify()
}

func (or partitionRangeOR) simplify() partitionRangeOR {
	// if the length of the `or` is zero. We should return early.
	if len(or) == 0 {
		return or
	}
	// Make the ranges order by start.
	sort.Sort(or)
	sorted := or

	// Iterate the sorted ranges, merge the adjacent two when their range overlap.
	// For example, [0, 1), [2, 7), [3, 5), ... => [0, 1), [2, 7) ...
	res := sorted[:1]
	for _, curr := range sorted[1:] {
		last := &res[len(res)-1]
		if curr.start > last.end {
			res = append(res, curr)
		} else {
			// Merge two.
			if curr.end > last.end {
				last.end = curr.end
			}
		}
	}
	return res
}

func (or partitionRangeOR) intersection(x partitionRangeOR) partitionRangeOR {
	if or.Len() == 1 {
		return x.intersectionRange(or[0].start, or[0].end)
	}
	if x.Len() == 1 {
		return or.intersectionRange(x[0].start, x[0].end)
	}

	// Rename to x, y where len(x) > len(y)
	var y partitionRangeOR
	if or.Len() > x.Len() {
		x, y = or, x
	} else {
		y = or
	}

	// (a U b) M (c U d) => (x M c) U (x M d), x = (a U b)
	res := make(partitionRangeOR, 0, len(y))
	for _, r := range y {
		// As intersectionRange modify the raw data, we have to make a copy.
		tmp := make(partitionRangeOR, len(x))
		copy(tmp, x)
		tmp = tmp.intersectionRange(r.start, r.end)
		res = append(res, tmp...)
	}
	return res.simplify()
}

// intersectionRange calculate the intersection of [start, end) and [newStart, newEnd)
func intersectionRange(start, end, newStart, newEnd int) (s int, e int) {
	if start > newStart {
		s = start
	} else {
		s = newStart
	}

	if end < newEnd {
		e = end
	} else {
		e = newEnd
	}
	return s, e
}

func (s *partitionProcessor) pruneRangePartition(ctx base.PlanContext, pi *model.PartitionInfo, tbl table.PartitionedTable, conds []expression.Expression,
	columns []*expression.Column, names types.NameSlice) (partitionRangeOR, error) {
	partExpr := tbl.(partitionTable).PartitionExpr()

	// Partition by range columns.
	if len(pi.Columns) > 0 {
		result, err := s.pruneRangeColumnsPartition(ctx, conds, pi, partExpr, columns)
		return result, err
	}

	// Partition by range.
	col, fn, mono, err := makePartitionByFnCol(ctx, columns, names, pi.Expr)
	if err != nil {
		return nil, err
	}
	result := fullRange(len(pi.Definitions))
	if col == nil {
		return result, nil
	}

	// Extract the partition column, if the column is not null, it's possible to prune.
	pruner := rangePruner{
		lessThan: lessThanDataInt{
			data:     partExpr.ForRangePruning.LessThan,
			unsigned: mysql.HasUnsignedFlag(col.GetStaticType().GetFlag()),
			maxvalue: partExpr.ForRangePruning.MaxValue,
		},
		col:        col,
		partFn:     fn,
		monotonous: mono,
	}
	result = partitionRangeForCNFExpr(ctx, conds, &pruner, result)

	return result, nil
}

func (s *partitionProcessor) processRangePartition(ds *DataSource, pi *model.PartitionInfo, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	used, err := s.pruneRangePartition(ds.SCtx(), pi, ds.table.(table.PartitionedTable), ds.allConds, ds.TblCols, ds.names)
	if err != nil {
		return nil, err
	}
	return s.makeUnionAllChildren(ds, pi, used, opt)
}

func (s *partitionProcessor) processListPartition(ds *DataSource, pi *model.PartitionInfo, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	used, err := s.pruneListPartition(ds.SCtx(), ds.table, ds.partitionNames, ds.allConds, ds.TblCols)
	if err != nil {
		return nil, err
	}
	return s.makeUnionAllChildren(ds, pi, convertToRangeOr(used, pi), opt)
}

// makePartitionByFnCol extracts the column and function information in 'partition by ... fn(col)'.
func makePartitionByFnCol(sctx base.PlanContext, columns []*expression.Column, names types.NameSlice, partitionExpr string) (*expression.Column, *expression.ScalarFunction, monotoneMode, error) {
	monotonous := monotoneModeInvalid
	schema := expression.NewSchema(columns...)
	// Increase the PlanID to make sure some tests will pass. The old implementation to rewrite AST builds a `TableDual`
	// that causes the `PlanID` increases, and many test cases hardcoded the output plan ID in the expected result.
	// Considering the new `ParseSimpleExpr` does not do the same thing and to make the test pass,
	// we have to increase the `PlanID` here. But it is safe to remove this line without introducing any bug.
	// TODO: remove this line after fixing the test cases.
	sctx.GetSessionVars().PlanID.Add(1)
	partExpr, err := expression.ParseSimpleExpr(sctx.GetExprCtx(), partitionExpr, expression.WithInputSchemaAndNames(schema, names, nil))
	if err != nil {
		return nil, nil, monotonous, err
	}
	var col *expression.Column
	var fn *expression.ScalarFunction
	switch raw := partExpr.(type) {
	case *expression.ScalarFunction:
		args := raw.GetArgs()
		// Special handle for floor(unix_timestamp(ts)) as partition expression.
		// This pattern is so common for timestamp(3) column as partition expression that it deserve an optimization.
		if raw.FuncName.L == ast.Floor {
			if ut, ok := args[0].(*expression.ScalarFunction); ok && ut.FuncName.L == ast.UnixTimestamp {
				args1 := ut.GetArgs()
				if len(args1) == 1 {
					if c, ok1 := args1[0].(*expression.Column); ok1 {
						return c, raw, monotoneModeNonStrict, nil
					}
				}
			}
		}

		fn = raw
		monotonous = getMonotoneMode(raw.FuncName.L)
		// Check the partitionExpr is in the form: fn(col, ...)
		// There should be only one column argument, and it should be the first parameter.
		if expression.ExtractColumnSet(args...).Len() == 1 {
			if col1, ok := args[0].(*expression.Column); ok {
				col = col1
			}
		}
	case *expression.Column:
		col = raw
	}
	return col, fn, monotonous, nil
}

func minCmp(ctx base.PlanContext, lowVal []types.Datum, columnsPruner *rangeColumnsPruner, comparer []collate.Collator, lowExclude bool, gotError *bool) func(i int) bool {
	return func(i int) bool {
		for j := range lowVal {
			expr := columnsPruner.lessThan[i][j]

			if expr == nil {
				// MAXVALUE
				return true
			}
			con, ok := (*expr).(*expression.Constant)
			if !ok {
				// Not a constant, pruning not possible, so value is considered less than all partitions
				return true
			}
			// Add Null as point here?
			cmp, err := con.Value.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &lowVal[j], comparer[j])
			if err != nil {
				*gotError = true
			}
			if cmp > 0 {
				return true
			}
			if cmp < 0 {
				return false
			}
		}
		if len(lowVal) < len(columnsPruner.lessThan[i]) {
			// Not all columns given
			if lowExclude {
				// prefix cols > const, do not include this partition
				return false
			}

			colIdx := len(lowVal)
			col := columnsPruner.partCols[colIdx]
			conExpr := columnsPruner.lessThan[i][colIdx]
			if conExpr == nil {
				// MAXVALUE
				return true
			}

			// Possible to optimize by getting minvalue of the column type
			// and if lessThan is equal to that
			// we can return false, since the partition definition is
			// LESS THAN (..., colN, minValOfColM, ... ) which cannot match colN == LowVal
			if !mysql.HasNotNullFlag(col.RetType.GetFlag()) {
				// NULL cannot be part of the partitioning expression: VALUES LESS THAN (NULL...)
				// NULL is allowed in the column and will be considered as lower than any other value
				// so this partition needs to be included!
				return true
			}
			if con, ok := (*conExpr).(*expression.Constant); ok && col != nil {
				switch col.RetType.EvalType() {
				case types.ETInt:
					if mysql.HasUnsignedFlag(col.RetType.GetFlag()) {
						if con.Value.GetUint64() == 0 {
							return false
						}
					} else {
						if con.Value.GetInt64() == types.IntergerSignedLowerBound(col.GetStaticType().GetType()) {
							return false
						}
					}
				case types.ETDatetime:
					if con.Value.GetMysqlTime().IsZero() {
						return false
					}
				case types.ETString:
					if len(con.Value.GetString()) == 0 {
						return false
					}
				}
			}
			// Also if not a constant, pruning not possible, so value is considered less than all partitions
			return true
		}
		return false
	}
}

func maxCmp(ctx base.PlanContext, hiVal []types.Datum, columnsPruner *rangeColumnsPruner, comparer []collate.Collator, hiExclude bool, gotError *bool) func(i int) bool {
	return func(i int) bool {
		for j := range hiVal {
			expr := columnsPruner.lessThan[i][j]
			if expr == nil {
				// MAXVALUE
				return true
			}
			con, ok := (*expr).(*expression.Constant)
			if !ok {
				// Not a constant, include every partition, i.e. value is not less than any partition
				return false
			}
			// Add Null as point here?
			cmp, err := con.Value.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &hiVal[j], comparer[j])
			if err != nil {
				*gotError = true
				// error pushed, we will still use the cmp value
			}
			if cmp > 0 {
				return true
			}
			if cmp < 0 {
				return false
			}
		}
		// All hiVal == columnsPruner.lessThan
		if len(hiVal) < len(columnsPruner.lessThan[i]) {
			// Not all columns given
			if columnsPruner.lessThan[i][len(hiVal)] == nil {
				// MAXVALUE
				return true
			}
		}
		// if point is included, then false, due to LESS THAN
		return hiExclude
	}
}

func multiColumnRangeColumnsPruner(sctx base.PlanContext, exprs []expression.Expression,
	columnsPruner *rangeColumnsPruner, result partitionRangeOR) partitionRangeOR {
	lens := make([]int, 0, len(columnsPruner.partCols))
	for i := range columnsPruner.partCols {
		lens = append(lens, columnsPruner.partCols[i].RetType.GetFlen())
	}

	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx.GetRangerCtx(), exprs, columnsPruner.partCols, lens, sctx.GetSessionVars().RangeMaxSize)
	if err != nil {
		return fullRange(len(columnsPruner.lessThan))
	}
	if len(res.Ranges) == 0 {
		if len(res.AccessConds) == 0 && len(res.RemainedConds) == 0 {
			// Impossible conditions, like: a > 2 AND a < 1
			return partitionRangeOR{}
		}
		// Could not extract any valid range, use all partitions
		return fullRange(len(columnsPruner.lessThan))
	}

	rangeOr := make([]partitionRange, 0, len(res.Ranges))

	comparer := make([]collate.Collator, 0, len(columnsPruner.partCols))
	for i := range columnsPruner.partCols {
		comparer = append(comparer, collate.GetCollator(columnsPruner.partCols[i].RetType.GetCollate()))
	}
	gotError := false
	// Create a sort.Search where the compare loops over ColumnValues
	// Loop over the different ranges and extend/include all the partitions found
	for idx := range res.Ranges {
		minComparer := minCmp(sctx, res.Ranges[idx].LowVal, columnsPruner, comparer, res.Ranges[idx].LowExclude, &gotError)
		maxComparer := maxCmp(sctx, res.Ranges[idx].HighVal, columnsPruner, comparer, res.Ranges[idx].HighExclude, &gotError)
		if gotError {
			// the compare function returned error, use all partitions.
			return fullRange(len(columnsPruner.lessThan))
		}
		// Can optimize if the range start is types.KindNull/types.MinNotNull
		// or range end is types.KindMaxValue
		start := sort.Search(len(columnsPruner.lessThan), minComparer)
		end := sort.Search(len(columnsPruner.lessThan), maxComparer)

		if end < len(columnsPruner.lessThan) {
			end++
		}
		rangeOr = append(rangeOr, partitionRange{start, end})
	}
	return result.intersection(rangeOr).simplify()
}

func partitionRangeForCNFExpr(sctx base.PlanContext, exprs []expression.Expression,
	pruner partitionRangePruner, result partitionRangeOR) partitionRangeOR {
	// TODO: When the ranger/detacher handles varchar_col_general_ci cmp constant bin collation
	// remove the check for single column RANGE COLUMNS and remove the single column implementation
	if columnsPruner, ok := pruner.(*rangeColumnsPruner); ok && len(columnsPruner.partCols) > 1 {
		return multiColumnRangeColumnsPruner(sctx, exprs, columnsPruner, result)
	}
	for i := 0; i < len(exprs); i++ {
		result = partitionRangeForExpr(sctx, exprs[i], pruner, result)
	}
	return result
}

// partitionRangeForExpr calculate the partitions for the expression.
func partitionRangeForExpr(sctx base.PlanContext, expr expression.Expression,
	pruner partitionRangePruner, result partitionRangeOR) partitionRangeOR {
	// Handle AND, OR respectively.
	if op, ok := expr.(*expression.ScalarFunction); ok {
		if op.FuncName.L == ast.LogicAnd {
			return partitionRangeForCNFExpr(sctx, op.GetArgs(), pruner, result)
		} else if op.FuncName.L == ast.LogicOr {
			args := op.GetArgs()
			newRange := partitionRangeForOrExpr(sctx, args[0], args[1], pruner)
			return result.intersection(newRange)
		} else if op.FuncName.L == ast.In {
			if p, ok := pruner.(*rangePruner); ok {
				newRange := partitionRangeForInExpr(sctx, op.GetArgs(), p)
				return result.intersection(newRange)
			} else if p, ok := pruner.(*rangeColumnsPruner); ok {
				newRange := partitionRangeColumnForInExpr(sctx, op.GetArgs(), p)
				return result.intersection(newRange)
			}
			return result
		}
	}

	// Handle a single expression.
	start, end, ok := pruner.partitionRangeForExpr(sctx, expr)
	if !ok {
		// Can't prune, return the whole range.
		return result
	}
	return result.intersectionRange(start, end)
}

type partitionRangePruner interface {
	partitionRangeForExpr(base.PlanContext, expression.Expression) (start, end int, succ bool)
	fullRange() partitionRangeOR
}

var _ partitionRangePruner = &rangePruner{}

// rangePruner is used by 'partition by range'.
type rangePruner struct {
	lessThan lessThanDataInt
	col      *expression.Column
	partFn   *expression.ScalarFunction
	// If partFn is not nil, monotonous indicates partFn is monotonous or not.
	monotonous monotoneMode
}

func (p *rangePruner) partitionRangeForExpr(sctx base.PlanContext, expr expression.Expression) (start int, end int, ok bool) {
	if constExpr, ok := expr.(*expression.Constant); ok {
		if b, err := constExpr.Value.ToBool(sctx.GetSessionVars().StmtCtx.TypeCtx()); err == nil && b == 0 {
			// A constant false expression.
			return 0, 0, true
		}
	}

	dataForPrune, ok := p.extractDataForPrune(sctx, expr)
	if !ok {
		return 0, 0, false
	}

	start, end = pruneUseBinarySearch(p.lessThan, dataForPrune)
	return start, end, true
}

func (p *rangePruner) fullRange() partitionRangeOR {
	return fullRange(p.lessThan.length())
}

// partitionRangeForOrExpr calculate the partitions for or(expr1, expr2)
func partitionRangeForOrExpr(sctx base.PlanContext, expr1, expr2 expression.Expression,
	pruner partitionRangePruner) partitionRangeOR {
	tmp1 := partitionRangeForExpr(sctx, expr1, pruner, pruner.fullRange())
	tmp2 := partitionRangeForExpr(sctx, expr2, pruner, pruner.fullRange())
	return tmp1.union(tmp2)
}

func partitionRangeColumnForInExpr(sctx base.PlanContext, args []expression.Expression,
	pruner *rangeColumnsPruner) partitionRangeOR {
	col, ok := args[0].(*expression.Column)
	if !ok || col.ID != pruner.partCols[0].ID {
		return pruner.fullRange()
	}

	var result partitionRangeOR
	for i := 1; i < len(args); i++ {
		constExpr, ok := args[i].(*expression.Constant)
		if !ok {
			return pruner.fullRange()
		}
		switch constExpr.Value.Kind() {
		case types.KindInt64, types.KindUint64, types.KindMysqlTime, types.KindString: // for safety, only support string,int and datetime now
		case types.KindNull:
			result = append(result, partitionRange{0, 1})
			continue
		default:
			return pruner.fullRange()
		}

		// convert all elements to EQ-exprs and prune them one by one
		sf, err := expression.NewFunction(sctx.GetExprCtx(), ast.EQ, types.NewFieldType(types.KindInt64), []expression.Expression{col, args[i]}...)
		if err != nil {
			return pruner.fullRange()
		}
		start, end, ok := pruner.partitionRangeForExpr(sctx, sf)
		if !ok {
			return pruner.fullRange()
		}
		result = append(result, partitionRange{start, end})
	}

	return result.simplify()
}

func partitionRangeForInExpr(sctx base.PlanContext, args []expression.Expression,
	pruner *rangePruner) partitionRangeOR {
	col, ok := args[0].(*expression.Column)
	if !ok || col.ID != pruner.col.ID {
		return pruner.fullRange()
	}

	var result partitionRangeOR
	for i := 1; i < len(args); i++ {
		constExpr, ok := args[i].(*expression.Constant)
		if !ok {
			return pruner.fullRange()
		}
		if constExpr.Value.Kind() == types.KindNull {
			result = append(result, partitionRange{0, 1})
			continue
		}

		var val int64
		var err error
		var unsigned bool
		if pruner.partFn != nil {
			// replace fn(col) to fn(const)
			partFnConst := replaceColumnWithConst(pruner.partFn, constExpr)
			val, _, err = partFnConst.EvalInt(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			unsigned = mysql.HasUnsignedFlag(partFnConst.GetStaticType().GetFlag())
		} else {
			val, _, err = constExpr.EvalInt(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			unsigned = mysql.HasUnsignedFlag(constExpr.GetType(sctx.GetExprCtx().GetEvalCtx()).GetFlag())
		}
		if err != nil {
			return pruner.fullRange()
		}

		start, end := pruneUseBinarySearch(pruner.lessThan, dataForPrune{op: ast.EQ, c: val, unsigned: unsigned})
		result = append(result, partitionRange{start, end})
	}
	return result.simplify()
}

type monotoneMode int

const (
	monotoneModeInvalid monotoneMode = iota
	monotoneModeStrict
	monotoneModeNonStrict
)

// monotoneIncFuncs are those functions that are monotone increasing.
// For any x y, if x > y => f(x) > f(y), function f is strict monotone .
// For any x y, if x > y => f(x) >= f(y), function f is non-strict monotone.
var monotoneIncFuncs = map[string]monotoneMode{
	ast.Year:          monotoneModeNonStrict,
	ast.ToDays:        monotoneModeNonStrict,
	ast.UnixTimestamp: monotoneModeStrict,
	// Only when the function form is fn(column, const)
	ast.Plus:  monotoneModeStrict,
	ast.Minus: monotoneModeStrict,
}

func getMonotoneMode(fnName string) monotoneMode {
	mode, ok := monotoneIncFuncs[fnName]
	if !ok {
		return monotoneModeInvalid
	}
	return mode
}

// f(x) op const, op is > = <
type dataForPrune struct {
	op       string
	c        int64
	unsigned bool
}

// extractDataForPrune extracts data from the expression for pruning.
// The expression should have this form:  'f(x) op const', otherwise it can't be pruned.
func (p *rangePruner) extractDataForPrune(sctx base.PlanContext, expr expression.Expression) (dataForPrune, bool) {
	var ret dataForPrune
	op, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return ret, false
	}
	switch op.FuncName.L {
	case ast.EQ, ast.LT, ast.GT, ast.LE, ast.GE:
		ret.op = op.FuncName.L
	case ast.IsNull:
		// isnull(col)
		if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && arg0.ID == p.col.ID {
			ret.op = ast.IsNull
			return ret, true
		}
		return ret, false
	default:
		return ret, false
	}

	var col *expression.Column
	var con *expression.Constant
	if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && arg0.ID == p.col.ID {
		if arg1, ok := op.GetArgs()[1].(*expression.Constant); ok {
			col, con = arg0, arg1
		}
	} else if arg0, ok := op.GetArgs()[1].(*expression.Column); ok && arg0.ID == p.col.ID {
		if arg1, ok := op.GetArgs()[0].(*expression.Constant); ok {
			ret.op = opposite(ret.op)
			col, con = arg0, arg1
		}
	}
	if col == nil || con == nil {
		return ret, false
	}

	// Current expression is 'col op const'
	var constExpr expression.Expression
	if p.partFn != nil {
		// If the partition function is not monotone, only EQ condition can be pruning.
		if p.monotonous == monotoneModeInvalid && ret.op != ast.EQ {
			return ret, false
		}

		// If the partition expression is fn(col), change constExpr to fn(constExpr).
		constExpr = replaceColumnWithConst(p.partFn, con)

		// When the partFn is not strict monotonous, we need to relax the condition < to <=, > to >=.
		// For example, the following case doesn't hold:
		// col < '2020-02-11 17:34:11' => to_days(col) < to_days(2020-02-11 17:34:11)
		// The correct transform should be:
		// col < '2020-02-11 17:34:11' => to_days(col) <= to_days(2020-02-11 17:34:11)
		if p.monotonous == monotoneModeNonStrict {
			ret.op = relaxOP(ret.op)
		}
	} else {
		// If the partition expression is col, use constExpr.
		constExpr = con
	}
	// If the partition expression is related with more than one columns such as 'a + b' or 'a * b' or something else,
	// the constExpr may not a really constant when coming here.
	// Suppose the partition expression is 'a + b' and we have a condition 'a = 2',
	// the constExpr is '2 + b' after the replacement which we can't evaluate.
	if !expression.ConstExprConsiderPlanCache(constExpr, sctx.GetSessionVars().StmtCtx.UseCache()) {
		return ret, false
	}
	c, isNull, err := constExpr.EvalInt(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	if err == nil && !isNull {
		ret.c = c
		ret.unsigned = mysql.HasUnsignedFlag(constExpr.GetType(sctx.GetExprCtx().GetEvalCtx()).GetFlag())
		return ret, true
	}
	return ret, false
}

// replaceColumnWithConst change fn(col) to fn(const)
func replaceColumnWithConst(partFn *expression.ScalarFunction, con *expression.Constant) *expression.ScalarFunction {
	args := partFn.GetArgs()
	// The partition function may be floor(unix_timestamp(ts)) instead of a simple fn(col).
	if partFn.FuncName.L == ast.Floor {
		if ut, ok := args[0].(*expression.ScalarFunction); ok && ut.FuncName.L == ast.UnixTimestamp {
			args = ut.GetArgs()
			args[0] = con
			return partFn
		}
	}

	// No 'copy on write' for the expression here, this is a dangerous operation.
	args[0] = con
	return partFn
}

// opposite turns > to <, >= to <= and so on.
func opposite(op string) string {
	switch op {
	case ast.EQ:
		return ast.EQ
	case ast.LT:
		return ast.GT
	case ast.GT:
		return ast.LT
	case ast.LE:
		return ast.GE
	case ast.GE:
		return ast.LE
	}
	panic("invalid input parameter" + op)
}

// relaxOP relax the op > to >= and < to <=
// Sometime we need to relax the condition, for example:
// col < const => f(col) <= const
// datetime < 2020-02-11 16:18:42 => to_days(datetime) <= to_days(2020-02-11)
// We can't say:
// datetime < 2020-02-11 16:18:42 => to_days(datetime) < to_days(2020-02-11)
func relaxOP(op string) string {
	switch op {
	case ast.LT:
		return ast.LE
	case ast.GT:
		return ast.GE
	}
	return op
}

// pruneUseBinarySearch returns the start and end of which partitions will match.
// If no match (i.e. value > last partition) the start partition will be the number of partition, not the first partition!
func pruneUseBinarySearch(lessThan lessThanDataInt, data dataForPrune) (start int, end int) {
	length := lessThan.length()
	switch data.op {
	case ast.EQ:
		// col = 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col = 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col = 10, lessThan = [4 7 11 14 17] => [2, 3)
		// col = 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, data.unsigned) > 0 })
		start, end = pos, pos+1
	case ast.LT:
		// col < 66, lessThan = [4 7 11 14 17] => [0, 5)
		// col < 14, lessThan = [4 7 11 14 17] => [0, 4)
		// col < 10, lessThan = [4 7 11 14 17] => [0, 3)
		// col < 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, data.unsigned) >= 0 })
		start, end = 0, pos+1
	case ast.GE:
		// col >= 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col >= 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col >= 10, lessThan = [4 7 11 14 17] => [2, 5)
		// col >= 3, lessThan = [4 7 11 14 17] => [0, 5)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, data.unsigned) > 0 })
		start, end = pos, length
	case ast.GT:
		// col > 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col > 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col > 10, lessThan = [4 7 11 14 17] => [3, 5)
		// col > 3, lessThan = [4 7 11 14 17] => [1, 5)
		// col > 2, lessThan = [4 7 11 14 17] => [0, 5)

		// Although `data.c+1` will overflow in sometime, this does not affect the correct results obtained.
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c+1, data.unsigned) > 0 })
		start, end = pos, length
	case ast.LE:
		// col <= 66, lessThan = [4 7 11 14 17] => [0, 6)
		// col <= 14, lessThan = [4 7 11 14 17] => [0, 5)
		// col <= 10, lessThan = [4 7 11 14 17] => [0, 3)
		// col <= 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, data.unsigned) > 0 })
		start, end = 0, pos+1
	case ast.IsNull:
		start, end = 0, 1
	default:
		start, end = 0, length
	}

	if end > length {
		end = length
	}
	return start, end
}

func (*partitionProcessor) resolveAccessPaths(ds *DataSource) error {
	possiblePaths, err := getPossibleAccessPaths(
		ds.SCtx(), &h.PlanHints{IndexMergeHintList: ds.indexMergeHints, IndexHintList: ds.IndexHints},
		ds.astIndexHints, ds.table, ds.DBName, ds.tableInfo.Name, ds.isForUpdateRead, true)
	if err != nil {
		return err
	}
	possiblePaths, err = filterPathByIsolationRead(ds.SCtx(), possiblePaths, ds.tableInfo.Name, ds.DBName)
	if err != nil {
		return err
	}
	ds.possibleAccessPaths = possiblePaths
	return nil
}

func (s *partitionProcessor) resolveOptimizeHint(ds *DataSource, partitionName model.CIStr) error {
	// index hint
	if len(ds.IndexHints) > 0 {
		newIndexHint := make([]h.HintedIndex, 0, len(ds.IndexHints))
		for _, idxHint := range ds.IndexHints {
			if len(idxHint.Partitions) == 0 {
				newIndexHint = append(newIndexHint, idxHint)
			} else {
				for _, p := range idxHint.Partitions {
					if p.String() == partitionName.String() {
						newIndexHint = append(newIndexHint, idxHint)
						break
					}
				}
			}
		}
		ds.IndexHints = newIndexHint
	}

	// index merge hint
	if len(ds.indexMergeHints) > 0 {
		newIndexMergeHint := make([]h.HintedIndex, 0, len(ds.indexMergeHints))
		for _, idxHint := range ds.indexMergeHints {
			if len(idxHint.Partitions) == 0 {
				newIndexMergeHint = append(newIndexMergeHint, idxHint)
			} else {
				for _, p := range idxHint.Partitions {
					if p.String() == partitionName.String() {
						newIndexMergeHint = append(newIndexMergeHint, idxHint)
						break
					}
				}
			}
		}
		ds.indexMergeHints = newIndexMergeHint
	}

	// read from storage hint
	if ds.preferStoreType&h.PreferTiKV > 0 {
		if len(ds.preferPartitions[h.PreferTiKV]) > 0 {
			ds.preferStoreType ^= h.PreferTiKV
			for _, p := range ds.preferPartitions[h.PreferTiKV] {
				if p.String() == partitionName.String() {
					ds.preferStoreType |= h.PreferTiKV
				}
			}
		}
	}
	if ds.preferStoreType&h.PreferTiFlash > 0 {
		if len(ds.preferPartitions[h.PreferTiFlash]) > 0 {
			ds.preferStoreType ^= h.PreferTiFlash
			for _, p := range ds.preferPartitions[h.PreferTiFlash] {
				if p.String() == partitionName.String() {
					ds.preferStoreType |= h.PreferTiFlash
				}
			}
		}
	}
	if ds.preferStoreType&h.PreferTiFlash != 0 && ds.preferStoreType&h.PreferTiKV != 0 {
		ds.SCtx().GetSessionVars().StmtCtx.AppendWarning(
			errors.NewNoStackError("hint `read_from_storage` has conflict storage type for the partition " + partitionName.L))
	}

	return s.resolveAccessPaths(ds)
}

func checkTableHintsApplicableForPartition(partitions []model.CIStr, partitionSet set.StringSet) []string {
	var unknownPartitions []string
	for _, p := range partitions {
		if !partitionSet.Exist(p.L) {
			unknownPartitions = append(unknownPartitions, p.L)
		}
	}
	return unknownPartitions
}

func appendWarnForUnknownPartitions(ctx base.PlanContext, hintName string, unknownPartitions []string) {
	if len(unknownPartitions) == 0 {
		return
	}

	warning := fmt.Errorf("unknown partitions (%s) in optimizer hint %s", strings.Join(unknownPartitions, ","), hintName)
	ctx.GetSessionVars().StmtCtx.SetHintWarningFromError(warning)
}

func (*partitionProcessor) checkHintsApplicable(ds *DataSource, partitionSet set.StringSet) {
	for _, idxHint := range ds.IndexHints {
		unknownPartitions := checkTableHintsApplicableForPartition(idxHint.Partitions, partitionSet)
		appendWarnForUnknownPartitions(ds.SCtx(), h.Restore2IndexHint(idxHint.HintTypeString(), idxHint), unknownPartitions)
	}
	for _, idxMergeHint := range ds.indexMergeHints {
		unknownPartitions := checkTableHintsApplicableForPartition(idxMergeHint.Partitions, partitionSet)
		appendWarnForUnknownPartitions(ds.SCtx(), h.Restore2IndexHint(h.HintIndexMerge, idxMergeHint), unknownPartitions)
	}
	unknownPartitions := checkTableHintsApplicableForPartition(ds.preferPartitions[h.PreferTiKV], partitionSet)
	unknownPartitions = append(unknownPartitions,
		checkTableHintsApplicableForPartition(ds.preferPartitions[h.PreferTiFlash], partitionSet)...)
	appendWarnForUnknownPartitions(ds.SCtx(), h.HintReadFromStorage, unknownPartitions)
}

func (s *partitionProcessor) makeUnionAllChildren(ds *DataSource, pi *model.PartitionInfo, or partitionRangeOR, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	children := make([]base.LogicalPlan, 0, len(pi.Definitions))
	partitionNameSet := make(set.StringSet)
	usedDefinition := make(map[int64]model.PartitionDefinition)
	for _, r := range or {
		for i := r.start; i < r.end; i++ {
			// This is for `table partition (p0,p1)` syntax, only union the specified partition if has specified partitions.
			if len(ds.partitionNames) != 0 {
				if !s.findByName(ds.partitionNames, pi.Definitions[i].Name.L) {
					continue
				}
			}
			// Not a deep copy.
			newDataSource := *ds
			newDataSource.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ds.SCtx(), plancodec.TypeTableScan, &newDataSource, ds.QueryBlockOffset())
			newDataSource.schema = ds.schema.Clone()
			newDataSource.Columns = make([]*model.ColumnInfo, len(ds.Columns))
			copy(newDataSource.Columns, ds.Columns)
			idx := i
			newDataSource.partitionDefIdx = &idx
			newDataSource.physicalTableID = pi.Definitions[i].ID

			// There are many expression nodes in the plan tree use the original datasource
			// id as FromID. So we set the id of the newDataSource with the original one to
			// avoid traversing the whole plan tree to update the references.
			newDataSource.SetID(ds.ID())
			err := s.resolveOptimizeHint(&newDataSource, pi.Definitions[i].Name)
			partitionNameSet.Insert(pi.Definitions[i].Name.L)
			if err != nil {
				return nil, err
			}
			children = append(children, &newDataSource)
			usedDefinition[pi.Definitions[i].ID] = pi.Definitions[i]
		}
	}
	s.checkHintsApplicable(ds, partitionNameSet)

	ds.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("Static partition pruning mode")
	if len(children) == 0 {
		// No result after table pruning.
		tableDual := LogicalTableDual{RowCount: 0}.Init(ds.SCtx(), ds.QueryBlockOffset())
		tableDual.schema = ds.Schema()
		appendMakeUnionAllChildrenTranceStep(ds, usedDefinition, tableDual, children, opt)
		return tableDual, nil
	}
	if len(children) == 1 {
		// No need for the union all.
		appendMakeUnionAllChildrenTranceStep(ds, usedDefinition, children[0], children, opt)
		return children[0], nil
	}
	unionAll := LogicalPartitionUnionAll{}.Init(ds.SCtx(), ds.QueryBlockOffset())
	unionAll.SetChildren(children...)
	unionAll.SetSchema(ds.schema.Clone())
	appendMakeUnionAllChildrenTranceStep(ds, usedDefinition, unionAll, children, opt)
	return unionAll, nil
}

func (*partitionProcessor) pruneRangeColumnsPartition(ctx base.PlanContext, conds []expression.Expression, pi *model.PartitionInfo, pe *tables.PartitionExpr, columns []*expression.Column) (partitionRangeOR, error) {
	result := fullRange(len(pi.Definitions))

	if len(pi.Columns) < 1 {
		return result, nil
	}

	pruner, err := makeRangeColumnPruner(columns, pi, pe.ForRangeColumnsPruning, pe.ColumnOffset)
	if err == nil {
		result = partitionRangeForCNFExpr(ctx, conds, pruner, result)
	}
	return result, nil
}

var _ partitionRangePruner = &rangeColumnsPruner{}

// rangeColumnsPruner is used by 'partition by range columns'.
type rangeColumnsPruner struct {
	lessThan [][]*expression.Expression
	partCols []*expression.Column
}

func makeRangeColumnPruner(columns []*expression.Column, pi *model.PartitionInfo, from *tables.ForRangeColumnsPruning, offsets []int) (*rangeColumnsPruner, error) {
	if len(pi.Definitions) != len(from.LessThan) {
		return nil, errors.Trace(fmt.Errorf("internal error len(pi.Definitions) != len(from.LessThan) %d != %d", len(pi.Definitions), len(from.LessThan)))
	}
	schema := expression.NewSchema(columns...)
	partCols := make([]*expression.Column, len(offsets))
	for i, offset := range offsets {
		partCols[i] = schema.Columns[offset]
	}
	lessThan := make([][]*expression.Expression, 0, len(from.LessThan))
	for i := range from.LessThan {
		colVals := make([]*expression.Expression, 0, len(from.LessThan[i]))
		for j := range from.LessThan[i] {
			if from.LessThan[i][j] != nil {
				tmp := (*from.LessThan[i][j]).Clone()
				colVals = append(colVals, &tmp)
			} else {
				colVals = append(colVals, nil)
			}
		}
		lessThan = append(lessThan, colVals)
	}
	return &rangeColumnsPruner{lessThan, partCols}, nil
}

func (p *rangeColumnsPruner) fullRange() partitionRangeOR {
	return fullRange(len(p.lessThan))
}

func (p *rangeColumnsPruner) getPartCol(colID int64) *expression.Column {
	for i := range p.partCols {
		if colID == p.partCols[i].ID {
			return p.partCols[i]
		}
	}
	return nil
}

func (p *rangeColumnsPruner) partitionRangeForExpr(sctx base.PlanContext, expr expression.Expression) (start int, end int, ok bool) {
	op, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return 0, len(p.lessThan), false
	}

	switch op.FuncName.L {
	case ast.EQ, ast.LT, ast.GT, ast.LE, ast.GE:
	case ast.IsNull:
		// isnull(col)
		if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && len(p.partCols) == 1 && arg0.ID == p.partCols[0].ID {
			// Single column RANGE COLUMNS, NULL sorts before all other values: match first partition
			return 0, 1, true
		}
		return 0, len(p.lessThan), false
	default:
		return 0, len(p.lessThan), false
	}
	opName := op.FuncName.L

	var col *expression.Column
	var con *expression.Constant
	var argCol0, argCol1 *expression.Column
	var argCon0, argCon1 *expression.Constant
	var okCol0, okCol1, okCon0, okCon1 bool
	argCol0, okCol0 = op.GetArgs()[0].(*expression.Column)
	argCol1, okCol1 = op.GetArgs()[1].(*expression.Column)
	argCon0, okCon0 = op.GetArgs()[0].(*expression.Constant)
	argCon1, okCon1 = op.GetArgs()[1].(*expression.Constant)
	if okCol0 && okCon1 {
		col, con = argCol0, argCon1
	} else if okCol1 && okCon0 {
		col, con = argCol1, argCon0
		opName = opposite(opName)
	} else {
		return 0, len(p.lessThan), false
	}
	partCol := p.getPartCol(col.ID)
	if partCol == nil {
		return 0, len(p.lessThan), false
	}

	// If different collation, we can only prune if:
	// - expression is binary collation (can only be found in one partition)
	// - EQ operator, consider values 'a','b','ä' where 'ä' would be in the same partition as 'a' if general_ci, but is binary after 'b'
	// otherwise return all partitions / no pruning
	_, exprColl := expr.CharsetAndCollation()
	colColl := partCol.RetType.GetCollate()
	if exprColl != colColl && (opName != ast.EQ || !collate.IsBinCollation(exprColl)) {
		return 0, len(p.lessThan), true
	}
	start, end = p.pruneUseBinarySearch(sctx, opName, con)
	return start, end, true
}

// pruneUseBinarySearch returns the start and end of which partitions will match.
// If no match (i.e. value > last partition) the start partition will be the number of partition, not the first partition!
func (p *rangeColumnsPruner) pruneUseBinarySearch(sctx base.PlanContext, op string, data *expression.Constant) (start int, end int) {
	var savedError error
	var isNull bool
	if len(p.partCols) > 1 {
		// Only one constant in the input, this will never be called with
		// multi-column RANGE COLUMNS :)
		return 0, len(p.lessThan)
	}
	charSet, collation := p.partCols[0].RetType.GetCharset(), p.partCols[0].RetType.GetCollate()
	compare := func(ith int, op string, v *expression.Constant) bool {
		for i := range p.partCols {
			if p.lessThan[ith][i] == nil { // MAXVALUE
				return true
			}
			expr, err := expression.NewFunctionBase(sctx.GetExprCtx(), op, types.NewFieldType(mysql.TypeLonglong), *p.lessThan[ith][i], v)
			if err != nil {
				savedError = err
				return true
			}
			expr.SetCharsetAndCollation(charSet, collation)
			var val int64
			val, isNull, err = expr.EvalInt(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			if err != nil {
				savedError = err
				return true
			}
			if val > 0 {
				return true
			}
		}
		return false
	}

	length := len(p.lessThan)
	switch op {
	case ast.EQ:
		pos := sort.Search(length, func(i int) bool { return compare(i, ast.GT, data) })
		start, end = pos, pos+1
	case ast.LT:
		pos := sort.Search(length, func(i int) bool { return compare(i, ast.GE, data) })
		start, end = 0, pos+1
	case ast.GE, ast.GT:
		pos := sort.Search(length, func(i int) bool { return compare(i, ast.GT, data) })
		start, end = pos, length
	case ast.LE:
		pos := sort.Search(length, func(i int) bool { return compare(i, ast.GT, data) })
		start, end = 0, pos+1
	default:
		start, end = 0, length
	}

	// Something goes wrong, abort this pruning.
	if savedError != nil || isNull {
		return 0, len(p.lessThan)
	}

	if end > length {
		end = length
	}
	return start, end
}

func appendMakeUnionAllChildrenTranceStep(origin *DataSource, usedMap map[int64]model.PartitionDefinition, plan base.LogicalPlan, children []base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
	if opt.TracerIsNil() {
		return
	}
	if len(children) == 0 {
		appendNoPartitionChildTraceStep(origin, plan, opt)
		return
	}
	var action, reason func() string
	used := make([]model.PartitionDefinition, 0, len(usedMap))
	for _, def := range usedMap {
		used = append(used, def)
	}
	slices.SortFunc(used, func(i, j model.PartitionDefinition) int {
		return cmp.Compare(i.ID, j.ID)
	})
	if len(children) == 1 {
		newDS := plan.(*DataSource)
		newDS.SetID(origin.SCtx().GetSessionVars().AllocNewPlanID())
		action = func() string {
			return fmt.Sprintf("%v_%v becomes %s_%v", origin.TP(), origin.ID(), newDS.TP(), newDS.ID())
		}
		reason = func() string {
			return fmt.Sprintf("%v_%v has one needed partition[%s] after pruning", origin.TP(), origin.ID(), used[0].Name)
		}
	} else {
		action = func() string {
			buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v becomes %s_%v with children[",
				origin.TP(), origin.ID(), plan.TP(), plan.ID()))
			for i, child := range children {
				if i > 0 {
					buffer.WriteString(",")
				}
				newDS := child.(*DataSource)
				newDS.SetID(origin.SCtx().GetSessionVars().AllocNewPlanID())
				fmt.Fprintf(buffer, "%s_%v", child.TP(), newDS.ID())
			}
			buffer.WriteString("]")
			return buffer.String()
		}
		reason = func() string {
			buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v has multiple needed partitions[",
				origin.TP(), origin.ID()))
			for i, u := range used {
				if i > 0 {
					buffer.WriteString(",")
				}
				buffer.WriteString(u.Name.String())
			}
			buffer.WriteString("] after pruning")
			return buffer.String()
		}
	}
	opt.AppendStepToCurrent(origin.ID(), origin.TP(), reason, action)
}

func appendNoPartitionChildTraceStep(ds *DataSource, dual base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v becomes %v_%v", ds.TP(), ds.ID(), dual.TP(), dual.ID())
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v doesn't have needed partition table after pruning", ds.TP(), ds.ID())
	}
	opt.AppendStepToCurrent(dual.ID(), dual.TP(), reason, action)
}
