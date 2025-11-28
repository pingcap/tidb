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
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
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
	"github.com/pingcap/tipb/go-tipb"
)

// FullRange represent used all partitions.
const FullRange = -1

// PartitionProcessor rewrites the ast for table partition.
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
// PartitionProcessor is here because it's easier to prune partition after predicate push down.
type PartitionProcessor struct{}

// Optimize implements the LogicalOptRule.<0th> interface.
func (s *PartitionProcessor) Optimize(_ context.Context, lp base.LogicalPlan) (base.LogicalPlan, bool, error) {
	planChanged := false
	p, err := s.rewriteDataSource(lp)
	return p, planChanged, err
}

// Name implements the LogicalOptRule.<1st> interface.
func (*PartitionProcessor) Name() string {
	return "partition_processor"
}

func (s *PartitionProcessor) rewriteDataSource(lp base.LogicalPlan) (base.LogicalPlan, error) {
	// Assert there will not be sel -> sel in the ast.
	switch p := lp.(type) {
	case *logicalop.DataSource:
		return s.prune(p)
	case *logicalop.LogicalUnionScan:
		ds := p.Children()[0]
		ds, err := s.prune(ds.(*logicalop.DataSource))
		if err != nil {
			return nil, err
		}
		if ua, ok := ds.(*logicalop.LogicalPartitionUnionAll); ok {
			// Adjust the UnionScan->Union->DataSource1, DataSource2 ... to
			// Union->(UnionScan->DataSource1), (UnionScan->DataSource2)
			children := make([]base.LogicalPlan, 0, len(ua.Children()))
			for _, child := range ua.Children() {
				us := logicalop.LogicalUnionScan{
					Conditions: p.Conditions,
					HandleCols: p.HandleCols,
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
	case *logicalop.LogicalCTE:
		return lp, nil
	default:
		children := lp.Children()
		for i, child := range children {
			newChild, err := s.rewriteDataSource(child)
			if err != nil {
				return nil, err
			}
			children[i] = newChild
		}
	}

	return lp, nil
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

func (s *PartitionProcessor) prune(ds *logicalop.DataSource) (base.LogicalPlan, error) {
	pi := ds.TableInfo.GetPartitionInfo()
	if pi == nil {
		return ds, nil
	}
	// PushDownNot here can convert condition 'not (a != 1)' to 'a = 1'. When we build range from ds.AllConds, the condition
	// like 'not (a != 1)' would not be handled so we need to convert it to 'a = 1', which can be handled when building range.
	// Now, PushDownNot have be done in the ApplyPredicateSimplification
	// AllConds and PushedDownConds may become inconsistent in subsequent ApplyPredicateSimplification calls.
	// They must be kept in sync to ensure correctness after PR #61571.
	ds.PushedDownConds = applyPredicateSimplification(ds.SCtx(), ds.PushedDownConds, false, nil)
	ds.AllConds = applyPredicateSimplification(ds.SCtx(), ds.AllConds, false, nil)
	// Return table dual when filter is constant false or null.
	dual := logicalop.Conds2TableDual(ds, ds.AllConds)
	if dual != nil {
		return dual, nil
	}
	// Try to locate partition directly for hash partition.
	// TODO: See if there is a way to remove conditions that does not
	// apply for some partitions like:
	// a = 1 OR a = 2 => for p1 only "a = 1" and for p2 only "a = 2"
	// since a cannot be 2 in p1 and a cannot be 1 in p2
	switch pi.Type {
	case ast.PartitionTypeRange:
		return s.processRangePartition(ds, pi)
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		return s.processHashOrKeyPartition(ds, pi)
	case ast.PartitionTypeList:
		return s.processListPartition(ds, pi)
	}

	return s.makeUnionAllChildren(ds, pi, GetFullRange(len(pi.Definitions)))
}

// FindByName checks whether object name exists in list.
func (*PartitionProcessor) FindByName(partitionNames []ast.CIStr, partitionName string) bool {
	for _, s := range partitionNames {
		if s.L == partitionName {
			return true
		}
	}
	return false
}

// LessThanDataInt is then less than structure in partition def.
type LessThanDataInt struct {
	Data     []int64
	Unsigned bool
	Maxvalue bool
}

// Length exported for test usage.
func (lt *LessThanDataInt) Length() int {
	return len(lt.Data)
}

func (lt *LessThanDataInt) compare(ith int, v int64, unsigned bool) int {
	// TODO: get an extra partition when `v` bigger than `lt.maxvalue``, but the result still correct.
	if ith == lt.Length()-1 && lt.Maxvalue {
		return 1
	}

	return types.CompareInt(lt.Data[ith], lt.Unsigned, v, unsigned)
}

// PartitionRange represents [start, end)
type PartitionRange struct {
	Start int
	End   int
}

// Cmp compare between two partition range.
func (p *PartitionRange) Cmp(a PartitionRange) int {
	return cmp.Compare(p.Start, a.Start)
}

// PartitionRangeOR represents OR(range1, range2, ...)
type PartitionRangeOR []PartitionRange

// GetFullRange get the full range.
func GetFullRange(end int) PartitionRangeOR {
	var reduceAllocation [3]PartitionRange
	reduceAllocation[0] = PartitionRange{0, end}
	return reduceAllocation[:1]
}

// IntersectionRange intersect the ranges.
func (or PartitionRangeOR) IntersectionRange(start, end int) PartitionRangeOR {
	// Let M = intersection, U = union, then
	// a M (b U c) == (a M b) U (a M c)
	ret := or[:0]
	for _, r1 := range or {
		newStart, newEnd := intersectionRange(r1.Start, r1.End, start, end)
		// Exclude the empty one.
		if newEnd > newStart {
			ret = append(ret, PartitionRange{newStart, newEnd})
		}
	}
	return ret
}

// Len returns the length.
func (or PartitionRangeOR) Len() int {
	return len(or)
}

// Union returns the union range.
func (or PartitionRangeOR) Union(x PartitionRangeOR) PartitionRangeOR {
	or = append(or, x...)
	return or.simplify()
}

func (or PartitionRangeOR) simplify() PartitionRangeOR {
	// if the length of the `or` is zero. We should return early.
	if len(or) == 0 {
		return or
	}
	// Make the ranges order by start.
	slices.SortFunc(or, func(i, j PartitionRange) int {
		return i.Cmp(j)
	})

	// Iterate the sorted ranges, merge the adjacent two when their range overlap.
	// For example, [0, 1), [2, 7), [3, 5), ... => [0, 1), [2, 7) ...
	res := or[:1]
	for _, curr := range or[1:] {
		last := &res[len(res)-1]
		if curr.Start > last.End {
			res = append(res, curr)
		} else {
			// Merge two.
			if curr.End > last.End {
				last.End = curr.End
			}
		}
	}
	return res
}

// Intersection intersect the ranges.
func (or PartitionRangeOR) Intersection(x PartitionRangeOR) PartitionRangeOR {
	if or.Len() == 1 {
		return x.IntersectionRange(or[0].Start, or[0].End)
	}
	if x.Len() == 1 {
		return or.IntersectionRange(x[0].Start, x[0].End)
	}

	// Rename to x, y where len(x) > len(y)
	var y PartitionRangeOR
	if or.Len() > x.Len() {
		x, y = or, x
	} else {
		y = or
	}

	// (a U b) M (c U d) => (x M c) U (x M d), x = (a U b)
	res := make(PartitionRangeOR, 0, len(y))
	for _, r := range y {
		// As IntersectionRange modify the raw data, we have to make a copy.
		tmp := make(PartitionRangeOR, len(x))
		copy(tmp, x)
		tmp = tmp.IntersectionRange(r.Start, r.End)
		res = append(res, tmp...)
	}
	return res.simplify()
}

// intersectionRange calculate the intersection of [start, end) and [newStart, newEnd)
func intersectionRange(start, end, newStart, newEnd int) (s int, e int) {
	s = max(start, newStart)

	e = min(end, newEnd)
	return s, e
}

// PruneRangePartition prune range partitions.
func (s *PartitionProcessor) PruneRangePartition(ctx base.PlanContext, pi *model.PartitionInfo, tbl table.PartitionedTable, conds []expression.Expression,
	columns []*expression.Column, names types.NameSlice) (PartitionRangeOR, error) {
	partExpr := tbl.(base.PartitionTable).PartitionExpr()

	// Partition by range columns.
	if len(pi.Columns) > 0 {
		result, err := s.pruneRangeColumnsPartition(ctx, conds, pi, partExpr, columns)
		return result, err
	}

	// Partition by range.
	col, fn, mono, err := MakePartitionByFnCol(ctx, columns, names, pi.Expr)
	if err != nil {
		return nil, err
	}
	result := GetFullRange(len(pi.Definitions))
	if col == nil {
		return result, nil
	}

	// Extract the partition column, if the column is not null, it's possible to prune.
	pruner := RangePruner{
		LessThan: LessThanDataInt{
			Data:     partExpr.ForRangePruning.LessThan,
			Unsigned: mysql.HasUnsignedFlag(col.GetStaticType().GetFlag()),
			Maxvalue: partExpr.ForRangePruning.MaxValue,
		},
		Col:        col,
		PartFn:     fn,
		Monotonous: mono,
	}
	result = PartitionRangeForCNFExpr(ctx, conds, &pruner, result)

	return result, nil
}

func (s *PartitionProcessor) processRangePartition(ds *logicalop.DataSource, pi *model.PartitionInfo) (base.LogicalPlan, error) {
	used, err := s.PruneRangePartition(ds.SCtx(), pi, ds.Table.(table.PartitionedTable), ds.AllConds, ds.TblCols, ds.OutputNames())
	if err != nil {
		return nil, err
	}
	return s.makeUnionAllChildren(ds, pi, used)
}

func (s *PartitionProcessor) processListPartition(ds *logicalop.DataSource, pi *model.PartitionInfo) (base.LogicalPlan, error) {
	used, err := s.PruneListPartition(ds.SCtx(), ds.Table, ds.PartitionNames, ds.AllConds, ds.TblCols)
	if err != nil {
		return nil, err
	}
	return s.makeUnionAllChildren(ds, pi, convertToRangeOr(used, pi))
}

// MakePartitionByFnCol extracts the column and function information in 'partition by ... fn(col)'.
func MakePartitionByFnCol(sctx base.PlanContext, columns []*expression.Column, names types.NameSlice, partitionExpr string) (*expression.Column, *expression.ScalarFunction, monotoneMode, error) {
	monotonous := MonotoneModeInvalid
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
		// Optimizations for a limited set of functions
		switch raw.FuncName.L {
		case ast.Floor:
			// Special handle for floor(unix_timestamp(ts)) as partition expression.
			// This pattern is so common for timestamp(3) column as partition expression that it deserve an optimization.
			if ut, ok := args[0].(*expression.ScalarFunction); ok && ut.FuncName.L == ast.UnixTimestamp {
				args1 := ut.GetArgs()
				if len(args1) == 1 {
					if c, ok1 := args1[0].(*expression.Column); ok1 {
						return c, raw, MonotoneModeNonStrict, nil
					}
				}
			}
		case ast.Extract:
			con, ok := args[0].(*expression.Constant)
			if !ok {
				break
			}
			col, ok = args[1].(*expression.Column)
			if !ok {
				// Special case where CastTimeToDuration is added
				expr, ok := args[1].(*expression.ScalarFunction)
				if !ok {
					break
				}
				if expr.Function.PbCode() != tipb.ScalarFuncSig_CastTimeAsDuration {
					break
				}
				castArgs := expr.GetArgs()
				col, ok = castArgs[0].(*expression.Column)
				if !ok {
					break
				}
			}
			if con.Value.Kind() != types.KindString {
				break
			}
			val := con.Value.GetString()
			colType := col.GetStaticType().GetType()
			switch colType {
			case mysql.TypeDate, mysql.TypeDatetime:
				switch val {
				// Only YEAR, YEAR_MONTH can be considered monotonic, the rest will wrap around!
				case "YEAR", "YEAR_MONTH":
					// Note, this function will not have the column as first argument,
					// so in replaceColumnWithConst it will replace the second argument, which
					// is special handling there too!
					return col, raw, MonotoneModeNonStrict, nil
				default:
					return col, raw, monotonous, nil
				}
			case mysql.TypeDuration:
				switch val {
				// Only HOUR* can be considered monotonic, the rest will wrap around!
				// TODO: if fsp match for HOUR_SECOND or HOUR_MICROSECOND we could
				// mark it as MonotoneModeStrict
				case "HOUR", "HOUR_MINUTE", "HOUR_SECOND", "HOUR_MICROSECOND":
					// Note, this function will not have the column as first argument,
					// so in replaceColumnWithConst it will replace the second argument, which
					// is special handling there too!
					return col, raw, MonotoneModeNonStrict, nil
				default:
					return col, raw, monotonous, nil
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

func minCmp(ctx base.PlanContext, lowVal []types.Datum, columnsPruner *RangeColumnsPruner, comparer []collate.Collator, lowExclude bool, gotError *bool) func(i int) bool {
	return func(i int) bool {
		for j := range lowVal {
			expr := columnsPruner.LessThan[i][j]

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
		if len(lowVal) < len(columnsPruner.LessThan[i]) {
			// Not all columns given
			if lowExclude {
				// prefix cols > const, do not include this partition
				return false
			}

			colIdx := len(lowVal)
			col := columnsPruner.PartCols[colIdx]
			conExpr := columnsPruner.LessThan[i][colIdx]
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
						if con.Value.GetInt64() == types.IntegerSignedLowerBound(col.GetStaticType().GetType()) {
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

func maxCmp(ctx base.PlanContext, hiVal []types.Datum, columnsPruner *RangeColumnsPruner, comparer []collate.Collator, hiExclude bool, gotError *bool) func(i int) bool {
	return func(i int) bool {
		for j := range hiVal {
			expr := columnsPruner.LessThan[i][j]
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
		if len(hiVal) < len(columnsPruner.LessThan[i]) {
			// Not all columns given
			if columnsPruner.LessThan[i][len(hiVal)] == nil {
				// MAXVALUE
				return true
			}
		}
		// if point is included, then false, due to LESS THAN
		return hiExclude
	}
}

func multiColumnRangeColumnsPruner(sctx base.PlanContext, exprs []expression.Expression,
	columnsPruner *RangeColumnsPruner, result PartitionRangeOR) PartitionRangeOR {
	lens := make([]int, 0, len(columnsPruner.PartCols))
	for i := range columnsPruner.PartCols {
		lens = append(lens, columnsPruner.PartCols[i].RetType.GetFlen())
	}

	res, err := ranger.DetachCondAndBuildRangeForPartition(sctx.GetRangerCtx(), exprs, columnsPruner.PartCols, lens, sctx.GetSessionVars().RangeMaxSize)
	if err != nil {
		return GetFullRange(len(columnsPruner.LessThan))
	}
	if len(res.Ranges) == 0 {
		if len(res.AccessConds) == 0 && len(res.RemainedConds) == 0 {
			// Impossible conditions, like: a > 2 AND a < 1
			return PartitionRangeOR{}
		}
		// Could not extract any valid range, use all partitions
		return GetFullRange(len(columnsPruner.LessThan))
	}

	rangeOr := make([]PartitionRange, 0, len(res.Ranges))

	gotError := false
	// Create a sort.Search where the compare loops over ColumnValues
	// Loop over the different ranges and extend/include all the partitions found
	for idx := range res.Ranges {
		minComparer := minCmp(sctx, res.Ranges[idx].LowVal, columnsPruner, res.Ranges[idx].Collators, res.Ranges[idx].LowExclude, &gotError)
		maxComparer := maxCmp(sctx, res.Ranges[idx].HighVal, columnsPruner, res.Ranges[idx].Collators, res.Ranges[idx].HighExclude, &gotError)
		if gotError {
			// the compare function returned error, use all partitions.
			return GetFullRange(len(columnsPruner.LessThan))
		}
		// Can optimize if the range start is types.KindNull/types.MinNotNull
		// or range end is types.KindMaxValue
		start := sort.Search(len(columnsPruner.LessThan), minComparer)
		end := sort.Search(len(columnsPruner.LessThan), maxComparer)

		if end < len(columnsPruner.LessThan) {
			end++
		}
		rangeOr = append(rangeOr, PartitionRange{start, end})
	}
	return result.Intersection(rangeOr).simplify()
}

// PartitionRangeForCNFExpr calculates the partitions for the CNF expression.
func PartitionRangeForCNFExpr(sctx base.PlanContext, exprs []expression.Expression,
	pruner partitionRangePruner, result PartitionRangeOR) PartitionRangeOR {
	// TODO: When the ranger/detacher handles varchar_col_general_ci cmp constant bin collation
	// remove the check for single column RANGE COLUMNS and remove the single column implementation
	if columnsPruner, ok := pruner.(*RangeColumnsPruner); ok && len(columnsPruner.PartCols) > 1 {
		return multiColumnRangeColumnsPruner(sctx, exprs, columnsPruner, result)
	}
	for i := range exprs {
		result = PartitionRangeForExpr(sctx, exprs[i], pruner, result)
	}
	return result
}

// PartitionRangeForExpr calculate the partitions for the expression.
func PartitionRangeForExpr(sctx base.PlanContext, expr expression.Expression,
	pruner partitionRangePruner, result PartitionRangeOR) PartitionRangeOR {
	// Handle AND, OR respectively.
	if op, ok := expr.(*expression.ScalarFunction); ok {
		switch op.FuncName.L {
		case ast.LogicAnd:
			return PartitionRangeForCNFExpr(sctx, op.GetArgs(), pruner, result)
		case ast.LogicOr:
			args := op.GetArgs()
			newRange := partitionRangeForOrExpr(sctx, args[0], args[1], pruner)
			return result.Intersection(newRange)
		case ast.In:
			if p, ok := pruner.(*RangePruner); ok {
				newRange := partitionRangeForInExpr(sctx, op.GetArgs(), p)
				return result.Intersection(newRange)
			} else if p, ok := pruner.(*RangeColumnsPruner); ok {
				newRange := partitionRangeColumnForInExpr(sctx, op.GetArgs(), p)
				return result.Intersection(newRange)
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
	return result.IntersectionRange(start, end)
}

type partitionRangePruner interface {
	partitionRangeForExpr(base.PlanContext, expression.Expression) (start, end int, succ bool)
	fullRange() PartitionRangeOR
}

var _ partitionRangePruner = &RangePruner{}

// RangePruner is used by 'partition by range'.
type RangePruner struct {
	LessThan LessThanDataInt
	Col      *expression.Column
	PartFn   *expression.ScalarFunction
	// If PartFn is not nil, monotonous indicates PartFn is monotonous or not.
	Monotonous monotoneMode
}

func (p *RangePruner) partitionRangeForExpr(sctx base.PlanContext, expr expression.Expression) (start int, end int, ok bool) {
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

	start, end = PruneUseBinarySearch(p.LessThan, dataForPrune)
	return start, end, true
}

func (p *RangePruner) fullRange() PartitionRangeOR {
	return GetFullRange(p.LessThan.Length())
}

// partitionRangeForOrExpr calculate the partitions for or(expr1, expr2)
func partitionRangeForOrExpr(sctx base.PlanContext, expr1, expr2 expression.Expression,
	pruner partitionRangePruner) PartitionRangeOR {
	tmp1 := PartitionRangeForExpr(sctx, expr1, pruner, pruner.fullRange())
	tmp2 := PartitionRangeForExpr(sctx, expr2, pruner, pruner.fullRange())
	return tmp1.Union(tmp2)
}

func partitionRangeColumnForInExpr(sctx base.PlanContext, args []expression.Expression,
	pruner *RangeColumnsPruner) PartitionRangeOR {
	col, ok := args[0].(*expression.Column)
	if !ok || col.ID != pruner.PartCols[0].ID {
		return pruner.fullRange()
	}

	var result PartitionRangeOR
	for i := 1; i < len(args); i++ {
		constExpr, ok := args[i].(*expression.Constant)
		if !ok {
			return pruner.fullRange()
		}
		switch constExpr.Value.Kind() {
		case types.KindInt64, types.KindUint64, types.KindMysqlTime, types.KindString: // for safety, only support string,int and datetime now
		case types.KindNull:
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
		result = append(result, PartitionRange{start, end})
	}

	return result.simplify()
}

func partitionRangeForInExpr(sctx base.PlanContext, args []expression.Expression,
	pruner *RangePruner) PartitionRangeOR {
	col, ok := args[0].(*expression.Column)
	if !ok || col.ID != pruner.Col.ID {
		return pruner.fullRange()
	}

	var result PartitionRangeOR
	for i := 1; i < len(args); i++ {
		constExpr, ok := args[i].(*expression.Constant)
		if !ok {
			return pruner.fullRange()
		}
		if constExpr.Value.Kind() == types.KindNull {
			continue
		}

		var val int64
		var err error
		var unsigned bool
		if pruner.PartFn != nil {
			// replace fn(col) to fn(const)
			partFnConst := replaceColumnWithConst(pruner.PartFn, constExpr)
			val, _, err = partFnConst.EvalInt(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			unsigned = mysql.HasUnsignedFlag(partFnConst.GetStaticType().GetFlag())
		} else {
			val, _, err = constExpr.EvalInt(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			unsigned = mysql.HasUnsignedFlag(constExpr.GetType(sctx.GetExprCtx().GetEvalCtx()).GetFlag())
		}
		if err != nil {
			return pruner.fullRange()
		}

		start, end := PruneUseBinarySearch(pruner.LessThan, DataForPrune{Op: ast.EQ, C: val, Unsigned: unsigned})
		result = append(result, PartitionRange{start, end})
	}
	return result.simplify()
}

type monotoneMode int

const (
	// MonotoneModeInvalid indicate the invalid mode.
	MonotoneModeInvalid monotoneMode = iota
	// MonotoneModeStrict indicate the strict mode.
	MonotoneModeStrict
	// MonotoneModeNonStrict indicate the non-strict mode.
	MonotoneModeNonStrict
)

// monotoneIncFuncs are those functions that are monotone increasing.
// For any x y, if x > y => f(x) > f(y), function f is strict monotone .
// For any x y, if x > y => f(x) >= f(y), function f is non-strict monotone.
var monotoneIncFuncs = map[string]monotoneMode{
	ast.Year:          MonotoneModeNonStrict,
	ast.ToDays:        MonotoneModeNonStrict,
	ast.UnixTimestamp: MonotoneModeStrict,
	// Only when the function form is fn(column, const)
	ast.Plus:  MonotoneModeStrict,
	ast.Minus: MonotoneModeStrict,
}

func getMonotoneMode(fnName string) monotoneMode {
	mode, ok := monotoneIncFuncs[fnName]
	if !ok {
		return MonotoneModeInvalid
	}
	return mode
}

// DataForPrune f(x) op const, op is > = <
type DataForPrune struct {
	Op       string
	C        int64
	Unsigned bool
}

// extractDataForPrune extracts data from the expression for pruning.
// The expression should have this form:  'f(x) op const', otherwise it can't be pruned.
func (p *RangePruner) extractDataForPrune(sctx base.PlanContext, expr expression.Expression) (DataForPrune, bool) {
	var ret DataForPrune
	op, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return ret, false
	}
	switch op.FuncName.L {
	case ast.EQ, ast.LT, ast.GT, ast.LE, ast.GE, ast.NullEQ:
		ret.Op = op.FuncName.L
	case ast.IsNull:
		// isnull(col)
		if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && arg0.ID == p.Col.ID {
			ret.Op = ast.IsNull
			return ret, true
		}
		return ret, false
	default:
		return ret, false
	}

	var col *expression.Column
	var con *expression.Constant
	if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && arg0.ID == p.Col.ID {
		if arg1, ok := op.GetArgs()[1].(*expression.Constant); ok {
			col, con = arg0, arg1
		}
	} else if arg0, ok := op.GetArgs()[1].(*expression.Column); ok && arg0.ID == p.Col.ID {
		if arg1, ok := op.GetArgs()[0].(*expression.Constant); ok {
			ret.Op = opposite(ret.Op)
			col, con = arg0, arg1
		}
	}
	if col == nil || con == nil {
		return ret, false
	}

	// Current expression is 'col op const'
	var constExpr expression.Expression
	if p.PartFn != nil {
		// If the partition function is not monotone, only EQ condition can be pruning.
		if p.Monotonous == MonotoneModeInvalid && ret.Op != ast.EQ {
			return ret, false
		}

		// If the partition expression is fn(col), change constExpr to fn(constExpr).
		constExpr = replaceColumnWithConst(p.PartFn, con)

		// When the PartFn is not strict monotonous, we need to relax the condition < to <=, > to >=.
		// For example, the following case doesn't hold:
		// col < '2020-02-11 17:34:11' => to_days(col) < to_days(2020-02-11 17:34:11)
		// The correct transform should be:
		// col < '2020-02-11 17:34:11' => to_days(col) <= to_days(2020-02-11 17:34:11)
		if p.Monotonous == MonotoneModeNonStrict {
			ret.Op = relaxOP(ret.Op)
		}
	} else {
		// If the partition expression is col, use constExpr.
		constExpr = con
	}
	c, isNull, err := constExpr.EvalInt(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	if err != nil {
		return ret, false
	}
	if !isNull {
		if ret.Op == ast.NullEQ {
			ret.Op = ast.EQ
		}
		ret.C = c
		ret.Unsigned = mysql.HasUnsignedFlag(constExpr.GetType(sctx.GetExprCtx().GetEvalCtx()).GetFlag())
		return ret, true
	} else if ret.Op == ast.NullEQ {
		// Mark it as IsNull, which is already handled in PruneUseBinarySearch.
		ret.Op = ast.IsNull
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
	} else if partFn.FuncName.L == ast.Extract {
		if expr, ok := args[1].(*expression.ScalarFunction); ok && expr.Function.PbCode() == tipb.ScalarFuncSig_CastTimeAsDuration {
			// Special handing if Cast is added
			funcArgs := expr.GetArgs()
			funcArgs[0] = con
			return partFn
		}
		args[1] = con
		return partFn
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

// PruneUseBinarySearch returns the start and end of which partitions will match.
// If no match (i.e. value > last partition) the start partition will be the number of partition, not the first partition!
func PruneUseBinarySearch(lessThan LessThanDataInt, data DataForPrune) (start int, end int) {
	length := lessThan.Length()
	switch data.Op {
	case ast.EQ:
		// col = 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col = 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col = 10, lessThan = [4 7 11 14 17] => [2, 3)
		// col = 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.C, data.Unsigned) > 0 })
		start, end = pos, pos+1
	case ast.LT:
		// col < 66, lessThan = [4 7 11 14 17] => [0, 5)
		// col < 14, lessThan = [4 7 11 14 17] => [0, 4)
		// col < 10, lessThan = [4 7 11 14 17] => [0, 3)
		// col < 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.C, data.Unsigned) >= 0 })
		start, end = 0, pos+1
	case ast.GE:
		// col >= 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col >= 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col >= 10, lessThan = [4 7 11 14 17] => [2, 5)
		// col >= 3, lessThan = [4 7 11 14 17] => [0, 5)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.C, data.Unsigned) > 0 })
		start, end = pos, length
	case ast.GT:
		// col > 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col > 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col > 10, lessThan = [4 7 11 14 17] => [3, 5)
		// col > 3, lessThan = [4 7 11 14 17] => [1, 5)
		// col > 2, lessThan = [4 7 11 14 17] => [0, 5)

		// Although `data.c+1` will overflow in sometime, this does not affect the correct results obtained.
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.C+1, data.Unsigned) > 0 })
		start, end = pos, length
	case ast.LE:
		// col <= 66, lessThan = [4 7 11 14 17] => [0, 6)
		// col <= 14, lessThan = [4 7 11 14 17] => [0, 5)
		// col <= 10, lessThan = [4 7 11 14 17] => [0, 3)
		// col <= 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.C, data.Unsigned) > 0 })
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

func (*PartitionProcessor) resolveAccessPaths(ds *logicalop.DataSource) error {
	possiblePaths, _, err := utilfuncp.GetPossibleAccessPaths(
		ds.SCtx(), &h.PlanHints{IndexMergeHintList: ds.IndexMergeHints, IndexHintList: ds.IndexHints},
		ds.AstIndexHints, ds.Table, ds.DBName, ds.TableInfo.Name, ds.IsForUpdateRead, true)
	if err != nil {
		return err
	}
	possiblePaths, err = util.FilterPathByIsolationRead(ds.SCtx(), possiblePaths, ds.TableInfo.Name, ds.DBName)
	if err != nil {
		return err
	}
	// partition processor path pruning should affect the all paths.
	allPaths := make([]*util.AccessPath, len(possiblePaths))
	copy(allPaths, possiblePaths)
	ds.AllPossibleAccessPaths = allPaths
	ds.PossibleAccessPaths = possiblePaths
	return nil
}

func (s *PartitionProcessor) resolveOptimizeHint(ds *logicalop.DataSource, partitionName ast.CIStr) error {
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
	if len(ds.IndexMergeHints) > 0 {
		newIndexMergeHint := make([]h.HintedIndex, 0, len(ds.IndexMergeHints))
		for _, idxHint := range ds.IndexMergeHints {
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
		ds.IndexMergeHints = newIndexMergeHint
	}

	// read from storage hint
	if ds.PreferStoreType&h.PreferTiKV > 0 {
		if len(ds.PreferPartitions[h.PreferTiKV]) > 0 {
			ds.PreferStoreType ^= h.PreferTiKV
			for _, p := range ds.PreferPartitions[h.PreferTiKV] {
				if p.String() == partitionName.String() {
					ds.PreferStoreType |= h.PreferTiKV
				}
			}
		}
	}
	if ds.PreferStoreType&h.PreferTiFlash > 0 {
		if len(ds.PreferPartitions[h.PreferTiFlash]) > 0 {
			ds.PreferStoreType ^= h.PreferTiFlash
			for _, p := range ds.PreferPartitions[h.PreferTiFlash] {
				if p.String() == partitionName.String() {
					ds.PreferStoreType |= h.PreferTiFlash
				}
			}
		}
	}
	if ds.PreferStoreType&h.PreferTiFlash != 0 && ds.PreferStoreType&h.PreferTiKV != 0 {
		ds.SCtx().GetSessionVars().StmtCtx.AppendWarning(
			errors.NewNoStackError("hint `read_from_storage` has conflict storage type for the partition " + partitionName.L))
	}

	return s.resolveAccessPaths(ds)
}

func checkTableHintsApplicableForPartition(partitions []ast.CIStr, partitionSet set.StringSet) []string {
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

func (*PartitionProcessor) checkHintsApplicable(ds *logicalop.DataSource, partitionSet set.StringSet) {
	for _, idxHint := range ds.IndexHints {
		unknownPartitions := checkTableHintsApplicableForPartition(idxHint.Partitions, partitionSet)
		appendWarnForUnknownPartitions(ds.SCtx(), h.Restore2IndexHint(idxHint.HintTypeString(), idxHint), unknownPartitions)
	}
	for _, idxMergeHint := range ds.IndexMergeHints {
		unknownPartitions := checkTableHintsApplicableForPartition(idxMergeHint.Partitions, partitionSet)
		appendWarnForUnknownPartitions(ds.SCtx(), h.Restore2IndexHint(h.HintIndexMerge, idxMergeHint), unknownPartitions)
	}
	unknownPartitions := checkTableHintsApplicableForPartition(ds.PreferPartitions[h.PreferTiKV], partitionSet)
	unknownPartitions = append(unknownPartitions,
		checkTableHintsApplicableForPartition(ds.PreferPartitions[h.PreferTiFlash], partitionSet)...)
	appendWarnForUnknownPartitions(ds.SCtx(), h.HintReadFromStorage, unknownPartitions)
}

func (s *PartitionProcessor) makeUnionAllChildren(ds *logicalop.DataSource, pi *model.PartitionInfo, or PartitionRangeOR) (base.LogicalPlan, error) {
	children := make([]base.LogicalPlan, 0, len(pi.Definitions))
	partitionNameSet := make(set.StringSet)
	usedDefinition := make(map[int64]model.PartitionDefinition)
	for _, r := range or {
		for i := r.Start; i < r.End; i++ {
			partIdx := pi.GetOverlappingDroppingPartitionIdx(i)
			if partIdx < 0 {
				continue
			}

			// This is for `table partition (p0,p1)` syntax, only union the specified partition if has specified partitions.
			if len(ds.PartitionNames) != 0 {
				if !s.FindByName(ds.PartitionNames, pi.Definitions[partIdx].Name.L) {
					continue
				}
			}
			if _, found := usedDefinition[pi.Definitions[partIdx].ID]; found {
				continue
			}
			// Not a deep copy.
			newDataSource := *ds
			newDataSource.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ds.SCtx(), plancodec.TypeTableScan, &newDataSource, ds.QueryBlockOffset())
			newDataSource.SetSchema(ds.Schema().Clone())
			newDataSource.Columns = make([]*model.ColumnInfo, len(ds.Columns))
			copy(newDataSource.Columns, ds.Columns)
			newDataSource.PartitionDefIdx = &partIdx
			newDataSource.PhysicalTableID = pi.Definitions[partIdx].ID

			// There are many expression nodes in the plan tree use the original datasource
			// id as FromID. So we set the id of the newDataSource with the original one to
			// avoid traversing the whole plan tree to update the references.
			newDataSource.SetID(ds.ID())
			err := s.resolveOptimizeHint(&newDataSource, pi.Definitions[partIdx].Name)
			partitionNameSet.Insert(pi.Definitions[partIdx].Name.L)
			if err != nil {
				return nil, err
			}
			children = append(children, &newDataSource)
			usedDefinition[pi.Definitions[partIdx].ID] = pi.Definitions[partIdx]
		}
	}
	s.checkHintsApplicable(ds, partitionNameSet)

	ds.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("Static partition pruning mode")
	if len(children) == 0 {
		// No result after table pruning.
		tableDual := logicalop.LogicalTableDual{RowCount: 0}.Init(ds.SCtx(), ds.QueryBlockOffset())
		tableDual.SetSchema(ds.Schema())
		return tableDual, nil
	}
	if len(children) == 1 {
		// No need for the union all.
		return children[0], nil
	}
	unionAll := logicalop.LogicalPartitionUnionAll{}.Init(ds.SCtx(), ds.QueryBlockOffset())
	unionAll.SetChildren(children...)
	unionAll.SetSchema(ds.Schema().Clone())
	return unionAll, nil
}

func (*PartitionProcessor) pruneRangeColumnsPartition(ctx base.PlanContext, conds []expression.Expression, pi *model.PartitionInfo, pe *tables.PartitionExpr, columns []*expression.Column) (PartitionRangeOR, error) {
	result := GetFullRange(len(pi.Definitions))

	if len(pi.Columns) < 1 {
		return result, nil
	}

	pruner, err := makeRangeColumnPruner(columns, pi, pe.ForRangeColumnsPruning, pe.ColumnOffset)
	if err == nil {
		result = PartitionRangeForCNFExpr(ctx, conds, pruner, result)
	}
	return result, nil
}

var _ partitionRangePruner = &RangeColumnsPruner{}

// RangeColumnsPruner is used by 'partition by range columns'.
type RangeColumnsPruner struct {
	LessThan [][]*expression.Expression
	PartCols []*expression.Column
}

func makeRangeColumnPruner(columns []*expression.Column, pi *model.PartitionInfo, from *tables.ForRangeColumnsPruning, offsets []int) (*RangeColumnsPruner, error) {
	if len(pi.Definitions) != len(from.LessThan) {
		return nil, errors.Trace(fmt.Errorf("internal error len(pi.Definitions) != len(from.LessThan) %d != %d", len(pi.Definitions), len(from.LessThan)))
	}
	partCols := make([]*expression.Column, len(offsets))
	for i, offset := range offsets {
		partCols[i] = columns[offset]
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
	return &RangeColumnsPruner{lessThan, partCols}, nil
}

func (p *RangeColumnsPruner) fullRange() PartitionRangeOR {
	return GetFullRange(len(p.LessThan))
}

func (p *RangeColumnsPruner) getPartCol(colID int64) *expression.Column {
	for i := range p.PartCols {
		if colID == p.PartCols[i].ID {
			return p.PartCols[i]
		}
	}
	return nil
}

func (p *RangeColumnsPruner) partitionRangeForExpr(sctx base.PlanContext, expr expression.Expression) (start int, end int, ok bool) {
	op, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return 0, len(p.LessThan), false
	}

	switch op.FuncName.L {
	case ast.EQ, ast.LT, ast.GT, ast.LE, ast.GE, ast.NullEQ:
	case ast.IsNull:
		// isnull(col)
		if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && len(p.PartCols) == 1 && arg0.ID == p.PartCols[0].ID {
			// Single column RANGE COLUMNS, NULL sorts before all other values: match first partition
			return 0, 1, true
		}
		return 0, len(p.LessThan), false
	default:
		return 0, len(p.LessThan), false
	}
	opName := op.FuncName.L

	var col *expression.Column
	var con *expression.Constant
	var argCol0, argCol1 *expression.Column
	var argCon0, argCon1 *expression.Constant
	var okCol0, okCol1, okCon0, okCon1 bool
	args := op.GetArgs()
	argCol1, okCol1 = args[1].(*expression.Column)
	argCon1, okCon1 = args[1].(*expression.Constant)
	argCol0, okCol0 = args[0].(*expression.Column)
	argCon0, okCon0 = args[0].(*expression.Constant)
	if okCol0 && okCon1 {
		col, con = argCol0, argCon1
	} else if okCol1 && okCon0 {
		col, con = argCol1, argCon0
		opName = opposite(opName)
	} else {
		return 0, len(p.LessThan), false
	}
	partCol := p.getPartCol(col.ID)
	if partCol == nil {
		return 0, len(p.LessThan), false
	}

	if opName == ast.NullEQ {
		if con.Value.IsNull() {
			return 0, 1, true
		}
		opName = ast.EQ
	}
	// If different collation, we can only prune if:
	// - expression is binary collation (can only be found in one partition)
	// - EQ operator, consider values 'a','b','ä' where 'ä' would be in the same partition as 'a' if general_ci, but is binary after 'b'
	// otherwise return all partitions / no pruning
	_, exprColl := expr.CharsetAndCollation()
	colColl := partCol.RetType.GetCollate()
	if exprColl != colColl && (opName != ast.EQ || !collate.IsBinCollation(exprColl)) {
		return 0, len(p.LessThan), true
	}
	start, end = p.pruneUseBinarySearch(sctx, opName, con)
	return start, end, true
}

// PruneUseBinarySearch returns the start and end of which partitions will match.
// If no match (i.e. value > last partition) the start partition will be the number of partition, not the first partition!
func (p *RangeColumnsPruner) pruneUseBinarySearch(sctx base.PlanContext, op string, data *expression.Constant) (start int, end int) {
	var savedError error
	var isNull bool
	if len(p.PartCols) > 1 {
		// Only one constant in the input, this will never be called with
		// multi-column RANGE COLUMNS :)
		return 0, len(p.LessThan)
	}
	charSet, collation := p.PartCols[0].RetType.GetCharset(), p.PartCols[0].RetType.GetCollate()
	compare := func(ith int, op string, v *expression.Constant) bool {
		for i := range p.PartCols {
			if p.LessThan[ith][i] == nil { // MAXVALUE
				return true
			}
			expr, err := expression.NewFunctionBase(sctx.GetExprCtx(), op, types.NewFieldType(mysql.TypeLonglong), *p.LessThan[ith][i], v)
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

	length := len(p.LessThan)
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
		return 0, len(p.LessThan)
	}

	if end > length {
		end = length
	}
	return start, end
}

// PushDownNot here can convert condition 'not (a != 1)' to 'a = 1'. When we build range from conds, the condition like
// 'not (a != 1)' would not be handled so we need to convert it to 'a = 1', which can be handled when building range.
func PushDownNot(ctx expression.BuildContext, conds []expression.Expression) []expression.Expression {
	if len(conds) == 0 {
		return conds
	}
	for i, cond := range conds {
		conds[i] = expression.PushDownNot(ctx, cond)
	}
	return conds
}
