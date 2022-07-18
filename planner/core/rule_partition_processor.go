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
	"context"
	"fmt"
	gomath "math"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/set"
)

// FullRange represent used all partitions.
const FullRange = -1

// partitionProcessor rewrites the ast for table partition.
// Used by static partition prune mode.
//
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
//
// partitionProcessor is here because it's easier to prune partition after predicate push down.
type partitionProcessor struct{}

func (s *partitionProcessor) optimize(ctx context.Context, lp LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	p, err := s.rewriteDataSource(lp, opt)
	return p, err
}

func (s *partitionProcessor) rewriteDataSource(lp LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
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
			children := make([]LogicalPlan, 0, len(ua.Children()))
			for _, child := range ua.Children() {
				us := LogicalUnionScan{
					conditions: p.conditions,
					handleCols: p.handleCols,
				}.Init(ua.ctx, ua.blockOffset)
				us.SetChildren(child)
				children = append(children, us)
			}
			ua.SetChildren(children...)
			return ua, nil
		}
		// Only one partition, no union all.
		p.SetChildren(ds)
		return p, nil
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
	PartitionExpr() (*tables.PartitionExpr, error)
}

func generateHashPartitionExpr(ctx sessionctx.Context, pi *model.PartitionInfo, columns []*expression.Column, names types.NameSlice) (expression.Expression, error) {
	schema := expression.NewSchema(columns...)
	exprs, err := expression.ParseSimpleExprsWithNames(ctx, pi.Expr, schema, names)
	if err != nil {
		return nil, err
	}
	exprs[0].HashCode(ctx.GetSessionVars().StmtCtx)
	return exprs[0], nil
}

func (s *partitionProcessor) findUsedPartitions(ctx sessionctx.Context, tbl table.Table, partitionNames []model.CIStr,
	conds []expression.Expression, columns []*expression.Column, names types.NameSlice) ([]int, []expression.Expression, error) {
	pi := tbl.Meta().Partition
	pe, err := generateHashPartitionExpr(ctx, pi, columns, names)
	if err != nil {
		return nil, nil, err
	}
	partIdx := expression.ExtractColumns(pe)
	colLen := make([]int, 0, len(partIdx))
	for i := 0; i < len(partIdx); i++ {
		partIdx[i].Index = i
		colLen = append(colLen, types.UnspecifiedLength)
	}
	detachedResult, err := ranger.DetachCondAndBuildRangeForPartition(ctx, conds, partIdx, colLen)
	if err != nil {
		return nil, nil, err
	}
	ranges := detachedResult.Ranges
	used := make([]int, 0, len(ranges))
	for _, r := range ranges {
		if r.IsPointNullable(ctx) {
			if !r.HighVal[0].IsNull() {
				if len(r.HighVal) != len(partIdx) {
					used = []int{-1}
					break
				}
			}
			highLowVals := make([]types.Datum, 0, len(r.HighVal)+len(r.LowVal))
			highLowVals = append(highLowVals, r.HighVal...)
			highLowVals = append(highLowVals, r.LowVal...)
			pos, isNull, err := pe.EvalInt(ctx, chunk.MutRowFromDatums(highLowVals).ToRow())
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
		} else {
			// processing hash partition pruning. eg:
			// create table t2 (a int, b bigint, index (a), index (b)) partition by hash(a) partitions 10;
			// desc select * from t2 where t2.a between 10 and 15;
			// determine whether the partition key is int
			if col, ok := pe.(*expression.Column); ok && col.RetType.EvalType() == types.ETInt {
				numPartitions := len(pi.Definitions)

				posHigh, highIsNull, err := pe.EvalInt(ctx, chunk.MutRowFromDatums(r.HighVal).ToRow())
				if err != nil {
					return nil, nil, err
				}

				posLow, lowIsNull, err := pe.EvalInt(ctx, chunk.MutRowFromDatums(r.LowVal).ToRow())
				if err != nil {
					return nil, nil, err
				}

				// consider whether the range is closed or open
				if r.LowExclude {
					posLow++
				}
				if r.HighExclude {
					posHigh--
				}

				var rangeScalar float64
				if mysql.HasUnsignedFlag(col.RetType.GetFlag()) {
					rangeScalar = float64(uint64(posHigh)) - float64(uint64(posLow)) // use float64 to avoid integer overflow
				} else {
					rangeScalar = float64(posHigh) - float64(posLow) // use float64 to avoid integer overflow
				}

				// if range is less than the number of partitions, there will be unused partitions we can prune out.
				if rangeScalar < float64(numPartitions) && !highIsNull && !lowIsNull {
					for i := posLow; i <= posHigh; i++ {
						idx := mathutil.Abs(i % int64(pi.Num))
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
					if col.RetType.GetFlen() > 0 && col.RetType.GetFlen() < int(gomath.Log2(ddl.PartitionCountLimit)) {
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
	}
	if len(partitionNames) > 0 && len(used) == 1 && used[0] == FullRange {
		or := partitionRangeOR{partitionRange{0, len(pi.Definitions)}}
		return s.convertToIntSlice(or, pi, partitionNames), nil, nil
	}
	sort.Ints(used)
	ret := used[:0]
	for i := 0; i < len(used); i++ {
		if i == 0 || used[i] != used[i-1] {
			ret = append(ret, used[i])
		}
	}
	return ret, detachedResult.RemainedConds, nil
}

func (s *partitionProcessor) convertToIntSlice(or partitionRangeOR, pi *model.PartitionInfo, partitionNames []model.CIStr) []int {
	if len(or) == 1 && or[0].start == 0 && or[0].end == len(pi.Definitions) {
		if len(partitionNames) == 0 {
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

func (s *partitionProcessor) pruneHashPartition(ctx sessionctx.Context, tbl table.Table, partitionNames []model.CIStr,
	conds []expression.Expression, columns []*expression.Column, names types.NameSlice) ([]int, error) {
	used, _, err := s.findUsedPartitions(ctx, tbl, partitionNames, conds, columns, names)
	if err != nil {
		return nil, err
	}
	return used, nil
}

// reconstructTableColNames reconstructs FieldsNames according to ds.TblCols.
// ds.names may not match ds.TblCols since ds.names is pruned while ds.TblCols contains all original columns.
// please see https://github.com/pingcap/tidb/issues/22635 for more details.
func (s *partitionProcessor) reconstructTableColNames(ds *DataSource) ([]*types.FieldName, error) {
	names := make([]*types.FieldName, 0, len(ds.TblCols))
	colsInfo := ds.table.FullHiddenColsAndVisibleCols()
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

func (s *partitionProcessor) processHashPartition(ds *DataSource, pi *model.PartitionInfo, opt *logicalOptimizeOp) (LogicalPlan, error) {
	names, err := s.reconstructTableColNames(ds)
	if err != nil {
		return nil, err
	}
	used, err := s.pruneHashPartition(ds.SCtx(), ds.table, ds.partitionNames, ds.allConds, ds.TblCols, names)
	if err != nil {
		return nil, err
	}
	if used != nil {
		return s.makeUnionAllChildren(ds, pi, convertToRangeOr(used, pi), opt)
	}
	tableDual := LogicalTableDual{RowCount: 0}.Init(ds.SCtx(), ds.blockOffset)
	tableDual.schema = ds.Schema()
	appendNoPartitionChildTraceStep(ds, tableDual, opt)
	return tableDual, nil
}

// listPartitionPruner uses to prune partition for list partition.
type listPartitionPruner struct {
	*partitionProcessor
	ctx             sessionctx.Context
	pi              *model.PartitionInfo
	partitionNames  []model.CIStr
	colIDToUniqueID map[int64]int64
	fullRange       map[int]struct{}
	listPrune       *tables.ForListPruning
}

func newListPartitionPruner(ctx sessionctx.Context, tbl table.Table, partitionNames []model.CIStr,
	s *partitionProcessor, conds []expression.Expression, pruneList *tables.ForListPruning) *listPartitionPruner {
	colIDToUniqueID := make(map[int64]int64)
	for _, cond := range conds {
		condCols := expression.ExtractColumns(cond)
		for _, c := range condCols {
			colIDToUniqueID[c.ID] = c.UniqueID
		}
	}
	fullRange := make(map[int]struct{})
	fullRange[FullRange] = struct{}{}
	return &listPartitionPruner{
		partitionProcessor: s,
		ctx:                ctx,
		pi:                 tbl.Meta().Partition,
		partitionNames:     partitionNames,
		colIDToUniqueID:    colIDToUniqueID,
		fullRange:          fullRange,
		listPrune:          pruneList,
	}
}

func (l *listPartitionPruner) locatePartition(cond expression.Expression) (tables.ListPartitionLocation, bool, error) {
	switch sf := cond.(type) {
	case *expression.Constant:
		b, err := sf.Value.ToBool(l.ctx.GetSessionVars().StmtCtx)
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
			// No partition for intersection, just return 0 partition.
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
	helper := tables.NewListPartitionLocationHelper()
	for _, r := range ranges {
		if len(r.LowVal) != 1 || len(r.HighVal) != 1 {
			return nil, true, nil
		}
		var locations []tables.ListPartitionLocation
		if r.IsPointNullable(l.ctx) {
			location, err := colPrune.LocatePartition(sc, r.HighVal[0])
			if types.ErrOverflow.Equal(err) {
				return nil, true, nil // return full-scan if over-flow
			}
			if err != nil {
				return nil, false, err
			}
			locations = []tables.ListPartitionLocation{location}
		} else {
			locations, err = colPrune.LocateRanges(sc, r)
			if types.ErrOverflow.Equal(err) {
				return nil, true, nil // return full-scan if over-flow
			}
			if err != nil {
				return nil, false, err
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
		if uniqueID, ok := l.colIDToUniqueID[c.ID]; ok {
			c.UniqueID = uniqueID
		}
		cols = append(cols, c)
		colLen = append(colLen, types.UnspecifiedLength)
	}

	detachedResult, err := ranger.DetachCondAndBuildRangeForPartition(l.ctx, conds, cols, colLen)
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
	for _, r := range ranges {
		if r.IsPointNullable(l.ctx) {
			if len(r.HighVal) != len(exprCols) {
				return l.fullRange, nil
			}
			value, isNull, err := pruneExpr.EvalInt(l.ctx, chunk.MutRowFromDatums(r.HighVal).ToRow())
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
		} else {
			return l.fullRange, nil
		}
	}
	return used, nil
}

func (s *partitionProcessor) findUsedListPartitions(ctx sessionctx.Context, tbl table.Table, partitionNames []model.CIStr,
	conds []expression.Expression) ([]int, error) {
	pi := tbl.Meta().Partition
	partExpr, err := tbl.(partitionTable).PartitionExpr()
	if err != nil {
		return nil, err
	}

	listPruner := newListPartitionPruner(ctx, tbl, partitionNames, s, conds, partExpr.ForListPruning)
	var used map[int]struct{}
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
	sort.Ints(ret)
	return ret, nil
}

func (s *partitionProcessor) pruneListPartition(ctx sessionctx.Context, tbl table.Table, partitionNames []model.CIStr,
	conds []expression.Expression) ([]int, error) {
	used, err := s.findUsedListPartitions(ctx, tbl, partitionNames, conds)
	if err != nil {
		return nil, err
	}
	return used, nil
}

func (s *partitionProcessor) prune(ds *DataSource, opt *logicalOptimizeOp) (LogicalPlan, error) {
	pi := ds.tableInfo.GetPartitionInfo()
	if pi == nil {
		return ds, nil
	}
	// PushDownNot here can convert condition 'not (a != 1)' to 'a = 1'. When we build range from ds.allConds, the condition
	// like 'not (a != 1)' would not be handled so we need to convert it to 'a = 1', which can be handled when building range.
	// TODO: there may be a better way to push down Not once for all.
	for i, cond := range ds.allConds {
		ds.allConds[i] = expression.PushDownNot(ds.ctx, cond)
	}
	// Try to locate partition directly for hash partition.
	switch pi.Type {
	case model.PartitionTypeRange:
		return s.processRangePartition(ds, pi, opt)
	case model.PartitionTypeHash:
		return s.processHashPartition(ds, pi, opt)
	case model.PartitionTypeList:
		return s.processListPartition(ds, pi, opt)
	}

	// We haven't implement partition by key and so on.
	return s.makeUnionAllChildren(ds, pi, fullRange(len(pi.Definitions)), opt)
}

// findByName checks whether object name exists in list.
func (s *partitionProcessor) findByName(partitionNames []model.CIStr, partitionName string) bool {
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
	maxvalue bool
}

func (lt *lessThanDataInt) length() int {
	return len(lt.data)
}

func compareUnsigned(v1, v2 int64) int {
	switch {
	case uint64(v1) > uint64(v2):
		return 1
	case uint64(v1) == uint64(v2):
		return 0
	}
	return -1
}

func (lt *lessThanDataInt) compare(ith int, v int64, unsigned bool) int {
	if ith == len(lt.data)-1 {
		if lt.maxvalue {
			return 1
		}
	}
	if unsigned {
		return compareUnsigned(lt.data[ith], v)
	}
	switch {
	case lt.data[ith] > v:
		return 1
	case lt.data[ith] == v:
		return 0
	}
	return -1
}

// partitionRange represents [start, range)
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
func intersectionRange(start, end, newStart, newEnd int) (int, int) {
	var s, e int
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

func (s *partitionProcessor) pruneRangePartition(ctx sessionctx.Context, pi *model.PartitionInfo, tbl table.PartitionedTable, conds []expression.Expression,
	columns []*expression.Column, names types.NameSlice, condsToBePruned *[]expression.Expression) (partitionRangeOR, []expression.Expression, error) {
	partExpr, err := tbl.(partitionTable).PartitionExpr()
	if err != nil {
		return nil, nil, err
	}

	// Partition by range columns.
	if len(pi.Columns) > 0 {
		result, err := s.pruneRangeColumnsPartition(ctx, conds, pi, partExpr, columns, names)
		return result, nil, err
	}

	// Partition by range.
	col, fn, mono, err := makePartitionByFnCol(ctx, columns, names, pi.Expr)
	if err != nil {
		return nil, nil, err
	}
	result := fullRange(len(pi.Definitions))
	if col == nil {
		return result, nil, nil
	}

	// Extract the partition column, if the column is not null, it's possible to prune.
	pruner := rangePruner{
		lessThan: lessThanDataInt{
			data:     partExpr.ForRangePruning.LessThan,
			maxvalue: partExpr.ForRangePruning.MaxValue,
		},
		col:        col,
		partFn:     fn,
		monotonous: mono,
	}
	result = partitionRangeForCNFExpr(ctx, conds, &pruner, result)

	if condsToBePruned == nil {
		return result, nil, nil
	}
	// remove useless predicates after pruning
	newConds := make([]expression.Expression, 0, len(*condsToBePruned))
	for _, cond := range *condsToBePruned {
		if dataForPrune, ok := pruner.extractDataForPrune(ctx, cond); ok {
			switch dataForPrune.op {
			case ast.EQ:
				unsigned := mysql.HasUnsignedFlag(pruner.col.RetType.GetFlag())
				start, _ := pruneUseBinarySearch(pruner.lessThan, dataForPrune, unsigned)
				// if the type of partition key is Int
				if pk, ok := partExpr.Expr.(*expression.Column); ok && pk.RetType.EvalType() == types.ETInt {
					// see if can be removed
					// see issue #22079: https://github.com/pingcap/tidb/issues/22079 for details
					if start > 0 && pruner.lessThan.data[start-1] == dataForPrune.c && (pruner.lessThan.data[start]-1) == dataForPrune.c {
						continue
					}
				}
			}
		}
		newConds = append(newConds, cond)
	}

	return result, newConds, nil
}

func (s *partitionProcessor) processRangePartition(ds *DataSource, pi *model.PartitionInfo, opt *logicalOptimizeOp) (LogicalPlan, error) {
	used, prunedConds, err := s.pruneRangePartition(ds.ctx, pi, ds.table.(table.PartitionedTable), ds.allConds, ds.TblCols, ds.names, &ds.pushedDownConds)
	if err != nil {
		return nil, err
	}
	if prunedConds != nil {
		ds.pushedDownConds = prunedConds
	}
	return s.makeUnionAllChildren(ds, pi, used, opt)
}

func (s *partitionProcessor) processListPartition(ds *DataSource, pi *model.PartitionInfo, opt *logicalOptimizeOp) (LogicalPlan, error) {
	used, err := s.pruneListPartition(ds.SCtx(), ds.table, ds.partitionNames, ds.allConds)
	if err != nil {
		return nil, err
	}
	if used != nil {
		return s.makeUnionAllChildren(ds, pi, convertToRangeOr(used, pi), opt)
	}
	tableDual := LogicalTableDual{RowCount: 0}.Init(ds.SCtx(), ds.blockOffset)
	tableDual.schema = ds.Schema()
	appendNoPartitionChildTraceStep(ds, tableDual, opt)
	return tableDual, nil
}

// makePartitionByFnCol extracts the column and function information in 'partition by ... fn(col)'.
func makePartitionByFnCol(sctx sessionctx.Context, columns []*expression.Column, names types.NameSlice, partitionExpr string) (*expression.Column, *expression.ScalarFunction, monotoneMode, error) {
	monotonous := monotoneModeInvalid
	schema := expression.NewSchema(columns...)
	tmp, err := expression.ParseSimpleExprsWithNames(sctx, partitionExpr, schema, names)
	if err != nil {
		return nil, nil, monotonous, err
	}
	partExpr := tmp[0]
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

func partitionRangeForCNFExpr(sctx sessionctx.Context, exprs []expression.Expression,
	pruner partitionRangePruner, result partitionRangeOR) partitionRangeOR {
	for i := 0; i < len(exprs); i++ {
		result = partitionRangeForExpr(sctx, exprs[i], pruner, result)
	}
	return result
}

// partitionRangeForExpr calculate the partitions for the expression.
func partitionRangeForExpr(sctx sessionctx.Context, expr expression.Expression,
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
	partitionRangeForExpr(sessionctx.Context, expression.Expression) (start, end int, succ bool)
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

func (p *rangePruner) partitionRangeForExpr(sctx sessionctx.Context, expr expression.Expression) (int, int, bool) {
	if constExpr, ok := expr.(*expression.Constant); ok {
		if b, err := constExpr.Value.ToBool(sctx.GetSessionVars().StmtCtx); err == nil && b == 0 {
			// A constant false expression.
			return 0, 0, true
		}
	}

	dataForPrune, ok := p.extractDataForPrune(sctx, expr)
	if !ok {
		return 0, 0, false
	}

	unsigned := mysql.HasUnsignedFlag(p.col.RetType.GetFlag())
	start, end := pruneUseBinarySearch(p.lessThan, dataForPrune, unsigned)
	return start, end, true
}

func (p *rangePruner) fullRange() partitionRangeOR {
	return fullRange(p.lessThan.length())
}

// partitionRangeForOrExpr calculate the partitions for or(expr1, expr2)
func partitionRangeForOrExpr(sctx sessionctx.Context, expr1, expr2 expression.Expression,
	pruner partitionRangePruner) partitionRangeOR {
	tmp1 := partitionRangeForExpr(sctx, expr1, pruner, pruner.fullRange())
	tmp2 := partitionRangeForExpr(sctx, expr2, pruner, pruner.fullRange())
	return tmp1.union(tmp2)
}

func partitionRangeColumnForInExpr(sctx sessionctx.Context, args []expression.Expression,
	pruner *rangeColumnsPruner) partitionRangeOR {
	col, ok := args[0].(*expression.Column)
	if !ok || col.ID != pruner.partCol.ID {
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
		sf, err := expression.NewFunction(sctx, ast.EQ, types.NewFieldType(types.KindInt64), []expression.Expression{col, args[i]}...)
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

func partitionRangeForInExpr(sctx sessionctx.Context, args []expression.Expression,
	pruner *rangePruner) partitionRangeOR {
	col, ok := args[0].(*expression.Column)
	if !ok || col.ID != pruner.col.ID {
		return pruner.fullRange()
	}

	var result partitionRangeOR
	unsigned := mysql.HasUnsignedFlag(col.RetType.GetFlag())
	for i := 1; i < len(args); i++ {
		constExpr, ok := args[i].(*expression.Constant)
		if !ok {
			return pruner.fullRange()
		}
		switch constExpr.Value.Kind() {
		case types.KindInt64, types.KindUint64:
		case types.KindNull:
			result = append(result, partitionRange{0, 1})
			continue
		default:
			return pruner.fullRange()
		}

		var val int64
		var err error
		if pruner.partFn != nil {
			// replace fn(col) to fn(const)
			partFnConst := replaceColumnWithConst(pruner.partFn, constExpr)
			val, _, err = partFnConst.EvalInt(sctx, chunk.Row{})
		} else {
			val, err = constExpr.Value.ToInt64(sctx.GetSessionVars().StmtCtx)
		}
		if err != nil {
			return pruner.fullRange()
		}

		start, end := pruneUseBinarySearch(pruner.lessThan, dataForPrune{op: ast.EQ, c: val}, unsigned)
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
	op string
	c  int64
}

// extractDataForPrune extracts data from the expression for pruning.
// The expression should have this form:  'f(x) op const', otherwise it can't be pruned.
func (p *rangePruner) extractDataForPrune(sctx sessionctx.Context, expr expression.Expression) (dataForPrune, bool) {
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
	if !constExpr.ConstItem(sctx.GetSessionVars().StmtCtx) {
		return ret, false
	}
	c, isNull, err := constExpr.EvalInt(sctx, chunk.Row{})
	if err == nil && !isNull {
		ret.c = c
		return ret, true
	}
	return ret, false
}

// replaceColumnWithConst change fn(col) to fn(const)
func replaceColumnWithConst(partFn *expression.ScalarFunction, con *expression.Constant) *expression.ScalarFunction {
	args := partFn.GetArgs()
	// The partition function may be floor(unix_timestamp(ts)) instead of a simple fn(col).
	if partFn.FuncName.L == ast.Floor {
		ut := args[0].(*expression.ScalarFunction)
		if ut.FuncName.L == ast.UnixTimestamp {
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

func pruneUseBinarySearch(lessThan lessThanDataInt, data dataForPrune, unsigned bool) (start int, end int) {
	length := lessThan.length()
	switch data.op {
	case ast.EQ:
		// col = 66, lessThan = [4 7 11 14 17] => [5, 6)
		// col = 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col = 10, lessThan = [4 7 11 14 17] => [2, 3)
		// col = 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, unsigned) > 0 })
		start, end = pos, pos+1
	case ast.LT:
		// col < 66, lessThan = [4 7 11 14 17] => [0, 5)
		// col < 14, lessThan = [4 7 11 14 17] => [0, 4)
		// col < 10, lessThan = [4 7 11 14 17] => [0, 3)
		// col < 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, unsigned) >= 0 })
		start, end = 0, pos+1
	case ast.GE:
		// col >= 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col >= 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col >= 10, lessThan = [4 7 11 14 17] => [2, 5)
		// col >= 3, lessThan = [4 7 11 14 17] => [0, 5)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, unsigned) > 0 })
		start, end = pos, length
	case ast.GT:
		// col > 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col > 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col > 10, lessThan = [4 7 11 14 17] => [3, 5)
		// col > 3, lessThan = [4 7 11 14 17] => [1, 5)
		// col > 2, lessThan = [4 7 11 14 17] => [0, 5)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c+1, unsigned) > 0 })
		start, end = pos, length
	case ast.LE:
		// col <= 66, lessThan = [4 7 11 14 17] => [0, 6)
		// col <= 14, lessThan = [4 7 11 14 17] => [0, 5)
		// col <= 10, lessThan = [4 7 11 14 17] => [0, 3)
		// col <= 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, unsigned) > 0 })
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

func (s *partitionProcessor) resolveAccessPaths(ds *DataSource) error {
	possiblePaths, err := getPossibleAccessPaths(
		ds.ctx, &tableHintInfo{indexMergeHintList: ds.indexMergeHints, indexHintList: ds.IndexHints},
		ds.astIndexHints, ds.table, ds.DBName, ds.tableInfo.Name, ds.isForUpdateRead, ds.is.SchemaMetaVersion())
	if err != nil {
		return err
	}
	possiblePaths, err = filterPathByIsolationRead(ds.ctx, possiblePaths, ds.tableInfo.Name, ds.DBName)
	if err != nil {
		return err
	}
	ds.possibleAccessPaths = possiblePaths
	return nil
}

func (s *partitionProcessor) resolveOptimizeHint(ds *DataSource, partitionName model.CIStr) error {
	// index hint
	if len(ds.IndexHints) > 0 {
		newIndexHint := make([]indexHintInfo, 0, len(ds.IndexHints))
		for _, idxHint := range ds.IndexHints {
			if len(idxHint.partitions) == 0 {
				newIndexHint = append(newIndexHint, idxHint)
			} else {
				for _, p := range idxHint.partitions {
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
		newIndexMergeHint := make([]indexHintInfo, 0, len(ds.indexMergeHints))
		for _, idxHint := range ds.indexMergeHints {
			if len(idxHint.partitions) == 0 {
				newIndexMergeHint = append(newIndexMergeHint, idxHint)
			} else {
				for _, p := range idxHint.partitions {
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
	if ds.preferStoreType&preferTiKV > 0 {
		if len(ds.preferPartitions[preferTiKV]) > 0 {
			ds.preferStoreType ^= preferTiKV
			for _, p := range ds.preferPartitions[preferTiKV] {
				if p.String() == partitionName.String() {
					ds.preferStoreType |= preferTiKV
				}
			}
		}
	}
	if ds.preferStoreType&preferTiFlash > 0 {
		if len(ds.preferPartitions[preferTiFlash]) > 0 {
			ds.preferStoreType ^= preferTiFlash
			for _, p := range ds.preferPartitions[preferTiFlash] {
				if p.String() == partitionName.String() {
					ds.preferStoreType |= preferTiFlash
				}
			}
		}
	}
	if ds.preferStoreType&preferTiFlash != 0 && ds.preferStoreType&preferTiKV != 0 {
		ds.ctx.GetSessionVars().StmtCtx.AppendWarning(
			errors.New("hint `read_from_storage` has conflict storage type for the partition " + partitionName.L))
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

func appendWarnForUnknownPartitions(ctx sessionctx.Context, hintName string, unknownPartitions []string) {
	if len(unknownPartitions) == 0 {
		return
	}

	warning := fmt.Errorf("Unknown partitions (%s) in optimizer hint %s", strings.Join(unknownPartitions, ","), hintName)
	ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
}

func (s *partitionProcessor) checkHintsApplicable(ds *DataSource, partitionSet set.StringSet) {
	for _, idxHint := range ds.IndexHints {
		unknownPartitions := checkTableHintsApplicableForPartition(idxHint.partitions, partitionSet)
		appendWarnForUnknownPartitions(ds.ctx, restore2IndexHint(idxHint.hintTypeString(), idxHint), unknownPartitions)
	}
	for _, idxMergeHint := range ds.indexMergeHints {
		unknownPartitions := checkTableHintsApplicableForPartition(idxMergeHint.partitions, partitionSet)
		appendWarnForUnknownPartitions(ds.ctx, restore2IndexHint(HintIndexMerge, idxMergeHint), unknownPartitions)
	}
	unknownPartitions := checkTableHintsApplicableForPartition(ds.preferPartitions[preferTiKV], partitionSet)
	unknownPartitions = append(unknownPartitions,
		checkTableHintsApplicableForPartition(ds.preferPartitions[preferTiFlash], partitionSet)...)
	appendWarnForUnknownPartitions(ds.ctx, HintReadFromStorage, unknownPartitions)
}

func (s *partitionProcessor) makeUnionAllChildren(ds *DataSource, pi *model.PartitionInfo, or partitionRangeOR, opt *logicalOptimizeOp) (LogicalPlan, error) {

	children := make([]LogicalPlan, 0, len(pi.Definitions))
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
			newDataSource.baseLogicalPlan = newBaseLogicalPlan(ds.SCtx(), plancodec.TypeTableScan, &newDataSource, ds.blockOffset)
			newDataSource.schema = ds.schema.Clone()
			newDataSource.Columns = make([]*model.ColumnInfo, len(ds.Columns))
			copy(newDataSource.Columns, ds.Columns)
			newDataSource.isPartition = true
			newDataSource.physicalTableID = pi.Definitions[i].ID

			// There are many expression nodes in the plan tree use the original datasource
			// id as FromID. So we set the id of the newDataSource with the original one to
			// avoid traversing the whole plan tree to update the references.
			newDataSource.id = ds.id
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

	if len(children) == 0 {
		// No result after table pruning.
		tableDual := LogicalTableDual{RowCount: 0}.Init(ds.SCtx(), ds.blockOffset)
		tableDual.schema = ds.Schema()
		appendMakeUnionAllChildrenTranceStep(ds, usedDefinition, tableDual, children, opt)
		return tableDual, nil
	}
	if len(children) == 1 {
		// No need for the union all.
		appendMakeUnionAllChildrenTranceStep(ds, usedDefinition, children[0], children, opt)
		return children[0], nil
	}
	unionAll := LogicalPartitionUnionAll{}.Init(ds.SCtx(), ds.blockOffset)
	unionAll.SetChildren(children...)
	unionAll.SetSchema(ds.schema.Clone())
	appendMakeUnionAllChildrenTranceStep(ds, usedDefinition, unionAll, children, opt)
	return unionAll, nil
}

func (s *partitionProcessor) pruneRangeColumnsPartition(ctx sessionctx.Context, conds []expression.Expression, pi *model.PartitionInfo, pe *tables.PartitionExpr, columns []*expression.Column, names types.NameSlice) (partitionRangeOR, error) {
	result := fullRange(len(pi.Definitions))

	if len(pi.Columns) != 1 {
		return result, nil
	}

	pruner, err := makeRangeColumnPruner(columns, names, pi, pe.ForRangeColumnsPruning)
	if err == nil {
		result = partitionRangeForCNFExpr(ctx, conds, pruner, result)
	}
	return result, nil
}

var _ partitionRangePruner = &rangeColumnsPruner{}

// rangeColumnsPruner is used by 'partition by range columns'.
type rangeColumnsPruner struct {
	data     []expression.Expression
	partCol  *expression.Column
	maxvalue bool
}

func makeRangeColumnPruner(columns []*expression.Column, names types.NameSlice, pi *model.PartitionInfo, from *tables.ForRangeColumnsPruning) (*rangeColumnsPruner, error) {
	schema := expression.NewSchema(columns...)
	idx := expression.FindFieldNameIdxByColName(names, pi.Columns[0].L)
	partCol := schema.Columns[idx]
	data := make([]expression.Expression, len(from.LessThan))
	for i := 0; i < len(from.LessThan); i++ {
		if from.LessThan[i] != nil {
			data[i] = from.LessThan[i].Clone()
		}
	}
	return &rangeColumnsPruner{data, partCol, from.MaxValue}, nil
}

func (p *rangeColumnsPruner) fullRange() partitionRangeOR {
	return fullRange(len(p.data))
}

func (p *rangeColumnsPruner) partitionRangeForExpr(sctx sessionctx.Context, expr expression.Expression) (int, int, bool) {
	op, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return 0, len(p.data), false
	}

	switch op.FuncName.L {
	case ast.EQ, ast.LT, ast.GT, ast.LE, ast.GE:
	case ast.IsNull:
		// isnull(col)
		if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && arg0.ID == p.partCol.ID {
			return 0, 1, true
		}
		return 0, len(p.data), false
	default:
		return 0, len(p.data), false
	}
	opName := op.FuncName.L

	var col *expression.Column
	var con *expression.Constant
	if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && arg0.ID == p.partCol.ID {
		if arg1, ok := op.GetArgs()[1].(*expression.Constant); ok {
			col, con = arg0, arg1
		}
	} else if arg0, ok := op.GetArgs()[1].(*expression.Column); ok && arg0.ID == p.partCol.ID {
		if arg1, ok := op.GetArgs()[0].(*expression.Constant); ok {
			opName = opposite(opName)
			col, con = arg0, arg1
		}
	}
	if col == nil || con == nil {
		return 0, len(p.data), false
	}

	// If different collation, we can only prune if:
	// - expression is binary collation (can only be found in one partition)
	// - EQ operator, consider values 'a','b','ä' where 'ä' would be in the same partition as 'a' if general_ci, but is binary after 'b'
	// otherwise return all partitions / no pruning
	_, exprColl := expr.CharsetAndCollation()
	colColl := p.partCol.RetType.GetCollate()
	if exprColl != colColl && (opName != ast.EQ || !collate.IsBinCollation(exprColl)) {
		return 0, len(p.data), true
	}
	start, end := p.pruneUseBinarySearch(sctx, opName, con)
	return start, end, true
}

func (p *rangeColumnsPruner) pruneUseBinarySearch(sctx sessionctx.Context, op string, data *expression.Constant) (start int, end int) {
	var err error
	var isNull bool
	charSet, collation := p.partCol.RetType.GetCharset(), p.partCol.RetType.GetCollate()
	compare := func(ith int, op string, v *expression.Constant) bool {
		if ith == len(p.data)-1 {
			if p.maxvalue {
				return true
			}
		}
		var expr expression.Expression
		expr, err = expression.NewFunctionBase(sctx, op, types.NewFieldType(mysql.TypeLonglong), p.data[ith], v)
		expr.SetCharsetAndCollation(charSet, collation)
		var val int64
		val, isNull, err = expr.EvalInt(sctx, chunk.Row{})
		return val > 0
	}

	length := len(p.data)
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

	// Something goes wrong, abort this prunning.
	if err != nil || isNull {
		return 0, len(p.data)
	}

	if end > length {
		end = length
	}
	return start, end
}

func appendMakeUnionAllChildrenTranceStep(ds *DataSource, usedMap map[int64]model.PartitionDefinition, plan LogicalPlan, children []LogicalPlan, opt *logicalOptimizeOp) {
	if len(children) == 0 {
		appendNoPartitionChildTraceStep(ds, plan, opt)
		return
	}
	var action, reason func() string
	used := make([]model.PartitionDefinition, 0, len(usedMap))
	for _, def := range usedMap {
		used = append(used, def)
	}
	sort.Slice(used, func(i, j int) bool {
		return used[i].ID < used[j].ID
	})
	if len(children) == 1 {
		action = func() string {
			return fmt.Sprintf("%v_%v becomes %s_%v", ds.TP(), ds.ID(), plan.TP(), plan.ID())
		}
		reason = func() string {
			return fmt.Sprintf("%v_%v has one needed partition[%s] after pruning", ds.TP(), ds.ID(), used[0].Name)
		}
	} else {
		action = func() string {
			buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v becomes %s_%v with children[", ds.TP(), ds.ID(), plan.TP(), plan.ID()))
			for i, child := range children {
				if i > 0 {
					buffer.WriteString(",")
				}
				buffer.WriteString(fmt.Sprintf("%s_%v", child.TP(), child.ID()))
			}
			buffer.WriteString("]")
			return buffer.String()
		}
		reason = func() string {
			buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v has multiple needed partitions[", ds.TP(), ds.ID()))
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
	opt.appendStepToCurrent(ds.ID(), ds.TP(), reason, action)
}

func appendNoPartitionChildTraceStep(ds *DataSource, dual LogicalPlan, opt *logicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v becomes %v_%v", ds.TP(), ds.ID(), dual.TP(), dual.ID())
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v doesn't have needed partition table after pruning", ds.TP(), ds.ID())
	}
	opt.appendStepToCurrent(dual.ID(), dual.TP(), reason, action)
}
