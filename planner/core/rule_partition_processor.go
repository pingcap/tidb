// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"errors"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/math"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/ranger"
)

// partitionProcessor rewrites the ast for table partition.
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

func (s *partitionProcessor) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	return s.rewriteDataSource(lp)
}

func (s *partitionProcessor) rewriteDataSource(lp LogicalPlan) (LogicalPlan, error) {
	// Assert there will not be sel -> sel in the ast.
	switch p := lp.(type) {
	case *DataSource:
		return s.prune(p)
	case *LogicalUnionScan:
		ds := p.Children()[0]
		ds, err := s.prune(ds.(*DataSource))
		if err != nil {
			return nil, err
		}
		if ua, ok := ds.(*LogicalUnionAll); ok {
			// Adjust the UnionScan->Union->DataSource1, DataSource2 ... to
			// Union->(UnionScan->DataSource1), (UnionScan->DataSource2)
			children := make([]LogicalPlan, 0, len(ua.Children()))
			for _, child := range ua.Children() {
				us := LogicalUnionScan{
					conditions: p.conditions,
					handleCol:  p.handleCol,
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
			newChild, err := s.rewriteDataSource(child)
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
	PartitionExpr(ctx sessionctx.Context, columns []*expression.Column, names types.NameSlice) (*tables.PartitionExpr, error)
}

func generateHashPartitionExpr(t table.Table, ctx sessionctx.Context, columns []*expression.Column, names types.NameSlice) (*tables.PartitionExpr, error) {
	tblInfo := t.Meta()
	pi := tblInfo.Partition
	var column *expression.Column
	schema := expression.NewSchema(columns...)
	exprs, err := expression.ParseSimpleExprsWithNames(ctx, pi.Expr, schema, names)
	if err != nil {
		return nil, err
	}
	exprs[0].HashCode(ctx.GetSessionVars().StmtCtx)
	if col, ok := exprs[0].(*expression.Column); ok {
		column = col
	}
	return &tables.PartitionExpr{
		Column: column,
		Expr:   exprs[0],
		Ranges: nil,
	}, nil
}

func (s *partitionProcessor) pruneHashPartition(ds *DataSource, pi *model.PartitionInfo) (LogicalPlan, error) {
	pExpr, err := generateHashPartitionExpr(ds.table, ds.ctx, ds.TblCols, ds.names)
	if err != nil {
		return nil, err
	}
	pe := pExpr.Expr
	filterConds := ds.allConds
	val, ok, hasConflict := expression.FastLocateHashPartition(ds.SCtx(), filterConds, pe)
	if hasConflict {
		// For condition like `a = 1 and a = 5`, return TableDual directly.
		tableDual := LogicalTableDual{RowCount: 0}.Init(ds.SCtx(), ds.blockOffset)
		tableDual.schema = ds.Schema()
		return tableDual, nil
	}
	if ok {
		idx := math.Abs(val) % int64(pi.Num)
		newDataSource := *ds
		newDataSource.baseLogicalPlan = newBaseLogicalPlan(ds.SCtx(), plancodec.TypeTableScan, &newDataSource, ds.blockOffset)
		newDataSource.isPartition = true
		newDataSource.physicalTableID = pi.Definitions[idx].ID
		// There are many expression nodes in the plan tree use the original datasource
		// id as FromID. So we set the id of the newDataSource with the original one to
		// avoid traversing the whole plan tree to update the references.
		newDataSource.id = ds.id
		newDataSource.statisticTable = getStatsTable(ds.SCtx(), ds.table.Meta(), pi.Definitions[idx].ID)
		pl := &newDataSource
		return pl, nil
	}
	// If can not hit partition by FastLocateHashPartition, try to prune all partition.
	sctx := ds.SCtx()
	filterConds = expression.PropagateConstant(sctx, filterConds)
	filterConds = solver.Solve(sctx, filterConds)
	alwaysFalse := false
	if len(filterConds) == 1 {
		// Constant false.
		if con, ok := filterConds[0].(*expression.Constant); ok && con.DeferredExpr == nil && con.ParamMarker == nil {
			ret, _, err := expression.EvalBool(sctx, expression.CNFExprs{con}, chunk.Row{})
			if err == nil && !ret {
				alwaysFalse = true
			}
		}
	}
	if alwaysFalse {
		tableDual := LogicalTableDual{RowCount: 0}.Init(ds.SCtx(), ds.blockOffset)
		tableDual.schema = ds.Schema()
		return tableDual, nil
	}
	children := make([]LogicalPlan, 0, len(pi.Definitions))
	for i := 0; i < len(pi.Definitions); i++ {
		// This is for `table partition (p0,p1)` syntax, only union the specified partition if has specified partitions.
		if len(ds.partitionNames) != 0 {
			if !s.findByName(ds.partitionNames, pi.Definitions[i].Name.L) {
				continue
			}
		}
		// Not a deep copy.
		newDataSource := *ds
		newDataSource.baseLogicalPlan = newBaseLogicalPlan(ds.SCtx(), plancodec.TypeTableScan, &newDataSource, ds.blockOffset)
		newDataSource.isPartition = true
		newDataSource.physicalTableID = pi.Definitions[i].ID
		newDataSource.possibleAccessPaths = make([]*util.AccessPath, len(ds.possibleAccessPaths))
		for i := range ds.possibleAccessPaths {
			newPath := *ds.possibleAccessPaths[i]
			newDataSource.possibleAccessPaths[i] = &newPath
		}
		// There are many expression nodes in the plan tree use the original datasource
		// id as FromID. So we set the id of the newDataSource with the original one to
		// avoid traversing the whole plan tree to update the references.
		newDataSource.id = ds.id
		newDataSource.statisticTable = getStatsTable(ds.SCtx(), ds.table.Meta(), pi.Definitions[i].ID)
		children = append(children, &newDataSource)
	}
	unionAll := LogicalUnionAll{}.Init(ds.SCtx(), ds.blockOffset)
	unionAll.SetChildren(children...)
	unionAll.SetSchema(ds.schema)
	return unionAll, nil
}

func (s *partitionProcessor) prune(ds *DataSource) (LogicalPlan, error) {
	pi := ds.tableInfo.GetPartitionInfo()
	if pi == nil {
		return ds, nil
	}
	partitionDefs := ds.table.Meta().Partition.Definitions
	filterConds := ds.allConds

	// Try to locate partition directly for hash partition.
	if pi.Type == model.PartitionTypeHash {
		return s.pruneHashPartition(ds, pi)
	}

	var partitionExprs []expression.Expression
	var col *expression.Column
	if table, ok := ds.table.(partitionTable); ok {
		pExpr, err := table.PartitionExpr(ds.ctx, ds.TblCols, ds.names)
		if err != nil {
			return nil, err
		}
		partitionExprs = pExpr.Ranges
		col = pExpr.Column
	}
	if len(partitionExprs) == 0 {
		return nil, errors.New("partition expression missing")
	}

	// do preSolve with filter exprs for situations like
	// where c1 = 1 and c2 > c1 + 10 and c2 < c3 + 1 and c3 = c1 - 10
	// no need to do partition pruning work for "alwaysFalse" filter results
	if len(partitionExprs) > 3 {
		sctx := ds.SCtx()
		filterConds = expression.PropagateConstant(sctx, filterConds)
		filterConds = solver.Solve(sctx, filterConds)
		alwaysFalse := false
		if len(filterConds) == 1 {
			// Constant false.
			if con, ok := filterConds[0].(*expression.Constant); ok && con.DeferredExpr == nil && con.ParamMarker == nil {
				ret, _, err := expression.EvalBool(sctx, expression.CNFExprs{con}, chunk.Row{})
				if err == nil && !ret {
					alwaysFalse = true
				}
			}
		}
		if alwaysFalse {
			tableDual := LogicalTableDual{RowCount: 0}.Init(ds.SCtx(), ds.blockOffset)
			tableDual.schema = ds.Schema()
			return tableDual, nil
		}
	}

	// Rewrite data source to union all partitions, during which we may prune some
	// partitions according to the filter conditions pushed to the DataSource.
	children := make([]LogicalPlan, 0, len(pi.Definitions))
	for i, expr := range partitionExprs {
		// If the select condition would never be satisified, prune that partition.
		pruned, err := s.canBePruned(ds.SCtx(), col, expr, filterConds)
		if err != nil {
			return nil, err
		}
		if pruned {
			continue
		}
		// This is for `table partition (p0,p1)` syntax, only union the specified partition if has specified partitions.
		if len(ds.partitionNames) != 0 {
			if !s.findByName(ds.partitionNames, partitionDefs[i].Name.L) {
				continue
			}
		}

		// Not a deep copy.
		newDataSource := *ds
		newDataSource.baseLogicalPlan = newBaseLogicalPlan(ds.SCtx(), plancodec.TypeTableScan, &newDataSource, ds.blockOffset)
		newDataSource.isPartition = true
		newDataSource.physicalTableID = pi.Definitions[i].ID
		newDataSource.possibleAccessPaths = make([]*util.AccessPath, len(ds.possibleAccessPaths))
		for i := range ds.possibleAccessPaths {
			newPath := *ds.possibleAccessPaths[i]
			newDataSource.possibleAccessPaths[i] = &newPath
		}
		// There are many expression nodes in the plan tree use the original datasource
		// id as FromID. So we set the id of the newDataSource with the original one to
		// avoid traversing the whole plan tree to update the references.
		newDataSource.id = ds.id
		newDataSource.statisticTable = getStatsTable(ds.SCtx(), ds.table.Meta(), pi.Definitions[i].ID)
		children = append(children, &newDataSource)
	}
	if len(children) == 0 {
		// No result after table pruning.
		tableDual := LogicalTableDual{RowCount: 0}.Init(ds.SCtx(), ds.blockOffset)
		tableDual.schema = ds.Schema()
		return tableDual, nil
	}
	if len(children) == 1 {
		// No need for the union all.
		return children[0], nil
	}
	unionAll := LogicalUnionAll{}.Init(ds.SCtx(), ds.blockOffset)
	unionAll.SetChildren(children...)
	unionAll.SetSchema(ds.schema)
	return unionAll, nil
}

var solver = expression.NewPartitionPruneSolver()

// canBePruned checks if partition expression will never meets the selection condition.
// For example, partition by column a > 3, and select condition is a < 3, then canBePrune returns true.
func (s *partitionProcessor) canBePruned(sctx sessionctx.Context, partCol *expression.Column, partExpr expression.Expression, filterExprs []expression.Expression) (bool, error) {
	conds := make([]expression.Expression, 0, 1+len(filterExprs))
	conds = append(conds, partExpr)
	conds = append(conds, filterExprs...)
	conds = expression.PropagateConstant(sctx, conds)
	conds = solver.Solve(sctx, conds)

	if len(conds) == 1 {
		// Constant false.
		if con, ok := conds[0].(*expression.Constant); ok && con.DeferredExpr == nil && con.ParamMarker == nil {
			ret, _, err := expression.EvalBool(sctx, expression.CNFExprs{con}, chunk.Row{})
			if err == nil && !ret {
				return true, nil
			}
		}
		// Not a constant false, but this is the only condition, it can't be pruned.
		return false, nil
	}

	// Calculates the column range to prune.
	if partCol == nil {
		// If partition column is nil, we can't calculate range, so we can't prune
		// partition by range.
		return false, nil
	}

	// TODO: Remove prune by calculating range. Current constraint propagate doesn't
	// handle the null condition, while calculate range can prune something like:
	// "select * from t where t is null"
	accessConds := ranger.ExtractAccessConditionsForColumn(conds, partCol.UniqueID)
	r, err := ranger.BuildColumnRange(accessConds, sctx.GetSessionVars().StmtCtx, partCol.RetType, types.UnspecifiedLength)
	if err != nil {
		return false, err
	}
	return len(r) == 0, nil
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
