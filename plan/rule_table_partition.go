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

package plan

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/ranger"
)

// tablePartition rewrites the ast for table partition.
//
// create table t (id int) partition by range (id)
//   (partition
//      p1 values less than (10),
//      p2 values less than (20),
//      p3 values less than (30))
//
// select * from t is equal to
// select * from (union all
//      select * from p1 where id < 10
//      select * from p2 where id < 20
//      select * from p3 where id < 30)
//
// tablePartition is here because it's easier to prune partition after predicate push down.
type tablePartition struct{}

func (s *tablePartition) optimize(lp LogicalPlan) (LogicalPlan, error) {
	return s.rewriteDataSource(nil, lp)
}

func (s *tablePartition) rewriteDataSource(sel *LogicalSelection, lp LogicalPlan) (LogicalPlan, error) {
	// Assert there will not be sel -> sel in the ast.
	switch lp.(type) {
	case *DataSource:
		return s.prunePartition(sel, lp.(*DataSource))
	case *LogicalSelection:
		return s.rewriteDataSource(lp.(*LogicalSelection), lp.Children()[0])
	default:
		children := lp.Children()
		for i, child := range children {
			child1, err := s.rewriteDataSource(nil, child)
			if err != nil {
				return nil, errors.Trace(err)
			}
			children[i] = child1
		}
	}

	return s.selectOnSomething(sel, lp)
}

func (s *tablePartition) selectOnSomething(sel *LogicalSelection, lp LogicalPlan) (LogicalPlan, error) {
	if sel != nil {
		sel.SetChildren(lp)
		return sel, nil
	}
	return lp, nil
}

// partitionTable is for those tables which implement partition.
type partitionTable interface {
	PartitionExprCache() *tables.PartitionExprCache
}

func (s *tablePartition) prunePartition(sel *LogicalSelection, ds *DataSource) (LogicalPlan, error) {
	pi := ds.tableInfo.GetPartitionInfo()
	if pi == nil {
		return s.selectOnSomething(sel, ds)
	}

	var partitionExprs []expression.Expression
	if itf, ok := ds.table.(partitionTable); ok {
		partitionExprs = itf.PartitionExprCache().PartitionPrune
	}
	if len(partitionExprs) == 0 {
		return nil, errors.New("partition expression missing")
	}

	// Rewrite data source to union all partitions, during which we may prune some
	// partitions according to selection condition.
	children := make([]LogicalPlan, 0, len(pi.Definitions))
	var selConds []expression.Expression
	if sel != nil {
		selConds = sel.Conditions
	}

	col := partitionExprAccessColumn(partitionExprs[0])
	for i, expr := range partitionExprs {
		if col != nil {
			// If the selection condition would never be satisified, prune that partition.
			prune, err := s.canBePrune(ds.context(), col, expr, selConds, ds.pushedDownConds)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if prune {
				continue
			}
		}

		// Not a deep copy.
		newDataSource := *ds
		newDataSource.baseLogicalPlan = newBaseLogicalPlan(ds.context(), TypeTableScan, &newDataSource)
		newDataSource.isPartition = true
		newDataSource.partitionID = pi.Definitions[i].ID
		children = append(children, &newDataSource)
	}
	if len(children) == 1 {
		// No need for the union all.
		return s.selectOnSomething(sel, children[0])
	}
	unionAll := LogicalUnionAll{}.init(ds.context())
	unionAll.SetChildren(children...)
	return s.selectOnSomething(sel, unionAll)
}

// canBePrune checks if partition expression will never meets the selection condition.
// For example, partition by column a > 3, and select condition is a < 3, then canBePrune returns true.
func (s *tablePartition) canBePrune(ctx sessionctx.Context, col *expression.Column, partitionCond expression.Expression, rootConds, copConds []expression.Expression) (bool, error) {
	conds := make([]expression.Expression, 0, 1+len(rootConds)+len(copConds))
	conds = append(conds, partitionCond)
	conds = append(conds, rootConds...)
	conds = append(conds, copConds...)
	conds = expression.PropagateConstant(ctx, conds)

	// Calculate the column range to prune.
	accessConds := ranger.ExtractAccessConditionsForColumn(conds, col.ColName)
	r, err := ranger.BuildColumnRange(accessConds, ctx.GetSessionVars().StmtCtx, col.RetType)
	if err != nil {
		return false, errors.Trace(err)
	}
	return len(r) == 0, nil
}

// partitionExprAccessColumn extracts the column visited from the partition expression.
// If the partition expression is not a simple operation on one column, return nil.
func partitionExprAccessColumn(expr expression.Expression) *expression.Column {
	lt, ok := expr.(*expression.ScalarFunction)
	if !ok || lt.FuncName.L != ast.LT {
		return nil
	}
	tmp := lt.GetArgs()[0]
	col, ok := tmp.(*expression.Column)
	if !ok {
		return nil
	}
	return col
}
