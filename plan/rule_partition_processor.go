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
	log "github.com/sirupsen/logrus"
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

func (s *partitionProcessor) optimize(lp LogicalPlan) (LogicalPlan, error) {
	// NOTE: partitionProcessor will assume all filter conditions are pushed down to
	// DataSource, there will not be a Selection->DataSource case, so the rewrite just
	// handle the DataSource node.
	return s.rewriteDataSource(lp)
}

func (s *partitionProcessor) rewriteDataSource(lp LogicalPlan) (LogicalPlan, error) {
	// Assert there will not be sel -> sel in the ast.
	switch lp.(type) {
	case *DataSource:
		return s.prune(lp.(*DataSource))
	default:
		children := lp.Children()
		for i, child := range children {
			newChild, err := s.rewriteDataSource(child)
			if err != nil {
				return nil, errors.Trace(err)
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

func (s *partitionProcessor) prune(ds *DataSource) (LogicalPlan, error) {
	pi := ds.tableInfo.GetPartitionInfo()
	if pi == nil {
		return ds, nil
	}

	var partitionExprs []expression.Expression
	if table, ok := ds.table.(partitionTable); ok {
		partitionExprs = table.PartitionExpr().Ranges
	}
	if len(partitionExprs) == 0 {
		return nil, errors.New("partition expression missing")
	}

	// Rewrite data source to union all partitions, during which we may prune some
	// partitions according to the filter conditions pushed to the DataSource.
	children := make([]LogicalPlan, 0, len(pi.Definitions))

	col := partitionExprAccessColumn(partitionExprs[0])
	for i, expr := range partitionExprs {
		if col != nil {
			// If the selection condition would never be satisified, prune that partition.
			prune, err := s.canBePrune(ds.context(), col, expr, ds.pushedDownConds)
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
		newDataSource.physicalTableID = pi.Definitions[i].ID
		// There are many expression nodes in the plan tree use the original datasource
		// id as FromID. So we set the id of the newDataSource with the original one to
		// avoid traversing the whole plan tree to update the references.
		newDataSource.id = ds.id
		newDataSource.statisticTable = getStatsTable(ds.context(), ds.table.Meta(), pi.Definitions[i].ID)
		children = append(children, &newDataSource)
	}
	if len(children) == 0 {
		// No result after table pruning.
		tableDual := LogicalTableDual{RowCount: 0}.init(ds.context())
		tableDual.schema = ds.Schema()
		return tableDual, nil
	}
	if len(children) == 1 {
		// No need for the union all.
		return children[0], nil
	}
	unionAll := LogicalUnionAll{}.init(ds.context())
	unionAll.SetChildren(children...)
	return unionAll, nil
}

// canBePrune checks if partition expression will never meets the selection condition.
// For example, partition by column a > 3, and select condition is a < 3, then canBePrune returns true.
func (s *partitionProcessor) canBePrune(ctx sessionctx.Context, col *expression.Column, partitionCond expression.Expression, copConds []expression.Expression) (bool, error) {
	conds := make([]expression.Expression, 0, 1+len(copConds))
	conds = append(conds, partitionCond)
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

// partitionExprAccessColumn extracts the column which is used by the partition expression.
// If the partition expression is not a simple operation on one column, return nil.
func partitionExprAccessColumn(expr expression.Expression) *expression.Column {
	lt, ok := expr.(*expression.ScalarFunction)
	if !ok || lt.FuncName.L != ast.LT {
		// The partition expression is constructed by us, its format should always be
		// expr < value, such as "id < 42", "timestamp(id) < maxvalue"
		log.Warnf("illegal partition expr:%s", expr.String())
		return nil
	}
	tmp := lt.GetArgs()[0]
	col, ok := tmp.(*expression.Column)
	if !ok {
		return nil
	}
	return col
}
