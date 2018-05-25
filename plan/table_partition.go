// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
)

// tablePartition rewrites the ast for table partition.
//
// create table t (id int) partition by range (id)
//   (partition
//	p1 values less than (10),
//	p2 values less than (20),
//	p3 values less than (30)
//   )
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
	return rewriteDataSource(nil, lp)
}

func rewriteDataSource(sel *LogicalSelection, lp LogicalPlan) (LogicalPlan, error) {
	// Assert there will not be sel -> sel in the ast.
	switch lp.(type) {
	case *DataSource:
		return prunePartition(sel, lp.(*DataSource))
	case *LogicalSelection:
		return rewriteDataSource(lp.(*LogicalSelection), lp.Children()[0])
	default:
		children := lp.Children()
		for i, child := range children {
			child1, err := rewriteDataSource(nil, child)
			if err != nil {
				return nil, errors.Trace(err)
			}
			children[i] = child1
		}
	}

	return selectOnSomething(sel, lp)
}

func selectOnSomething(sel *LogicalSelection, lp LogicalPlan) (LogicalPlan, error) {
	if sel != nil {
		sel.SetChildren(lp)
		return sel, nil
	}
	return lp, nil
}

func prunePartition(sel *LogicalSelection, ds *DataSource) (LogicalPlan, error) {
	pi := ds.tableInfo.Partition
	if pi == nil || !pi.Enable {
		return selectOnSomething(sel, ds)
	}

	fmt.Println("prune partition....fuck!!", ds)

	// Rewrite data source to union all partitions, during which we may prune some
	// partitions according to selection condition.
	children := make([]LogicalPlan, 0, len(pi.Definitions))
	var selConds []expression.Expression
	if sel != nil {
		selConds = sel.Conditions
	}
	var buf bytes.Buffer
	for i, def := range pi.Definitions {
		buf.Reset()
		fmt.Fprintf(&buf, "%s < %s", pi.Expr, def.LessThan[0])
		expr, err := expression.ParseSimpleExpr(ds.context(), buf.String(), ds.tableInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// If the selection condition would never satisify, prune that partition.
		prune, err := canBePrune(ds.context(), expr, selConds, ds.pushedDownConds)
		fmt.Println(i, "prune = ", prune, err)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if prune {
			continue
		}

		// Not a deep copy.
		newDataSource := *ds
		newDataSource.baseLogicalPlan = newBaseLogicalPlan(ds.context(), TypeTableScan, &newDataSource)
		newDataSource.isPartition = true
		newDataSource.partitionID = def.ID
		children = append(children, &newDataSource)
	}
	fmt.Println("len children = ", len(children), children)
	unionAll := LogicalUnionAll{}.init(ds.context())
	unionAll.SetChildren(children...)
	return selectOnSomething(sel, unionAll)
}

// canBePrune checks if partition expression will never meets the selection condition.
// For example, partition by column a > 3, and select condition is a < 3, then
// canBePrune would be true.
func canBePrune(ctx sessionctx.Context, expr expression.Expression, rootConds, copConds []expression.Expression) (bool, error) {
	conds := make([]expression.Expression, 0, 1+len(rootConds)+len(copConds))
	conds = append(conds, expr)
	conds = append(conds, rootConds...)
	conds = append(conds, copConds...)
	res := expression.PropagateConstant(ctx, conds)
	if len(res) == 1 {
		if x, ok := res[0].(*expression.Constant); ok {
			v, _, err := x.EvalInt(ctx, nil)
			if err != nil {
				return false, errors.Trace(err)
			}
			if v == 0 {
				return true, nil
			}
		}
	}
	return false, nil
}
