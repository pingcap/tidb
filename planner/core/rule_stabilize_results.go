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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/util"
)

/*
	resultsStabilizer stabilizes query results.
	Results of some queries are not stable, for example:
		create table t (a int);
		insert into t values (1), (2);
		select a from t;
	In the case above, the result can be `1 2` or `2 1`, which is not stable.
	This rule stabilizes query results by modifying Sort in the plan or inject new Sort into the plan

	First, all operators are divided into 2 types:
	1. input-order keepers: Selection, Projection, Limit;
	2. all other operators.

	The basic idea of this rule is:
	1. iterate the plan from the root, and ignore all input-order keepers;
	2. when meeting the first non-input-order keeper,
		2.1. if it's a Sort, complete it by appending its output columns into its order-by list,
		2.2. otherwise, inject a new Sort upon this operator.
*/
type resultsStabilizer struct {
}

func (rs *resultsStabilizer) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	stable := rs.completeSort(lp)
	if !stable {
		lp = rs.injectSort(lp)
	}
	return lp, nil
}

func (rs *resultsStabilizer) completeSort(lp LogicalPlan) bool {
	if rs.isInputOrderKeeper(lp) {
		return rs.completeSort(lp.Children()[0])
	} else if sort, ok := lp.(*LogicalSort); ok {
		cols := sort.Schema().Columns // sort results by all output columns
		if handleCol := rs.extractHandleCol(sort.Children()[0]); handleCol != nil {
			cols = []*expression.Column{handleCol} // sort results by the handle column if we can get it
		}
		for _, col := range cols {
			exist := false
			for _, byItem := range sort.ByItems {
				if col.Equal(nil, byItem.Expr) {
					exist = true
					break
				}
			}
			if !exist {
				sort.ByItems = append(sort.ByItems, &util.ByItems{Expr: col})
			}
		}
		return true
	}
	return false
}

func (rs *resultsStabilizer) injectSort(lp LogicalPlan) LogicalPlan {
	if rs.isInputOrderKeeper(lp) {
		lp.SetChildren(rs.injectSort(lp.Children()[0]))
		return lp
	}

	byItems := make([]*util.ByItems, 0, len(lp.Schema().Columns))
	cols := lp.Schema().Columns
	if handleCol := rs.extractHandleCol(lp); handleCol != nil {
		cols = []*expression.Column{handleCol}
	}
	for _, col := range cols {
		byItems = append(byItems, &util.ByItems{Expr: col})
	}
	sort := LogicalSort{
		ByItems: byItems,
	}.Init(lp.SCtx(), lp.SelectBlockOffset())
	sort.SetChildren(lp)
	return sort
}

func (rs *resultsStabilizer) isInputOrderKeeper(lp LogicalPlan) bool {
	switch lp.(type) {
	case *LogicalSelection, *LogicalProjection, *LogicalLimit:
		return true
	}
	return false
}

// extractHandleCols does the best effort to get the handle column.
func (rs *resultsStabilizer) extractHandleCol(lp LogicalPlan) *expression.Column {
	switch x := lp.(type) {
	case *LogicalSelection, *LogicalLimit:
		return rs.extractHandleCol(lp.Children()[0])
	case *DataSource:
		handleCol := x.getPKIsHandleCol()
		if handleCol != nil {
			return handleCol
		}
	}
	return nil
}

func (rs *resultsStabilizer) name() string {
	return "stabilize_results"
}
