// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

// maxMinEliminator tries to eliminate max/min aggregate function.
// For SQL like `select max(id) from t;`, we could optimize it to `select max(id) from (select id from t order by id desc limit 1 where id is not null) t;`.
// For SQL like `select min(id) from t;`, we could optimize it to `select max(id) from (select id from t order by id limit 1 where id is not null) t;`.
type maxMinEliminator struct {
}

func (a *maxMinEliminator) optimize(p LogicalPlan) (LogicalPlan, error) {
	a.eliminateMaxMin(p)
	return p, nil
}

// Try to convert max/min to Limit+Sort operators.
func (a *maxMinEliminator) eliminateMaxMin(p LogicalPlan) {
	// We don't need to guarantee that the child of it is a data source. This transformation won't be worse than previous.
	if agg, ok := p.(*LogicalAggregation); ok {
		// We only consider case with single max/min function.
		if len(agg.AggFuncs) != 1 || len(agg.GroupByItems) != 0 {
			return
		}
		f := agg.AggFuncs[0]
		if f.Name != ast.AggFuncMax && f.Name != ast.AggFuncMin {
			return
		}

		child := p.Children()[0]
		ctx := p.context()

		// If there's no column in f.GetArgs()[0], we still need limit and read data from real table because the result should NULL if the below is empty.
		if len(expression.ExtractColumns(f.Args[0])) > 0 {
			// If it can be NULL, we need to filter NULL out first.
			if !mysql.HasNotNullFlag(f.Args[0].GetType().Flag) {
				sel := LogicalSelection{}.init(ctx)
				isNullFunc := expression.NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), f.Args[0])
				notNullFunc := expression.NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), isNullFunc)
				sel.Conditions = []expression.Expression{notNullFunc}
				sel.SetChildren(p.Children()[0])
				child = sel
			}

			// Add Sort and Limit operators.
			// For max function, the sort order should be desc.
			desc := f.Name == ast.AggFuncMax
			// Compose Sort operator.
			sort := LogicalSort{}.init(ctx)
			sort.ByItems = append(sort.ByItems, &ByItems{f.Args[0], desc})
			sort.SetChildren(child)
			child = sort
		}

		// Compose Limit operator.
		li := LogicalLimit{Count: 1}.init(ctx)
		li.SetChildren(child)

		// If no data in the child, we need to return NULL instead of empty. This cannot be done by sort and limit themselves.
		// Since now it's almost one row returned, a agg operator is okay to do this.
		p.SetChildren(li)
		return
	}

	for _, child := range p.Children() {
		a.eliminateMaxMin(child)
	}
}
