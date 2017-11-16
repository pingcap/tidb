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
	"github.com/pingcap/tidb/context"
)

// aggEliminater tries to elimiate max/min aggregate function.
// For SQL like `select max(c) from t;`, we could optimize it to `select c from t order by c desc limit 1;`.
// For SQL like `select min(c) from t;`, we could optimize it to `select c from t order by c limit 1;`.
type aggEliminater struct {
	ctx context.Context
}

func (a *aggEliminater) optimize(p LogicalPlan, ctx context.Context) (LogicalPlan, error) {
	a.ctx = ctx
	return a.eliminateAgg(p), nil
}

// Try to convert max/min to Limit+Sort operators.
func (a *aggEliminater) eliminateAgg(p LogicalPlan) LogicalPlan {
	if agg, ok := p.(*LogicalAggregation); ok {
		// We only consider case with single max/min function.
		if len(agg.AggFuncs) != 1 || len(agg.GroupByItems) != 0 {
			return p
		}
		f := agg.AggFuncs[0]
		if f.GetName() != ast.AggFuncMax && f.GetName() != ast.AggFuncMin {
			return p
		}

		// Add Sort and Limit operators.
		// For max function, the sort order should be desc.
		desc := f.GetName() == ast.AggFuncMax
		// Compose Sort operator.
		sort := Sort{}.init(a.ctx)
		sort.ByItems = append(sort.ByItems, &ByItems{f.GetArgs()[0], desc})
		sort.SetSchema(p.Children()[0].Schema().Clone())
		setParentAndChildren(sort, p.Children()...)
		// Compose Limit operator.
		li := Limit{Count: 1}.init(a.ctx)
		li.SetSchema(sort.Schema().Clone())
		setParentAndChildren(li, sort)

		// Add a projection operator here.
		// During topn_push_down, the sort/limit operator will be converted to a topn operator and the schema of sort/limit will be ignored.
		// So the schema of the LogicalAggregation will be lost. We add this projection operator to keep the schema unlost.
		// For SQL like `select * from t where v=(select min(t1.v) from t t1, t t2, t t3 where t1.id=t2.id and t2.id=t3.id and t1.id=t.id);`,
		// the min(t1.v) will be refered in the outer selection. So we should keep the schema from LogicalAggragation.
		proj := Projection{}.init(a.ctx)
		proj.Exprs = append(proj.Exprs, f.GetArgs()[0])
		proj.SetSchema(p.Schema().Clone())
		setParentAndChildren(proj, li)
		return proj
	}

	newChildren := make([]Plan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild := a.eliminateAgg(child.(LogicalPlan))
		newChildren = append(newChildren, newChild)
	}
	setParentAndChildren(p, newChildren...)
	return p
}
