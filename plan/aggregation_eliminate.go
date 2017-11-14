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

type aggEliminater struct {
	allocator *idAllocator
	ctx       context.Context
}

func (a *aggEliminater) optimize(p LogicalPlan, ctx context.Context, alloc *idAllocator) (LogicalPlan, error) {
	a.ctx = ctx
	a.allocator = alloc
	return a.eliminateAgg(p), nil
}

func (a *aggEliminater) eliminateAgg(p LogicalPlan) LogicalPlan {
	if agg, ok := p.(*LogicalAggregation); ok {
		if len(agg.AggFuncs) != 1 {
			return p
		}
		desc := false
		f := agg.AggFuncs[0]
		if f.GetName() == ast.AggFuncMax {
			desc = true
		}
		// Add Sort and Limit operators.
		// Compose Sort operator.
		sort := Sort{}.init(a.allocator, a.ctx)
		sort.ByItems = append(sort.ByItems, &ByItems{f.GetArgs()[0], desc})
		sort.SetSchema(p.Schema().Clone())
		sort.SetChildren(p.Children()...)
		// Compose Limit operator.
		li := Limit{Count: 1}.init(a.allocator, a.ctx)
		li.SetSchema(p.Schema().Clone())
		setParentAndChildren(li, sort)

		// Add a prjection operator here.
		proj := Projection{}.init(a.allocator, a.ctx)
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
