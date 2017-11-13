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
	"fmt"

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
	a.eliminateAgg(p)
	return p, nil
}

func (a *aggEliminater) eliminateAgg(p LogicalPlan) LogicalPlan {
	if agg, ok := p.(*LogicalAggregation); ok {
		desc := false
		f := agg.AggFuncs[0]
		if f.GetName() == ast.AggFuncMax {
			desc = true
		}
		// Add a TopN operator.
		topn := TopN{Count: 1}.init(a.allocator, a.ctx)
		topn.ByItems = append(topn.ByItems, &ByItems{f.GetArgs()[0], desc})

		topn.SetSchema(p.Schema().Clone())
		topn.setChild(p.Children()[0].(LogicalPlan), false)
		return topn
	}

	newChildren := make([]Plan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild := a.eliminateAgg(child.(LogicalPlan))
		newChildren = append(newChildren, newChild)
	}
	setParentAndChildren(p, newChildren...)
	return p
}
