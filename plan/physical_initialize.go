// Copyright 2016 PingCAP, Inc.
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

package plan

import (
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
)

type physicalInitializer struct {
	ctx       context.Context
	allocator *idAllocator
}

func (ps *physicalInitializer) initialize(p PhysicalPlan) {
	for _, child := range p.GetChildren() {
		ps.initialize(child.(PhysicalPlan))

		// init the attribute that will be affected by its children.
		p.SetCorrelated(p.IsCorrelated() || child.IsCorrelated())
		log.Warnf("%s %v\n", child.GetID(), child.IsCorrelated())
	}
	switch pp := p.(type) {
	case *PhysicalAggregation:
		pp.allocator = ps.allocator
		pp.initIDAndContext(ps.ctx)
		for _, item := range pp.GroupByItems {
			pp.correlated = pp.correlated || item.IsCorrelated()
		}
		for _, aggFunc := range pp.AggFuncs {
			for _, arg := range aggFunc.GetArgs() {
				pp.correlated = pp.correlated || arg.IsCorrelated()
			}
		}
	case *PhysicalApply:
		corColumns := p.GetChildren()[1].extractCorrelatedCols()
		pp.correlated = pp.GetChildren()[0].IsCorrelated()
		for _, corCol := range corColumns {
			if idx := pp.GetChildren()[0].GetSchema().GetIndex(&corCol.Column); idx == -1 {
				pp.correlated = true
				break
			}
		}
	}
}

// addCachePlan will add a Cache plan above the plan whose father's IsCorrelated() is true but its own IsCorrelated() is false.
func addCachePlan(p PhysicalPlan, allocator *idAllocator) PhysicalPlan {
	if len(p.GetChildren()) == 0 {
		return p
	}
	np := p
	newChildren := make([]Plan, 0, len(np.GetChildren()))
	for _, child := range p.GetChildren() {
		if !p.IsCorrelated() || child.IsCorrelated() {
			newChildren = append(newChildren, addCachePlan(child.(PhysicalPlan), allocator))
		} else {
			newChild := &Cache{}
			newChild.tp = "Cache"
			newChild.allocator = allocator
			newChild.initIDAndContext(np.context())
			addChild(newChild, child)
			newChild.SetSchema(child.GetSchema())
			newChild.SetParents(np)
			newChildren = append(newChildren, newChild)
			addCachePlan(child.(PhysicalPlan), allocator)
		}
	}
	np.SetChildren(newChildren...)
	return np
}
