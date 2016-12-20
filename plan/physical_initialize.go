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
	"github.com/pingcap/tidb/context"
)

type physicalInitializer struct {
	ctx       context.Context
	allocator *idAllocator
}

func (ps *physicalInitializer) initialize(p PhysicalPlan) {
	for _, child := range p.GetChildren() {
		ps.initialize(child.(PhysicalPlan))
	}
	p.SetCorrelated()
}

// addCachePlan will add a Cache plan above the plan whose father's IsCorrelated() is true but its own IsCorrelated() is false.
func addCachePlan(p PhysicalPlan, allocator *idAllocator) {
	if len(p.GetChildren()) == 0 {
		return
	}
	newChildren := make([]Plan, 0, len(p.GetChildren()))
	for _, child := range p.GetChildren() {
		addCachePlan(child.(PhysicalPlan), allocator)
		if p.IsCorrelated() && !child.IsCorrelated() {
			newChild := &Cache{}
			newChild.tp = "Cache"
			newChild.allocator = allocator
			newChild.initIDAndContext(p.context())
			newChild.SetSchema(child.GetSchema())

			addChild(newChild, child)
			newChild.SetParents(p)

			newChildren = append(newChildren, newChild)
		} else {
			newChildren = append(newChildren, child)
		}
	}
	p.SetChildren(newChildren...)
}
