// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package physicalop

import (
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// PlanShape summarizes the properties of a physical plan that the alternative
// engine-restricted logical plan rounds arm on: which storage engines the plan
// reads from, and whether it contains a join or aggregation.
type PlanShape struct {
	// ReadsFromTiKV is true when some access path in the plan reads from TiKV.
	ReadsFromTiKV bool
	// ReadsFromTiFlash is true when some access path in the plan reads from TiFlash.
	ReadsFromTiFlash bool
	// HasJoinOrAgg is true when the plan contains a join or an aggregation,
	// including ones pushed down into a cop or MPP task.
	HasJoinOrAgg bool
}

// InspectPlanShape walks a physical plan tree and reports its PlanShape.
//
// The walk descends through storage readers into their pushed-down subtrees
// (TablePlan / IndexPlan / partial plans), because an aggregation or an MPP
// join frequently sits inside a cop or MPP task rather than above the reader.
func InspectPlanShape(plan base.PhysicalPlan) PlanShape {
	var shape PlanShape
	shape.inspect(plan)
	return shape
}

func (s *PlanShape) inspect(plan base.PhysicalPlan) {
	if plan == nil {
		return
	}
	if isJoinOrAgg(plan) {
		s.HasJoinOrAgg = true
	}
	switch x := plan.(type) {
	case *PhysicalTableReader:
		// A table reader is the only reader that can read from TiFlash; every
		// other reader below is an index access path, which TiFlash lacks.
		if x.StoreType == kv.TiFlash {
			s.ReadsFromTiFlash = true
		} else {
			s.ReadsFromTiKV = true
		}
		s.inspect(x.TablePlan)
		return
	case *PhysicalIndexReader:
		s.ReadsFromTiKV = true
		s.inspect(x.IndexPlan)
		return
	case *PhysicalIndexLookUpReader:
		s.ReadsFromTiKV = true
		s.inspect(x.IndexPlan)
		s.inspect(x.TablePlan)
		return
	case *PhysicalIndexMergeReader:
		s.ReadsFromTiKV = true
		for _, partial := range x.PartialPlansRaw {
			s.inspect(partial)
		}
		s.inspect(x.TablePlan)
		return
	case *PointGetPlan, *BatchPointGetPlan:
		s.ReadsFromTiKV = true
		return
	case *PhysicalCTE:
		s.inspect(x.SeedPlan)
		s.inspect(x.RecurPlan)
	}
	for _, child := range plan.Children() {
		s.inspect(child)
	}
}

// isJoinOrAgg reports whether the operator is a join or an aggregation.
// PhysicalApply is checked separately: it overrides PhysicalJoinImplement with
// a different signature and so deliberately does not satisfy base.PhysicalJoin.
func isJoinOrAgg(plan base.PhysicalPlan) bool {
	switch plan.(type) {
	case *PhysicalHashAgg, *PhysicalStreamAgg, *PhysicalApply:
		return true
	}
	_, isJoin := plan.(base.PhysicalJoin)
	return isJoin
}
