// Copyright 2018 PingCAP, Inc.
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

package cascades

import (
	"math"

	plannercore "github.com/pingcap/tidb/planner/core"
)

// Implementation defines the interface for cost of physical plan.
type Implementation interface {
	calcCost(outCount float64, childCosts []float64, children ...*Group) float64
	setCost(cost float64)
	getCost() float64
	getPlan() plannercore.PhysicalPlan
}

type baseImplementation struct {
	cost float64
	plan plannercore.PhysicalPlan
}

func (impl *baseImplementation) calcCost(outCount float64, childCosts []float64, children ...*Group) float64 {
	impl.cost = childCosts[0]
	return impl.cost
}

func (impl *baseImplementation) setCost(cost float64) {
	impl.cost = cost
}

func (impl *baseImplementation) getCost() float64 {
	return impl.cost
}

func (impl *baseImplementation) getPlan() plannercore.PhysicalPlan {
	return impl.plan
}

// SortImpl implementation of PhysicalSort.
type SortImpl struct {
	baseImplementation
}

func (impl *SortImpl) calcCost(outCount float64, childCosts []float64, children ...*Group) float64 {
	cnt := math.Min(children[0].prop.Stats.RowCount, impl.plan.GetChildReqProps(0).ExpectedCnt)
	sort := impl.plan.(*plannercore.PhysicalSort)
	impl.cost = sort.GetCost(cnt) + childCosts[0]
	return impl.cost
}
