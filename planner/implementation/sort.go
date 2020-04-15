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

package implementation

import (
	"math"

	plannercore "github.com/pingcap/tidb/v4/planner/core"
	"github.com/pingcap/tidb/v4/planner/memo"
)

// SortImpl implementation of PhysicalSort.
type SortImpl struct {
	baseImpl
}

// NewSortImpl creates a new sort Implementation.
func NewSortImpl(sort *plannercore.PhysicalSort) *SortImpl {
	return &SortImpl{baseImpl{plan: sort}}
}

// CalcCost calculates the cost of the sort Implementation.
func (impl *SortImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	cnt := math.Min(children[0].GetPlan().Stats().RowCount, impl.plan.GetChildReqProps(0).ExpectedCnt)
	sort := impl.plan.(*plannercore.PhysicalSort)
	impl.cost = sort.GetCost(cnt, children[0].GetPlan().Schema()) + children[0].GetCost()
	return impl.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (impl *SortImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	sort := impl.plan.(*plannercore.PhysicalSort)
	sort.SetChildren(children[0].GetPlan())
	// When the Sort orderByItems contain ScalarFunction, we need
	// to inject two Projections below and above the Sort.
	impl.plan = plannercore.InjectProjBelowSort(sort, sort.ByItems)
	return impl
}

// NominalSortImpl is the implementation of NominalSort.
type NominalSortImpl struct {
	baseImpl
}

// AttachChildren implements Implementation AttachChildren interface.
func (impl *NominalSortImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	return children[0]
}

// NewNominalSortImpl creates a new NominalSort Implementation.
func NewNominalSortImpl(sort *plannercore.NominalSort) *NominalSortImpl {
	return &NominalSortImpl{baseImpl{plan: sort}}
}
