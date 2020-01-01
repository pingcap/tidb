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

package memo

import (
	plannercore "github.com/pingcap/tidb/planner/core"
)

// Implementation defines the interface for cost of physical plan.
type Implementation interface {
	CalcCost(outCount float64, children ...Implementation) float64
	SetCost(cost float64)
	GetCost() float64
	GetPlan() plannercore.PhysicalPlan

	// AttachChildren is used to attach children implementations and returns it self.
	AttachChildren(children ...Implementation) Implementation

	// ScaleCostLimit scales costLimit by the Implementation's concurrency factor.
	// Implementation like TiKVSingleGather may divide the cost by its scan concurrency,
	// so when we pass the costLimit for pruning the search space, we have to scale
	// the costLimit by its concurrency factor.
	ScaleCostLimit(costLimit float64) float64
}
