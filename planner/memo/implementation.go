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
	CalcCost(outCount float64, childCosts []float64, children ...*Group) float64
	SetCost(cost float64)
	GetCost() float64
	GetPlan() plannercore.PhysicalPlan
}
