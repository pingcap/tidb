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
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
)

type baseImpl struct {
	cost float64
	plan plannercore.PhysicalPlan
}

func (impl *baseImpl) CalcCost(outCount float64, childCosts []float64, children ...*memo.Group) float64 {
	impl.cost = childCosts[0]
	return impl.cost
}

func (impl *baseImpl) SetCost(cost float64) {
	impl.cost = cost
}

func (impl *baseImpl) GetCost() float64 {
	return impl.cost
}

func (impl *baseImpl) GetPlan() plannercore.PhysicalPlan {
	return impl.plan
}
