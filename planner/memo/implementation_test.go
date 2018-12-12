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
	. "github.com/pingcap/check"
	plannercore "github.com/pingcap/tidb/planner/core"
)

func (s *testCascadesSuite) TestBaseImplementation(c *C) {
	p := &plannercore.PhysicalLimit{}
	impl := &baseImplementation{plan: p}
	c.Assert(impl.getPlan(), Equals, p)
	childCosts := []float64{5.0}
	cost := impl.calcCost(10, childCosts, nil)
	c.Assert(cost, Equals, 5.0)
	c.Assert(impl.getCost(), Equals, 5.0)
	impl.setCost(6.0)
	c.Assert(impl.getCost(), Equals, 6.0)
}
