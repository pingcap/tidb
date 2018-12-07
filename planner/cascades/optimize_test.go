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

	. "github.com/pingcap/check"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/property"
)

func (s *testCascadesSuite) TestImplGroupZeroCost(c *C) {
	stmt, err := s.ParseOneStmt("select t1.a, t2.a from t as t1 left join t as t2 on t1.a = t2.a where t1.a < 1.0", "", "")
	c.Assert(err, IsNil)
	p, err := plannercore.BuildLogicalPlan(s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	rootGroup := convert2Group(logic)
	// TODO remove these hard code about logical property after we support deriving stats in exploration phase.
	rootGroup.prop = &property.LogicalProperty{}
	rootGroup.prop.Stats = property.NewSimpleStats(10.0)

	prop := &property.PhysicalProperty{
		ExpectedCnt: math.MaxFloat64,
	}
	impl, err := implGroup(rootGroup, prop, 0.0)
	c.Assert(impl, IsNil)
	c.Assert(err, IsNil)
}
