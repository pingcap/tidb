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
	. "github.com/pingcap/check"
	plannercore "github.com/pingcap/tidb/planner/core"
)

func (s *testCascadesSuite) TestConvert2Group(c *C) {
	stmt, err := s.ParseOneStmt("select a from t", "", "")
	c.Assert(err, IsNil)
	p, err := plannercore.BuildLogicalPlan(s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	g, err := convert2Group(logic)
	c.Assert(err, IsNil)
	c.Assert(g.prop, NotNil)
	c.Assert(g.prop.Schema.Len(), Equals, 1)
	c.Assert(g.prop.Stats, NotNil)
}
