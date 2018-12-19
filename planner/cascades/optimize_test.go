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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testCascadesSuite{})

type testCascadesSuite struct {
	*parser.Parser
	is   infoschema.InfoSchema
	sctx sessionctx.Context
}

func (s *testCascadesSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockTable()})
	s.sctx = plannercore.MockContext()
	s.Parser = parser.New()
}

func (s *testCascadesSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testCascadesSuite) TestImplGroupZeroCost(c *C) {
	stmt, err := s.ParseOneStmt("select t1.a, t2.a from t as t1 left join t as t2 on t1.a = t2.a where t1.a < 1.0", "", "")
	c.Assert(err, IsNil)
	p, err := plannercore.BuildLogicalPlan(s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	rootGroup := convert2Group(logic)
	// TODO remove these hard code about logical property after we support deriving stats in exploration phase.
	rootGroup.LogicalProperty = &property.LogicalProperty{}
	rootGroup.LogicalProperty.Stats = property.NewSimpleStats(10.0)

	prop := &property.PhysicalProperty{
		ExpectedCnt: math.MaxFloat64,
	}
	impl, err := implGroup(rootGroup, prop, 0.0)
	c.Assert(impl, IsNil)
	c.Assert(err, IsNil)
}
