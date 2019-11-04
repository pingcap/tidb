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
	"context"
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
	is        infoschema.InfoSchema
	sctx      sessionctx.Context
	optimizer *Optimizer
}

func (s *testCascadesSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	s.sctx = plannercore.MockContext()
	s.Parser = parser.New()
	s.optimizer = NewOptimizer()
}

func (s *testCascadesSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testCascadesSuite) TestImplGroupZeroCost(c *C) {
	stmt, err := s.ParseOneStmt("select t1.a, t2.a from t as t1 left join t as t2 on t1.a = t2.a where t1.a < 1.0", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	rootGroup := convert2Group(logic)
	prop := &property.PhysicalProperty{
		ExpectedCnt: math.MaxFloat64,
	}
	impl, err := s.optimizer.implGroup(rootGroup, prop, 0.0)
	c.Assert(impl, IsNil)
	c.Assert(err, IsNil)
}

func (s *testCascadesSuite) TestInitGroupSchema(c *C) {
	stmt, err := s.ParseOneStmt("select a from t", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	g := convert2Group(logic)
	c.Assert(g, NotNil)
	c.Assert(g.Prop, NotNil)
	c.Assert(g.Prop.Schema.Len(), Equals, 1)
	c.Assert(g.Prop.Stats, IsNil)
}

func (s *testCascadesSuite) TestFillGroupStats(c *C) {
	stmt, err := s.ParseOneStmt("select * from t t1 join t t2 on t1.a = t2.a", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	rootGroup := convert2Group(logic)
	err = s.optimizer.fillGroupStats(rootGroup)
	c.Assert(err, IsNil)
	c.Assert(rootGroup.Prop.Stats, NotNil)
}
