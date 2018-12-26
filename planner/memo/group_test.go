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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
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

var _ = Suite(&testMemoSuite{})

type testMemoSuite struct {
	*parser.Parser
	is   infoschema.InfoSchema
	sctx sessionctx.Context
}

func (s *testMemoSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockTable()})
	s.sctx = plannercore.MockContext()
	s.Parser = parser.New()
}

func (s *testMemoSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testMemoSuite) TestNewGroup(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroup(expr)

	c.Assert(g.Equivalents.Len(), Equals, 1)
	c.Assert(g.Equivalents.Front().Value.(*GroupExpr), Equals, expr)
	c.Assert(len(g.Fingerprints), Equals, 1)
	c.Assert(g.Explored, IsFalse)
}

func (s *testMemoSuite) TestGroupInsert(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroup(expr)
	c.Assert(g.Insert(expr), IsFalse)
	expr.selfFingerprint = "1"
	c.Assert(g.Insert(expr), IsTrue)
}

func (s *testMemoSuite) TestGroupDelete(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroup(expr)
	c.Assert(g.Equivalents.Len(), Equals, 1)

	g.Delete(expr)
	c.Assert(g.Equivalents.Len(), Equals, 0)

	g.Delete(expr)
	c.Assert(g.Equivalents.Len(), Equals, 0)
}

func (s *testMemoSuite) TestGroupExists(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroup(expr)
	c.Assert(g.Exists(expr), IsTrue)

	g.Delete(expr)
	c.Assert(g.Exists(expr), IsFalse)
}

func (s *testMemoSuite) TestGroupGetFirstElem(c *C) {
	expr0 := NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx))
	expr1 := NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx))
	expr2 := NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx))
	expr3 := NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx))
	expr4 := NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx))

	g := NewGroup(expr0)
	g.Insert(expr1)
	g.Insert(expr2)
	g.Insert(expr3)
	g.Insert(expr4)

	c.Assert(g.GetFirstElem(OperandProjection).Value.(*GroupExpr), Equals, expr0)
	c.Assert(g.GetFirstElem(OperandLimit).Value.(*GroupExpr), Equals, expr1)
	c.Assert(g.GetFirstElem(OperandAny).Value.(*GroupExpr), Equals, expr0)
}

type fakeImpl struct {
	cost float64
	plan plannercore.PhysicalPlan
}

func (impl *fakeImpl) CalcCost(float64, []float64, ...*Group) float64 { return 0 }
func (impl *fakeImpl) SetCost(float64)                                {}
func (impl *fakeImpl) GetCost() float64                               { return 0 }
func (impl *fakeImpl) GetPlan() plannercore.PhysicalPlan              { return impl.plan }

func (s *testMemoSuite) TestGetInsertGroupImpl(c *C) {
	g := NewGroup(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	emptyProp := &property.PhysicalProperty{}
	orderProp := &property.PhysicalProperty{Items: []property.Item{{Col: &expression.Column{}}}}

	impl := g.GetImpl(emptyProp)
	c.Assert(impl, IsNil)

	impl = &fakeImpl{plan: &plannercore.PhysicalLimit{}}
	g.InsertImpl(emptyProp, impl)

	newImpl := g.GetImpl(emptyProp)
	c.Assert(newImpl, Equals, impl)

	newImpl = g.GetImpl(orderProp)
	c.Assert(newImpl, IsNil)
}
