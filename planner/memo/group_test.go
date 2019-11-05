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
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	s.sctx = plannercore.MockContext()
	s.Parser = parser.New()
}

func (s *testMemoSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testMemoSuite) TestNewGroup(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, nil)

	c.Assert(g.Equivalents.Len(), Equals, 1)
	c.Assert(g.Equivalents.Front().Value.(*GroupExpr), Equals, expr)
	c.Assert(len(g.Fingerprints), Equals, 1)
	c.Assert(g.Explored, IsFalse)
}

func (s *testMemoSuite) TestGroupInsert(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, nil)
	c.Assert(g.Insert(expr), IsFalse)
	expr.selfFingerprint = "1"
	c.Assert(g.Insert(expr), IsTrue)
}

func (s *testMemoSuite) TestGroupDelete(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, nil)
	c.Assert(g.Equivalents.Len(), Equals, 1)

	g.Delete(expr)
	c.Assert(g.Equivalents.Len(), Equals, 0)

	g.Delete(expr)
	c.Assert(g.Equivalents.Len(), Equals, 0)
}

func (s *testMemoSuite) TestGroupExists(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, nil)
	c.Assert(g.Exists(expr), IsTrue)

	g.Delete(expr)
	c.Assert(g.Exists(expr), IsFalse)
}

func (s *testMemoSuite) TestGroupGetFirstElem(c *C) {
	expr0 := NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx, 0))
	expr1 := NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0))
	expr2 := NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx, 0))
	expr3 := NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0))
	expr4 := NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx, 0))

	g := NewGroupWithSchema(expr0, nil)
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

func (impl *fakeImpl) CalcCost(float64, ...Implementation) float64     { return 0 }
func (impl *fakeImpl) SetCost(float64)                                 {}
func (impl *fakeImpl) GetCost() float64                                { return 0 }
func (impl *fakeImpl) GetPlan() plannercore.PhysicalPlan               { return impl.plan }
func (impl *fakeImpl) AttachChildren(...Implementation) Implementation { return nil }
func (impl *fakeImpl) ScaleCostLimit(float64) float64                  { return 0 }
func (s *testMemoSuite) TestGetInsertGroupImpl(c *C) {
	g := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0)), nil)
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

func (s *testMemoSuite) TestEngineTypeSet(c *C) {
	c.Assert(EngineAll.Contains(EngineTiDB), IsTrue)
	c.Assert(EngineAll.Contains(EngineTiKV), IsTrue)
	c.Assert(EngineAll.Contains(EngineTiFlash), IsTrue)

	c.Assert(EngineTiDBOnly.Contains(EngineTiDB), IsTrue)
	c.Assert(EngineTiDBOnly.Contains(EngineTiKV), IsFalse)
	c.Assert(EngineTiDBOnly.Contains(EngineTiFlash), IsFalse)

	c.Assert(EngineTiKVOnly.Contains(EngineTiDB), IsFalse)
	c.Assert(EngineTiKVOnly.Contains(EngineTiKV), IsTrue)
	c.Assert(EngineTiKVOnly.Contains(EngineTiFlash), IsFalse)

	c.Assert(EngineTiFlashOnly.Contains(EngineTiDB), IsFalse)
	c.Assert(EngineTiFlashOnly.Contains(EngineTiKV), IsFalse)
	c.Assert(EngineTiFlashOnly.Contains(EngineTiFlash), IsTrue)

	c.Assert(EngineTiKVOrTiFlash.Contains(EngineTiDB), IsFalse)
	c.Assert(EngineTiKVOrTiFlash.Contains(EngineTiKV), IsTrue)
	c.Assert(EngineTiKVOrTiFlash.Contains(EngineTiFlash), IsTrue)
}

func (s *testMemoSuite) TestFirstElemAfterDelete(c *C) {
	oldExpr := NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0))
	g := NewGroupWithSchema(oldExpr, nil)
	newExpr := NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0))
	g.Insert(newExpr)
	c.Assert(g.GetFirstElem(OperandLimit), NotNil)
	c.Assert(g.GetFirstElem(OperandLimit).Value, Equals, oldExpr)
	g.Delete(oldExpr)
	c.Assert(g.GetFirstElem(OperandLimit), NotNil)
	c.Assert(g.GetFirstElem(OperandLimit).Value, Equals, newExpr)
	g.Delete(newExpr)
	c.Assert(g.GetFirstElem(OperandLimit), IsNil)
}
