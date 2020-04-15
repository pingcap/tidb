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
	"context"
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
	is     infoschema.InfoSchema
	schema *expression.Schema
	sctx   sessionctx.Context
}

func (s *testMemoSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	s.sctx = plannercore.MockContext()
	s.Parser = parser.New()
	s.schema = expression.NewSchema()
}

func (s *testMemoSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testMemoSuite) TestNewGroup(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, s.schema)

	c.Assert(g.Equivalents.Len(), Equals, 1)
	c.Assert(g.Equivalents.Front().Value.(*GroupExpr), Equals, expr)
	c.Assert(len(g.Fingerprints), Equals, 1)
	c.Assert(g.Explored(0), IsFalse)
}

func (s *testMemoSuite) TestGroupInsert(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, s.schema)
	c.Assert(g.Insert(expr), IsFalse)
	expr.selfFingerprint = "1"
	c.Assert(g.Insert(expr), IsTrue)
}

func (s *testMemoSuite) TestGroupDelete(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, s.schema)
	c.Assert(g.Equivalents.Len(), Equals, 1)

	g.Delete(expr)
	c.Assert(g.Equivalents.Len(), Equals, 0)

	g.Delete(expr)
	c.Assert(g.Equivalents.Len(), Equals, 0)
}

func (s *testMemoSuite) TestGroupDeleteAll(c *C) {
	expr := NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx, 0))
	g := NewGroupWithSchema(expr, s.schema)
	c.Assert(g.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0))), IsTrue)
	c.Assert(g.Insert(NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx, 0))), IsTrue)
	c.Assert(g.Equivalents.Len(), Equals, 3)
	c.Assert(g.GetFirstElem(OperandProjection), NotNil)
	c.Assert(g.Exists(expr), IsTrue)

	g.DeleteAll()
	c.Assert(g.Equivalents.Len(), Equals, 0)
	c.Assert(g.GetFirstElem(OperandProjection), IsNil)
	c.Assert(g.Exists(expr), IsFalse)
}

func (s *testMemoSuite) TestGroupExists(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, s.schema)
	c.Assert(g.Exists(expr), IsTrue)

	g.Delete(expr)
	c.Assert(g.Exists(expr), IsFalse)
}

func (s *testMemoSuite) TestGroupFingerPrint(c *C) {
	stmt1, err := s.ParseOneStmt("select * from t where a > 1 and a < 100", "", "")
	c.Assert(err, IsNil)
	p1, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt1, s.is)
	c.Assert(err, IsNil)
	logic1, ok := p1.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	// Plan tree should be: DataSource -> Selection -> Projection
	proj, ok := logic1.(*plannercore.LogicalProjection)
	c.Assert(ok, IsTrue)
	sel, ok := logic1.Children()[0].(*plannercore.LogicalSelection)
	c.Assert(ok, IsTrue)
	group1 := Convert2Group(logic1)
	oldGroupExpr := group1.Equivalents.Front().Value.(*GroupExpr)

	// Insert a GroupExpr with the same ExprNode.
	newGroupExpr := NewGroupExpr(proj)
	newGroupExpr.SetChildren(oldGroupExpr.Children[0])
	group1.Insert(newGroupExpr)
	c.Assert(group1.Equivalents.Len(), Equals, 1)

	// Insert a GroupExpr with different children。
	newGroupExpr2 := NewGroupExpr(proj)
	newGroup := NewGroupWithSchema(oldGroupExpr, group1.Prop.Schema)
	newGroupExpr2.SetChildren(newGroup)
	group1.Insert(newGroupExpr2)
	c.Assert(group1.Equivalents.Len(), Equals, 2)

	// Insert a GroupExpr with different ExprNode.
	limit := plannercore.LogicalLimit{}.Init(proj.SCtx(), 0)
	newGroupExpr3 := NewGroupExpr(limit)
	newGroupExpr3.SetChildren(oldGroupExpr.Children[0])
	group1.Insert(newGroupExpr3)
	c.Assert(group1.Equivalents.Len(), Equals, 3)

	// Insert two LogicalSelections with same conditions but different order.
	c.Assert(len(sel.Conditions), Equals, 2)
	newSelection := plannercore.LogicalSelection{
		Conditions: make([]expression.Expression, 2)}.Init(sel.SCtx(), sel.SelectBlockOffset())
	newSelection.Conditions[0], newSelection.Conditions[1] = sel.Conditions[1], sel.Conditions[0]
	newGroupExpr4 := NewGroupExpr(sel)
	newGroupExpr5 := NewGroupExpr(newSelection)
	newGroupExpr4.SetChildren(oldGroupExpr.Children[0])
	newGroupExpr5.SetChildren(oldGroupExpr.Children[0])
	group1.Insert(newGroupExpr4)
	c.Assert(group1.Equivalents.Len(), Equals, 4)
	group1.Insert(newGroupExpr5)
	c.Assert(group1.Equivalents.Len(), Equals, 4)
}

func (s *testMemoSuite) TestGroupGetFirstElem(c *C) {
	expr0 := NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx, 0))
	expr1 := NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0))
	expr2 := NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx, 0))
	expr3 := NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0))
	expr4 := NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx, 0))

	g := NewGroupWithSchema(expr0, s.schema)
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
func (impl *fakeImpl) GetCostLimit(float64, ...Implementation) float64 { return 0 }
func (s *testMemoSuite) TestGetInsertGroupImpl(c *C) {
	g := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0)), s.schema)
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
	oldExpr := NewGroupExpr(plannercore.LogicalLimit{Count: 10}.Init(s.sctx, 0))
	g := NewGroupWithSchema(oldExpr, s.schema)
	newExpr := NewGroupExpr(plannercore.LogicalLimit{Count: 20}.Init(s.sctx, 0))
	g.Insert(newExpr)
	c.Assert(g.GetFirstElem(OperandLimit), NotNil)
	c.Assert(g.GetFirstElem(OperandLimit).Value, Equals, oldExpr)
	g.Delete(oldExpr)
	c.Assert(g.GetFirstElem(OperandLimit), NotNil)
	c.Assert(g.GetFirstElem(OperandLimit).Value, Equals, newExpr)
	g.Delete(newExpr)
	c.Assert(g.GetFirstElem(OperandLimit), IsNil)
}

func (s *testMemoSuite) TestBuildKeyInfo(c *C) {
	// case 1: primary key has constant constraint
	stmt1, err := s.ParseOneStmt("select a from t where a = 10", "", "")
	c.Assert(err, IsNil)
	p1, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt1, s.is)
	c.Assert(err, IsNil)
	logic1, ok := p1.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	group1 := Convert2Group(logic1)
	group1.BuildKeyInfo()
	c.Assert(group1.Prop.MaxOneRow, IsTrue)
	c.Assert(len(group1.Prop.Schema.Keys), Equals, 1)

	// case 2: group by column is key
	stmt2, err := s.ParseOneStmt("select b, sum(a) from t group by b", "", "")
	c.Assert(err, IsNil)
	p2, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt2, s.is)
	c.Assert(err, IsNil)
	logic2, ok := p2.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	group2 := Convert2Group(logic2)
	group2.BuildKeyInfo()
	c.Assert(group2.Prop.MaxOneRow, IsFalse)
	c.Assert(len(group2.Prop.Schema.Keys), Equals, 1)

	// case 3: build key info for new Group
	newSel := plannercore.LogicalSelection{}.Init(s.sctx, 0)
	newExpr1 := NewGroupExpr(newSel)
	newExpr1.SetChildren(group2)
	newGroup1 := NewGroupWithSchema(newExpr1, group2.Prop.Schema)
	newGroup1.BuildKeyInfo()
	c.Assert(len(newGroup1.Prop.Schema.Keys), Equals, 1)

	// case 4: build maxOneRow for new Group
	newLimit := plannercore.LogicalLimit{Count: 1}.Init(s.sctx, 0)
	newExpr2 := NewGroupExpr(newLimit)
	newExpr2.SetChildren(group2)
	newGroup2 := NewGroupWithSchema(newExpr2, group2.Prop.Schema)
	newGroup2.BuildKeyInfo()
	c.Assert(newGroup2.Prop.MaxOneRow, IsTrue)
}

func (s *testMemoSuite) TestExploreMark(c *C) {
	mark := ExploreMark(0)
	c.Assert(mark.Explored(0), IsFalse)
	c.Assert(mark.Explored(1), IsFalse)
	mark.SetExplored(0)
	mark.SetExplored(1)
	c.Assert(mark.Explored(0), IsTrue)
	c.Assert(mark.Explored(1), IsTrue)
	mark.SetUnexplored(1)
	c.Assert(mark.Explored(0), IsTrue)
	c.Assert(mark.Explored(1), IsFalse)
}
