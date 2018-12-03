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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
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

func (s *testCascadesSuite) TestNewGroup(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroup(expr)

	c.Assert(g.equivalents.Len(), Equals, 1)
	c.Assert(g.equivalents.Front().Value.(*GroupExpr), Equals, expr)
	c.Assert(len(g.fingerprints), Equals, 1)
	c.Assert(g.explored, IsFalse)
}

func (s *testCascadesSuite) TestGroupInsert(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroup(expr)
	c.Assert(g.Insert(expr), IsFalse)
	expr.selfFingerprint = "1"
	c.Assert(g.Insert(expr), IsTrue)
}

func (s *testCascadesSuite) TestGroupDelete(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroup(expr)
	c.Assert(g.equivalents.Len(), Equals, 1)

	g.Delete(expr)
	c.Assert(g.equivalents.Len(), Equals, 0)

	g.Delete(expr)
	c.Assert(g.equivalents.Len(), Equals, 0)
}

func (s *testCascadesSuite) TestGroupExists(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroup(expr)
	c.Assert(g.Exists(expr), IsTrue)

	g.Delete(expr)
	c.Assert(g.Exists(expr), IsFalse)
}

func (s *testCascadesSuite) TestGroupGetFirstElem(c *C) {
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
