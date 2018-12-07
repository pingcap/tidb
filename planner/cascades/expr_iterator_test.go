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

func (s *testCascadesSuite) TestNewExprIterFromGroupElem(c *C) {
	g0 := NewGroup(NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx)))
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	g0.Insert(NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx)))
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))

	g1 := NewGroup(NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx)))
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	g1.Insert(NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx)))
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))

	expr := NewGroupExpr(plannercore.LogicalJoin{}.Init(s.sctx))
	expr.children = append(expr.children, g0)
	expr.children = append(expr.children, g1)
	g2 := NewGroup(expr)

	pattern := BuildPattern(OperandJoin, BuildPattern(OperandProjection), BuildPattern(OperandSelection))
	iter := NewExprIterFromGroupElem(g2.equivalents.Front(), pattern)

	c.Assert(iter, NotNil)
	c.Assert(iter.group, IsNil)
	c.Assert(iter.element, Equals, g2.equivalents.Front())
	c.Assert(iter.matched, Equals, true)
	c.Assert(iter.operand, Equals, OperandJoin)
	c.Assert(len(iter.children), Equals, 2)

	c.Assert(iter.children[0].group, Equals, g0)
	c.Assert(iter.children[0].element, Equals, g0.GetFirstElem(OperandProjection))
	c.Assert(iter.children[0].matched, Equals, true)
	c.Assert(iter.children[0].operand, Equals, OperandProjection)
	c.Assert(len(iter.children[0].children), Equals, 0)

	c.Assert(iter.children[1].group, Equals, g1)
	c.Assert(iter.children[1].element, Equals, g1.GetFirstElem(OperandSelection))
	c.Assert(iter.children[1].matched, Equals, true)
	c.Assert(iter.children[1].operand, Equals, OperandSelection)
	c.Assert(len(iter.children[0].children), Equals, 0)
}

func (s *testCascadesSuite) TestExprIterNext(c *C) {
	g0 := NewGroup(NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx)))
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	g0.Insert(NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx)))
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	g0.Insert(NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx)))

	g1 := NewGroup(NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx)))
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	g1.Insert(NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx)))
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	g1.Insert(NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx)))

	expr := NewGroupExpr(plannercore.LogicalJoin{}.Init(s.sctx))
	expr.children = append(expr.children, g0)
	expr.children = append(expr.children, g1)
	g2 := NewGroup(expr)

	pattern := BuildPattern(OperandJoin, BuildPattern(OperandProjection), BuildPattern(OperandSelection))
	iter := NewExprIterFromGroupElem(g2.equivalents.Front(), pattern)
	c.Assert(iter, NotNil)

	count := 0
	for ; iter.Matched(); iter.Next() {
		count++
		c.Assert(iter.group, IsNil)
		c.Assert(iter.matched, Equals, true)
		c.Assert(iter.operand, Equals, OperandJoin)
		c.Assert(len(iter.children), Equals, 2)

		c.Assert(iter.children[0].group, Equals, g0)
		c.Assert(iter.children[0].matched, Equals, true)
		c.Assert(iter.children[0].operand, Equals, OperandProjection)
		c.Assert(len(iter.children[0].children), Equals, 0)

		c.Assert(iter.children[1].group, Equals, g1)
		c.Assert(iter.children[1].matched, Equals, true)
		c.Assert(iter.children[1].operand, Equals, OperandSelection)
		c.Assert(len(iter.children[1].children), Equals, 0)
	}

	c.Assert(count, Equals, 9)
}

func (s *testCascadesSuite) TestExprIterReset(c *C) {
	g0 := NewGroup(NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx)))
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	g0.Insert(NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx)))
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	g0.Insert(NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx)))

	sel1 := NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx))
	sel2 := NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx))
	sel3 := NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx))
	g1 := NewGroup(sel1)
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	g1.Insert(sel2)
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	g1.Insert(sel3)

	g2 := NewGroup(NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx)))
	g2.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	g2.Insert(NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx)))
	g2.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx)))
	g2.Insert(NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx)))

	// link join with group 0 and 1
	expr := NewGroupExpr(plannercore.LogicalJoin{}.Init(s.sctx))
	expr.children = append(expr.children, g0)
	expr.children = append(expr.children, g1)
	g3 := NewGroup(expr)

	// link sel 1~3 with group 2
	sel1.children = append(sel1.children, g2)
	sel2.children = append(sel2.children, g2)
	sel3.children = append(sel3.children, g2)

	// create a pattern: join(proj, sel(limit))
	lhsPattern := BuildPattern(OperandProjection)
	rhsPattern := BuildPattern(OperandSelection, BuildPattern(OperandLimit))
	pattern := BuildPattern(OperandJoin, lhsPattern, rhsPattern)

	// create expression iterator for the pattern on join
	iter := NewExprIterFromGroupElem(g3.equivalents.Front(), pattern)
	c.Assert(iter, NotNil)

	count := 0
	for ; iter.Matched(); iter.Next() {
		count++
		c.Assert(iter.group, IsNil)
		c.Assert(iter.matched, Equals, true)
		c.Assert(iter.operand, Equals, OperandJoin)
		c.Assert(len(iter.children), Equals, 2)

		c.Assert(iter.children[0].group, Equals, g0)
		c.Assert(iter.children[0].matched, Equals, true)
		c.Assert(iter.children[0].operand, Equals, OperandProjection)
		c.Assert(len(iter.children[0].children), Equals, 0)

		c.Assert(iter.children[1].group, Equals, g1)
		c.Assert(iter.children[1].matched, Equals, true)
		c.Assert(iter.children[1].operand, Equals, OperandSelection)
		c.Assert(len(iter.children[1].children), Equals, 1)

		c.Assert(iter.children[1].children[0].group, Equals, g2)
		c.Assert(iter.children[1].children[0].matched, Equals, true)
		c.Assert(iter.children[1].children[0].operand, Equals, OperandLimit)
		c.Assert(len(iter.children[1].children[0].children), Equals, 0)
	}

	c.Assert(count, Equals, 18)
}
