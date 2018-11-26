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

func (s *testCascadesSuite) TestGetOperand(c *C) {
	c.Assert(GetOperand(&plannercore.LogicalJoin{}), Equals, OperandJoin)
	c.Assert(GetOperand(&plannercore.LogicalAggregation{}), Equals, OperandAggregation)
	c.Assert(GetOperand(&plannercore.LogicalProjection{}), Equals, OperandProjection)
	c.Assert(GetOperand(&plannercore.LogicalSelection{}), Equals, OperandSelection)
	c.Assert(GetOperand(&plannercore.LogicalApply{}), Equals, OperandApply)
	c.Assert(GetOperand(&plannercore.LogicalMaxOneRow{}), Equals, OperandMaxOneRow)
	c.Assert(GetOperand(&plannercore.LogicalTableDual{}), Equals, OperandTableDual)
	c.Assert(GetOperand(&plannercore.DataSource{}), Equals, OperandDataSource)
	c.Assert(GetOperand(&plannercore.LogicalUnionScan{}), Equals, OperandUnionScan)
	c.Assert(GetOperand(&plannercore.LogicalUnionAll{}), Equals, OperandUnionAll)
	c.Assert(GetOperand(&plannercore.LogicalSort{}), Equals, OperandSort)
	c.Assert(GetOperand(&plannercore.LogicalTopN{}), Equals, OperandTopN)
	c.Assert(GetOperand(&plannercore.LogicalLock{}), Equals, OperandLock)
	c.Assert(GetOperand(&plannercore.LogicalLimit{}), Equals, OperandLimit)
}

func (s *testCascadesSuite) TestOperandMatch(c *C) {
	c.Assert(OperandAny.match(OperandLimit), IsTrue)
	c.Assert(OperandAny.match(OperandSelection), IsTrue)
	c.Assert(OperandAny.match(OperandJoin), IsTrue)
	c.Assert(OperandAny.match(OperandMaxOneRow), IsTrue)
	c.Assert(OperandAny.match(OperandAny), IsTrue)

	c.Assert(OperandLimit.match(OperandAny), IsTrue)
	c.Assert(OperandSelection.match(OperandAny), IsTrue)
	c.Assert(OperandJoin.match(OperandAny), IsTrue)
	c.Assert(OperandMaxOneRow.match(OperandAny), IsTrue)
	c.Assert(OperandAny.match(OperandAny), IsTrue)

	c.Assert(OperandLimit.match(OperandLimit), IsTrue)
	c.Assert(OperandSelection.match(OperandSelection), IsTrue)
	c.Assert(OperandJoin.match(OperandJoin), IsTrue)
	c.Assert(OperandMaxOneRow.match(OperandMaxOneRow), IsTrue)
	c.Assert(OperandAny.match(OperandAny), IsTrue)

	c.Assert(OperandLimit.match(OperandSelection), IsFalse)
	c.Assert(OperandLimit.match(OperandJoin), IsFalse)
	c.Assert(OperandLimit.match(OperandMaxOneRow), IsFalse)
}

func (s *testCascadesSuite) TestNewPattern(c *C) {
	p := NewPattern(OperandAny)
	c.Assert(p.operand, Equals, OperandAny)
	c.Assert(p.children, IsNil)

	p = NewPattern(OperandJoin)
	c.Assert(p.operand, Equals, OperandJoin)
	c.Assert(p.children, IsNil)
}

func (s *testCascadesSuite) TestPatternSetChildren(c *C) {
	p := NewPattern(OperandAny)
	p.SetChildren(NewPattern(OperandLimit))
	c.Assert(len(p.children), Equals, 1)
	c.Assert(p.children[0].operand, Equals, OperandLimit)
	c.Assert(p.children[0].children, IsNil)

	p = NewPattern(OperandJoin)
	p.SetChildren(NewPattern(OperandProjection), NewPattern(OperandSelection))
	c.Assert(len(p.children), Equals, 2)
	c.Assert(p.children[0].operand, Equals, OperandProjection)
	c.Assert(p.children[0].children, IsNil)
	c.Assert(p.children[1].operand, Equals, OperandSelection)
	c.Assert(p.children[1].children, IsNil)
}
