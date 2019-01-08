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
	. "github.com/pingcap/check"
	plannercore "github.com/pingcap/tidb/planner/core"
)

func (s *testMemoSuite) TestNewGroupExpr(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)
	c.Assert(expr.ExprNode, Equals, p)
	c.Assert(expr.Children, IsNil)
	c.Assert(expr.Explored, IsFalse)
}

func (s *testMemoSuite) TestGroupExprFingerprint(c *C) {
	p := &plannercore.LogicalLimit{}
	expr := NewGroupExpr(p)

	// we haven't set the id of the created LogicalLimit, so the result is 0.
	c.Assert(expr.FingerPrint(), Equals, "0")
}
