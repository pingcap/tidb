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
	// "container/list"
	//"fmt"
	"testing"

	. "github.com/pingcap/check"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testCascadesSuite{})

type testCascadesSuite struct {
	sctx sessionctx.Context
}

func (s *testCascadesSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.sctx = mock.NewContext()
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
