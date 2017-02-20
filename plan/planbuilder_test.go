// Copyright 2017 PingCAP, Inc.
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

package plan_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testPlanBuilderSuite{})

func (s *testPlanBuilderSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
}

type testPlanBuilderSuite struct {
	*parser.Parser
}

func (ts *testPlanBuilderSuite) TestShow(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	ctx := testKit.Se.(context.Context)
	domain := sessionctx.GetDomain(ctx)
	ctx.GetSessionVars().CurrentDB = "test"
	node, err := ts.ParseOneStmt("Show Databases;", "", "")
	c.Assert(err, IsNil)
	err = plan.ResolveName(node, domain.InfoSchema(), ctx)
	c.Assert(err, IsNil)

	pb := &PlanBuilder{
		ctx: ctx,
	}
	p := pb.build(node)
	schema := p.Schema()
	c.Assert(schema, HasLen, 1)
	c.Assert(schema[0].Flen, Equals, 256)
}
