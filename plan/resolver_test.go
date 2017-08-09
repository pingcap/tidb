// Copyright 2015 PingCAP, Inc.
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

var _ = Suite(&testNameResolverSuite{})

func (s *testNameResolverSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
}

type testNameResolverSuite struct {
	*parser.Parser
}

type resolverVerifier struct {
	src string
	c   *C
}

func (rv *resolverVerifier) Enter(node ast.Node) (ast.Node, bool) {
	return node, false
}

func (rv *resolverVerifier) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch v := in.(type) {
	case *ast.ColumnNameExpr:
		rv.c.Assert(v.Refer, NotNil, Commentf("%s", rv.src))
	case *ast.TableName:
		rv.c.Assert(v.TableInfo, NotNil, Commentf("%s", rv.src))
	}
	return in, true
}

type resolverTestCase struct {
	src   string
	valid bool
	err   string
}

var resolverTests = []resolverTestCase{
	{"select c1 from t1", true, ""},
	{"select c3 from t1", false, "[plan:1054]Unknown column 'c3' in 'field list'"},
	{"select c1 from t4", false, "[schema:1146]Table 'test.t4' doesn't exist"},
	{"select * from t1", true, ""},
	{"select t1.* from t1", true, ""},
	{"select t2.* from t1", false, "[plan:1054]Unknown table 't2'"},
	{"select c1 as a, c1 as a from t1 group by a", true, ""},
	{"select 1 as a, c1 as a, c2 as a from t1 group by a", true, ""},
	{"select c1, c2 as c1 from t1 group by c1+1", true, ""},
	{"select c1, c2 as c1 from t1 order by c1+1", true, ""},
	{"select * from t1, t2 join t3 on t1.c1 = t2.c1", false, "[plan:1054]Unknown column 't1.c1' in 'on clause'"},
	{"select * from t1, t2 join t3 on t2.c1 = t3.c1", true, ""},
	{"select c1 from t1 group by c1 having c1 = 3", true, ""},
	{"select c1 from t1 group by c1 having c2 = 3", false, "[plan:1054]Unknown column 'c2' in 'having clause'"},
	{"select c1 from t1 where exists (select c2)", true, ""},
	{"select cnt from (select count(c2) as cnt from t1 group by c1) t2 group by cnt", true, ""},
	{"select c1 from t2 where t11.c1 < t2.c1", false, "[plan:1054]Unknown column 't11.c1' in 'where clause'"},
	{"select c1 from t2 having t11.c1 < t2.c1", false, "[plan:1054]Unknown column 't11.c1' in 'having clause'"},
	{"select c1 from t2 where t2.c1 < t2.c1 order by t11.c1", false, "[plan:1054]Unknown column 't11.c1' in 'order clause'"},
	{"select c1 from t2 group by t11.c1", false, "[plan:1054]Unknown column 't11.c1' in 'group statement'"},
}

func (ts *testNameResolverSuite) TestNameResolver(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int, c2 int)")
	testKit.MustExec("create table t2 (c1 int, c2 int)")
	testKit.MustExec("create table t3 (c1 int, c2 int)")
	ctx := testKit.Se.(context.Context)
	domain := sessionctx.GetDomain(ctx)
	ctx.GetSessionVars().CurrentDB = "test"
	for _, tt := range resolverTests {
		node, err := ts.ParseOneStmt(tt.src, "", "")
		c.Assert(err, IsNil)
		resolveErr := plan.ResolveName(node, domain.InfoSchema(), ctx)
		if tt.valid {
			c.Assert(resolveErr, IsNil)
			verifier := &resolverVerifier{c: c, src: tt.src}
			node.Accept(verifier)
		} else {
			c.Assert(resolveErr, NotNil, Commentf("%s", tt.src))
			c.Assert(resolveErr.Error(), Equals, tt.err, Commentf("%s", resolveErr.Error()))
		}
	}
}
