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
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testAnalyzeSuite{})

type testAnalyzeSuite struct {
}

func (s *testAnalyzeSuite) TestAnalyze(c *C) {
	defer func() {
		testleak.AfterTest(c)()
	}()
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int)")
	testKit.MustExec("create index a on t (a)")
	testKit.MustExec("create index b on t (b)")
	testKit.MustExec("insert into t (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
	testKit.MustExec("analyze table t")
	testKit.MustExec("create table t1 (a int, b int)")
	testKit.MustExec("create index a on t1 (a)")
	testKit.MustExec("create index b on t1 (b)")
	testKit.MustExec("insert into t1 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
	cases := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from t where t.a <= 2",
			best: "Table(t)",
		},
		{
			sql:  "select * from t where t.a = 1 and t.b <= 2",
			best: "Index(t.b)[[-inf,2]]",
		},
		{
			sql:  "select * from t1 where t1.a <= 2",
			best: "Index(t1.a)[[-inf,2]]",
		},
		{
			sql:  "select * from t1 where t1.a = 1 and t1.b <= 2",
			best: "Index(t1.a)[[1,1]]",
		},
	}
	for _, ca := range cases {
		ctx := testKit.Se.(context.Context)
		stmts, err := tidb.Parse(ctx, ca.sql)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		is := sessionctx.GetDomain(ctx).InfoSchema()
		err = plan.ResolveName(stmt, is, ctx)
		c.Assert(err, IsNil)
		err = plan.InferType(ctx.GetSessionVars().StmtCtx, stmt)
		c.Assert(err, IsNil)
		p, err := plan.Optimize(ctx, stmt, is)
		c.Assert(plan.ToString(p), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}
