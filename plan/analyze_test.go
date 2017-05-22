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
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
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
	testKit := testkit.NewTestKit(c, store)
	defer func() {
		testKit.MustExec("drop table t, t1, t2")
		store.Close()
	}()
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

	testKit.MustExec("create table t2 (a int, b int)")
	testKit.MustExec("create index a on t2 (a)")
	testKit.MustExec("create index b on t2 (b)")
	testKit.MustExec("insert into t2 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
	testKit.MustExec("analyze table t2 index a")

	tests := []struct {
		sql  string
		best string
	}{
		// Test analyze full table.
		{
			sql:  "select * from t where t.a <= 2",
			best: "TableReader(Table(t)->Sel([le(test.t.a, 2)]))",
		},
		{
			sql:  "select * from t where t.a < 2",
			best: "IndexLookUp(Index(t.a)[[-inf,2)], Table(t))",
		},
		{
			sql:  "select * from t where t.a = 1 and t.b <= 2",
			best: "IndexLookUp(Index(t.b)[[-inf,2]], Table(t)->Sel([eq(test.t.a, 1)]))",
		},
		// Test not analyzed table.
		{
			sql:  "select * from t1 where t1.a <= 2",
			best: "IndexLookUp(Index(t1.a)[[-inf,2]], Table(t1))",
		},
		{
			sql:  "select * from t1 where t1.a = 1 and t1.b <= 2",
			best: "IndexLookUp(Index(t1.a)[[1,1]], Table(t1)->Sel([le(test.t1.b, 2)]))",
		},
		// Test analyze single index.
		{
			sql: "select * from t2 where t2.a <= 2",
			// This is not the best because the histogram for index b is pseudo, then the row count calculated for such
			// a small table is always tableRowCount/3, so the cost is smaller.
			// FIXME: Fix it after implementing selectivity estimation for normal column.
			best: "IndexLookUp(Index(t2.b)[[<nil>,+inf]], Table(t2)->Sel([le(test.t2.a, 2)]))",
		},
		{
			sql:  "select * from t2 where t2.a = 1 and t2.b <= 2",
			best: "IndexLookUp(Index(t2.b)[[-inf,2]], Table(t2)->Sel([eq(test.t2.a, 1)]))",
		},
	}
	for _, tt := range tests {
		ctx := testKit.Se.(context.Context)
		stmts, err := tidb.Parse(ctx, tt.sql)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		is := sessionctx.GetDomain(ctx).InfoSchema()
		err = plan.ResolveName(stmt, is, ctx)
		c.Assert(err, IsNil)
		err = expression.InferType(ctx.GetSessionVars().StmtCtx, stmt)
		c.Assert(err, IsNil)
		p, err := plan.Optimize(ctx, stmt, is)
		c.Assert(plan.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func newStoreWithBootstrap() (kv.Storage, error) {
	store, err := tikv.NewMockTikvStore("")
	if err != nil {
		return nil, errors.Trace(err)
	}
	tidb.SetSchemaLease(0)
	_, err = tidb.BootstrapSession(store)
	return store, errors.Trace(err)
}
