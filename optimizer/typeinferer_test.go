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

package optimizer_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/optimizer"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testTypeInferrerSuite{})

type testTypeInferrerSuite struct {
}

func (ts *testTypeInferrerSuite) TestInterType(c *C) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 double, c3 text)")
	cases := []struct {
		expr string
		tp   byte
	}{
		{"c1", mysql.TypeLong},
		{"+1", mysql.TypeLonglong},
		{"-1", mysql.TypeLonglong},
		{"-'1'", mysql.TypeDouble},
		{"~1", mysql.TypeLonglong},
		{"!true", mysql.TypeLonglong},

		{"c1 is true", mysql.TypeLonglong},
		{"c2 is null", mysql.TypeLonglong},
		{"cast(1 as decimal)", mysql.TypeNewDecimal},

		{"1 and 1", mysql.TypeLonglong},
		{"1 or 1", mysql.TypeLonglong},
		{"1 xor 1", mysql.TypeLonglong},

		{"'1' & 2", mysql.TypeLonglong},
		{"'1' | 2", mysql.TypeLonglong},
		{"'1' ^ 2", mysql.TypeLonglong},
		{"'1' << 1", mysql.TypeLonglong},
		{"'1' >> 1", mysql.TypeLonglong},

		{"1 + '1'", mysql.TypeDouble},
		{"1 + 1.1", mysql.TypeNewDecimal},
		{"1 div 2", mysql.TypeLonglong},

		{"1 > any (select 1)", mysql.TypeLonglong},
		{"exists (select 1)", mysql.TypeLonglong},
		{"1 in (2, 3)", mysql.TypeLonglong},
		{"'abc' like 'abc'", mysql.TypeLonglong},
		{"'abc' rlike 'abc'", mysql.TypeLonglong},
		{"(1+1)", mysql.TypeLonglong},
	}
	for _, ca := range cases {
		ctx := testKit.Se.(context.Context)
		stmts, err := tidb.Parse(ctx, "select "+ca.expr+" from t")
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0].(*ast.SelectStmt)
		is := sessionctx.GetDomain(ctx).InfoSchema()
		err = optimizer.ResolveName(stmt, is, ctx)
		c.Assert(err, IsNil)
		optimizer.InferType(stmt)
		tp := stmt.GetResultFields()[0].Column.Tp
		c.Assert(tp, Equals, ca.tp, Commentf("for %s", ca.expr))
	}
}
