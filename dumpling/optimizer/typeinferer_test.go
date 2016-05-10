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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/optimizer"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testTypeInferrerSuite{})

type testTypeInferrerSuite struct {
}

func (ts *testTypeInferrerSuite) TestInferType(c *C) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 double, c3 text)")
	cases := []struct {
		expr string
		tp   byte
		chs  string
	}{
		{"c1", mysql.TypeLong, charset.CharsetBin},
		{"+1", mysql.TypeLonglong, charset.CharsetBin},
		{"-1", mysql.TypeLonglong, charset.CharsetBin},
		{"-'1'", mysql.TypeDouble, charset.CharsetBin},
		{"~1", mysql.TypeLonglong, charset.CharsetBin},
		{"!true", mysql.TypeLonglong, charset.CharsetBin},

		{"c1 is true", mysql.TypeLonglong, charset.CharsetBin},
		{"c2 is null", mysql.TypeLonglong, charset.CharsetBin},
		{"isnull(1/0)", mysql.TypeLonglong, charset.CharsetBin},
		{"cast(1 as decimal)", mysql.TypeNewDecimal, charset.CharsetBin},

		{"1 and 1", mysql.TypeLonglong, charset.CharsetBin},
		{"1 or 1", mysql.TypeLonglong, charset.CharsetBin},
		{"1 xor 1", mysql.TypeLonglong, charset.CharsetBin},

		{"'1' & 2", mysql.TypeLonglong, charset.CharsetBin},
		{"'1' | 2", mysql.TypeLonglong, charset.CharsetBin},
		{"'1' ^ 2", mysql.TypeLonglong, charset.CharsetBin},
		{"'1' << 1", mysql.TypeLonglong, charset.CharsetBin},
		{"'1' >> 1", mysql.TypeLonglong, charset.CharsetBin},

		{"1 + '1'", mysql.TypeDouble, charset.CharsetBin},
		{"1 + 1.1", mysql.TypeNewDecimal, charset.CharsetBin},
		{"1 div 2", mysql.TypeLonglong, charset.CharsetBin},

		{"1 > any (select 1)", mysql.TypeLonglong, charset.CharsetBin},
		{"exists (select 1)", mysql.TypeLonglong, charset.CharsetBin},
		{"1 in (2, 3)", mysql.TypeLonglong, charset.CharsetBin},
		{"'abc' like 'abc'", mysql.TypeLonglong, charset.CharsetBin},
		{"'abc' rlike 'abc'", mysql.TypeLonglong, charset.CharsetBin},
		{"(1+1)", mysql.TypeLonglong, charset.CharsetBin},

		// Functions
		{"version()", mysql.TypeVarString, "utf8"},
		{"count(c1)", mysql.TypeLonglong, charset.CharsetBin},
		{"abs(1)", mysql.TypeLonglong, charset.CharsetBin},
		{"abs(1.1)", mysql.TypeNewDecimal, charset.CharsetBin},
		{"abs(cast(\"20150817015609\" as DATETIME))", mysql.TypeDouble, charset.CharsetBin},
		{"IF(1>2,2,3)", mysql.TypeLonglong, charset.CharsetBin},
		{"IFNULL(1,0)", mysql.TypeLonglong, charset.CharsetBin},
		{"POW(2,2)", mysql.TypeDouble, charset.CharsetBin},
		{"POWER(2,2)", mysql.TypeDouble, charset.CharsetBin},
		{"rand()", mysql.TypeDouble, charset.CharsetBin},
		{"curdate()", mysql.TypeDate, charset.CharsetBin},
		{"current_date()", mysql.TypeDate, charset.CharsetBin},
		{"DATE('2003-12-31 01:02:03')", mysql.TypeDate, charset.CharsetBin},
		{"curtime()", mysql.TypeDuration, charset.CharsetBin},
		{"current_time()", mysql.TypeDuration, charset.CharsetBin},
		{"curtime()", mysql.TypeDuration, charset.CharsetBin},
		{"current_timestamp()", mysql.TypeDatetime, charset.CharsetBin},
		{"microsecond('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"second('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"minute('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"hour('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"day('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"week('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"month('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"year('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"dayofweek('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"dayofmonth('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"dayofyear('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"weekday('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"weekofyear('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"yearweek('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin},
		{"found_rows()", mysql.TypeLonglong, charset.CharsetBin},
		{"length('tidb')", mysql.TypeLonglong, charset.CharsetBin},
		{"now()", mysql.TypeDatetime, charset.CharsetBin},
		{"sysdate()", mysql.TypeDatetime, charset.CharsetBin},
		{"dayname('2007-02-03')", mysql.TypeVarString, "utf8"},
		{"version()", mysql.TypeVarString, "utf8"},
		{"database()", mysql.TypeVarString, "utf8"},
		{"user()", mysql.TypeVarString, "utf8"},
		{"current_user()", mysql.TypeVarString, "utf8"},
		{"CONCAT('T', 'i', 'DB')", mysql.TypeVarString, "utf8"},
		{"CONCAT_WS('-', 'T', 'i', 'DB')", mysql.TypeVarString, "utf8"},
		{"left('TiDB', 2)", mysql.TypeVarString, "utf8"},
		{"lower('TiDB')", mysql.TypeVarString, "utf8"},
		{"lcase('TiDB')", mysql.TypeVarString, "utf8"},
		{"repeat('TiDB', 3)", mysql.TypeVarString, "utf8"},
		{"replace('TiDB', 'D', 'd')", mysql.TypeVarString, "utf8"},
		{"upper('TiDB')", mysql.TypeVarString, "utf8"},
		{"ucase('TiDB')", mysql.TypeVarString, "utf8"},
		{"trim(' TiDB ')", mysql.TypeVarString, "utf8"},
		{"ltrim(' TiDB')", mysql.TypeVarString, "utf8"},
		{"rtrim('TiDB ')", mysql.TypeVarString, "utf8"},
		{"connection_id()", mysql.TypeLonglong, charset.CharsetBin},
		{"if(1>2, 2, 3)", mysql.TypeLonglong, charset.CharsetBin},
		{"case c1 when null then 2 when 2 then 1.1 else 1 END", mysql.TypeNewDecimal, charset.CharsetBin},
		{"case c1 when null then 2 when 2 then 'tidb' else 1.1 END", mysql.TypeVarchar, "utf8"},
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
		chs := stmt.GetResultFields()[0].Column.Charset
		c.Assert(tp, Equals, ca.tp, Commentf("Tp for %s", ca.expr))
		c.Assert(chs, Equals, ca.chs, Commentf("Charset for %s", ca.expr))
	}
}

func (s *testTypeInferrerSuite) TestColumnInfoModified(c *C) {
	defer testleak.AfterTest(c)()
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists tab0")
	testKit.MustExec("CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)")
	testKit.MustExec("SELECT + - (- CASE + col0 WHEN + CAST( col0 AS SIGNED ) THEN col1 WHEN 79 THEN NULL WHEN + - col1 THEN col0 / + col0 END ) * - 16 FROM tab0")
	ctx := testKit.Se.(context.Context)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	col, _ := is.ColumnByName(model.NewCIStr("test"), model.NewCIStr("tab0"), model.NewCIStr("col1"))
	c.Assert(col.Tp, Equals, mysql.TypeLong)
}
