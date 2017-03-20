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
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testTypeInferrerSuite{})

type testTypeInferrerSuite struct {
}

func (ts *testTypeInferrerSuite) TestInferType(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	sql := `create table t (
		c_int int,
		c_bigint bigint,
		c_double double,
		c_decimal decimal,
		c_datetime datetime,
		c_time time,
		c_timestamp timestamp,
		c_char char,
		c_varchar varchar(20),
		c_text text,
		c_binary binary,
		c_varbinary varbinary(20),
		c_blob blob,
		c_set set('a', 'b', 'c'),
		c_enum enum('a', 'b', 'c'))`
	testKit.MustExec(sql)
	cases := []struct {
		expr string
		tp   byte
		chs  string
	}{
		{"c_int", mysql.TypeLong, charset.CharsetBin},
		{"+1", mysql.TypeLonglong, charset.CharsetBin},
		{"-1", mysql.TypeLonglong, charset.CharsetBin},
		{"-'1'", mysql.TypeDouble, charset.CharsetBin},
		{"-curtime()", mysql.TypeDouble, charset.CharsetBin},
		{"-now()", mysql.TypeDouble, charset.CharsetBin},
		{"~1", mysql.TypeLonglong, charset.CharsetBin},
		{"1e0", mysql.TypeDouble, charset.CharsetBin},
		{"1.0", mysql.TypeNewDecimal, charset.CharsetBin},
		{"!true", mysql.TypeLonglong, charset.CharsetBin},

		{"c_int is true", mysql.TypeLonglong, charset.CharsetBin},
		{"c_double is null", mysql.TypeLonglong, charset.CharsetBin},
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
		{"now() + 0", mysql.TypeLonglong, charset.CharsetBin},
		{"curtime() + 0", mysql.TypeLonglong, charset.CharsetBin},
		{"now(0) + 0", mysql.TypeLonglong, charset.CharsetBin},
		{"now(2) + 0", mysql.TypeNewDecimal, charset.CharsetBin},
		{"now() + 1.1", mysql.TypeNewDecimal, charset.CharsetBin},
		{"now() + '1'", mysql.TypeDouble, charset.CharsetBin},
		{"now(2) + '1'", mysql.TypeDouble, charset.CharsetBin},
		{"now() + curtime()", mysql.TypeLonglong, charset.CharsetBin},
		{"now() + now()", mysql.TypeLonglong, charset.CharsetBin},
		{"now() + now(2)", mysql.TypeNewDecimal, charset.CharsetBin},
		{"c_double + now()", mysql.TypeDouble, charset.CharsetBin},
		{"c_timestamp + 1", mysql.TypeLonglong, charset.CharsetBin},
		{"c_timestamp + 1.1", mysql.TypeNewDecimal, charset.CharsetBin},
		{"c_timestamp + '1.1'", mysql.TypeDouble, charset.CharsetBin},
		{"1.1 + now()", mysql.TypeNewDecimal, charset.CharsetBin},
		{"1 + now()", mysql.TypeLonglong, charset.CharsetBin},
		{"1 div 2", mysql.TypeLonglong, charset.CharsetBin},
		{"1 / 2", mysql.TypeNewDecimal, charset.CharsetBin},

		{"1 > any (select 1)", mysql.TypeLonglong, charset.CharsetBin},
		{"exists (select 1)", mysql.TypeLonglong, charset.CharsetBin},
		{"1 in (2, 3)", mysql.TypeLonglong, charset.CharsetBin},
		{"'abc' like 'abc'", mysql.TypeLonglong, charset.CharsetBin},
		{"'abc' rlike 'abc'", mysql.TypeLonglong, charset.CharsetBin},
		{"(1+1)", mysql.TypeLonglong, charset.CharsetBin},

		// Functions
		{"version()", mysql.TypeVarString, "utf8"},
		{"count(c_int)", mysql.TypeLonglong, charset.CharsetBin},
		{"abs(1)", mysql.TypeLonglong, charset.CharsetBin},
		{"abs(1.1)", mysql.TypeNewDecimal, charset.CharsetBin},
		{"abs(cast(\"20150817015609\" as DATETIME))", mysql.TypeDouble, charset.CharsetBin},
		{"IF(1>2,2,3)", mysql.TypeLonglong, charset.CharsetBin},
		{"IFNULL(1,0)", mysql.TypeLonglong, charset.CharsetBin},
		{"POW(2,2)", mysql.TypeDouble, charset.CharsetBin},
		{"POWER(2,2)", mysql.TypeDouble, charset.CharsetBin},
		{"LN(3)", mysql.TypeDouble, charset.CharsetBin},
		{"LOG(3)", mysql.TypeDouble, charset.CharsetBin},
		{"LOG(3, 10)", mysql.TypeDouble, charset.CharsetBin},
		{"LOG2(3)", mysql.TypeDouble, charset.CharsetBin},
		{"LOG10(3)", mysql.TypeDouble, charset.CharsetBin},
		{"SQRT(3)", mysql.TypeDouble, charset.CharsetBin},
		{"PI()", mysql.TypeDouble, charset.CharsetBin},
		{"PI() + 0.000000000000000000", mysql.TypeDouble, charset.CharsetBin},
		{"ACOS(1)", mysql.TypeDouble, charset.CharsetBin},
		{"ASIN(1)", mysql.TypeDouble, charset.CharsetBin},
		{"ATAN(1)", mysql.TypeDouble, charset.CharsetBin},
		{"ATAN(0, 1)", mysql.TypeDouble, charset.CharsetBin},
		{"rand()", mysql.TypeDouble, charset.CharsetBin},
		{"curdate()", mysql.TypeDate, charset.CharsetBin},
		{"current_date()", mysql.TypeDate, charset.CharsetBin},
		{"DATE('2003-12-31 01:02:03')", mysql.TypeDate, charset.CharsetBin},
		{"curtime()", mysql.TypeDuration, charset.CharsetBin},
		{"current_time()", mysql.TypeDuration, charset.CharsetBin},
		{"curtime()", mysql.TypeDuration, charset.CharsetBin},
		{"curtime(2)", mysql.TypeDuration, charset.CharsetBin},
		{"current_timestamp()", mysql.TypeDatetime, charset.CharsetBin},
		{"utc_timestamp()", mysql.TypeDatetime, charset.CharsetBin},
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
		{"from_unixtime(1447430881)", mysql.TypeDatetime, charset.CharsetBin},
		{"from_unixtime(1447430881, '%Y %D %M %h:%i:%s %x')", mysql.TypeVarString, "utf8"},
		{"sysdate()", mysql.TypeDatetime, charset.CharsetBin},
		{"dayname('2007-02-03')", mysql.TypeVarString, "utf8"},
		{"version()", mysql.TypeVarString, "utf8"},
		{"database()", mysql.TypeVarString, "utf8"},
		{"schema()", mysql.TypeVarString, "utf8"},
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
		{"case c_int when null then 2 when 2 then 1.1 else 1 END", mysql.TypeNewDecimal, charset.CharsetBin},
		{"case c_int when null then 2 when 2 then 'tidb' else 1.1 END", mysql.TypeVarchar, "utf8"},
		// least() is the same as greatest()
		{"greatest('TiDB', 'D', 'd')", mysql.TypeVarString, "utf8"},
		{"greatest(1.1, 2.2)", mysql.TypeNewDecimal, charset.CharsetBin},
		{"greatest('TiDB', 3)", mysql.TypeVarString, charset.CharsetBin},
		{"greatest(c_decimal, c_double)", mysql.TypeDouble, charset.CharsetBin},
		{"greatest(c_int, c_int)", mysql.TypeLong, charset.CharsetBin},
		{"greatest(c_int, c_double)", mysql.TypeDouble, charset.CharsetBin},
		{"greatest(c_bigint, c_int, c_int)", mysql.TypeLonglong, charset.CharsetBin},
		{"greatest(c_varchar, c_text)", mysql.TypeBlob, "utf8"},
		{"greatest(c_binary, c_text)", mysql.TypeBlob, charset.CharsetBin},
		{"greatest(c_varchar, c_varbinary)", mysql.TypeVarString, charset.CharsetBin},
		{"greatest(c_enum, c_int)", mysql.TypeString, charset.CharsetBin},
		{"greatest(c_set, c_int)", mysql.TypeString, charset.CharsetBin},
		{"greatest(c_enum, c_set)", mysql.TypeString, "utf8"},
		{"interval(1, 2, 3)", mysql.TypeLonglong, charset.CharsetBin},
		{"interval(1.0, 2.0, 3.0)", mysql.TypeLonglong, charset.CharsetBin},
		{"interval('1', '2', '3')", mysql.TypeLonglong, charset.CharsetBin},
		{"round(null, 2)", mysql.TypeDouble, charset.CharsetBin},
		{"round('1.2', 2)", mysql.TypeDouble, charset.CharsetBin},
		{"round(1e2, 2)", mysql.TypeDouble, charset.CharsetBin},
		{"round(1.2, 2)", mysql.TypeNewDecimal, charset.CharsetBin},
		{"round(true, 2)", mysql.TypeLonglong, charset.CharsetBin},
		{"round(1000, 2)", mysql.TypeLonglong, charset.CharsetBin},
		{"hex('TiDB')", mysql.TypeVarString, "utf8"},
		{"hex(12)", mysql.TypeVarString, "utf8"},
		{"unhex('TiDB')", mysql.TypeVarString, "utf8"},
		{"unhex(12)", mysql.TypeVarString, "utf8"},
		{"DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y')", mysql.TypeVarString, "utf8"},
		{"rpad('TiDB', 12, 'go')", mysql.TypeVarString, charset.CharsetUTF8},
		{"lpad('TiDB', 12, 'go')", mysql.TypeVarString, charset.CharsetUTF8},
		{"elt(1, 'TiDB', 17, 'go')", mysql.TypeVarString, charset.CharsetUTF8},
		{"bit_length('TiDB')", mysql.TypeLonglong, charset.CharsetBin},
		{"char(66)", mysql.TypeVarString, charset.CharsetUTF8},
		{"char_length('TiDB')", mysql.TypeLonglong, charset.CharsetBin},
		{"character_length('TiDB')", mysql.TypeLonglong, charset.CharsetBin},
		{"crc32('TiDB')", mysql.TypeLonglong, charset.CharsetBin},
		{"timestampdiff(MINUTE,'2003-02-01','2003-05-01 12:05:55')", mysql.TypeLonglong, charset.CharsetBin},
		{"sign(0)", mysql.TypeLonglong, charset.CharsetBin},
		{"sign(null)", mysql.TypeLonglong, charset.CharsetBin},
		{"unix_timestamp()", mysql.TypeLonglong, charset.CharsetBin},
		{"unix_timestamp('2015-11-13 10:20:19')", mysql.TypeLonglong, charset.CharsetBin},
		{"floor(1.23)", mysql.TypeLonglong, charset.CharsetBin},
		{"field('foo', null)", mysql.TypeLonglong, charset.CharsetBin},
		{"find_in_set('foo', 'foo,bar')", mysql.TypeLonglong, charset.CharsetBin},
		{"find_in_set('foo', null)", mysql.TypeLonglong, charset.CharsetBin},
		{"find_in_set(null, 'bar')", mysql.TypeLonglong, charset.CharsetBin},
		{"conv('TiDB',36,10)", mysql.TypeVarString, charset.CharsetUTF8},
		{"timestamp('2003-12-31 12:00:00')", mysql.TypeDatetime, charset.CharsetBin},
		{"timestamp('2003-12-31 12:00:00','12:00:00')", mysql.TypeDatetime, charset.CharsetBin},
		{`aes_encrypt("pingcap", "fit2cloud@2014")`, mysql.TypeVarString, "utf8"},
		{`aes_decrypt("pingcap", "fit2cloud@2014")`, mysql.TypeVarString, "utf8"},
		{`md5(123)`, mysql.TypeVarString, "utf8"},
		{`sha1(123)`, mysql.TypeVarString, "utf8"},
		{`sha(123)`, mysql.TypeVarString, "utf8"},
		{`coalesce(null, 0)`, mysql.TypeLonglong, charset.CharsetBin},
		{`coalesce(null, 0.1)`, mysql.TypeNewDecimal, charset.CharsetBin},
		{`coalesce(1, "1" + 1)`, mysql.TypeDouble, charset.CharsetBin},
		{`coalesce(1, "abc")`, mysql.TypeVarString, charset.CharsetUTF8},
		{`any_value("abc")`, mysql.TypeVarString, charset.CharsetUTF8},
		{`any_value(1)`, mysql.TypeLonglong, charset.CharsetBin},
		{`any_value(1.234)`, mysql.TypeNewDecimal, charset.CharsetBin},
		{`degrees(1)`, mysql.TypeDouble, charset.CharsetBin},
		{`radians(90)`, mysql.TypeDouble, charset.CharsetBin},
		{`make_set(1 | 3, "hello", "nice", null, "world")`, mysql.TypeVarString, charset.CharsetUTF8},
		{`oct(12)`, mysql.TypeVarString, charset.CharsetUTF8},
		{`exp(1)`, mysql.TypeDouble, charset.CharsetBin},
		{`exp(1.23)`, mysql.TypeDouble, charset.CharsetBin},
		{`exp('1.23')`, mysql.TypeDouble, charset.CharsetBin},
		{`is_ipv6('FE80::AAAA:0000:00C2:0002')`, mysql.TypeLonglong, charset.CharsetBin},
		{`ord('2')`, mysql.TypeLonglong, charset.CharsetBin},
		{`ord(2)`, mysql.TypeLonglong, charset.CharsetBin},
		{`ord(true)`, mysql.TypeLonglong, charset.CharsetBin},
		{`ord(null)`, mysql.TypeLonglong, charset.CharsetBin},
	}
	for _, ca := range cases {
		ctx := testKit.Se.(context.Context)
		stmts, err := tidb.Parse(ctx, "select "+ca.expr+" from t")
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0].(*ast.SelectStmt)
		is := sessionctx.GetDomain(ctx).InfoSchema()
		err = plan.ResolveName(stmt, is, ctx)
		c.Assert(err, IsNil)
		plan.InferType(ctx.GetSessionVars().StmtCtx, stmt)
		tp := stmt.GetResultFields()[0].Column.Tp
		chs := stmt.GetResultFields()[0].Column.Charset
		c.Assert(tp, Equals, ca.tp, Commentf("Tp for %s", ca.expr))
		c.Assert(chs, Equals, ca.chs, Commentf("Charset for %s", ca.expr))
	}
}

func (s *testTypeInferrerSuite) TestColumnInfoModified(c *C) {
	defer testleak.AfterTest(c)()
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists tab0")
	testKit.MustExec("CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)")
	testKit.MustExec("SELECT + - (- CASE + col0 WHEN + CAST( col0 AS SIGNED ) THEN col1 WHEN 79 THEN NULL WHEN + - col1 THEN col0 / + col0 END ) * - 16 FROM tab0")
	ctx := testKit.Se.(context.Context)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	tbl, _ := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tab0"))
	col := table.FindCol(tbl.Cols(), "col1")
	c.Assert(col.Tp, Equals, mysql.TypeLong)
}

func newStoreWithBootstrap() (kv.Storage, error) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = tidb.BootstrapSession(store)
	return store, errors.Trace(err)
}
