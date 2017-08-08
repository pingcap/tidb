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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

type typeInferTestCase struct {
	sql     string
	tp      byte
	chs     string
	flag    byte
	flen    int
	decimal int
}

func (s *testPlanSuite) TestInferType(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)
	defer func() {
		testleak.AfterTest(c)()
	}()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	sql := `create table t (
		c_int int,
		c_bigint bigint,
		c_float float,
		c_double double,
		c_decimal decimal(6, 3),
		c_datetime datetime(2),
		c_time time,
		c_timestamp timestamp,
		c_char char(20),
		c_binary_char char(20) binary,
		c_varchar varchar(20),
		c_text text,
		c_binary binary(20),
		c_varbinary varbinary(20),
		c_blob blob,
		c_set set('a', 'b', 'c'),
		c_enum enum('a', 'b', 'c'))`
	testKit.MustExec(sql)

	tests := []typeInferTestCase{}
	tests = append(tests, s.createTestCase4Constants()...)
	tests = append(tests, s.createTestCase4Columns()...)
	tests = append(tests, s.createTestCase4StrFuncs()...)
	tests = append(tests, s.createTestCase4MathFuncs()...)
	tests = append(tests, s.createTestCase4ArithmeticFuncs()...)
	tests = append(tests, s.createTestCase4LogicalFuncs()...)
	tests = append(tests, s.createTestCase4ControlFuncs()...)
	tests = append(tests, s.createTestCase4Aggregations()...)

	for _, tt := range tests {
		ctx := testKit.Se.(context.Context)
		sql := "select " + tt.sql + " from t"
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn()
		c.Assert(err, IsNil)

		is := sessionctx.GetDomain(ctx).InfoSchema()
		err = plan.ResolveName(stmt, is, ctx)
		c.Assert(err, IsNil)
		p, err := plan.BuildLogicalPlan(ctx, stmt, is)
		c.Assert(err, IsNil)
		tp := p.Schema().Columns[0].RetType

		c.Assert(tp.Tp, Equals, tt.tp, comment)
		c.Assert(tp.Charset, Equals, tt.chs, comment)
		c.Assert(tp.Flag^uint(tt.flag), Equals, uint(0x0), comment)
		c.Assert(tp.Flen, Equals, tt.flen, comment)
		c.Assert(tp.Decimal, Equals, tt.decimal, comment)
	}
}

func (s *testPlanSuite) createTestCase4Constants() []typeInferTestCase {
	return []typeInferTestCase{
		{"1", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"1.23", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 4, 2},
		{"'1234'", mysql.TypeVarString, charset.CharsetUTF8, 0, 4, types.UnspecifiedLength},
	}
}

func (s *testPlanSuite) createTestCase4Columns() []typeInferTestCase {
	return []typeInferTestCase{
		{"c_int", mysql.TypeLong, charset.CharsetBin, mysql.BinaryFlag, 11, types.UnspecifiedLength},
		{"c_char", mysql.TypeString, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},
		{"c_enum", mysql.TypeEnum, charset.CharsetUTF8, 0, types.UnspecifiedLength, types.UnspecifiedLength},
	}
}

func (s *testPlanSuite) createTestCase4StrFuncs() []typeInferTestCase {
	return []typeInferTestCase{
		{"strcmp(c_char, c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"md5(c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"space(c_int)", mysql.TypeLongBlob, charset.CharsetUTF8, 0, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"CONCAT(c_binary, c_int)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 31, types.UnspecifiedLength},
		{"CONCAT(c_binary_char, c_int)", mysql.TypeVarString, charset.CharsetUTF8, mysql.BinaryFlag, 31, types.UnspecifiedLength},
		{"CONCAT('T', 'i', 'DB')", mysql.TypeVarString, charset.CharsetUTF8, 0, 4, types.UnspecifiedLength},
		{"CONCAT('T', 'i', 'DB', c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 24, types.UnspecifiedLength},
		{"CONCAT_WS('-', 'T', 'i', 'DB')", mysql.TypeVarString, charset.CharsetUTF8, 0, 6, types.UnspecifiedLength},
		{"CONCAT_WS(',', 'TiDB', c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 25, types.UnspecifiedLength},
		{"left(c_int, c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 11, types.UnspecifiedLength},
		{"right(c_int, c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 11, types.UnspecifiedLength},
		{"lower(c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 11, types.UnspecifiedLength},
		{"lower(c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"upper(c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 11, types.UnspecifiedLength},
		{"upper(c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"replace(1234, 2, 55)", mysql.TypeVarString, charset.CharsetUTF8, 0, 8, types.UnspecifiedLength},
		{"replace(c_binary, 1, 2)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"to_base64(c_binary)", mysql.TypeVarString, charset.CharsetUTF8, 0, 28, types.UnspecifiedLength},
		{"substr(c_int, c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 11, types.UnspecifiedLength},
		{"substr(c_binary, c_int)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"uuid()", mysql.TypeVarString, charset.CharsetUTF8, 0, 36, types.UnspecifiedLength},
		{"bit_length(c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 10, 0},
		{"substring_index(c_int, '.', 1)", mysql.TypeVarString, charset.CharsetUTF8, 0, 11, types.UnspecifiedLength},
		{"substring_index(c_binary, '.', 1)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"hex(c_char)", mysql.TypeVarString, charset.CharsetUTF8, 0, 120, types.UnspecifiedLength},
		{"hex(c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 22, types.UnspecifiedLength},
		{"unhex(c_int)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 6, types.UnspecifiedLength},
		{"unhex(c_char)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 30, types.UnspecifiedLength},
		{"ltrim(c_char)", mysql.TypeVarString, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},
		{"ltrim(c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"rtrim(c_char)", mysql.TypeVarString, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},
		{"rtrim(c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"trim(c_char)", mysql.TypeVarString, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},
		{"trim(c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"ascii(c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"ord(c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 10, 0},
		{"c_int like 'abc%'", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"tidb_version()", mysql.TypeVarString, charset.CharsetUTF8, 0, len(printer.GetTiDBInfo()), types.UnspecifiedLength},
		{"password(c_char)", mysql.TypeVarString, charset.CharsetUTF8, 0, mysql.PWDHashLen + 1, types.UnspecifiedLength},

		{"from_base64(c_int      )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_bigint   )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_float    )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_double   )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_decimal  )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_datetime )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_time     )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_timestamp)", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_char     )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_varchar  )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_text     )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_binary   )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_varbinary)", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_blob     )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_set      )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"from_base64(c_enum     )", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},

		{"bin(c_int      )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_bigint   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_float    )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_double   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_decimal  )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_datetime )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_time     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_timestamp)", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_char     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_varchar  )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_text     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_binary   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_varbinary)", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_blob     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_set      )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"bin(c_enum     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},

		{"char(c_int      )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_bigint   )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_float    )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_double   )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_decimal  )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_datetime )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_time     )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_timestamp)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_char     )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_varchar  )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_text     )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_binary   )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_varbinary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_blob     )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_set      )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_enum     )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 4, types.UnspecifiedLength},
		{"char(c_int      , c_int       using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_bigint   , c_bigint    using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_float    , c_float     using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_double   , c_double    using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_decimal  , c_decimal   using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_datetime , c_datetime  using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_time     , c_time      using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_timestamp, c_timestamp using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_char     , c_char      using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_varchar  , c_varchar   using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_text     , c_text      using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_binary   , c_binary    using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_varbinary, c_varbinary using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_blob     , c_blob      using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_set      , c_set       using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
		{"char(c_enum     , c_enum      using utf8)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 8, types.UnspecifiedLength},
	}
}

func (s *testPlanSuite) createTestCase4MathFuncs() []typeInferTestCase {
	return []typeInferTestCase{
		{"cos(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sin(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"tan(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"exp(c_int)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"exp(c_float)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"exp(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"exp(c_decimal)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"exp(c_datetime)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"exp(c_time)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"exp(c_timestamp)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"exp(c_binary)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"pi()", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 17, 15},
		{"~c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, mysql.MaxIntWidth, 0},
		{"!c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_int & c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, mysql.MaxIntWidth, 0},
		{"c_int | c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, mysql.MaxIntWidth, 0},
		{"c_int ^ c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, mysql.MaxIntWidth, 0},
		{"c_int << c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, mysql.MaxIntWidth, 0},
		{"c_int >> c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, mysql.MaxIntWidth, 0},
		{"log2(c_int)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"log10(c_int)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"log(c_int)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"log(2, c_int)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"degrees(c_int)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"atan(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"atan(c_double,c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"asin(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"acos(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},

		{"cot(c_int)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"cot(c_float)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"cot(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"cot(c_decimal)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"cot(c_datetime)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"cot(c_time)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"cot(c_timestamp)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"cot(c_binary)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},

		{"floor(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 11, 0},
		{"floor(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"floor(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, 0},
		{"floor(c_float)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, 0},
		{"floor(c_datetime)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"floor(c_time)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"floor(c_enum)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"floor(c_text)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"floor(18446744073709551615)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 20, 0},
		{"floor(18446744073709551615.1)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 22, 0},

		{"ceil(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 11, 0},
		{"ceil(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"ceil(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, 0},
		{"ceil(c_float)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, 0},
		{"ceil(c_datetime)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceil(c_time)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceil(c_enum)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceil(c_text)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceil(18446744073709551615)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 20, 0},
		{"ceil(18446744073709551615.1)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 22, 0},

		{"ceiling(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 11, 0},
		{"ceiling(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"ceiling(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, 0},
		{"ceiling(c_float)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, 0},
		{"ceiling(c_datetime)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceiling(c_time)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceiling(c_enum)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceiling(c_text)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceiling(18446744073709551615)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 20, 0},
		{"ceiling(18446744073709551615.1)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 22, 0},
	}
}

func (s *testPlanSuite) createTestCase4ArithmeticFuncs() []typeInferTestCase {
	return []typeInferTestCase{
		{"c_int + c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int + c_bigint", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int + c_char", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_int + c_time", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int + c_double", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_int + c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_datetime + c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_bigint + c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_double + c_decimal", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_double + c_char", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_double + c_enum", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},

		{"c_int - c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int - c_bigint", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int - c_char", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_int - c_time", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int - c_double", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_int - c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_datetime - c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_bigint - c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_double - c_decimal", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_double - c_char", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_double - c_enum", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
	}
}

func (s *testPlanSuite) createTestCase4LogicalFuncs() []typeInferTestCase {
	return []typeInferTestCase{
		{"c_int and c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_int xor c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},

		{"c_int && c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_int || c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
	}
}

func (s *testPlanSuite) createTestCase4ControlFuncs() []typeInferTestCase {
	return []typeInferTestCase{
		{"ifnull(c_int, c_int)", mysql.TypeLong, charset.CharsetBin, mysql.BinaryFlag, 22, 0},
		{"ifnull(c_int, c_decimal)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 17, 3},
	}
}

func (s *testPlanSuite) createTestCase4Aggregations() []typeInferTestCase {
	return []typeInferTestCase{
		{"sum(c_int)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
	}
}
