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
		c_int_unsigned int unsigned,
		c_bigint bigint,
		c_bigint_unsigned bigint unsigned,
		c_float float,
		c_float_unsigned float unsigned,
		c_double double,
		c_double_unsigned double unsigned,
		c_decimal decimal(6, 3),
		c_decimal_unsigned decimal(10, 3) unsigned,
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
	tests = append(tests, s.createTestCase4InfoFunc()...)
	tests = append(tests, s.createTestCase4EncryptionFuncs()...)
	tests = append(tests, s.createTestCase4CompareFuncs()...)
	tests = append(tests, s.createTestCase4Miscellaneous()...)
	tests = append(tests, s.createTestCase4OpFuncs()...)
	tests = append(tests, s.createTestCase4OtherFuncs()...)
	tests = append(tests, s.createTestCase4TimeFuncs()...)

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
		{"elt(c_int, c_char, c_char, c_char)", mysql.TypeVarString, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},
		{"elt(c_int, c_char, c_char, c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"elt(c_int, c_char, c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},
		{"elt(c_int, c_char, c_double, c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 22, types.UnspecifiedLength},
		{"elt(c_int, c_char, c_double, c_int, c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 22, types.UnspecifiedLength},

		{"locate(c_char, c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"locate(c_binary, c_binary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"locate(c_char, c_binary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"locate(c_binary, c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"locate(c_char, c_char, c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"locate(c_char, c_binary, c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"locate(c_binary, c_char, c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"locate(c_binary, c_binary, c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},

		{"lpad('TiDB',   12,    'go'    )", mysql.TypeVarString, charset.CharsetUTF8, 0, 48, types.UnspecifiedLength},
		{"lpad(c_binary, 12,    'go'    )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 12, types.UnspecifiedLength},
		{"lpad(c_char,   c_int, c_binary)", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"lpad(c_char,   c_int, c_char  )", mysql.TypeLongBlob, charset.CharsetUTF8, 0, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"rpad('TiDB',   12,    'go'    )", mysql.TypeVarString, charset.CharsetUTF8, 0, 48, types.UnspecifiedLength},
		{"rpad(c_binary, 12,    'go'    )", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 12, types.UnspecifiedLength},
		{"rpad(c_char,   c_int, c_binary)", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"rpad(c_char,   c_int, c_char  )", mysql.TypeLongBlob, charset.CharsetUTF8, 0, mysql.MaxBlobWidth, types.UnspecifiedLength},

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

		{"char_length(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_float)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_double)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_datetime)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_time)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_varchar)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_text)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_binary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_varbinary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_blob)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_set)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"char_length(c_enum)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},

		{"character_length(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_float)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_double)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_datetime)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_time)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_varchar)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_text)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_binary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_varbinary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_blob)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_set)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"character_length(c_enum)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},

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

		{"instr(c_char, c_binary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 11, 0},
		{"instr(c_char, c_char  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 11, 0},
		{"instr(c_char, c_time  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 11, 0},

		{"reverse(c_int      )", mysql.TypeVarString, charset.CharsetUTF8, 0, 11, types.UnspecifiedLength},
		{"reverse(c_bigint   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 21, types.UnspecifiedLength},
		{"reverse(c_float    )", mysql.TypeVarString, charset.CharsetUTF8, 0, 12, types.UnspecifiedLength},
		{"reverse(c_double   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 22, types.UnspecifiedLength},
		{"reverse(c_decimal  )", mysql.TypeVarString, charset.CharsetUTF8, 0, 6, types.UnspecifiedLength},
		{"reverse(c_char     )", mysql.TypeString, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},
		{"reverse(c_varchar  )", mysql.TypeVarchar, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},
		{"reverse(c_text     )", mysql.TypeBlob, charset.CharsetUTF8, 0, 65535, types.UnspecifiedLength},
		{"reverse(c_binary   )", mysql.TypeString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"reverse(c_varbinary)", mysql.TypeVarchar, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"reverse(c_blob     )", mysql.TypeBlob, charset.CharsetBin, mysql.BinaryFlag, 65535, types.UnspecifiedLength},
		{"reverse(c_set      )", mysql.TypeSet, charset.CharsetUTF8, 0, types.UnspecifiedLength, types.UnspecifiedLength},
		{"reverse(c_enum     )", mysql.TypeEnum, charset.CharsetUTF8, 0, types.UnspecifiedLength, types.UnspecifiedLength},

		{"oct(c_int      )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_bigint   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_float    )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_double   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_decimal  )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_datetime )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_time     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_timestamp)", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_char     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_varchar  )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_text     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_binary   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_varbinary)", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_blob     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_set      )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"oct(c_enum     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
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
		{"pi()", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 8, 6},
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
		{"floor(c_int_unsigned)", mysql.TypeLonglong, charset.CharsetBin, mysql.UnsignedFlag | mysql.BinaryFlag, 11, 0},
		{"floor(c_bigint)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 21, 0},
		{"floor(c_bigint_unsigned)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 21, 0},
		{"floor(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"floor(c_decimal_unsigned)", mysql.TypeLonglong, charset.CharsetBin, mysql.UnsignedFlag | mysql.BinaryFlag, 10, 0},
		{"floor(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 22, 0},
		{"floor(c_double_unsigned)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 22, 0},
		{"floor(c_float)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 12, 0},
		{"floor(c_float_unsigned)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 12, 0},
		{"floor(c_datetime)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"floor(c_timestamp)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"floor(c_time)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"floor(c_enum)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"floor(c_text)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"floor(18446744073709551615)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 20, 0},
		{"floor(18446744073709551615.1)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 22, 0},

		{"ceil(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 11, 0},
		{"ceil(c_int_unsigned)", mysql.TypeLonglong, charset.CharsetBin, mysql.UnsignedFlag | mysql.BinaryFlag, 11, 0},
		{"ceil(c_bigint)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 21, 0},
		{"ceil(c_bigint_unsigned)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 21, 0},
		{"ceil(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"floor(c_decimal_unsigned)", mysql.TypeLonglong, charset.CharsetBin, mysql.UnsignedFlag | mysql.BinaryFlag, 10, 0},
		{"ceil(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 22, 0},
		{"floor(c_double_unsigned)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 22, 0},
		{"ceil(c_float)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 12, 0},
		{"floor(c_float_unsigned)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 12, 0},
		{"ceil(c_datetime)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceil(c_timestamp)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceil(c_time)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceil(c_enum)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceil(c_text)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceil(18446744073709551615)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 20, 0},
		{"ceil(18446744073709551615.1)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 22, 0},

		{"ceiling(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 11, 0},
		{"ceiling(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"ceiling(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 22, 0},
		{"ceiling(c_float)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 12, 0},
		{"ceiling(c_datetime)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceiling(c_time)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceiling(c_enum)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceiling(c_text)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"ceiling(18446744073709551615)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 20, 0},
		{"ceiling(18446744073709551615.1)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 22, 0},

		{"conv(c_char, c_int, c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},
		{"conv(c_int, c_int, c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 64, types.UnspecifiedLength},

		{"abs(c_int      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 11, types.UnspecifiedLength},
		{"abs(c_bigint   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 21, types.UnspecifiedLength},
		{"abs(c_float    )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 12, types.UnspecifiedLength}, // Should be 17.
		{"abs(c_double   )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 22, types.UnspecifiedLength}, // Should be 17.
		{"abs(c_decimal  )", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 6, 3},
		{"abs(c_datetime )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"abs(c_time     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"abs(c_timestamp)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"abs(c_char     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"abs(c_varchar  )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"abs(c_text     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"abs(c_binary   )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"abs(c_varbinary)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"abs(c_blob     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"abs(c_set      )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"abs(c_enum     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},

		{"round(c_int      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 11, 0},
		{"round(c_bigint   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 21, 0},
		{"round(c_float    )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 12, 0},    // Should be 17.
		{"round(c_double   )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 22, 0},    // Should be 17.
		{"round(c_decimal  )", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 6, 0}, // Should be 5.
		{"round(c_datetime )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"round(c_time     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"round(c_timestamp)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"round(c_char     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"round(c_varchar  )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"round(c_text     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"round(c_binary   )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"round(c_varbinary)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"round(c_blob     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"round(c_set      )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},
		{"round(c_enum     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, 0},

		{"truncate(c_int,   1)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 12, 0},
		{"truncate(c_int,  -5)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 12, 0},
		{"truncate(c_int, 100)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 12, 0},
		{"truncate(c_double,   1)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 24, 1},
		{"truncate(c_double,   5)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 28, 5},
		{"truncate(c_double, 100)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 53, 30},

		{"rand(     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"rand(c_int)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},

		{"pow(c_int,   c_int)   ", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"pow(c_float, c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"pow(c_int,   c_bigint)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},

		{"sign(c_int      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_bigint   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_float    )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_double   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_decimal  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_datetime )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_time     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_char     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_varchar  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_text     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_binary   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_varbinary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_blob     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_set      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"sign(c_enum     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},

		{"sqrt(c_int      )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_bigint   )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_float    )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_double   )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_decimal  )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_datetime )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_time     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_timestamp)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_char     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_varchar  )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_text     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_binary   )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_varbinary)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_blob     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_set      )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sqrt(c_enum     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},

		{"radians(c_int      )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"radians(c_bigint   )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"radians(c_float    )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength}, // Should be 17.
		{"radians(c_double   )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength}, // Should be 17.
		{"radians(c_decimal  )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength}, // Should be 5.
		{"radians(c_datetime )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"radians(c_time     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"radians(c_timestamp)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"radians(c_char     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"radians(c_varchar  )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"radians(c_text     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"radians(c_binary   )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"radians(c_varbinary)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"radians(c_blob     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"radians(c_set      )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"radians(c_enum     )", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
	}
}

func (s *testPlanSuite) createTestCase4ArithmeticFuncs() []typeInferTestCase {
	return []typeInferTestCase{
		{"c_int + c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int + c_bigint", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int + c_char", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_int + c_time", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int + c_double", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_int + c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 17, 3},
		{"c_datetime + c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_bigint + c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 27, 3},
		{"c_double + c_decimal", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_double + c_char", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_double + c_enum", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},

		{"c_int - c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int - c_bigint", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int - c_char", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_int - c_time", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int - c_double", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_int - c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 17, 3},
		{"c_datetime - c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_bigint - c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 27, 3},
		{"c_double - c_decimal", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_double - c_char", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_double - c_enum", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},

		{"c_int * c_int", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int * c_bigint", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int * c_char", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_int * c_time", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxIntWidth, 0},
		{"c_int * c_double", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_int * c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 17, 3},
		{"c_datetime * c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_bigint * c_decimal", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 27, 3},
		{"c_double * c_decimal", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_double * c_char", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
		{"c_double * c_enum", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, types.UnspecifiedLength, types.UnspecifiedLength},
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
		{"ifnull(c_int, c_int    )", mysql.TypeLong, charset.CharsetBin, mysql.BinaryFlag, 22, 0},
		{"ifnull(c_int, c_decimal)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 17, 3},
		{"if(c_int, c_decimal, c_int)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 15, 3},
		{"if(c_int, c_char, c_int)", mysql.TypeString, charset.CharsetUTF8, 0, 20, -1},
		{"if(c_int, c_binary, c_int)", mysql.TypeString, charset.CharsetBin, mysql.BinaryFlag, 20, -1},
		{"if(c_int, c_binary_char, c_int)", mysql.TypeString, charset.CharsetUTF8, mysql.BinaryFlag, 20, -1},
		{"if(c_int, c_char, c_decimal)", mysql.TypeString, charset.CharsetUTF8, 0, 20, 3},
		{"if(c_int, c_datetime, c_int)", mysql.TypeVarchar, charset.CharsetBin, mysql.BinaryFlag, 19, 2},
		{"if(c_int, c_int, c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 22, types.UnspecifiedLength},
		{"if(c_int, c_time, c_datetime)", mysql.TypeDatetime, charset.CharsetBin, mysql.BinaryFlag, 19, 2},
	}
}

func (s *testPlanSuite) createTestCase4Aggregations() []typeInferTestCase {
	return []typeInferTestCase{
		{"sum(c_int)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
	}
}

func (s *testPlanSuite) createTestCase4InfoFunc() []typeInferTestCase {
	return []typeInferTestCase{
		{"last_insert_id()", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, mysql.MaxIntWidth, 0},
		{"last_insert_id(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, mysql.MaxIntWidth, 0},
	}
}

func (s *testPlanSuite) createTestCase4EncryptionFuncs() []typeInferTestCase {
	return []typeInferTestCase{
		{"md5(c_int      )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_bigint   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_float    )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_double   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_decimal  )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_datetime )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_time     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_timestamp)", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_char     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_varchar  )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_text     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_binary   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_varbinary)", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_blob     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_set      )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(c_enum     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5('1234'     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"md5(1234       )", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},

		{"sha(c_int      )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_bigint   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_float    )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_double   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_decimal  )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_datetime )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_time     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_timestamp)", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_char     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_varchar  )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_text     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_binary   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_varbinary)", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_blob     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_set      )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(c_enum     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha('1234'     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha(1234       )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},

		{"sha1(c_int      )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_bigint   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_float    )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_double   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_decimal  )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_datetime )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_time     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_timestamp)", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_char     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_varchar  )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_text     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_binary   )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_varbinary)", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_blob     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_set      )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(c_enum     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1('1234'     )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},
		{"sha1(1234       )", mysql.TypeVarString, charset.CharsetUTF8, 0, 40, types.UnspecifiedLength},

		{"sha2(c_int      , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_bigint   , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_float    , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_double   , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_decimal  , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_datetime , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_time     , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_timestamp, 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_char     , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_varchar  , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_text     , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_binary   , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_varbinary, 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_blob     , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_set      , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_enum     , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2('1234'     , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(1234       , 0)", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},

		{"sha2(c_int      , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_bigint   , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_float    , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_double   , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_decimal  , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_datetime , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_time     , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_timestamp, '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_char     , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_varchar  , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_text     , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_binary   , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_varbinary, '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_blob     , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_set      , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(c_enum     , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2('1234'     , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},
		{"sha2(1234       , '256')", mysql.TypeVarString, charset.CharsetUTF8, 0, 128, types.UnspecifiedLength},

		{"AES_ENCRYPT(c_int, 'key')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, types.UnspecifiedLength},
		{"AES_ENCRYPT(c_char, 'key')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 32, types.UnspecifiedLength},
		{"AES_ENCRYPT(c_varchar, 'key')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 32, types.UnspecifiedLength},
		{"AES_ENCRYPT(c_binary, 'key')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 32, types.UnspecifiedLength},
		{"AES_ENCRYPT(c_varbinary, 'key')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 32, types.UnspecifiedLength},
		{"AES_ENCRYPT('', 'key')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, types.UnspecifiedLength},
		{"AES_ENCRYPT('111111', 'key')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, types.UnspecifiedLength},
		{"AES_ENCRYPT('111111111111111', 'key')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, types.UnspecifiedLength},
		{"AES_ENCRYPT('1111111111111111', 'key')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 32, types.UnspecifiedLength},
		{"AES_ENCRYPT('11111111111111111', 'key')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 32, types.UnspecifiedLength},

		{"AES_DECRYPT('1111111111111111', 'key')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, types.UnspecifiedLength},
		{"AES_DECRYPT('11111111111111112222222222222222', 'key')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 32, types.UnspecifiedLength},

		{"COMPRESS(c_int)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 24, types.UnspecifiedLength},
		{"COMPRESS(c_char)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 33, types.UnspecifiedLength},
		{"COMPRESS(c_binary_char)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 33, types.UnspecifiedLength},
		{"COMPRESS(c_varchar)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 33, types.UnspecifiedLength},
		{"COMPRESS(c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 33, types.UnspecifiedLength},
		{"COMPRESS(c_varbinary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 33, types.UnspecifiedLength},
		{"COMPRESS('')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 13, types.UnspecifiedLength},
		{"COMPRESS('abcde')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 18, types.UnspecifiedLength},

		{"UNCOMPRESS(c_int)", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"UNCOMPRESS(c_char)", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"UNCOMPRESS(c_binary_char)", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"UNCOMPRESS(c_varchar)", mysql.TypeLongBlob, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"UNCOMPRESSED_LENGTH(c_varchar)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 10, 0},
		{"UNCOMPRESSED_LENGTH(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 10, 0},

		{"RANDOM_BYTES(5)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 1024, types.UnspecifiedLength},
		{"RANDOM_BYTES('123')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 1024, types.UnspecifiedLength},
		{"RANDOM_BYTES('abc')", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 1024, types.UnspecifiedLength},
	}
}

func (s *testPlanSuite) createTestCase4CompareFuncs() []typeInferTestCase {
	return []typeInferTestCase{
		{"isnull(c_int      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_bigint   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_float    )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_double   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_decimal  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_datetime )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_time     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_char     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_varchar  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_text     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_binary   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_varbinary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_blob     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_set      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"isnull(c_enum     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},

		{"nullif(c_int      , 123)", mysql.TypeLong, charset.CharsetBin, mysql.BinaryFlag, 11, types.UnspecifiedLength},     // TODO: tp should be TypeLonglong, decimal should be 0
		{"nullif(c_bigint   , 123)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 21, types.UnspecifiedLength}, // TODO: flen should be 20, decimal should be 0
		{"nullif(c_float    , 123)", mysql.TypeFloat, charset.CharsetBin, mysql.BinaryFlag, 12, types.UnspecifiedLength},    // TODO: tp should be TypeDouble
		{"nullif(c_double   , 123)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 22, types.UnspecifiedLength},
		{"nullif(c_decimal  , 123)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 6, 3},
		{"nullif(c_datetime , 123)", mysql.TypeVarchar, charset.CharsetBin, mysql.BinaryFlag, 19, 2},                       // TODO: tp should be TypeVarString, binary flag
		{"nullif(c_time     , 123)", mysql.TypeVarchar, charset.CharsetBin, mysql.BinaryFlag, 10, 0},                       // TODO: tp should be TypeVarString, no binary flag
		{"nullif(c_timestamp, 123)", mysql.TypeVarchar, charset.CharsetBin, mysql.BinaryFlag, 19, types.UnspecifiedLength}, // TODO: tp should be TypeVarString, decimal should be 0, no binary flag
		{"nullif(c_char     , 123)", mysql.TypeString, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},
		{"nullif(c_varchar  , 123)", mysql.TypeVarchar, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},               // TODO: tp should be TypeVarString
		{"nullif(c_text     , 123)", mysql.TypeBlob, charset.CharsetUTF8, 0, 65535, types.UnspecifiedLength},               // TODO: tp should be TypeMediumBlob
		{"nullif(c_binary   , 123)", mysql.TypeString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},  // TODO: tp should be TypeVarString
		{"nullif(c_varbinary, 123)", mysql.TypeVarchar, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength}, // TODO: tp should be TypeVarString
		{"nullif(c_blob     , 123)", mysql.TypeBlob, charset.CharsetBin, mysql.BinaryFlag, 65535, types.UnspecifiedLength}, // TODO: tp should be TypeVarString
	}
}

func (s *testPlanSuite) createTestCase4Miscellaneous() []typeInferTestCase {
	return []typeInferTestCase{
		{"sleep(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 21, 0},
		{"sleep(c_float)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 21, 0},
		{"sleep(c_double)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 21, 0},
		{"sleep(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 21, 0},
		{"sleep(c_datetime)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 21, 0},
		{"sleep(c_time)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 21, 0},
		{"sleep(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 21, 0},
		{"sleep(c_binary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 21, 0},

		{"inet_aton(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, 21, 0},
		{"inet_aton(c_float)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, 21, 0},
		{"inet_aton(c_double)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, 21, 0},
		{"inet_aton(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, 21, 0},
		{"inet_aton(c_datetime)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, 21, 0},
		{"inet_aton(c_time)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, 21, 0},
		{"inet_aton(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, 21, 0},
		{"inet_aton(c_binary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag, 21, 0},

		{"inet_ntoa(c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 93, 0},
		{"inet_ntoa(c_float)", mysql.TypeVarString, charset.CharsetUTF8, 0, 93, 0},
		{"inet_ntoa(c_double)", mysql.TypeVarString, charset.CharsetUTF8, 0, 93, 0},
		{"inet_ntoa(c_decimal)", mysql.TypeVarString, charset.CharsetUTF8, 0, 93, 0},
		{"inet_ntoa(c_datetime)", mysql.TypeVarString, charset.CharsetUTF8, 0, 93, 0},
		{"inet_ntoa(c_time)", mysql.TypeVarString, charset.CharsetUTF8, 0, 93, 0},
		{"inet_ntoa(c_timestamp)", mysql.TypeVarString, charset.CharsetUTF8, 0, 93, 0},
		{"inet_ntoa(c_binary)", mysql.TypeVarString, charset.CharsetUTF8, 0, 93, 0},

		{"inet6_aton(c_int)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, 0},
		{"inet6_aton(c_float)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, 0},
		{"inet6_aton(c_double)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, 0},
		{"inet6_aton(c_decimal)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, 0},
		{"inet6_aton(c_datetime)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, 0},
		{"inet6_aton(c_time)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, 0},
		{"inet6_aton(c_timestamp)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, 0},
		{"inet6_aton(c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 16, 0},

		{"inet6_ntoa(c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 117, 0},
		{"inet6_ntoa(c_float)", mysql.TypeVarString, charset.CharsetUTF8, 0, 117, 0},
		{"inet6_ntoa(c_double)", mysql.TypeVarString, charset.CharsetUTF8, 0, 117, 0},
		{"inet6_ntoa(c_decimal)", mysql.TypeVarString, charset.CharsetUTF8, 0, 117, 0},
		{"inet6_ntoa(c_datetime)", mysql.TypeVarString, charset.CharsetUTF8, 0, 117, 0},
		{"inet6_ntoa(c_time)", mysql.TypeVarString, charset.CharsetUTF8, 0, 117, 0},
		{"inet6_ntoa(c_timestamp)", mysql.TypeVarString, charset.CharsetUTF8, 0, 117, 0},
		{"inet6_ntoa(c_binary)", mysql.TypeVarString, charset.CharsetUTF8, 0, 117, 0},

		{"is_ipv4(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4(c_float)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4(c_double)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4(c_datetime)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4(c_time)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4(c_binary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},

		{"is_ipv4_compat(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_compat(c_float)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_compat(c_double)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_compat(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_compat(c_datetime)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_compat(c_time)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_compat(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_compat(c_binary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},

		{"is_ipv4_mapped(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_mapped(c_float)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_mapped(c_double)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_mapped(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_mapped(c_datetime)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_mapped(c_time)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_mapped(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv4_mapped(c_binary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},

		{"is_ipv6(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv6(c_float)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv6(c_double)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv6(c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv6(c_datetime)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv6(c_time)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv6(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"is_ipv6(c_binary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},

		{"any_value(c_int)", mysql.TypeLong, charset.CharsetBin, mysql.BinaryFlag, 11, types.UnspecifiedLength},
		{"any_value(c_bigint)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 21, types.UnspecifiedLength},
		{"any_value(c_float)", mysql.TypeFloat, charset.CharsetBin, mysql.BinaryFlag, 12, types.UnspecifiedLength},
		{"any_value(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, 22, types.UnspecifiedLength},
		{"any_value(c_decimal)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 6, 3},
		{"any_value(c_datetime)", mysql.TypeDatetime, charset.CharsetBin, mysql.BinaryFlag, 19, 2},
		{"any_value(c_time)", mysql.TypeDuration, charset.CharsetBin, mysql.BinaryFlag, 10, 0},
		{"any_value(c_timestamp)", mysql.TypeDatetime, charset.CharsetBin, mysql.BinaryFlag, 19, types.UnspecifiedLength},
		{"any_value(c_char)", mysql.TypeString, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},
		{"any_value(c_binary_char)", mysql.TypeString, charset.CharsetUTF8, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"any_value(c_varchar)", mysql.TypeVarchar, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},
		{"any_value(c_text)", mysql.TypeBlob, charset.CharsetUTF8, 0, 65535, types.UnspecifiedLength},
		{"any_value(c_binary)", mysql.TypeString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"any_value(c_varbinary)", mysql.TypeVarchar, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"any_value(c_blob)", mysql.TypeBlob, charset.CharsetBin, mysql.BinaryFlag, 65535, types.UnspecifiedLength},
		{"any_value(c_set)", mysql.TypeSet, charset.CharsetUTF8, 0, types.UnspecifiedLength, types.UnspecifiedLength},
		{"any_value(c_enum)", mysql.TypeEnum, charset.CharsetUTF8, 0, types.UnspecifiedLength, types.UnspecifiedLength},
	}
}

func (s *testPlanSuite) createTestCase4OpFuncs() []typeInferTestCase {
	return []typeInferTestCase{
		{"c_int      is true", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_decimal  is true", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_double   is true", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_float    is true", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_datetime is true", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_time     is true", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_enum     is true", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_text     is true", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"18446      is true", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"1844674.1  is true", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},

		{"c_int      is false", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_decimal  is false", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_double   is false", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_float    is false", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_datetime is false", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_time     is false", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_enum     is false", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"c_text     is false", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"18446      is false", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"1844674.1  is false", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
	}
}

func (s *testPlanSuite) createTestCase4OtherFuncs() []typeInferTestCase {
	return []typeInferTestCase{
		{"1 in (c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"1 in (c_decimal)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"1 in (c_double)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"1 in (c_float)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"1 in (c_datetime)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"1 in (c_time)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"1 in (c_enum)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"1 in (c_text)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
	}
}

func (s *testPlanSuite) createTestCase4TimeFuncs() []typeInferTestCase {
	return []typeInferTestCase{
		{"time_format('150:02:28', '%r%r%r%r')", mysql.TypeVarString, charset.CharsetUTF8, 0, 44, types.UnspecifiedLength},
		{"time_format(123456, '%r%r%r%r')", mysql.TypeVarString, charset.CharsetUTF8, 0, 44, types.UnspecifiedLength},
		{"time_format('bad string', '%r%r%r%r')", mysql.TypeVarString, charset.CharsetUTF8, 0, 44, types.UnspecifiedLength},
		{"time_format(null, '%r%r%r%r')", mysql.TypeVarString, charset.CharsetUTF8, 0, 44, types.UnspecifiedLength},
		{"timestampadd(HOUR, c_int, c_timestamp)", mysql.TypeString, charset.CharsetUTF8, 0, 19, -1},
		{"timestampadd(minute, c_double, c_timestamp)", mysql.TypeString, charset.CharsetUTF8, 0, 19, -1},
		{"timestampadd(SeconD, c_int, c_char)", mysql.TypeString, charset.CharsetUTF8, 0, 19, -1},
		{"timestampadd(SeconD, c_varchar, c_time)", mysql.TypeString, charset.CharsetUTF8, 0, 19, -1},
		{"timestampadd(SeconD, c_int, c_datetime)", mysql.TypeString, charset.CharsetUTF8, 0, 19, -1},
		{"timestampadd(SeconD, c_double, c_binary_char)", mysql.TypeString, charset.CharsetUTF8, 0, 19, -1},
		{"timestampadd(SeconD, c_int, c_blob)", mysql.TypeString, charset.CharsetUTF8, 0, 19, -1},
		{"to_seconds(c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 20, 0},
		{"to_days(c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 20, 0},

		{"hour(c_int      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_bigint   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_float    )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_double   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_decimal  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_datetime )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_time     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_char     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_varchar  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_text     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_binary   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_varbinary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_blob     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_set      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"hour(c_enum     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},

		{"minute(c_int      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_bigint   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_float    )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_double   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_decimal  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_datetime )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_time     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_char     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_varchar  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_text     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_binary   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_varbinary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_blob     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_set      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"minute(c_enum     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},

		{"second(c_int      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_bigint   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_float    )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_double   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_decimal  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_datetime )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_time     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_char     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_varchar  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_text     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_binary   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_varbinary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_blob     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_set      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"second(c_enum     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},

		{"microsecond(c_int      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_bigint   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_float    )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_double   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_decimal  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_datetime )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_time     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_timestamp)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_char     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_varchar  )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_text     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_binary   )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_varbinary)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_blob     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_set      )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
		{"microsecond(c_enum     )", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 6, 0},
	}
}
