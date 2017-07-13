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
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

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
		c_varchar varchar(20),
		c_text text,
		c_binary binary(20),
		c_varbinary varbinary(20),
		c_blob blob,
		c_set set('a', 'b', 'c'),
		c_enum enum('a', 'b', 'c'))`
	testKit.MustExec(sql)

	tests := []struct {
		sql     string
		tp      byte
		chs     string
		flag    byte
		flen    int
		decimal int
	}{
		{"sum(c_int)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"strcmp(c_char, c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 2, 0},
		{"concat(c_binary, c_int)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 31, types.UnspecifiedLength},
		{"cos(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"sin(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"tan(c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"ascii(c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 3, 0},
		{"c_int", mysql.TypeLong, charset.CharsetBin, mysql.BinaryFlag, 11, types.UnspecifiedLength},
		{"c_char", mysql.TypeString, charset.CharsetUTF8, 0, 20, types.UnspecifiedLength},
		{"c_enum", mysql.TypeEnum, charset.CharsetUTF8, 0, types.UnspecifiedLength, types.UnspecifiedLength},
		{"1", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"1.23", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag, 4, 2},
		{"'1234'", mysql.TypeVarString, charset.CharsetUTF8, 0, 4, types.UnspecifiedLength},
		{"c_int like 'abc%'", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 1, 0},
		{"password(c_char)", mysql.TypeVarString, charset.CharsetUTF8, 0, mysql.PWDHashLen + 1, types.UnspecifiedLength},
		{"ord(c_char)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag, 10, 0},
		{"log2(c_int)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"log10(c_int)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"degrees(c_int)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag, mysql.MaxRealWidth, types.UnspecifiedLength},
		{"md5(c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 32, types.UnspecifiedLength},
		{"space(c_int)", mysql.TypeLongBlob, charset.CharsetUTF8, 0, mysql.MaxBlobWidth, types.UnspecifiedLength},
		{"CONCAT('T', 'i', 'DB')", mysql.TypeVarString, charset.CharsetUTF8, 0, 4, types.UnspecifiedLength},
		{"CONCAT('T', 'i', 'DB', c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 24, types.UnspecifiedLength},
		{"CONCAT_WS('-', 'T', 'i', 'DB')", mysql.TypeVarString, charset.CharsetUTF8, 0, 6, types.UnspecifiedLength},
		{"CONCAT_WS(',', 'TiDB', c_binary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 25, types.UnspecifiedLength},
		{"left(c_int, c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 11, types.UnspecifiedLength},
		{"right(c_int, c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 11, types.UnspecifiedLength},
		{"lower(c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 11, types.UnspecifiedLength},
		{"upper(c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 11, types.UnspecifiedLength},
		{"replace(1234, 2, 55)", mysql.TypeVarString, charset.CharsetUTF8, 0, 8, types.UnspecifiedLength},
		{"replace(c_binary, 1, 2)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
		{"to_base64(c_binary)", mysql.TypeVarString, charset.CharsetUTF8, 0, 28, types.UnspecifiedLength},
		{"substr(c_int, c_int)", mysql.TypeVarString, charset.CharsetUTF8, 0, 11, types.UnspecifiedLength},
		{"substr(c_binary, c_int)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag, 20, types.UnspecifiedLength},
	}
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
