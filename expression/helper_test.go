// Copyright 2016 PingCAP, Inc.
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

package expression

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/varsutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func (s *testExpressionSuite) TestGetTimeValue(c *C) {
	defer testleak.AfterTest(c)()
	v, err := GetTimeValue(nil, "2012-12-12 00:00:00", mysql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)

	c.Assert(v.Kind(), Equals, types.KindMysqlTime)
	timeValue := v.GetMysqlTime()
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()
	varsutil.SetSessionSystemVar(sessionVars, "timestamp", types.NewStringDatum(""))
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)

	c.Assert(v.Kind(), Equals, types.KindMysqlTime)
	timeValue = v.GetMysqlTime()
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	varsutil.SetSessionSystemVar(sessionVars, "timestamp", types.NewStringDatum("0"))
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)

	c.Assert(v.Kind(), Equals, types.KindMysqlTime)
	timeValue = v.GetMysqlTime()
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	varsutil.SetSessionSystemVar(sessionVars, "timestamp", types.Datum{})
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)

	c.Assert(v.Kind(), Equals, types.KindMysqlTime)
	timeValue = v.GetMysqlTime()
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	varsutil.SetSessionSystemVar(sessionVars, "timestamp", types.NewStringDatum("1234"))

	tbl := []struct {
		Expr interface{}
		Ret  interface{}
	}{
		{"2012-12-12 00:00:00", "2012-12-12 00:00:00"},
		{CurrentTimestamp, time.Unix(1234, 0).Format(types.TimeFormat)},
		{ZeroTimestamp, "0000-00-00 00:00:00"},
		{ast.NewValueExpr("2012-12-12 00:00:00"), "2012-12-12 00:00:00"},
		{ast.NewValueExpr(int64(0)), "0000-00-00 00:00:00"},
		{ast.NewValueExpr(nil), nil},
		{&ast.FuncCallExpr{FnName: model.NewCIStr(CurrentTimestamp)}, CurrentTimestamp},
		//{&ast.UnaryOperationExpr{Op: opcode.Minus, V: ast.NewValueExpr(int64(0))}, "0000-00-00 00:00:00"},
	}

	for i, t := range tbl {
		comment := Commentf("expr: %d", i)
		v, err := GetTimeValue(ctx, t.Expr, mysql.TypeTimestamp, types.MinFsp)
		c.Assert(err, IsNil)

		switch v.Kind() {
		case types.KindMysqlTime:
			c.Assert(v.GetMysqlTime().String(), DeepEquals, t.Ret, comment)
		default:
			c.Assert(v.GetValue(), DeepEquals, t.Ret, comment)
		}
	}

	errTbl := []struct {
		Expr interface{}
	}{
		{"2012-13-12 00:00:00"},
		{ast.NewValueExpr("2012-13-12 00:00:00")},
		{ast.NewValueExpr(int64(1))},
		{&ast.FuncCallExpr{FnName: model.NewCIStr("xxx")}},
		//{&ast.UnaryOperationExpr{Op: opcode.Minus, V: ast.NewValueExpr(int64(1))}},
	}

	for _, t := range errTbl {
		_, err := GetTimeValue(ctx, t.Expr, mysql.TypeTimestamp, types.MinFsp)
		c.Assert(err, NotNil)
	}
}

func (s *testExpressionSuite) TestIsCurrentTimeExpr(c *C) {
	defer testleak.AfterTest(c)()
	v := IsCurrentTimeExpr(ast.NewValueExpr("abc"))
	c.Assert(v, IsFalse)

	v = IsCurrentTimeExpr(&ast.FuncCallExpr{FnName: model.NewCIStr("CURRENT_TIMESTAMP")})
	c.Assert(v, IsTrue)
}
