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
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/sessionctx/variable"
	"github.com/pingcap/tidb/v4/types"
	driver "github.com/pingcap/tidb/v4/types/parser_driver"
	"github.com/pingcap/tidb/v4/util/mock"
)

func (s *testExpressionSuite) TestGetTimeValue(c *C) {
	ctx := mock.NewContext()
	v, err := GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)

	c.Assert(v.Kind(), Equals, types.KindMysqlTime)
	timeValue := v.GetMysqlTime()
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")
	sessionVars := ctx.GetSessionVars()
	variable.SetSessionSystemVar(sessionVars, "timestamp", types.NewStringDatum(""))
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)

	c.Assert(v.Kind(), Equals, types.KindMysqlTime)
	timeValue = v.GetMysqlTime()
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	variable.SetSessionSystemVar(sessionVars, "timestamp", types.NewStringDatum("0"))
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)

	c.Assert(v.Kind(), Equals, types.KindMysqlTime)
	timeValue = v.GetMysqlTime()
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	variable.SetSessionSystemVar(sessionVars, "timestamp", types.Datum{})
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)

	c.Assert(v.Kind(), Equals, types.KindMysqlTime)
	timeValue = v.GetMysqlTime()
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	variable.SetSessionSystemVar(sessionVars, "timestamp", types.NewStringDatum("1234"))

	tbl := []struct {
		Expr interface{}
		Ret  interface{}
	}{
		{"2012-12-12 00:00:00", "2012-12-12 00:00:00"},
		{ast.CurrentTimestamp, time.Unix(1234, 0).Format(types.TimeFormat)},
		{types.ZeroDatetimeStr, "0000-00-00 00:00:00"},
		{ast.NewValueExpr("2012-12-12 00:00:00", charset.CharsetUTF8MB4, charset.CollationUTF8MB4), "2012-12-12 00:00:00"},
		{ast.NewValueExpr(int64(0), "", ""), "0000-00-00 00:00:00"},
		{ast.NewValueExpr(nil, "", ""), nil},
		{&ast.FuncCallExpr{FnName: model.NewCIStr(ast.CurrentTimestamp)}, strings.ToUpper(ast.CurrentTimestamp)},
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
		{ast.NewValueExpr("2012-13-12 00:00:00", charset.CharsetUTF8MB4, charset.CollationUTF8MB4)},
		{ast.NewValueExpr(int64(1), "", "")},
		{&ast.FuncCallExpr{FnName: model.NewCIStr("xxx")}},
		//{&ast.UnaryOperationExpr{Op: opcode.Minus, V: ast.NewValueExpr(int64(1))}},
	}

	for _, t := range errTbl {
		_, err := GetTimeValue(ctx, t.Expr, mysql.TypeTimestamp, types.MinFsp)
		c.Assert(err, NotNil)
	}
}

func (s *testExpressionSuite) TestIsCurrentTimestampExpr(c *C) {
	buildTimestampFuncCallExpr := func(i int64) *ast.FuncCallExpr {
		var args []ast.ExprNode
		if i != 0 {
			args = []ast.ExprNode{&driver.ValueExpr{Datum: types.NewIntDatum(i)}}
		}
		return &ast.FuncCallExpr{FnName: model.NewCIStr("CURRENT_TIMESTAMP"), Args: args}
	}

	v := IsValidCurrentTimestampExpr(ast.NewValueExpr("abc", charset.CharsetUTF8MB4, charset.CollationUTF8MB4), nil)
	c.Assert(v, IsFalse)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(0), nil)
	c.Assert(v, IsTrue)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(3), &types.FieldType{Decimal: 3})
	c.Assert(v, IsTrue)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(1), &types.FieldType{Decimal: 3})
	c.Assert(v, IsFalse)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(0), &types.FieldType{Decimal: 3})
	c.Assert(v, IsFalse)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(2), &types.FieldType{Decimal: 0})
	c.Assert(v, IsFalse)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(2), nil)
	c.Assert(v, IsFalse)
}

func (s *testExpressionSuite) TestCurrentTimestampTimeZone(c *C) {
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()

	variable.SetSessionSystemVar(sessionVars, "timestamp", types.NewStringDatum("1234"))
	variable.SetSessionSystemVar(sessionVars, "time_zone", types.NewStringDatum("+00:00"))
	v, err := GetTimeValue(ctx, ast.CurrentTimestamp, mysql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)
	c.Assert(v.GetMysqlTime(), DeepEquals, types.NewTime(
		types.FromDate(1970, 1, 1, 0, 20, 34, 0),
		mysql.TypeTimestamp, types.DefaultFsp))

	// CurrentTimestamp from "timestamp" session variable is based on UTC, so change timezone
	// would get different value.
	variable.SetSessionSystemVar(sessionVars, "time_zone", types.NewStringDatum("+08:00"))
	v, err = GetTimeValue(ctx, ast.CurrentTimestamp, mysql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)
	c.Assert(v.GetMysqlTime(), DeepEquals, types.NewTime(
		types.FromDate(1970, 1, 1, 8, 20, 34, 0),
		mysql.TypeTimestamp, types.DefaultFsp))
}
