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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testExpressionSuite) TestGetTimeValue(c *C) {
	defer testleak.AfterTest(c)()
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
		{ast.NewValueExpr("2012-12-12 00:00:00"), "2012-12-12 00:00:00"},
		{ast.NewValueExpr(int64(0)), "0000-00-00 00:00:00"},
		{ast.NewValueExpr(nil), nil},
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

func (s *testExpressionSuite) TestIsCurrentTimestampExpr(c *C) {
	defer testleak.AfterTest(c)()
	v := IsCurrentTimestampExpr(ast.NewValueExpr("abc"))
	c.Assert(v, IsFalse)

	v = IsCurrentTimestampExpr(&ast.FuncCallExpr{FnName: model.NewCIStr("CURRENT_TIMESTAMP")})
	c.Assert(v, IsTrue)
}

func (s *testExpressionSuite) TestCurrentTimestampTimeZone(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()

	variable.SetSessionSystemVar(sessionVars, "timestamp", types.NewStringDatum("1234"))
	variable.SetSessionSystemVar(sessionVars, "time_zone", types.NewStringDatum("+00:00"))
	v, err := GetTimeValue(ctx, ast.CurrentTimestamp, mysql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)
	c.Assert(v.GetMysqlTime(), DeepEquals, types.Time{
		Time: types.FromDate(1970, 1, 1, 0, 20, 34, 0),
		Type: mysql.TypeTimestamp})

	// CurrentTimestamp from "timestamp" session variable is based on UTC, so change timezone
	// would get different value.
	variable.SetSessionSystemVar(sessionVars, "time_zone", types.NewStringDatum("+08:00"))
	v, err = GetTimeValue(ctx, ast.CurrentTimestamp, mysql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)
	c.Assert(v.GetMysqlTime(), DeepEquals, types.Time{
		Time: types.FromDate(1970, 1, 1, 8, 20, 34, 0),
		Type: mysql.TypeTimestamp})
}
