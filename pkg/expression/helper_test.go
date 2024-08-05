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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestGetTimeValue(t *testing.T) {
	ctx := mock.NewContext()
	v, err := GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, types.MinFsp, nil)
	require.NoError(t, err)

	require.Equal(t, types.KindMysqlTime, v.Kind())
	timeValue := v.GetMysqlTime()
	require.Equal(t, "2012-12-12 00:00:00", timeValue.String())

	sessionVars := ctx.GetSessionVars()
	err = sessionVars.SetSystemVar("timestamp", "0")
	require.NoError(t, err)
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, types.MinFsp, nil)
	require.NoError(t, err)

	require.Equal(t, types.KindMysqlTime, v.Kind())
	timeValue = v.GetMysqlTime()
	require.Equal(t, "2012-12-12 00:00:00", timeValue.String())

	err = sessionVars.SetSystemVar("timestamp", "0")
	require.NoError(t, err)
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, types.MinFsp, nil)
	require.NoError(t, err)

	require.Equal(t, types.KindMysqlTime, v.Kind())
	timeValue = v.GetMysqlTime()
	require.Equal(t, "2012-12-12 00:00:00", timeValue.String())

	err = sessionVars.SetSystemVar("timestamp", "")
	require.Error(t, err, "Incorrect argument type to variable 'timestamp'")
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, types.MinFsp, nil)
	require.NoError(t, err)

	require.Equal(t, types.KindMysqlTime, v.Kind())
	timeValue = v.GetMysqlTime()
	require.Equal(t, "2012-12-12 00:00:00", timeValue.String())

	// trigger the stmt context cache.
	err = sessionVars.SetSystemVar("timestamp", "0")
	require.NoError(t, err)

	v1, err := GetTimeCurrentTimestamp(ctx, mysql.TypeTimestamp, types.MinFsp)
	require.NoError(t, err)

	v2, err := GetTimeCurrentTimestamp(ctx, mysql.TypeTimestamp, types.MinFsp)
	require.NoError(t, err)

	require.Equal(t, v1, v2)

	err = sessionVars.SetSystemVar("timestamp", "1234")
	require.NoError(t, err)

	tbls := []struct {
		Expr any
		Ret  any
	}{
		{"2012-12-12 00:00:00", "2012-12-12 00:00:00"},
		{ast.CurrentTimestamp, time.Unix(1234, 0).In(ctx.GetSessionVars().TimeZone).Format(types.TimeFormat)},
		{types.ZeroDatetimeStr, "0000-00-00 00:00:00"},
		{ast.NewValueExpr("2012-12-12 00:00:00", charset.CharsetUTF8MB4, charset.CollationUTF8MB4), "2012-12-12 00:00:00"},
		{ast.NewValueExpr(int64(0), "", ""), "0000-00-00 00:00:00"},
		{ast.NewValueExpr(nil, "", ""), nil},
		{&ast.FuncCallExpr{FnName: model.NewCIStr(ast.CurrentTimestamp)}, strings.ToUpper(ast.CurrentTimestamp)},
		// {&ast.UnaryOperationExpr{Op: opcode.Minus, V: ast.NewValueExpr(int64(0))}, "0000-00-00 00:00:00"},
	}

	for i, tbl := range tbls {
		comment := fmt.Sprintf("expr: %d", i)
		v, err := GetTimeValue(ctx, tbl.Expr, mysql.TypeTimestamp, types.MinFsp, nil)
		require.NoError(t, err)

		switch v.Kind() {
		case types.KindMysqlTime:
			require.EqualValues(t, tbl.Ret, v.GetMysqlTime().String(), comment)
		default:
			require.EqualValues(t, tbl.Ret, v.GetValue(), comment)
		}
	}

	errTbl := []struct {
		Expr any
	}{
		{"2012-13-12 00:00:00"},
		{ast.NewValueExpr("2012-13-12 00:00:00", charset.CharsetUTF8MB4, charset.CollationUTF8MB4)},
		{ast.NewValueExpr(int64(1), "", "")},
		{&ast.FuncCallExpr{FnName: model.NewCIStr("xxx")}},
		// {&ast.UnaryOperationExpr{Op: opcode.Minus, V: ast.NewValueExpr(int64(1))}},
	}

	for _, tbl := range errTbl {
		_, err := GetTimeValue(ctx, tbl.Expr, mysql.TypeTimestamp, types.MinFsp, nil)
		require.Error(t, err)
	}
}

func TestIsCurrentTimestampExpr(t *testing.T) {
	buildTimestampFuncCallExpr := func(i int64) *ast.FuncCallExpr {
		var args []ast.ExprNode
		if i != 0 {
			args = []ast.ExprNode{&driver.ValueExpr{Datum: types.NewIntDatum(i)}}
		}
		return &ast.FuncCallExpr{FnName: model.NewCIStr("CURRENT_TIMESTAMP"), Args: args}
	}

	v := IsValidCurrentTimestampExpr(ast.NewValueExpr("abc", charset.CharsetUTF8MB4, charset.CollationUTF8MB4), nil)
	require.False(t, v)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(0), nil)
	require.True(t, v)
	ft := &types.FieldType{}
	ft.SetDecimal(3)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(3), ft)
	require.True(t, v)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(1), ft)
	require.False(t, v)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(0), ft)
	require.False(t, v)

	ft1 := &types.FieldType{}
	ft1.SetDecimal(0)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(2), ft1)
	require.False(t, v)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(2), nil)
	require.False(t, v)
}

func TestCurrentTimestampTimeZone(t *testing.T) {
	ctx := mock.NewContext()
	sessionVars := ctx.GetSessionVars()

	err := sessionVars.SetSystemVar("timestamp", "1234")
	require.NoError(t, err)
	err = sessionVars.SetSystemVar("time_zone", "+00:00")
	require.NoError(t, err)
	sessionVars.StmtCtx.SetTimeZone(sessionVars.Location())
	v, err := GetTimeValue(ctx, ast.CurrentTimestamp, mysql.TypeTimestamp, types.MinFsp, nil)
	require.NoError(t, err)
	require.EqualValues(t, types.NewTime(
		types.FromDate(1970, 1, 1, 0, 20, 34, 0),
		mysql.TypeTimestamp, types.DefaultFsp),
		v.GetMysqlTime())

	// CurrentTimestamp from "timestamp" session variable is based on UTC, so change timezone
	// would get different value.
	err = sessionVars.SetSystemVar("time_zone", "+08:00")
	require.NoError(t, err)
	sessionVars.StmtCtx.SetTimeZone(sessionVars.Location())
	v, err = GetTimeValue(ctx, ast.CurrentTimestamp, mysql.TypeTimestamp, types.MinFsp, nil)
	require.NoError(t, err)
	require.EqualValues(t, types.NewTime(
		types.FromDate(1970, 1, 1, 8, 20, 34, 0),
		mysql.TypeTimestamp, types.DefaultFsp),
		v.GetMysqlTime())
}
