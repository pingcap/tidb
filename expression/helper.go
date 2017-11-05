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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/varsutil"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
)

func boolToInt64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

// IsCurrentTimestampExpr returns whether e is CurrentTimestamp expression.
func IsCurrentTimestampExpr(e ast.ExprNode) bool {
	if fn, ok := e.(*ast.FuncCallExpr); ok && fn.FnName.L == ast.CurrentTimestamp {
		return true
	}
	return false
}

// GetTimeValue gets the time value with type tp.
func GetTimeValue(ctx context.Context, v interface{}, tp byte, fsp int) (d types.Datum, err error) {
	value := types.Time{
		Type: tp,
		Fsp:  fsp,
	}

	defaultTime, err := getSystemTimestamp(ctx)
	if err != nil {
		return d, errors.Trace(err)
	}
	sc := ctx.GetSessionVars().StmtCtx
	switch x := v.(type) {
	case string:
		upperX := strings.ToUpper(x)
		if upperX == strings.ToUpper(ast.CurrentTimestamp) {
			value.Time = types.FromGoTime(defaultTime)
			if tp == mysql.TypeTimestamp {
				err = value.ConvertTimeZone(time.Local, ctx.GetSessionVars().GetTimeZone())
				if err != nil {
					return d, errors.Trace(err)
				}
			}
		} else if upperX == types.ZeroDatetimeStr {
			value, err = types.ParseTimeFromNum(sc, 0, tp, fsp)
			terror.Log(errors.Trace(err))
		} else {
			value, err = types.ParseTime(sc, x, tp, fsp)
			if err != nil {
				return d, errors.Trace(err)
			}
		}
	case *ast.ValueExpr:
		switch x.Kind() {
		case types.KindString:
			value, err = types.ParseTime(sc, x.GetString(), tp, fsp)
			if err != nil {
				return d, errors.Trace(err)
			}
		case types.KindInt64:
			value, err = types.ParseTimeFromNum(sc, x.GetInt64(), tp, fsp)
			if err != nil {
				return d, errors.Trace(err)
			}
		case types.KindNull:
			return d, nil
		default:
			return d, errors.Trace(errDefaultValue)
		}
	case *ast.FuncCallExpr:
		if x.FnName.L == ast.CurrentTimestamp {
			d.SetString(strings.ToUpper(ast.CurrentTimestamp))
			return d, nil
		}
		return d, errors.Trace(errDefaultValue)
	case *ast.UnaryOperationExpr:
		// support some expression, like `-1`
		v, err := EvalAstExpr(x, ctx)
		if err != nil {
			return d, errors.Trace(err)
		}
		ft := types.NewFieldType(mysql.TypeLonglong)
		xval, err := v.ConvertTo(ctx.GetSessionVars().StmtCtx, ft)
		if err != nil {
			return d, errors.Trace(err)
		}

		value, err = types.ParseTimeFromNum(sc, xval.GetInt64(), tp, fsp)
		if err != nil {
			return d, errors.Trace(err)
		}
	default:
		return d, nil
	}
	if tp == mysql.TypeTimestamp {
		value.TimeZone = ctx.GetSessionVars().GetTimeZone()
	}
	d.SetMysqlTime(value)
	return d, nil
}

func getSystemTimestamp(ctx context.Context) (time.Time, error) {
	now := time.Now()

	if ctx == nil {
		return now, nil
	}

	sessionVars := ctx.GetSessionVars()
	timestampStr, err := varsutil.GetSessionSystemVar(sessionVars, "timestamp")
	if err != nil {
		return now, errors.Trace(err)
	}

	if timestampStr == "" {
		return now, nil
	}
	timestamp, err := types.StrToInt(sessionVars.StmtCtx, timestampStr)
	if err != nil {
		return time.Time{}, errors.Trace(err)
	}
	if timestamp <= 0 {
		return now, nil
	}
	return time.Unix(timestamp, 0), nil
}
