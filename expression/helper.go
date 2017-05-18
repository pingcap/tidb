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
	"github.com/pingcap/tidb/util/types"
)

const (
	zeroI64 int64 = 0
	oneI64  int64 = 1
)

func boolToInt64(v bool) int64 {
	if v {
		return int64(1)
	}
	return int64(0)
}

var (
	// CurrentTimestamp is the keyword getting default value for datetime and timestamp type.
	CurrentTimestamp  = "CURRENT_TIMESTAMP"
	currentTimestampL = "current_timestamp"
	// ZeroTimestamp shows the zero datetime and timestamp.
	ZeroTimestamp = "0000-00-00 00:00:00"
)

var (
	errDefaultValue = errors.New("invalid default value")
)

// GetTimeValue gets the time value with type tp.
func GetTimeValue(ctx context.Context, v interface{}, tp byte, fsp int) (types.Datum, error) {
	return getTimeValue(ctx, v, tp, fsp)
}

func getTimeValue(ctx context.Context, v interface{}, tp byte, fsp int) (d types.Datum, err error) {
	value := types.Time{
		Type: tp,
		Fsp:  fsp,
	}

	defaultTime, err := getSystemTimestamp(ctx)
	if err != nil {
		return d, errors.Trace(err)
	}

	switch x := v.(type) {
	case string:
		upperX := strings.ToUpper(x)
		if upperX == CurrentTimestamp {
			value.Time = types.FromGoTime(defaultTime)
		} else if upperX == ZeroTimestamp {
			value, _ = types.ParseTimeFromNum(0, tp, fsp)
		} else {
			value, err = types.ParseTime(x, tp, fsp)
			if err != nil {
				return d, errors.Trace(err)
			}
		}
	case *ast.ValueExpr:
		switch x.Kind() {
		case types.KindString:
			value, err = types.ParseTime(x.GetString(), tp, fsp)
			if err != nil {
				return d, errors.Trace(err)
			}
		case types.KindInt64:
			value, err = types.ParseTimeFromNum(x.GetInt64(), tp, fsp)
			if err != nil {
				return d, errors.Trace(err)
			}
		case types.KindNull:
			return d, nil
		default:
			return d, errors.Trace(errDefaultValue)
		}
	case *ast.FuncCallExpr:
		if x.FnName.L == currentTimestampL {
			d.SetString(CurrentTimestamp)
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

		value, err = types.ParseTimeFromNum(xval.GetInt64(), tp, fsp)
		if err != nil {
			return d, errors.Trace(err)
		}
	default:
		return d, nil
	}

	d.SetMysqlTime(value)
	return d, nil
}

// IsCurrentTimeExpr returns whether e is CurrentTimeExpr.
func IsCurrentTimeExpr(e ast.ExprNode) bool {
	x, ok := e.(*ast.FuncCallExpr)
	if !ok {
		return false
	}
	return x.FnName.L == currentTimestampL
}

func getSystemTimestamp(ctx context.Context) (time.Time, error) {
	value := time.Now()

	if ctx == nil {
		return value, nil
	}

	// check whether use timestamp variable
	sessionVars := ctx.GetSessionVars()
	val, err := varsutil.GetSessionSystemVar(sessionVars, "timestamp")
	if err != nil {
		return value, errors.Trace(err)
	}
	if val != "" {
		timestamp, err := types.StrToInt(sessionVars.StmtCtx, val)
		if err != nil {
			return time.Time{}, errors.Trace(err)
		}
		if timestamp <= 0 {
			return value, nil
		}
		return time.Unix(timestamp, 0), nil
	}
	return value, nil
}
