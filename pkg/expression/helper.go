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
	"math"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
)

func boolToInt64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

// IsValidCurrentTimestampExpr returns true if exprNode is a valid CurrentTimestamp expression.
// Here `valid` means it is consistent with the given fieldType's decimal.
func IsValidCurrentTimestampExpr(exprNode ast.ExprNode, fieldType *types.FieldType) bool {
	fn, isFuncCall := exprNode.(*ast.FuncCallExpr)
	if !isFuncCall || fn.FnName.L != ast.CurrentTimestamp {
		return false
	}

	containsArg := len(fn.Args) > 0
	// Fsp represents fractional seconds precision.
	containsFsp := fieldType != nil && fieldType.GetDecimal() > 0
	var isConsistent bool
	if containsArg {
		v, ok := fn.Args[0].(*driver.ValueExpr)
		isConsistent = ok && fieldType != nil && v.Datum.GetInt64() == int64(fieldType.GetDecimal())
	}

	return (containsArg && isConsistent) || (!containsArg && !containsFsp)
}

// GetTimeCurrentTimestamp is used for generating a timestamp for some special cases: cast null value to timestamp type with not null flag.
func GetTimeCurrentTimestamp(ctx EvalContext, tp byte, fsp int) (d types.Datum, err error) {
	var t types.Time
	t, err = getTimeCurrentTimeStamp(ctx, tp, fsp)
	if err != nil {
		return d, err
	}
	d.SetMysqlTime(t)
	return d, nil
}

func getTimeCurrentTimeStamp(ctx EvalContext, tp byte, fsp int) (t types.Time, err error) {
	value := types.NewTime(types.ZeroCoreTime, tp, fsp)
	defaultTime, err := getStmtTimestamp(ctx)
	if err != nil {
		return value, err
	}
	value.SetCoreTime(types.FromGoTime(defaultTime.Truncate(time.Duration(math.Pow10(9-fsp)) * time.Nanosecond)))
	if tp == mysql.TypeTimestamp || tp == mysql.TypeDatetime || tp == mysql.TypeDate {
		err = value.ConvertTimeZone(time.Local, ctx.Location())
		if err != nil {
			return value, err
		}
	}
	return value, nil
}

// GetTimeValue gets the time value with type tp.
func GetTimeValue(ctx BuildContext, v any, tp byte, fsp int, explicitTz *time.Location) (d types.Datum, err error) {
	var value types.Time
	tc := ctx.GetEvalCtx().TypeCtx()
	if explicitTz != nil {
		tc = tc.WithLocation(explicitTz)
	}

	switch x := v.(type) {
	case string:
		lowerX := strings.ToLower(x)
		if lowerX == ast.CurrentTimestamp || lowerX == ast.CurrentDate {
			if value, err = getTimeCurrentTimeStamp(ctx.GetEvalCtx(), tp, fsp); err != nil {
				return d, err
			}
		} else if lowerX == types.ZeroDatetimeStr {
			value, err = types.ParseTimeFromNum(tc, 0, tp, fsp)
			terror.Log(err)
		} else {
			value, err = types.ParseTime(tc, x, tp, fsp)
			if err != nil {
				return d, err
			}
		}
	case *driver.ValueExpr:
		switch x.Kind() {
		case types.KindString:
			value, err = types.ParseTime(tc, x.GetString(), tp, fsp)
			if err != nil {
				return d, err
			}
		case types.KindInt64:
			value, err = types.ParseTimeFromNum(tc, x.GetInt64(), tp, fsp)
			if err != nil {
				return d, err
			}
		case types.KindNull:
			return d, nil
		default:
			return d, errDefaultValue
		}
	case *ast.FuncCallExpr:
		if x.FnName.L == ast.CurrentTimestamp || x.FnName.L == ast.CurrentDate {
			d.SetString(strings.ToUpper(x.FnName.L), mysql.DefaultCollationName)
			return d, nil
		}
		return d, errDefaultValue
	case *ast.UnaryOperationExpr:
		// support some expression, like `-1`
		v, err := EvalSimpleAst(ctx, x)
		if err != nil {
			return d, err
		}
		ft := types.NewFieldType(mysql.TypeLonglong)
		xval, err := v.ConvertTo(tc, ft)
		if err != nil {
			return d, err
		}

		value, err = types.ParseTimeFromNum(tc, xval.GetInt64(), tp, fsp)
		if err != nil {
			return d, err
		}
	default:
		return d, nil
	}
	d.SetMysqlTime(value)
	return d, nil
}

// if timestamp session variable set, use session variable as current time, otherwise use cached time
// during one sql statement, the "current_time" should be the same
func getStmtTimestamp(ctx EvalContext) (time.Time, error) {
	failpoint.Inject("injectNow", func(val failpoint.Value) {
		v := time.Unix(int64(val.(int)), 0)
		failpoint.Return(v, nil)
	})
	return ctx.CurrentTime()
}
