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
	"github.com/pingcap/tidb/util/types"
)

// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_sleep
func builtinSleep(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	sessVars := ctx.GetSessionVars()
	if args[0].IsNull() {
		if sessVars.StrictSQLMode {
			return d, errors.New("incorrect arguments to sleep")
		}
		d.SetInt64(0)
		return
	}
	// processing argument is negative
	zero := types.NewIntDatum(0)
	sc := sessVars.StmtCtx
	ret, err := args[0].CompareDatum(sc, zero)
	if err != nil {
		return d, errors.Trace(err)
	}
	if ret == -1 {
		if sessVars.StrictSQLMode {
			return d, errors.New("incorrect arguments to sleep")
		}
		d.SetInt64(0)
		return
	}

	// TODO: consider it's interrupted using KILL QUERY from other session, or
	// interrupted by time out.
	duration := time.Duration(args[0].GetFloat64() * float64(time.Second.Nanoseconds()))
	time.Sleep(duration)
	d.SetInt64(0)
	return
}

// See http://dev.mysql.com/doc/refman/5.7/en/any-in-some-subqueries.html
func builtinIn(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	if args[0].IsNull() {
		return
	}
	sc := ctx.GetSessionVars().StmtCtx
	var hasNull bool
	for _, v := range args[1:] {
		if v.IsNull() {
			hasNull = true
			continue
		}

		a, b, err := types.CoerceDatum(sc, args[0], v)
		if err != nil {
			return d, errors.Trace(err)
		}
		ret, err := a.CompareDatum(sc, b)
		if err != nil {
			return d, errors.Trace(err)
		}
		if ret == 0 {
			d.SetInt64(1)
			return d, nil
		}
	}

	if hasNull {
		// If it's no matched but we get null in In, returns null.
		// e.g 1 in (null, 2, 3) returns null.
		return
	}
	d.SetInt64(0)
	return
}

func builtinRow(row []types.Datum, _ context.Context) (d types.Datum, err error) {
	d.SetRow(row)
	return
}

// CastFuncFactory produces builtin function according to field types.
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html
func CastFuncFactory(tp *types.FieldType) (BuiltinFunc, error) {
	switch tp.Tp {
	// Parser has restricted this.
	case mysql.TypeString, mysql.TypeDuration, mysql.TypeDatetime,
		mysql.TypeDate, mysql.TypeLonglong, mysql.TypeNewDecimal:
		return func(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
			d = args[0]
			if d.IsNull() {
				return
			}
			return d.ConvertTo(ctx.GetSessionVars().StmtCtx, tp)
		}, nil
	}
	return nil, errors.Errorf("unknown cast type - %v", tp)
}

func builtinSetVar(args []types.Datum, ctx context.Context) (types.Datum, error) {
	sessionVars := ctx.GetSessionVars()
	varName, _ := args[0].ToString()
	if !args[1].IsNull() {
		strVal, err := args[1].ToString()
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		sessionVars.Users[varName] = strings.ToLower(strVal)
	}
	return args[1], nil
}

func builtinGetVar(args []types.Datum, ctx context.Context) (types.Datum, error) {
	sessionVars := ctx.GetSessionVars()
	varName, _ := args[0].ToString()
	if v, ok := sessionVars.Users[varName]; ok {
		return types.NewDatum(v), nil
	}
	return types.Datum{}, nil
}

// The lock function will do nothing.
// Warning: get_lock() function is parsed but ignored.
func builtinLock(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	d.SetInt64(1)
	return d, nil
}

// The release lock function will do nothing.
// Warning: release_lock() function is parsed but ignored.
func builtinReleaseLock(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	d.SetInt64(1)
	return d, nil
}

// BuildinValuesFactory generates values builtin function.
func BuildinValuesFactory(v *ast.ValuesExpr) BuiltinFunc {
	return func(_ []types.Datum, ctx context.Context) (d types.Datum, err error) {
		values := ctx.GetSessionVars().CurrInsertValues
		if values == nil {
			err = errors.New("Session current insert values is nil")
			return
		}
		row := values.([]types.Datum)
		offset := v.Column.Refer.Column.Offset
		if len(row) > offset {
			return row[offset], nil
		}
		err = errors.Errorf("Session current insert values len %d and column's offset %v don't match", len(row), offset)
		return
	}
}
