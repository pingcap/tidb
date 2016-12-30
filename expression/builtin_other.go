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
	"github.com/pingcap/tidb/parser/opcode"
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

func builtinAndAnd(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	leftDatum := args[0]
	rightDatum := args[1]
	sc := ctx.GetSessionVars().StmtCtx
	if !leftDatum.IsNull() {
		var x int64
		x, err = leftDatum.ToBool(sc)
		if err != nil {
			return d, errors.Trace(err)
		} else if x == 0 {
			// false && any other types is false
			d.SetInt64(x)
			return
		}
	}
	if !rightDatum.IsNull() {
		var y int64
		y, err = rightDatum.ToBool(sc)
		if err != nil {
			return d, errors.Trace(err)
		} else if y == 0 {
			d.SetInt64(y)
			return
		}
	}
	if leftDatum.IsNull() || rightDatum.IsNull() {
		return
	}
	d.SetInt64(int64(1))
	return
}

func builtinOrOr(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	leftDatum := args[0]
	rightDatum := args[1]
	if !leftDatum.IsNull() {
		var x int64
		x, err = leftDatum.ToBool(sc)
		if err != nil {
			return d, errors.Trace(err)
		} else if x == 1 {
			// false && any other types is false
			d.SetInt64(x)
			return
		}
	}
	if !rightDatum.IsNull() {
		var y int64
		y, err = rightDatum.ToBool(sc)
		if err != nil {
			return d, errors.Trace(err)
		} else if y == 1 {
			d.SetInt64(y)
			return
		}
	}
	if leftDatum.IsNull() || rightDatum.IsNull() {
		return
	}
	d.SetInt64(int64(0))
	return
}

// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func builtinCaseWhen(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	l := len(args)
	for i := 0; i < l-1; i += 2 {
		if args[i].IsNull() {
			continue
		}
		b, err1 := args[i].ToBool(sc)
		if err1 != nil {
			return d, errors.Trace(err1)
		}
		if b == 1 {
			d = args[i+1]
			return
		}
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		d = args[l-1]
	}
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

func builtinLogicXor(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	leftDatum := args[0]
	righDatum := args[1]
	if leftDatum.IsNull() || righDatum.IsNull() {
		return
	}
	sc := ctx.GetSessionVars().StmtCtx
	x, err := leftDatum.ToBool(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	y, err := righDatum.ToBool(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	if x == y {
		d.SetInt64(zeroI64)
	} else {
		d.SetInt64(oneI64)
	}
	return
}

func compareFuncFactory(op opcode.Op) BuiltinFunc {
	return func(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
		sc := ctx.GetSessionVars().StmtCtx
		var a, b = args[0], args[1]
		if op != opcode.NullEQ {
			a, b, err = types.CoerceDatum(sc, a, b)
			if err != nil {
				return d, errors.Trace(err)
			}
		}
		if a.IsNull() || b.IsNull() {
			// for <=>, if a and b are both nil, return true.
			// if a or b is nil, return false.
			if op == opcode.NullEQ {
				if a.IsNull() && b.IsNull() {
					d.SetInt64(oneI64)
				} else {
					d.SetInt64(zeroI64)
				}
			}
			return
		}

		n, err := a.CompareDatum(sc, b)
		if err != nil {
			return d, errors.Trace(err)
		}
		var result bool
		switch op {
		case opcode.LT:
			result = n < 0
		case opcode.LE:
			result = n <= 0
		case opcode.EQ, opcode.NullEQ:
			result = n == 0
		case opcode.GT:
			result = n > 0
		case opcode.GE:
			result = n >= 0
		case opcode.NE:
			result = n != 0
		default:
			return d, errInvalidOperation.Gen("invalid op %v in comparison operation", op)
		}
		if result {
			d.SetInt64(oneI64)
		} else {
			d.SetInt64(zeroI64)
		}
		return
	}
}

func bitOpFactory(op opcode.Op) BuiltinFunc {
	return func(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
		sc := ctx.GetSessionVars().StmtCtx
		a, b, err := types.CoerceDatum(sc, args[0], args[1])
		if err != nil {
			return d, errors.Trace(err)
		}
		if a.IsNull() || b.IsNull() {
			return
		}

		x, err := a.ToInt64(sc)
		if err != nil {
			return d, errors.Trace(err)
		}

		y, err := b.ToInt64(sc)
		if err != nil {
			return d, errors.Trace(err)
		}

		// use a int64 for bit operator, return uint64
		switch op {
		case opcode.And:
			d.SetUint64(uint64(x & y))
		case opcode.Or:
			d.SetUint64(uint64(x | y))
		case opcode.Xor:
			d.SetUint64(uint64(x ^ y))
		case opcode.RightShift:
			d.SetUint64(uint64(x) >> uint64(y))
		case opcode.LeftShift:
			d.SetUint64(uint64(x) << uint64(y))
		default:
			return d, errInvalidOperation.Gen("invalid op %v in bit operation", op)
		}
		return
	}
}

func arithmeticFuncFactory(op opcode.Op) BuiltinFunc {
	return func(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
		sc := ctx.GetSessionVars().StmtCtx
		a, err := types.CoerceArithmetic(sc, args[0])
		if err != nil {
			return d, errors.Trace(err)
		}

		b, err := types.CoerceArithmetic(sc, args[1])
		if err != nil {
			return d, errors.Trace(err)
		}
		a, b, err = types.CoerceDatum(sc, a, b)
		if err != nil {
			return d, errors.Trace(err)
		}
		if a.IsNull() || b.IsNull() {
			return
		}

		switch op {
		case opcode.Plus:
			return types.ComputePlus(a, b)
		case opcode.Minus:
			return types.ComputeMinus(a, b)
		case opcode.Mul:
			return types.ComputeMul(a, b)
		case opcode.Div:
			return types.ComputeDiv(sc, a, b)
		case opcode.Mod:
			return types.ComputeMod(sc, a, b)
		case opcode.IntDiv:
			return types.ComputeIntDiv(sc, a, b)
		default:
			return d, errInvalidOperation.Gen("invalid op %v in arithmetic operation", op)
		}
	}
}

func builtinRow(row []types.Datum, _ context.Context) (d types.Datum, err error) {
	d.SetRow(row)
	return
}

func isTrueOpFactory(op opcode.Op) BuiltinFunc {
	return func(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
		var boolVal bool
		if !args[0].IsNull() {
			iVal, err := args[0].ToBool(ctx.GetSessionVars().StmtCtx)
			if err != nil {
				return d, errors.Trace(err)
			}
			if (op == opcode.IsTruth && iVal == 1) || (op == opcode.IsFalsity && iVal == 0) {
				boolVal = true
			}
		}
		d.SetInt64(boolToInt64(boolVal))
		return
	}
}

func unaryOpFactory(op opcode.Op) BuiltinFunc {
	return func(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
		defer func() {
			if er := recover(); er != nil {
				err = errors.Errorf("%v", er)
			}
		}()
		aDatum := args[0]
		if aDatum.IsNull() {
			return
		}
		sc := ctx.GetSessionVars().StmtCtx
		switch op {
		case opcode.Not:
			var n int64
			n, err = aDatum.ToBool(sc)
			if err != nil {
				err = errors.Trace(err)
			} else if n == 0 {
				d.SetInt64(1)
			} else {
				d.SetInt64(0)
			}
		case opcode.BitNeg:
			var n int64
			// for bit operation, we will use int64 first, then return uint64
			n, err = aDatum.ToInt64(sc)
			if err != nil {
				return d, errors.Trace(err)
			}
			d.SetUint64(uint64(^n))
		case opcode.Plus:
			switch aDatum.Kind() {
			case types.KindInt64,
				types.KindUint64,
				types.KindFloat64,
				types.KindFloat32,
				types.KindMysqlDuration,
				types.KindMysqlTime,
				types.KindString,
				types.KindMysqlDecimal,
				types.KindBytes,
				types.KindMysqlHex,
				types.KindMysqlBit,
				types.KindMysqlEnum,
				types.KindMysqlSet:
				d = aDatum
			default:
				return d, errInvalidOperation.Gen("Unsupported type %v for op.Plus", aDatum.Kind())
			}
		case opcode.Minus:
			switch aDatum.Kind() {
			case types.KindInt64:
				d.SetInt64(-aDatum.GetInt64())
			case types.KindUint64:
				d.SetInt64(-int64(aDatum.GetUint64()))
			case types.KindFloat64:
				d.SetFloat64(-aDatum.GetFloat64())
			case types.KindFloat32:
				d.SetFloat32(-aDatum.GetFloat32())
			case types.KindMysqlDuration:
				dec := new(types.MyDecimal)
				err = types.DecimalSub(new(types.MyDecimal), aDatum.GetMysqlDuration().ToNumber(), dec)
				d.SetMysqlDecimal(dec)
			case types.KindMysqlTime:
				dec := new(types.MyDecimal)
				err = types.DecimalSub(new(types.MyDecimal), aDatum.GetMysqlTime().ToNumber(), dec)
				d.SetMysqlDecimal(dec)
			case types.KindString, types.KindBytes:
				f, err1 := types.StrToFloat(sc, aDatum.GetString())
				err = errors.Trace(err1)
				d.SetFloat64(-f)
			case types.KindMysqlDecimal:
				dec := new(types.MyDecimal)
				err = types.DecimalSub(new(types.MyDecimal), aDatum.GetMysqlDecimal(), dec)
				d.SetMysqlDecimal(dec)
			case types.KindMysqlHex:
				d.SetFloat64(-aDatum.GetMysqlHex().ToNumber())
			case types.KindMysqlBit:
				d.SetFloat64(-aDatum.GetMysqlBit().ToNumber())
			case types.KindMysqlEnum:
				d.SetFloat64(-aDatum.GetMysqlEnum().ToNumber())
			case types.KindMysqlSet:
				d.SetFloat64(-aDatum.GetMysqlSet().ToNumber())
			default:
				return d, errInvalidOperation.Gen("Unsupported type %v for op.Minus", aDatum.Kind())
			}
		default:
			return d, errInvalidOperation.Gen("Unsupported op %v for unary op", op)
		}
		return
	}
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
