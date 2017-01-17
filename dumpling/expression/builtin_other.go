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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &sleepFunctionClass{}
	_ functionClass = &inFunctionClass{}
	_ functionClass = &rowFunctionClass{}
	_ functionClass = &castFunctionClass{}
	_ functionClass = &setVarFunctionClass{}
	_ functionClass = &getVarFunctionClass{}
	_ functionClass = &lockFunctionClass{}
	_ functionClass = &releaseLockFunctionClass{}
	_ functionClass = &valuesFunctionClass{}
)

var (
	_ builtinFunc = &builtinSleepSig{}
	_ builtinFunc = &builtinInSig{}
	_ builtinFunc = &builtinRowSig{}
	_ builtinFunc = &builtinCastSig{}
	_ builtinFunc = &builtinSetVarSig{}
	_ builtinFunc = &builtinGetVarSig{}
	_ builtinFunc = &builtinLockSig{}
	_ builtinFunc = &builtinReleaseLockSig{}
	_ builtinFunc = &builtinValuesSig{}
)

type sleepFunctionClass struct {
	baseFunctionClass
}

func (c *sleepFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, errors.Trace(err)
	}
	bt := &builtinSleepSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, nil
}

type builtinSleepSig struct {
	baseBuiltinFunc
}

func (b *builtinSleepSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinSleep(args, b.ctx)
}

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

type inFunctionClass struct {
	baseFunctionClass
}

func (c *inFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinInSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinInSig struct {
	baseBuiltinFunc
}

func (b *builtinInSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinIn(args, b.ctx)
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

type rowFunctionClass struct {
	baseFunctionClass
}

func (c *rowFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinRowSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinRowSig struct {
	baseBuiltinFunc
}

func (b *builtinRowSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinRow(args, b.ctx)
}

func builtinRow(row []types.Datum, _ context.Context) (d types.Datum, err error) {
	d.SetRow(row)
	return
}

type castFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinCastSig{newBaseBuiltinFunc(args, ctx), c.tp}, errors.Trace(c.verifyArgs(args))
}

type builtinCastSig struct {
	baseBuiltinFunc

	tp *types.FieldType
}

func (b *builtinCastSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	f, err := CastFuncFactory(b.tp)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return f(args, b.ctx)
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

type setVarFunctionClass struct {
	baseFunctionClass
}

func (c *setVarFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, errors.Trace(err)
	}
	bt := &builtinSetVarSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, nil
}

type builtinSetVarSig struct {
	baseBuiltinFunc
}

func (b *builtinSetVarSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinSetVar(args, b.ctx)
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

type getVarFunctionClass struct {
	baseFunctionClass
}

func (c *getVarFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, errors.Trace(err)
	}
	bt := &builtinGetVarSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, nil
}

type builtinGetVarSig struct {
	baseBuiltinFunc
}

func (b *builtinGetVarSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinGetVar(args, b.ctx)
}

func builtinGetVar(args []types.Datum, ctx context.Context) (types.Datum, error) {
	sessionVars := ctx.GetSessionVars()
	varName, _ := args[0].ToString()
	if v, ok := sessionVars.Users[varName]; ok {
		return types.NewDatum(v), nil
	}
	return types.Datum{}, nil
}

type lockFunctionClass struct {
	baseFunctionClass
}

func (c *lockFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinLockSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinLockSig struct {
	baseBuiltinFunc
}

func (b *builtinLockSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinLock(args, b.ctx)
}

// The lock function will do nothing.
// Warning: get_lock() function is parsed but ignored.
func builtinLock(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	d.SetInt64(1)
	return d, nil
}

type releaseLockFunctionClass struct {
	baseFunctionClass
}

func (c *releaseLockFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinReleaseLockSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinReleaseLockSig struct {
	baseBuiltinFunc
}

func (b *builtinReleaseLockSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinReleaseLock(args, b.ctx)
}

// The release lock function will do nothing.
// Warning: release_lock() function is parsed but ignored.
func builtinReleaseLock(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	d.SetInt64(1)
	return d, nil
}

type valuesFunctionClass struct {
	baseFunctionClass

	offset int
}

func (c *valuesFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, errors.Trace(err)
	}
	bt := &builtinValuesSig{newBaseBuiltinFunc(args, ctx), c.offset}
	bt.deterministic = false
	return bt, nil
}

type builtinValuesSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return BuiltinValuesFactory(b.offset)(args, b.ctx)
}

// BuiltinValuesFactory generates values builtin function.
func BuiltinValuesFactory(offset int) BuiltinFunc {
	return func(_ []types.Datum, ctx context.Context) (d types.Datum, err error) {
		values := ctx.GetSessionVars().CurrInsertValues
		if values == nil {
			err = errors.New("Session current insert values is nil")
			return
		}
		row := values.([]types.Datum)
		if len(row) > offset {
			return row[offset], nil
		}
		err = errors.Errorf("Session current insert values len %d and column's offset %v don't match", len(row), offset)
		return
	}
}
