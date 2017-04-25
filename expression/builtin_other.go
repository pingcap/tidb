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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &inFunctionClass{}
	_ functionClass = &rowFunctionClass{}
	_ functionClass = &castFunctionClass{}
	_ functionClass = &setVarFunctionClass{}
	_ functionClass = &getVarFunctionClass{}
	_ functionClass = &lockFunctionClass{}
	_ functionClass = &releaseLockFunctionClass{}
	_ functionClass = &valuesFunctionClass{}
	_ functionClass = &bitCountFunctionClass{}
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
	_ builtinFunc = &builtinBitCountSig{}
)

type inFunctionClass struct {
	baseFunctionClass
}

func (c *inFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinInSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinInSig struct {
	baseBuiltinFunc
}

// eval evals a builtinInSig.
// See https://dev.mysql.com/doc/refman/5.7/en/any-in-some-subqueries.html
func (b *builtinInSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		return
	}
	sc := b.ctx.GetSessionVars().StmtCtx
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

func (b *builtinRowSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d.SetRow(args)
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

// eval evals a builtinCastSig.
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html
// CastFuncFactory produces builtin function according to field types.
func (b *builtinCastSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	switch b.tp.Tp {
	// Parser has restricted this.
	// TypeDouble is used during plan optimization.
	case mysql.TypeString, mysql.TypeDuration, mysql.TypeDatetime,
		mysql.TypeDate, mysql.TypeLonglong, mysql.TypeNewDecimal, mysql.TypeDouble:
		d = args[0]
		if d.IsNull() {
			return
		}
		return d.ConvertTo(b.ctx.GetSessionVars().StmtCtx, b.tp)
	}
	return d, errors.Errorf("unknown cast type - %v", b.tp)
}

type setVarFunctionClass struct {
	baseFunctionClass
}

func (c *setVarFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinSetVarSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, errors.Trace(err)
}

type builtinSetVarSig struct {
	baseBuiltinFunc
}

func (b *builtinSetVarSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	sessionVars := b.ctx.GetSessionVars()
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
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinGetVarSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, errors.Trace(err)
}

type builtinGetVarSig struct {
	baseBuiltinFunc
}

func (b *builtinGetVarSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	sessionVars := b.ctx.GetSessionVars()
	varName, _ := args[0].ToString()
	if v, ok := sessionVars.Users[varName]; ok {
		return types.NewDatum(v), nil
	}
	return types.Datum{}, nil
}

type valuesFunctionClass struct {
	baseFunctionClass

	offset int
}

func (c *valuesFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinValuesSig{newBaseBuiltinFunc(args, ctx), c.offset}
	bt.deterministic = false
	return bt, errors.Trace(err)
}

type builtinValuesSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesSig) eval(_ []types.Datum) (types.Datum, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return types.Datum{}, errors.New("Session current insert values is nil")
	}
	row := values.([]types.Datum)
	if len(row) > b.offset {
		return row[b.offset], nil
	}
	return types.Datum{}, errors.Errorf("Session current insert values len %d and column's offset %v don't match", len(row), b.offset)
}

type bitCountFunctionClass struct {
	baseFunctionClass
}

func (c *bitCountFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinBitCountSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinBitCountSig struct {
	baseBuiltinFunc
}

// eval evals a builtinBitCountSig.
// See https://dev.mysql.com/doc/refman/5.7/en/bit-functions.html#function_bit-count
func (b *builtinBitCountSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	arg := args[0]
	if arg.IsNull() {
		return d, nil
	}
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = true
	bin, err := arg.ToInt64(sc)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			d.SetInt64(64)
			return d, nil

		}
		return d, errors.Trace(err)
	}
	var count int64
	for bin != 0 {
		count++
		bin = (bin - 1) & bin
	}
	d.SetInt64(count)
	return d, nil
}
