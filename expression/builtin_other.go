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
	"github.com/pingcap/tidb/util/types"
)

var (
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
	_ builtinFunc = &builtinRowSig{}
	_ builtinFunc = &builtinCastSig{}
	_ builtinFunc = &builtinSetVarSig{}
	_ builtinFunc = &builtinGetVarSig{}
	_ builtinFunc = &builtinLockSig{}
	_ builtinFunc = &builtinReleaseLockSig{}
	_ builtinFunc = &builtinValuesSig{}
	_ builtinFunc = &builtinBitCountSig{}
)

type rowFunctionClass struct {
	baseFunctionClass
}

func (c *rowFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinRowSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinRowSig struct {
	baseBuiltinFunc
}

func (b *builtinRowSig) isDeterministic() bool {
	return false
}

func (b *builtinRowSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d.SetRow(args)
	return
}

type setVarFunctionClass struct {
	baseFunctionClass
}

func (c *setVarFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinSetVarSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt.setSelf(bt), errors.Trace(err)
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
		sessionVars.UsersLock.Lock()
		sessionVars.Users[varName] = strings.ToLower(strVal)
		sessionVars.UsersLock.Unlock()
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
	return bt.setSelf(bt), errors.Trace(err)
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
	sessionVars.UsersLock.RLock()
	defer sessionVars.UsersLock.RUnlock()
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
	return bt.setSelf(bt), errors.Trace(err)
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
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 2
	sig := &builtinBitCountSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinBitCountSig struct {
	baseIntBuiltinFunc
}

// evalInt evals BIT_COUNT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/bit-functions.html#function_bit-count
func (b *builtinBitCountSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	n, isNull, err := b.args[0].EvalInt(row, sc)
	if err != nil || isNull {
		if err != nil && types.ErrOverflow.Equal(err) {
			return 64, false, nil
		}
		return 0, true, errors.Trace(err)
	}

	var count int64
	for ; n != 0; n = (n - 1) & n {
		count++
	}
	return count, false, nil
}
