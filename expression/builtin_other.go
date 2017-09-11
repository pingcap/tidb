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
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &rowFunctionClass{}
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

func (c *rowFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinRowSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinRowSig struct {
	baseBuiltinFunc
}

func (b *builtinRowSig) canBeFolded() bool {
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

func (c *setVarFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString)
	bf.foldable = false
	// TODO: we should consider the type of the argument, but not take it as string for all situations.
	sig = &builtinSetVarSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(err)
}

type builtinSetVarSig struct {
	baseStringBuiltinFunc
}

func (b *builtinSetVarSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	var varName string
	sessionVars := b.ctx.GetSessionVars()
	sc := sessionVars.StmtCtx
	varName, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	res, isNull, err = b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	varName, res = strings.ToLower(varName), strings.ToLower(res)
	sessionVars.UsersLock.Lock()
	sessionVars.Users[varName] = res
	sessionVars.UsersLock.Unlock()
	return res, false, nil
}

type getVarFunctionClass struct {
	baseFunctionClass
}

func (c *getVarFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, err
	}
	// TODO: we should consider the type of the argument, but not take it as string for all situations.
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	bf.tp.Flen, bf.foldable = mysql.MaxFieldVarCharLength, false
	sig = &builtinGetVarSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinGetVarSig struct {
	baseStringBuiltinFunc
}

func (b *builtinGetVarSig) evalString(row []types.Datum) (string, bool, error) {
	sessionVars := b.ctx.GetSessionVars()
	sc := sessionVars.StmtCtx
	varName, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	varName = strings.ToLower(varName)
	sessionVars.UsersLock.RLock()
	defer sessionVars.UsersLock.RUnlock()
	if v, ok := sessionVars.Users[varName]; ok {
		return v, false, nil
	}
	return "", true, nil
}

type valuesFunctionClass struct {
	baseFunctionClass

	offset int
}

func (c *valuesFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinValuesSig{newBaseBuiltinFunc(args, ctx), c.offset}
	bt.foldable = false
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

func (c *bitCountFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt)
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
