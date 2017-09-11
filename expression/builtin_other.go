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
	"github.com/pingcap/tidb/util/types/json"
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
	_ builtinFunc = &builtinValuesIntSig{}
	_ builtinFunc = &builtinValuesRealSig{}
	_ builtinFunc = &builtinValuesDecimalSig{}
	_ builtinFunc = &builtinValuesStringSig{}
	_ builtinFunc = &builtinValuesTimeSig{}
	_ builtinFunc = &builtinValuesDurationSig{}
	_ builtinFunc = &builtinValuesJSONSig{}
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

func (c *setVarFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinSetVarSig{newBaseBuiltinFunc(args, ctx)}
	bt.foldable = false
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

func (c *getVarFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinGetVarSig{newBaseBuiltinFunc(args, ctx)}
	bt.foldable = false
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
	tp     *types.FieldType
}

func (c *valuesFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFunc(args, ctx)
	bf.tp = c.tp
	bf.foldable = false
	switch fieldTp2EvalTp(c.tp) {
	case tpInt:
		sig = &builtinValuesIntSig{baseIntBuiltinFunc{bf}, c.offset}
	case tpReal:
		sig = &builtinValuesRealSig{baseRealBuiltinFunc{bf}, c.offset}
	case tpDecimal:
		sig = &builtinValuesRealSig{baseRealBuiltinFunc{bf}, c.offset}
	case tpString:
		sig = &builtinValuesStringSig{baseStringBuiltinFunc{bf}, c.offset}
	case tpDatetime, tpTimestamp:
		sig = &builtinValuesTimeSig{baseTimeBuiltinFunc{bf}, c.offset}
	case tpDuration:
		sig = &builtinValuesDurationSig{baseDurationBuiltinFunc{bf}, c.offset}
	case tpJSON:
		sig = &builtinValuesJSONSig{baseJSONBuiltinFunc{bf}, c.offset}
	}
	return sig.setSelf(sig), nil
}

type builtinValuesIntSig struct {
	baseIntBuiltinFunc

	offset int
}

// evalInt evals a builtinValuesIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesIntSig) evalInt(_ []types.Datum) (int64, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return 0, true, errors.New("Session current insert values is nil")
	}
	row := values.([]types.Datum)
	if len(row) > b.offset {
		return row[b.offset].GetInt64(), false, nil
	}
	return 0, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", len(row), b.offset)
}

type builtinValuesRealSig struct {
	baseRealBuiltinFunc

	offset int
}

// evalReal evals a builtinValuesRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesRealSig) evalReal(_ []types.Datum) (float64, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return 0, true, errors.New("Session current insert values is nil")
	}
	row := values.([]types.Datum)
	if len(row) > b.offset {
		return row[b.offset].GetFloat64(), false, nil
	}
	return 0, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", len(row), b.offset)
}

type builtinValuesDecimalSig struct {
	baseDecimalBuiltinFunc

	offset int
}

// evalDecimal evals a builtinValuesDecimalSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDecimalSig) evalDecimal(_ []types.Datum) (*types.MyDecimal, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return nil, true, errors.New("Session current insert values is nil")
	}
	row := values.([]types.Datum)
	if len(row) > b.offset {
		return row[b.offset].GetMysqlDecimal(), false, nil
	}
	return nil, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", len(row), b.offset)
}

type builtinValuesStringSig struct {
	baseStringBuiltinFunc

	offset int
}

// evalString evals a builtinValuesStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesStringSig) evalString(_ []types.Datum) (string, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return "", true, errors.New("Session current insert values is nil")
	}
	row := values.([]types.Datum)
	if len(row) > b.offset {
		return row[b.offset].GetString(), false, nil
	}
	return "", true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", len(row), b.offset)
}

type builtinValuesTimeSig struct {
	baseTimeBuiltinFunc

	offset int
}

// // evalTime evals a builtinValuesTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesTimeSig) evalTime(_ []types.Datum) (types.Time, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return types.Time{}, true, errors.New("Session current insert values is nil")
	}
	row := values.([]types.Datum)
	if len(row) > b.offset {
		return row[b.offset].GetMysqlTime(), false, nil
	}
	return types.Time{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", len(row), b.offset)
}

type builtinValuesDurationSig struct {
	baseDurationBuiltinFunc

	offset int
}

// // evalDuration evals a builtinValuesDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDurationSig) evalDuration(_ []types.Datum) (types.Duration, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return types.Duration{}, true, errors.New("Session current insert values is nil")
	}
	row := values.([]types.Datum)
	if len(row) > b.offset {
		return row[b.offset].GetMysqlDuration(), false, nil
	}
	return types.Duration{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", len(row), b.offset)
}

type builtinValuesJSONSig struct {
	baseJSONBuiltinFunc

	offset int
}

// evalJSON evals a builtinValuesJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesJSONSig) evalJSON(_ []types.Datum) (json.JSON, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return json.JSON{}, true, errors.New("Session current insert values is nil")
	}
	row := values.([]types.Datum)
	if len(row) > b.offset {
		return row[b.offset].GetMysqlJSON(), false, nil
	}
	return json.JSON{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", len(row), b.offset)
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
