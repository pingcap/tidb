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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &inFunctionClass{}
	_ functionClass = &rowFunctionClass{}
	_ functionClass = &setVarFunctionClass{}
	_ functionClass = &getVarFunctionClass{}
	_ functionClass = &lockFunctionClass{}
	_ functionClass = &releaseLockFunctionClass{}
	_ functionClass = &valuesFunctionClass{}
	_ functionClass = &bitCountFunctionClass{}
	_ functionClass = &getParamFunctionClass{}
)

var (
	_ builtinFunc = &builtinSleepSig{}
	_ builtinFunc = &builtinInIntSig{}
	_ builtinFunc = &builtinInStringSig{}
	_ builtinFunc = &builtinInDecimalSig{}
	_ builtinFunc = &builtinInRealSig{}
	_ builtinFunc = &builtinInTimeSig{}
	_ builtinFunc = &builtinInDurationSig{}
	_ builtinFunc = &builtinInJSONSig{}
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
	_ builtinFunc = &builtinGetParamStringSig{}
)

type inFunctionClass struct {
	baseFunctionClass
}

func (c *inFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = args[0].GetType().EvalType()
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	bf.tp.Flen = 1
	switch args[0].GetType().EvalType() {
	case types.ETInt:
		sig = &builtinInIntSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InInt)
	case types.ETString:
		sig = &builtinInStringSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InString)
	case types.ETReal:
		sig = &builtinInRealSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InReal)
	case types.ETDecimal:
		sig = &builtinInDecimalSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InDecimal)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinInTimeSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InTime)
	case types.ETDuration:
		sig = &builtinInDurationSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InDuration)
	case types.ETJson:
		sig = &builtinInJSONSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InJson)
	}
	return sig, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInIntSig struct {
	baseBuiltinFunc
}

func (b *builtinInIntSig) evalInt(row types.Row) (int64, bool, error) {
	sc, args := b.ctx.GetSessionVars().StmtCtx, b.getArgs()
	arg0, isNull0, err := args[0].EvalInt(row, sc)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	isUnsigned0 := mysql.HasUnsignedFlag(args[0].GetType().Flag)
	var hasNull bool
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalInt(row, sc)
		if err != nil {
			return 0, true, errors.Trace(err)
		}
		if isNull {
			hasNull = true
			continue
		}
		isUnsigned := mysql.HasUnsignedFlag(arg.GetType().Flag)
		if isUnsigned0 && isUnsigned {
			if evaledArg == arg0 {
				return 1, false, nil
			}
		} else if !isUnsigned0 && !isUnsigned {
			if evaledArg == arg0 {
				return 1, false, nil
			}
		} else if !isUnsigned0 && isUnsigned {
			if arg0 >= 0 && evaledArg == arg0 {
				return 1, false, nil
			}
		} else {
			if evaledArg >= 0 && evaledArg == arg0 {
				return 1, false, nil
			}
		}
	}
	return 0, hasNull, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInStringSig struct {
	baseBuiltinFunc
}

func (b *builtinInStringSig) evalInt(row types.Row) (int64, bool, error) {
	sc, args := b.ctx.GetSessionVars().StmtCtx, b.getArgs()
	arg0, isNull0, err := args[0].EvalString(row, sc)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	var hasNull bool
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalString(row, sc)
		if err != nil {
			return 0, true, errors.Trace(err)
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0 == evaledArg {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInRealSig struct {
	baseBuiltinFunc
}

func (b *builtinInRealSig) evalInt(row types.Row) (int64, bool, error) {
	sc, args := b.ctx.GetSessionVars().StmtCtx, b.getArgs()
	arg0, isNull0, err := args[0].EvalReal(row, sc)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	var hasNull bool
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalReal(row, sc)
		if err != nil {
			return 0, true, errors.Trace(err)
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0 == evaledArg {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinInDecimalSig) evalInt(row types.Row) (int64, bool, error) {
	sc, args := b.ctx.GetSessionVars().StmtCtx, b.getArgs()
	arg0, isNull0, err := args[0].EvalDecimal(row, sc)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	var hasNull bool
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalDecimal(row, sc)
		if err != nil {
			return 0, true, errors.Trace(err)
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0.Compare(evaledArg) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinInTimeSig) evalInt(row types.Row) (int64, bool, error) {
	sc, args := b.ctx.GetSessionVars().StmtCtx, b.getArgs()
	arg0, isNull0, err := args[0].EvalTime(row, sc)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	var hasNull bool
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalTime(row, sc)
		if err != nil {
			return 0, true, errors.Trace(err)
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0.Compare(evaledArg) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinInDurationSig) evalInt(row types.Row) (int64, bool, error) {
	sc, args := b.ctx.GetSessionVars().StmtCtx, b.getArgs()
	arg0, isNull0, err := args[0].EvalDuration(row, sc)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	var hasNull bool
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalDuration(row, sc)
		if err != nil {
			return 0, true, errors.Trace(err)
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0.Compare(evaledArg) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinInJSONSig) evalInt(row types.Row) (int64, bool, error) {
	sc, args := b.ctx.GetSessionVars().StmtCtx, b.getArgs()
	arg0, isNull0, err := args[0].EvalJSON(row, sc)
	if isNull0 || err != nil {
		return 0, isNull0, errors.Trace(err)
	}
	var hasNull bool
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalJSON(row, sc)
		if err != nil {
			return 0, true, errors.Trace(err)
		}
		if isNull {
			hasNull = true
			continue
		}
		result := json.CompareBinary(evaledArg, arg0)
		if result == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

type rowFunctionClass struct {
	baseFunctionClass
}

func (c *rowFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := make([]types.EvalType, len(args))
	for i := range argTps {
		argTps[i] = args[i].GetType().EvalType()
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	sig = &builtinRowSig{bf}
	return sig, nil
}

type builtinRowSig struct {
	baseBuiltinFunc
}

// evalString rowFunc should always be flattened in expression rewrite phrase.
func (b *builtinRowSig) evalString(row types.Row) (string, bool, error) {
	panic("builtinRowSig.evalString() should never be called.")
}

type setVarFunctionClass struct {
	baseFunctionClass
}

func (c *setVarFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)
	bf.tp.Flen = args[1].GetType().Flen
	// TODO: we should consider the type of the argument, but not take it as string for all situations.
	sig = &builtinSetVarSig{bf}
	return sig, errors.Trace(err)
}

type builtinSetVarSig struct {
	baseBuiltinFunc
}

func (b *builtinSetVarSig) evalString(row types.Row) (res string, isNull bool, err error) {
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
	varName = strings.ToLower(varName)
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
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.tp.Flen = mysql.MaxFieldVarCharLength
	sig = &builtinGetVarSig{bf}
	return sig, nil
}

type builtinGetVarSig struct {
	baseBuiltinFunc
}

func (b *builtinGetVarSig) evalString(row types.Row) (string, bool, error) {
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
	tp     *types.FieldType
}

func (c *valuesFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = c.tp
	switch c.tp.EvalType() {
	case types.ETInt:
		sig = &builtinValuesIntSig{bf, c.offset}
	case types.ETReal:
		sig = &builtinValuesRealSig{bf, c.offset}
	case types.ETDecimal:
		sig = &builtinValuesDecimalSig{bf, c.offset}
	case types.ETString:
		sig = &builtinValuesStringSig{bf, c.offset}
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinValuesTimeSig{bf, c.offset}
	case types.ETDuration:
		sig = &builtinValuesDurationSig{bf, c.offset}
	case types.ETJson:
		sig = &builtinValuesJSONSig{bf, c.offset}
	}
	return sig, nil
}

type builtinValuesIntSig struct {
	baseBuiltinFunc

	offset int
}

// evalInt evals a builtinValuesIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesIntSig) evalInt(_ types.Row) (int64, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return 0, true, errors.New("Session current insert values is nil")
	}
	row := values.(types.Row)
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return 0, true, nil
		}
		return row.GetInt64(b.offset), false, nil
	}
	return 0, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesRealSig struct {
	baseBuiltinFunc

	offset int
}

// evalReal evals a builtinValuesRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesRealSig) evalReal(_ types.Row) (float64, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return 0, true, errors.New("Session current insert values is nil")
	}
	row := values.(types.Row)
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return 0, true, nil
		}
		return row.GetFloat64(b.offset), false, nil
	}
	return 0, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesDecimalSig struct {
	baseBuiltinFunc

	offset int
}

// evalDecimal evals a builtinValuesDecimalSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDecimalSig) evalDecimal(_ types.Row) (*types.MyDecimal, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return nil, true, errors.New("Session current insert values is nil")
	}
	row := values.(types.Row)
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return nil, true, nil
		}
		return row.GetMyDecimal(b.offset), false, nil
	}
	return nil, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesStringSig struct {
	baseBuiltinFunc

	offset int
}

// evalString evals a builtinValuesStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesStringSig) evalString(_ types.Row) (string, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return "", true, errors.New("Session current insert values is nil")
	}
	row := values.(types.Row)
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return "", true, nil
		}
		return row.GetString(b.offset), false, nil
	}
	return "", true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesTimeSig struct {
	baseBuiltinFunc

	offset int
}

// evalTime evals a builtinValuesTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesTimeSig) evalTime(_ types.Row) (types.Time, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return types.Time{}, true, errors.New("Session current insert values is nil")
	}
	row := values.(types.Row)
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return types.Time{}, true, nil
		}
		return row.GetTime(b.offset), false, nil
	}
	return types.Time{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesDurationSig struct {
	baseBuiltinFunc

	offset int
}

// evalDuration evals a builtinValuesDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDurationSig) evalDuration(_ types.Row) (types.Duration, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return types.Duration{}, true, errors.New("Session current insert values is nil")
	}
	row := values.(types.Row)
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return types.Duration{}, true, nil
		}
		return row.GetDuration(b.offset), false, nil
	}
	return types.Duration{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesJSONSig struct {
	baseBuiltinFunc

	offset int
}

// evalJSON evals a builtinValuesJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesJSONSig) evalJSON(_ types.Row) (json.BinaryJSON, bool, error) {
	values := b.ctx.GetSessionVars().CurrInsertValues
	if values == nil {
		return json.BinaryJSON{}, true, errors.New("Session current insert values is nil")
	}
	row := values.(types.Row)
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return json.BinaryJSON{}, true, nil
		}
		return row.GetJSON(b.offset), false, nil
	}
	return json.BinaryJSON{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type bitCountFunctionClass struct {
	baseFunctionClass
}

func (c *bitCountFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt)
	bf.tp.Flen = 2
	sig := &builtinBitCountSig{bf}
	return sig, nil
}

type builtinBitCountSig struct {
	baseBuiltinFunc
}

// evalInt evals BIT_COUNT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/bit-functions.html#function_bit-count
func (b *builtinBitCountSig) evalInt(row types.Row) (int64, bool, error) {
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

// getParamFunctionClass for plan cache of prepared statements
type getParamFunctionClass struct {
	baseFunctionClass
}

// getFunction gets function
// TODO: more typed functions will be added when typed parameters are supported.
func (c *getParamFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
	bf.tp.Flen = mysql.MaxFieldVarCharLength
	sig := &builtinGetParamStringSig{bf}
	return sig, nil
}

type builtinGetParamStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGetParamStringSig) evalString(row types.Row) (string, bool, error) {
	sessionVars := b.ctx.GetSessionVars()
	sc := sessionVars.StmtCtx
	idx, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	v := sessionVars.PreparedParams[idx]

	dt := v.(types.Datum)
	str, err := (&dt).ToString()
	if err != nil {
		return "", true, nil
	}
	return str, false, nil
}
