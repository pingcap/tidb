// Copyright 2015 PingCAP, Inc.
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
	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &caseWhenFunctionClass{}
	_ functionClass = &ifFunctionClass{}
	_ functionClass = &ifNullFunctionClass{}
	_ functionClass = &nullIfFunctionClass{}
)

var (
	_ builtinFunc = &builtinCaseWhenSig{}
	_ builtinFunc = &builtinIfSig{}
	_ builtinFunc = &builtinNullIfSig{}
	_ builtinFunc = &builtinIfNullIntSig{}
	_ builtinFunc = &builtinIfNullRealSig{}
	_ builtinFunc = &builtinIfNullDecimalSig{}
	_ builtinFunc = &builtinIfNullStringSig{}
	_ builtinFunc = &builtinIfNullTimeSig{}
	_ builtinFunc = &builtinIfNullDurationSig{}
)

type caseWhenFunctionClass struct {
	baseFunctionClass
}

func (c *caseWhenFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinCaseWhenSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinCaseWhenSig struct {
	baseBuiltinFunc
}

// eval evals a builtinCaseWhenSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	sc := b.ctx.GetSessionVars().StmtCtx
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

type ifFunctionClass struct {
	baseFunctionClass
}

func (c *ifFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinIfSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinIfSig struct {
	baseBuiltinFunc
}

// eval evals a builtinIfSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_if
func (s *builtinIfSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := s.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// if(expr1, expr2, expr3)
	// if expr1 is true, return expr2, otherwise, return expr3
	v1 := args[0]
	v2 := args[1]
	v3 := args[2]

	if v1.IsNull() {
		return v3, nil
	}

	b, err := v1.ToBool(s.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		d := types.Datum{}
		return d, errors.Trace(err)
	}

	// TODO: check return type, must be numeric or string
	if b == 1 {
		return v2, nil
	}

	return v3, nil
}

type ifNullFunctionClass struct {
	baseFunctionClass
}

func (c *ifNullFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	if err = errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, errors.Trace(err)
	}
	tp0, tp1 := args[0].GetType(), args[1].GetType()
	fieldTp := types.AggFieldType([]*types.FieldType{tp0, tp1})
	types.SetBinChsClnFlag(fieldTp)
	classType := types.AggTypeClass([]*types.FieldType{tp0, tp1}, &fieldTp.Flag)
	fieldTp.Decimal = mathutil.Max(tp0.Decimal, tp1.Decimal)
	// TODO: make it more accurate when inferring FLEN
	fieldTp.Flen = tp0.Flen + tp1.Flen

	var evalTps evalTp
	switch classType {
	case types.ClassInt:
		evalTps = tpInt
		fieldTp.Decimal = 0
	case types.ClassReal:
		evalTps = tpReal
	case types.ClassDecimal:
		evalTps = tpDecimal
	case types.ClassString:
		evalTps = tpString
		if !types.IsBinaryStr(tp0) && !types.IsBinaryStr(tp1) {
			fieldTp.Charset, fieldTp.Collate = mysql.DefaultCharset, mysql.DefaultCollationName
			fieldTp.Flag ^= mysql.BinaryFlag
		}
		if types.IsTypeTime(fieldTp.Tp) {
			evalTps = tpTime
		} else if fieldTp.Tp == mysql.TypeDuration {
			evalTps = tpDuration
		}
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, evalTps, evalTps, evalTps)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp = fieldTp
	switch classType {
	case types.ClassInt:
		sig = &builtinIfNullIntSig{baseIntBuiltinFunc{bf}}
	case types.ClassReal:
		sig = &builtinIfNullRealSig{baseRealBuiltinFunc{bf}}
	case types.ClassDecimal:
		sig = &builtinIfNullDecimalSig{baseDecimalBuiltinFunc{bf}}
	case types.ClassString:
		sig = &builtinIfNullStringSig{baseStringBuiltinFunc{bf}}
		if types.IsTypeTime(fieldTp.Tp) {
			sig = &builtinIfNullTimeSig{baseTimeBuiltinFunc{bf}}
		} else if fieldTp.Tp == mysql.TypeDuration {
			sig = &builtinIfNullDurationSig{baseDurationBuiltinFunc{bf}}
		}
	}
	return sig.setSelf(sig), nil
}

type builtinIfNullIntSig struct {
	baseIntBuiltinFunc
}

func (b *builtinIfNullIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalInt(row, sc)
	if !isNull {
		return arg0, false, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalInt(row, sc)
	return arg1, isNull, errors.Trace(err)
}

type builtinIfNullRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinIfNullRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalReal(row, sc)
	if !isNull {
		return arg0, false, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalReal(row, sc)
	return arg1, isNull, errors.Trace(err)
}

type builtinIfNullDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (b *builtinIfNullDecimalSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalDecimal(row, sc)
	if !isNull {
		return arg0, false, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalDecimal(row, sc)
	return arg1, isNull, errors.Trace(err)
}

type builtinIfNullStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinIfNullStringSig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalString(row, sc)
	if !isNull {
		return arg0, false, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalString(row, sc)
	return arg1, isNull, errors.Trace(err)
}

type builtinIfNullTimeSig struct {
	baseTimeBuiltinFunc
}

func (b *builtinIfNullTimeSig) evalTime(row []types.Datum) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalTime(row, sc)
	if !isNull {
		return arg0, false, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalTime(row, sc)
	return arg1, isNull, errors.Trace(err)
}

type builtinIfNullDurationSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinIfNullDurationSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalDuration(row, sc)
	if !isNull {
		return arg0, false, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalDuration(row, sc)
	return arg1, isNull, errors.Trace(err)
}

type nullIfFunctionClass struct {
	baseFunctionClass
}

func (c *nullIfFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinNullIfSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinNullIfSig struct {
	baseBuiltinFunc
}

// eval evals a builtinNullIfSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_nullif
func (b *builtinNullIfSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// nullif(expr1, expr2)
	// returns null if expr1 = expr2 is true, otherwise returns expr1
	v1 := args[0]
	v2 := args[1]

	if v1.IsNull() || v2.IsNull() {
		return v1, nil
	}

	if n, err1 := v1.CompareDatum(b.ctx.GetSessionVars().StmtCtx, v2); err1 != nil || n == 0 {
		d := types.Datum{}
		return d, errors.Trace(err1)
	}

	return v1, nil
}
