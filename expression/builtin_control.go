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
	"github.com/pingcap/tidb/util/charset"
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
	_ builtinFunc = &builtinNullIfSig{}
	_ builtinFunc = &builtinIfNullIntSig{}
	_ builtinFunc = &builtinIfNullRealSig{}
	_ builtinFunc = &builtinIfNullDecimalSig{}
	_ builtinFunc = &builtinIfNullStringSig{}
	_ builtinFunc = &builtinIfNullTimeSig{}
	_ builtinFunc = &builtinIfNullDurationSig{}
	_ builtinFunc = &builtinIfIntSig{}
	_ builtinFunc = &builtinIfRealSig{}
	_ builtinFunc = &builtinIfDecimalSig{}
	_ builtinFunc = &builtinIfStringSig{}
	_ builtinFunc = &builtinIfDurationSig{}
	_ builtinFunc = &builtinIfTimeSig{}
)

type caseWhenFunctionClass struct {
	baseFunctionClass
}

func (c *caseWhenFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinCaseWhenSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
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

// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_if
func (c *ifFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	retTp := c.inferType(args[1].GetType(), args[2].GetType())
	evalTps := fieldTp2EvalTp(retTp)
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, evalTps, tpInt, evalTps, evalTps)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp = retTp
	switch evalTps {
	case tpInt:
		sig = &builtinIfIntSig{baseIntBuiltinFunc{bf}}
	case tpReal:
		sig = &builtinIfRealSig{baseRealBuiltinFunc{bf}}
	case tpDecimal:
		sig = &builtinIfDecimalSig{baseDecimalBuiltinFunc{bf}}
	case tpString:
		sig = &builtinIfStringSig{baseStringBuiltinFunc{bf}}
	case tpTime:
		sig = &builtinIfTimeSig{baseTimeBuiltinFunc{bf}}
	case tpDuration:
		sig = &builtinIfDurationSig{baseDurationBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

func (c *ifFunctionClass) inferType(tp1, tp2 *types.FieldType) *types.FieldType {
	retTp, typeClass := &types.FieldType{}, types.ClassString
	if tp1.Tp == mysql.TypeNull {
		*retTp, typeClass = *tp2, tp2.ToClass()
		// If both arguments are NULL, make resulting type BINARY(0).
		if tp2.Tp == mysql.TypeNull {
			retTp.Tp, typeClass = mysql.TypeString, types.ClassString
			retTp.Flen, retTp.Decimal = 0, 0
			types.SetBinChsClnFlag(retTp)
		}
	} else if tp2.Tp == mysql.TypeNull {
		*retTp, typeClass = *tp1, tp1.ToClass()
	} else {
		var unsignedFlag uint
		typeClass = types.AggTypeClass([]*types.FieldType{tp1, tp2}, &unsignedFlag)
		retTp = types.AggFieldType([]*types.FieldType{tp1, tp2})
		retTp.Decimal = mathutil.Max(tp1.Decimal, tp2.Decimal)
		types.SetBinChsClnFlag(retTp)
		if types.IsNonBinaryStr(tp1) && !types.IsBinaryStr(tp2) {
			retTp.Charset, retTp.Collate, retTp.Flag = charset.CharsetUTF8, charset.CollationUTF8, 0
			if mysql.HasBinaryFlag(tp1.Flag) {
				retTp.Flag |= mysql.BinaryFlag
			}
		} else if types.IsNonBinaryStr(tp2) && !types.IsBinaryStr(tp1) {
			retTp.Charset, retTp.Collate, retTp.Flag = charset.CharsetUTF8, charset.CollationUTF8, 0
			if mysql.HasBinaryFlag(tp2.Flag) {
				retTp.Flag |= mysql.BinaryFlag
			}
		}
		if typeClass == types.ClassDecimal || typeClass == types.ClassInt {
			unsignedFlag1, unsignedFlag2 := mysql.HasUnsignedFlag(tp1.Flag), mysql.HasUnsignedFlag(tp2.Flag)
			flagLen1, flagLen2 := 0, 0
			if !unsignedFlag1 {
				flagLen1 = 1
			}
			if !unsignedFlag2 {
				flagLen2 = 1
			}
			len1 := tp1.Flen - tp1.Decimal - flagLen1
			len2 := tp2.Flen - tp2.Decimal - flagLen2
			retTp.Flen = mathutil.Max(len1, len2) + retTp.Decimal + 1
		} else {
			retTp.Flen = mathutil.Max(tp1.Flen, tp2.Flen)
		}
	}
	return retTp
}

type builtinIfIntSig struct {
	baseIntBuiltinFunc
}

func (b *builtinIfIntSig) evalInt(row []types.Datum) (ret int64, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalInt(row, sc)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalInt(row, sc)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	switch {
	case isNull0 || arg0 == 0:
		ret, isNull = arg2, isNull2
	case arg0 != 0:
		ret, isNull = arg1, isNull1
	}
	return
}

type builtinIfRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinIfRealSig) evalReal(row []types.Datum) (ret float64, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalReal(row, sc)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalReal(row, sc)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	switch {
	case isNull0 || arg0 == 0:
		ret, isNull = arg2, isNull2
	case arg0 != 0:
		ret, isNull = arg1, isNull1
	}
	return
}

type builtinIfDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (b *builtinIfDecimalSig) evalDecimal(row []types.Datum) (ret *types.MyDecimal, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalDecimal(row, sc)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalDecimal(row, sc)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	switch {
	case isNull0 || arg0 == 0:
		ret, isNull = arg2, isNull2
	case arg0 != 0:
		ret, isNull = arg1, isNull1
	}
	return
}

type builtinIfStringSig struct {
	baseStringBuiltinFunc
}

func (b *builtinIfStringSig) evalString(row []types.Datum) (ret string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return "", false, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalString(row, sc)
	if err != nil {
		return "", false, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalString(row, sc)
	if err != nil {
		return "", false, errors.Trace(err)
	}
	switch {
	case isNull0 || arg0 == 0:
		ret, isNull = arg2, isNull2
	case arg0 != 0:
		ret, isNull = arg1, isNull1
	}
	return
}

type builtinIfTimeSig struct {
	baseTimeBuiltinFunc
}

func (b *builtinIfTimeSig) evalTime(row []types.Datum) (ret types.Time, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return ret, false, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalTime(row, sc)
	if err != nil {
		return ret, false, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalTime(row, sc)
	if err != nil {
		return ret, false, errors.Trace(err)
	}
	switch {
	case isNull0 || arg0 == 0:
		ret, isNull = arg2, isNull2
	case arg0 != 0:
		ret, isNull = arg1, isNull1
	}
	return
}

type builtinIfDurationSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinIfDurationSig) evalDuration(row []types.Datum) (ret types.Duration, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return ret, false, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalDuration(row, sc)
	if err != nil {
		return ret, false, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalDuration(row, sc)
	if err != nil {
		return ret, false, errors.Trace(err)
	}
	switch {
	case isNull0 || arg0 == 0:
		ret, isNull = arg2, isNull2
	case arg0 != 0:
		ret, isNull = arg1, isNull1
	}
	return
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
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinNullIfSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
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
