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
	_ builtinFunc = &builtinCaseWhenIntSig{}
	_ builtinFunc = &builtinCaseWhenRealSig{}
	_ builtinFunc = &builtinCaseWhenDecimalSig{}
	_ builtinFunc = &builtinCaseWhenStringSig{}
	_ builtinFunc = &builtinCaseWhenTimeSig{}
	_ builtinFunc = &builtinCaseWhenDurationSig{}
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

func (c *caseWhenFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	l := len(args)
	// Fill in each 'THEN' clause parameter type.
	fieldTps := make([]*types.FieldType, 0, (l+1)/2)
	decimal, flen, isBinaryStr := args[1].GetType().Decimal, 0, false
	for i := 1; i < l; i += 2 {
		fieldTps = append(fieldTps, args[i].GetType())
		decimal = mathutil.Max(decimal, args[i].GetType().Decimal)
		flen = mathutil.Max(flen, args[i].GetType().Flen)
		isBinaryStr = isBinaryStr || types.IsBinaryStr(args[i].GetType())
	}
	if l%2 == 1 {
		fieldTps = append(fieldTps, args[l-1].GetType())
		decimal = mathutil.Max(decimal, args[l-1].GetType().Decimal)
		flen = mathutil.Max(flen, args[l-1].GetType().Flen)
		isBinaryStr = isBinaryStr || types.IsBinaryStr(args[l-1].GetType())
	}
	fieldTp := types.AggFieldType(fieldTps)
	classType := types.AggTypeClass(fieldTps, &fieldTp.Flag)
	// Set retType to BINARY(0) if all arguments are of type NULL
	if fieldTp.Tp == mysql.TypeNull {
		fieldTp.Flen, fieldTp.Decimal = 0, 0
		types.SetBinChsClnFlag(fieldTp)
	}
	fieldTp.Decimal, fieldTp.Flen = decimal, flen
	var tp evalTp
	switch classType {
	case types.ClassInt:
		tp = tpInt
		fieldTp.Decimal = 0
	case types.ClassReal:
		tp = tpReal
	case types.ClassDecimal:
		tp = tpDecimal
	case types.ClassString:
		tp = tpString
		if !isBinaryStr {
			fieldTp.Charset, fieldTp.Collate = mysql.DefaultCharset, mysql.DefaultCollationName
		}
		if types.IsTypeTime(fieldTp.Tp) {
			tp = tpTime
		} else if fieldTp.Tp == mysql.TypeDuration {
			tp = tpDuration
		}
	}

	argTps := make([]evalTp, 0, l)
	for i := 0; i < l-1; i += 2 {
		argTps = append(argTps, tpInt, tp)
	}
	if l%2 == 1 {
		argTps = append(argTps, tp)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tp, argTps...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp = fieldTp
	switch classType {
	case types.ClassInt:
		sig = &builtinCaseWhenIntSig{baseIntBuiltinFunc{bf}}
	case types.ClassReal:
		sig = &builtinCaseWhenRealSig{baseRealBuiltinFunc{bf}}
	case types.ClassDecimal:
		sig = &builtinCaseWhenDecimalSig{baseDecimalBuiltinFunc{bf}}
	case types.ClassString:
		sig = &builtinCaseWhenStringSig{baseStringBuiltinFunc{bf}}
		if types.IsTypeTime(fieldTp.Tp) {
			sig = &builtinCaseWhenTimeSig{baseTimeBuiltinFunc{bf}}
		} else if fieldTp.Tp == mysql.TypeDuration {
			sig = &builtinCaseWhenDurationSig{baseDurationBuiltinFunc{bf}}
		}
	}
	return sig.setSelf(sig), nil
}

type builtinCaseWhenIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinCaseWhenIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	var (
		arg    int64
		isNull bool
		err    error
	)
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		arg, isNull, err = args[i].EvalInt(row, sc)
		if err != nil {
			return 0, isNull, errors.Trace(err)
		}
		if isNull {
			continue
		}
		if arg != 0 {
			arg, isNull, err = args[i+1].EvalInt(row, sc)
			if err != nil {
				return 0, isNull, errors.Trace(err)
			}
			return arg, isNull, nil
		}
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		arg, isNull, err = args[l-1].EvalInt(row, sc)
		if err != nil {
			return arg, isNull, errors.Trace(err)
		}
		return arg, isNull, nil
	}
	return 0, true, nil
}

type builtinCaseWhenRealSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinCaseWhenRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	var (
		condition int64
		ret       float64
		isNull    bool
		err       error
	)
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(row, sc)
		if err != nil {
			return 0, isNull, errors.Trace(err)
		}
		if isNull {
			continue
		}
		if condition != 0 {
			ret, isNull, err = args[i+1].EvalReal(row, sc)
			if err != nil {
				return ret, isNull, errors.Trace(err)
			}
			return ret, isNull, nil
		}
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalReal(row, sc)
		if err != nil {
			return ret, isNull, errors.Trace(err)
		}
		return ret, isNull, nil
	}
	return 0, true, nil
}

type builtinCaseWhenDecimalSig struct {
	baseDecimalBuiltinFunc
}

// evalDecimal evals a builtinCaseWhenDecimalSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenDecimalSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	var (
		condition int64
		ret       *types.MyDecimal
		isNull    bool
		err       error
	)
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(row, sc)
		if err != nil {
			return nil, isNull, errors.Trace(err)
		}
		if isNull {
			continue
		}
		if condition != 0 {
			ret, isNull, err = args[i+1].EvalDecimal(row, sc)
			if err != nil {
				return nil, isNull, errors.Trace(err)
			}
			return ret, isNull, nil
		}
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDecimal(row, sc)
		if err != nil {
			return ret, isNull, errors.Trace(err)
		}
		return ret, isNull, nil
	}
	return nil, true, nil
}

type builtinCaseWhenStringSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinCaseWhenStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenStringSig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	var (
		condition int64
		ret       string
		isNull    bool
		err       error
	)
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(row, sc)
		if err != nil {
			return "", isNull, errors.Trace(err)
		}
		if isNull {
			continue
		}
		if condition != 0 {
			ret, isNull, err = args[i+1].EvalString(row, sc)
			if err != nil {
				return "", isNull, errors.Trace(err)
			}
			return ret, isNull, nil
		}
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalString(row, sc)
		if err != nil {
			return "", isNull, errors.Trace(err)
		}
		return ret, isNull, nil
	}
	return "", true, nil
}

type builtinCaseWhenTimeSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals a builtinCaseWhenTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenTimeSig) evalTime(row []types.Datum) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	var (
		condition int64
		ret       types.Time
		isNull    bool
		err       error
	)
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(row, sc)
		if err != nil {
			return ret, isNull, errors.Trace(err)
		}
		if isNull {
			continue
		}
		if condition != 0 {
			ret, isNull, err = args[i+1].EvalTime(row, sc)
			if err != nil {
				return ret, isNull, errors.Trace(err)
			}
			return ret, isNull, nil
		}
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalTime(row, sc)
		if err != nil {
			return ret, isNull, errors.Trace(err)
		}
		return ret, isNull, nil
	}
	return types.Time{}, true, nil
}

type builtinCaseWhenDurationSig struct {
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinCaseWhenDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenDurationSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	var (
		condition int64
		ret       types.Duration
		isNull    bool
		err       error
	)
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(row, sc)
		if err != nil {
			return ret, false, errors.Trace(err)
		}
		if isNull {
			continue
		}
		if condition != 0 {
			ret, isNull, err = args[i+1].EvalDuration(row, sc)
			if err != nil {
				return ret, isNull, errors.Trace(err)
			}
			return ret, isNull, nil
		}
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDuration(row, sc)
		if err != nil {
			return ret, isNull, errors.Trace(err)
		}
		return ret, isNull, nil
	}
	return types.Duration{}, true, nil
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
