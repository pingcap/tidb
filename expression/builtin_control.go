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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &caseWhenFunctionClass{}
	_ functionClass = &ifFunctionClass{}
	_ functionClass = &ifNullFunctionClass{}
)

var (
	_ builtinFunc = &builtinCaseWhenIntSig{}
	_ builtinFunc = &builtinCaseWhenRealSig{}
	_ builtinFunc = &builtinCaseWhenDecimalSig{}
	_ builtinFunc = &builtinCaseWhenStringSig{}
	_ builtinFunc = &builtinCaseWhenTimeSig{}
	_ builtinFunc = &builtinCaseWhenDurationSig{}
	_ builtinFunc = &builtinIfNullIntSig{}
	_ builtinFunc = &builtinIfNullRealSig{}
	_ builtinFunc = &builtinIfNullDecimalSig{}
	_ builtinFunc = &builtinIfNullStringSig{}
	_ builtinFunc = &builtinIfNullTimeSig{}
	_ builtinFunc = &builtinIfNullDurationSig{}
	_ builtinFunc = &builtinIfNullJSONSig{}
	_ builtinFunc = &builtinIfIntSig{}
	_ builtinFunc = &builtinIfRealSig{}
	_ builtinFunc = &builtinIfDecimalSig{}
	_ builtinFunc = &builtinIfStringSig{}
	_ builtinFunc = &builtinIfTimeSig{}
	_ builtinFunc = &builtinIfDurationSig{}
	_ builtinFunc = &builtinIfJSONSig{}
)

type caseWhenFunctionClass struct {
	baseFunctionClass
}

// Infer result type for builtin IF, IFNULL && NULLIF.
func inferType4ControlFuncs(lhs, rhs *types.FieldType) *types.FieldType {
	resultFieldType := &types.FieldType{}
	if lhs.Tp == mysql.TypeNull {
		*resultFieldType = *rhs
		// If both arguments are NULL, make resulting type BINARY(0).
		if rhs.Tp == mysql.TypeNull {
			resultFieldType.Tp = mysql.TypeString
			resultFieldType.Flen, resultFieldType.Decimal = 0, 0
			types.SetBinChsClnFlag(resultFieldType)
		}
	} else if rhs.Tp == mysql.TypeNull {
		*resultFieldType = *lhs
	} else {
		var unsignedFlag uint
		evalType := types.AggregateEvalType([]*types.FieldType{lhs, rhs}, &unsignedFlag)
		resultFieldType = types.AggFieldType([]*types.FieldType{lhs, rhs})
		if evalType == types.ETInt {
			resultFieldType.Decimal = 0
		} else {
			if lhs.Decimal == types.UnspecifiedLength || rhs.Decimal == types.UnspecifiedLength {
				resultFieldType.Decimal = types.UnspecifiedLength
			} else {
				resultFieldType.Decimal = mathutil.Max(lhs.Decimal, rhs.Decimal)
			}
		}
		if types.IsNonBinaryStr(lhs) && !types.IsBinaryStr(rhs) {
			resultFieldType.Charset, resultFieldType.Collate, resultFieldType.Flag = charset.CharsetUTF8, charset.CollationUTF8, 0
			if mysql.HasBinaryFlag(lhs.Flag) {
				resultFieldType.Flag |= mysql.BinaryFlag
			}
		} else if types.IsNonBinaryStr(rhs) && !types.IsBinaryStr(lhs) {
			resultFieldType.Charset, resultFieldType.Collate, resultFieldType.Flag = charset.CharsetUTF8, charset.CollationUTF8, 0
			if mysql.HasBinaryFlag(rhs.Flag) {
				resultFieldType.Flag |= mysql.BinaryFlag
			}
		} else if types.IsBinaryStr(lhs) || types.IsBinaryStr(rhs) || !evalType.IsStringKind() {
			types.SetBinChsClnFlag(resultFieldType)
		} else {
			resultFieldType.Charset, resultFieldType.Collate, resultFieldType.Flag = charset.CharsetUTF8, charset.CollationUTF8, 0
		}
		if evalType == types.ETDecimal || evalType == types.ETInt {
			lhsUnsignedFlag, rhsUnsignedFlag := mysql.HasUnsignedFlag(lhs.Flag), mysql.HasUnsignedFlag(rhs.Flag)
			lhsFlagLen, rhsFlagLen := 0, 0
			if !lhsUnsignedFlag {
				lhsFlagLen = 1
			}
			if !rhsUnsignedFlag {
				rhsFlagLen = 1
			}
			lhsFlen := lhs.Flen - lhsFlagLen
			rhsFlen := rhs.Flen - rhsFlagLen
			if lhs.Decimal != types.UnspecifiedLength {
				lhsFlen -= lhs.Decimal
			}
			if lhs.Decimal != types.UnspecifiedLength {
				rhsFlen -= rhs.Decimal
			}
			resultFieldType.Flen = mathutil.Max(lhsFlen, rhsFlen) + resultFieldType.Decimal + 1
		} else {
			resultFieldType.Flen = mathutil.Max(lhs.Flen, rhs.Flen)
		}
	}
	// Fix decimal for int and string.
	resultEvalType := resultFieldType.EvalType()
	if resultEvalType == types.ETInt {
		resultFieldType.Decimal = 0
	} else if resultEvalType == types.ETString {
		if lhs.Tp != mysql.TypeNull || rhs.Tp != mysql.TypeNull {
			resultFieldType.Decimal = types.UnspecifiedLength
		}
	}
	return resultFieldType
}

func (c *caseWhenFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
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
	tp := fieldTp.EvalType()

	if tp == types.ETInt {
		decimal = 0
	}
	fieldTp.Decimal, fieldTp.Flen = decimal, flen
	if fieldTp.EvalType().IsStringKind() && !isBinaryStr {
		fieldTp.Charset, fieldTp.Collate = mysql.DefaultCharset, mysql.DefaultCollationName
	}
	// Set retType to BINARY(0) if all arguments are of type NULL.
	if fieldTp.Tp == mysql.TypeNull {
		fieldTp.Flen, fieldTp.Decimal = 0, -1
		types.SetBinChsClnFlag(fieldTp)
	}
	argTps := make([]types.EvalType, 0, l)
	for i := 0; i < l-1; i += 2 {
		argTps = append(argTps, types.ETInt, tp)
	}
	if l%2 == 1 {
		argTps = append(argTps, tp)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, tp, argTps...)
	bf.tp = fieldTp

	switch tp {
	case types.ETInt:
		bf.tp.Decimal = 0
		sig = &builtinCaseWhenIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenInt)
	case types.ETReal:
		sig = &builtinCaseWhenRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenReal)
	case types.ETDecimal:
		sig = &builtinCaseWhenDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenDecimal)
	case types.ETString:
		bf.tp.Decimal = types.UnspecifiedLength
		sig = &builtinCaseWhenStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCaseWhenTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenTime)
	case types.ETDuration:
		sig = &builtinCaseWhenDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenDuration)
	}
	return sig, nil
}

type builtinCaseWhenIntSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinCaseWhenIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenIntSig) evalInt(row types.Row) (ret int64, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(row, sc)
		if err != nil {
			return 0, isNull, errors.Trace(err)
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalInt(row, sc)
		return ret, isNull, errors.Trace(err)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalInt(row, sc)
		return ret, isNull, errors.Trace(err)
	}
	return ret, true, nil
}

type builtinCaseWhenRealSig struct {
	baseBuiltinFunc
}

// evalReal evals a builtinCaseWhenRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenRealSig) evalReal(row types.Row) (ret float64, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(row, sc)
		if err != nil {
			return 0, isNull, errors.Trace(err)
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalReal(row, sc)
		return ret, isNull, errors.Trace(err)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalReal(row, sc)
		return ret, isNull, errors.Trace(err)
	}
	return ret, true, nil
}

type builtinCaseWhenDecimalSig struct {
	baseBuiltinFunc
}

// evalDecimal evals a builtinCaseWhenDecimalSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenDecimalSig) evalDecimal(row types.Row) (ret *types.MyDecimal, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(row, sc)
		if err != nil {
			return nil, isNull, errors.Trace(err)
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalDecimal(row, sc)
		return ret, isNull, errors.Trace(err)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDecimal(row, sc)
		return ret, isNull, errors.Trace(err)
	}
	return ret, true, nil
}

type builtinCaseWhenStringSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinCaseWhenStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenStringSig) evalString(row types.Row) (ret string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(row, sc)
		if err != nil {
			return "", isNull, errors.Trace(err)
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalString(row, sc)
		return ret, isNull, errors.Trace(err)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalString(row, sc)
		return ret, isNull, errors.Trace(err)
	}
	return ret, true, nil
}

type builtinCaseWhenTimeSig struct {
	baseBuiltinFunc
}

// evalTime evals a builtinCaseWhenTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenTimeSig) evalTime(row types.Row) (ret types.Time, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(row, sc)
		if err != nil {
			return ret, isNull, errors.Trace(err)
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalTime(row, sc)
		return ret, isNull, errors.Trace(err)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalTime(row, sc)
		return ret, isNull, errors.Trace(err)
	}
	return ret, true, nil
}

type builtinCaseWhenDurationSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinCaseWhenDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenDurationSig) evalDuration(row types.Row) (ret types.Duration, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(row, sc)
		if err != nil {
			return ret, true, errors.Trace(err)
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalDuration(row, sc)
		return ret, isNull, errors.Trace(err)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDuration(row, sc)
		return ret, isNull, errors.Trace(err)
	}
	return ret, true, nil
}

type ifFunctionClass struct {
	baseFunctionClass
}

// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_if
func (c *ifFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	retTp := inferType4ControlFuncs(args[1].GetType(), args[2].GetType())
	evalTps := retTp.EvalType()
	bf := newBaseBuiltinFuncWithTp(ctx, args, evalTps, types.ETInt, evalTps, evalTps)
	bf.tp = retTp
	switch evalTps {
	case types.ETInt:
		sig = &builtinIfIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfInt)
	case types.ETReal:
		sig = &builtinIfRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfReal)
	case types.ETDecimal:
		sig = &builtinIfDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfDecimal)
	case types.ETString:
		sig = &builtinIfStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinIfTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfTime)
	case types.ETDuration:
		sig = &builtinIfDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfDuration)
	case types.ETJson:
		sig = &builtinIfJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfJson)
	}
	return sig, nil
}

type builtinIfIntSig struct {
	baseBuiltinFunc
}

func (b *builtinIfIntSig) evalInt(row types.Row) (ret int64, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalInt(row, sc)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalInt(row, sc)
	return arg2, isNull2, errors.Trace(err)
}

type builtinIfRealSig struct {
	baseBuiltinFunc
}

func (b *builtinIfRealSig) evalReal(row types.Row) (ret float64, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalReal(row, sc)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalReal(row, sc)
	return arg2, isNull2, errors.Trace(err)
}

type builtinIfDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinIfDecimalSig) evalDecimal(row types.Row) (ret *types.MyDecimal, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return nil, true, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalDecimal(row, sc)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalDecimal(row, sc)
	return arg2, isNull2, errors.Trace(err)
}

type builtinIfStringSig struct {
	baseBuiltinFunc
}

func (b *builtinIfStringSig) evalString(row types.Row) (ret string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return "", true, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalString(row, sc)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalString(row, sc)
	return arg2, isNull2, errors.Trace(err)
}

type builtinIfTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinIfTimeSig) evalTime(row types.Row) (ret types.Time, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return ret, true, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalTime(row, sc)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalTime(row, sc)
	return arg2, isNull2, errors.Trace(err)
}

type builtinIfDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinIfDurationSig) evalDuration(row types.Row) (ret types.Duration, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return ret, true, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalDuration(row, sc)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalDuration(row, sc)
	return arg2, isNull2, errors.Trace(err)
}

type builtinIfJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinIfJSONSig) evalJSON(row types.Row) (ret json.JSON, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return ret, true, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalJSON(row, sc)
	if err != nil {
		return ret, true, errors.Trace(err)
	}
	arg2, isNull2, err := b.args[2].EvalJSON(row, sc)
	if err != nil {
		return ret, true, errors.Trace(err)
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

func (c *ifNullFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = errors.Trace(c.verifyArgs(args)); err != nil {
		return nil, errors.Trace(err)
	}
	lhs, rhs := args[0].GetType(), args[1].GetType()
	retTp := inferType4ControlFuncs(lhs, rhs)
	retTp.Flag |= (lhs.Flag & mysql.NotNullFlag) | (rhs.Flag & mysql.NotNullFlag)
	if lhs.Tp == mysql.TypeNull && rhs.Tp == mysql.TypeNull {
		retTp.Tp = mysql.TypeNull
		retTp.Flen, retTp.Decimal = 0, -1
		types.SetBinChsClnFlag(retTp)
	}
	evalTps := retTp.EvalType()
	bf := newBaseBuiltinFuncWithTp(ctx, args, evalTps, evalTps, evalTps)
	bf.tp = retTp
	switch evalTps {
	case types.ETInt:
		sig = &builtinIfNullIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullInt)
	case types.ETReal:
		sig = &builtinIfNullRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullReal)
	case types.ETDecimal:
		sig = &builtinIfNullDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullDecimal)
	case types.ETString:
		sig = &builtinIfNullStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinIfNullTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullTime)
	case types.ETDuration:
		sig = &builtinIfNullDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullDuration)
	case types.ETJson:
		sig = &builtinIfNullJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IfNullJson)
	}
	return sig, nil
}

type builtinIfNullIntSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullIntSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalInt(row, sc)
	if !isNull || err != nil {
		return arg0, err != nil, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalInt(row, sc)
	return arg1, isNull || err != nil, errors.Trace(err)
}

type builtinIfNullRealSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullRealSig) evalReal(row types.Row) (float64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalReal(row, sc)
	if !isNull || err != nil {
		return arg0, err != nil, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalReal(row, sc)
	return arg1, isNull || err != nil, errors.Trace(err)
}

type builtinIfNullDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullDecimalSig) evalDecimal(row types.Row) (*types.MyDecimal, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalDecimal(row, sc)
	if !isNull || err != nil {
		return arg0, err != nil, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalDecimal(row, sc)
	return arg1, isNull || err != nil, errors.Trace(err)
}

type builtinIfNullStringSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullStringSig) evalString(row types.Row) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalString(row, sc)
	if !isNull || err != nil {
		return arg0, err != nil, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalString(row, sc)
	return arg1, isNull || err != nil, errors.Trace(err)
}

type builtinIfNullTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullTimeSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalTime(row, sc)
	if !isNull || err != nil {
		return arg0, err != nil, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalTime(row, sc)
	return arg1, isNull || err != nil, errors.Trace(err)
}

type builtinIfNullDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullDurationSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalDuration(row, sc)
	if !isNull || err != nil {
		return arg0, err != nil, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalDuration(row, sc)
	return arg1, isNull || err != nil, errors.Trace(err)
}

type builtinIfNullJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullJSONSig) evalJSON(row types.Row) (json.JSON, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalJSON(row, sc)
	if !isNull {
		return arg0, err != nil, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalJSON(row, sc)
	return arg1, isNull || err != nil, errors.Trace(err)
}
