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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mathutil"
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
	_ builtinFunc = &builtinCaseWhenJSONSig{}
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

func maxlen(lhsFlen, rhsFlen int) int {
	// -1 indicates that the length is unknown, such as the case for expressions.
	if lhsFlen < 0 || rhsFlen < 0 {
		return mysql.MaxRealWidth
	}
	return mathutil.Max(lhsFlen, rhsFlen)
}

// InferType4ControlFuncs infer result type for builtin IF, IFNULL, NULLIF, LEAD and LAG.
func InferType4ControlFuncs(ctx sessionctx.Context, funcName string, lexp, rexp Expression) (*types.FieldType, error) {
	lhs, rhs := lexp.GetType(), rexp.GetType()
	resultFieldType := &types.FieldType{}
	if lhs.GetType() == mysql.TypeNull {
		*resultFieldType = *rhs
		// If any of arg is NULL, result type need unset NotNullFlag.
		tempFlag := resultFieldType.GetFlag()
		types.SetTypeFlag(&tempFlag, mysql.NotNullFlag, false)
		resultFieldType.SetFlag(tempFlag)
		// If both arguments are NULL, make resulting type BINARY(0).
		if rhs.GetType() == mysql.TypeNull {
			resultFieldType.SetType(mysql.TypeString)
			resultFieldType.SetFlen(0)
			resultFieldType.SetDecimal(0)
			types.SetBinChsClnFlag(resultFieldType)
		}
	} else if rhs.GetType() == mysql.TypeNull {
		*resultFieldType = *lhs
		tempFlag := resultFieldType.GetFlag()
		types.SetTypeFlag(&tempFlag, mysql.NotNullFlag, false)
		resultFieldType.SetFlag(tempFlag)
	} else {
		resultFieldType = types.AggFieldType([]*types.FieldType{lhs, rhs})
		var tempFlag uint
		evalType := types.AggregateEvalType([]*types.FieldType{lhs, rhs}, &tempFlag)
		resultFieldType.SetFlag(tempFlag)
		if evalType == types.ETInt {
			resultFieldType.SetDecimal(0)
		} else {
			if lhs.GetDecimal() == types.UnspecifiedLength || rhs.GetDecimal() == types.UnspecifiedLength {
				resultFieldType.SetDecimal(types.UnspecifiedLength)
			} else {
				resultFieldType.SetDecimal(mathutil.Max(lhs.GetDecimal(), rhs.GetDecimal()))
			}
		}

		if types.IsNonBinaryStr(lhs) && !types.IsBinaryStr(rhs) {
			ec, err := CheckAndDeriveCollationFromExprs(ctx, funcName, evalType, lexp, rexp)
			if err != nil {
				return nil, err
			}
			resultFieldType.SetCollate(ec.Collation)
			resultFieldType.SetCharset(ec.Charset)
			resultFieldType.SetFlag(0)
			if mysql.HasBinaryFlag(lhs.GetFlag()) || !types.IsNonBinaryStr(rhs) {
				resultFieldType.AddFlag(mysql.BinaryFlag)
			}
		} else if types.IsNonBinaryStr(rhs) && !types.IsBinaryStr(lhs) {
			ec, err := CheckAndDeriveCollationFromExprs(ctx, funcName, evalType, lexp, rexp)
			if err != nil {
				return nil, err
			}
			resultFieldType.SetCollate(ec.Collation)
			resultFieldType.SetCharset(ec.Charset)
			resultFieldType.SetFlag(0)
			if mysql.HasBinaryFlag(rhs.GetFlag()) || !types.IsNonBinaryStr(lhs) {
				resultFieldType.AddFlag(mysql.BinaryFlag)
			}
		} else if types.IsBinaryStr(lhs) || types.IsBinaryStr(rhs) || !evalType.IsStringKind() {
			types.SetBinChsClnFlag(resultFieldType)
		} else {
			resultFieldType.SetCharset(mysql.DefaultCharset)
			resultFieldType.SetCollate(mysql.DefaultCollationName)
			resultFieldType.SetFlag(0)
		}
		if evalType == types.ETDecimal || evalType == types.ETInt {
			lhsUnsignedFlag, rhsUnsignedFlag := mysql.HasUnsignedFlag(lhs.GetFlag()), mysql.HasUnsignedFlag(rhs.GetFlag())
			lhsFlagLen, rhsFlagLen := 0, 0
			if !lhsUnsignedFlag {
				lhsFlagLen = 1
			}
			if !rhsUnsignedFlag {
				rhsFlagLen = 1
			}
			lhsFlen := lhs.GetFlen() - lhsFlagLen
			rhsFlen := rhs.GetFlen() - rhsFlagLen
			if lhs.GetDecimal() != types.UnspecifiedLength {
				lhsFlen -= lhs.GetDecimal()
			}
			if lhs.GetDecimal() != types.UnspecifiedLength {
				rhsFlen -= rhs.GetDecimal()
			}
			flen := maxlen(lhsFlen, rhsFlen) + resultFieldType.GetDecimal() + 1 // account for -1 len fields
			resultFieldType.SetFlen(mathutil.Min(flen, mysql.MaxDecimalWidth))  // make sure it doesn't overflow

		} else {
			resultFieldType.SetFlen(maxlen(lhs.GetFlen(), rhs.GetFlen()))
		}
	}
	// Fix decimal for int and string.
	resultEvalType := resultFieldType.EvalType()
	if resultEvalType == types.ETInt {
		resultFieldType.SetDecimal(0)
		if resultFieldType.GetType() == mysql.TypeEnum || resultFieldType.GetType() == mysql.TypeSet {
			resultFieldType.SetType(mysql.TypeLonglong)
		}
	} else if resultEvalType == types.ETString {
		if lhs.GetType() != mysql.TypeNull || rhs.GetType() != mysql.TypeNull {
			resultFieldType.SetDecimal(types.UnspecifiedLength)
		}
		if resultFieldType.GetType() == mysql.TypeEnum || resultFieldType.GetType() == mysql.TypeSet {
			resultFieldType.SetType(mysql.TypeVarchar)
		}
	} else if resultFieldType.GetType() == mysql.TypeDatetime {
		types.TryToFixFlenOfDatetime(resultFieldType)
	}
	return resultFieldType, nil
}

type caseWhenFunctionClass struct {
	baseFunctionClass
}

func (c *caseWhenFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	l := len(args)
	// Fill in each 'THEN' clause parameter type.
	fieldTps := make([]*types.FieldType, 0, (l+1)/2)
	decimal, flen, isBinaryFlag := args[1].GetType().GetDecimal(), 0, false
	for i := 1; i < l; i += 2 {
		fieldTps = append(fieldTps, args[i].GetType())
		decimal = mathutil.Max(decimal, args[i].GetType().GetDecimal())
		if args[i].GetType().GetFlen() == -1 {
			flen = -1
		} else if flen != -1 {
			flen = mathutil.Max(flen, args[i].GetType().GetFlen())
		}
		isBinaryFlag = isBinaryFlag || !types.IsNonBinaryStr(args[i].GetType())
	}
	if l%2 == 1 {
		fieldTps = append(fieldTps, args[l-1].GetType())
		decimal = mathutil.Max(decimal, args[l-1].GetType().GetDecimal())
		if args[l-1].GetType().GetFlen() == -1 {
			flen = -1
		} else if flen != -1 {
			flen = mathutil.Max(flen, args[l-1].GetType().GetFlen())
		}
		isBinaryFlag = isBinaryFlag || !types.IsNonBinaryStr(args[l-1].GetType())
	}

	fieldTp := types.AggFieldType(fieldTps)
	// Here we turn off NotNullFlag. Because if all when-clauses are false,
	// the result of case-when expr is NULL.
	tempFlag := fieldTp.GetFlag()
	types.SetTypeFlag(&tempFlag, mysql.NotNullFlag, false)
	fieldTp.SetFlag(tempFlag)
	tp := fieldTp.EvalType()

	if tp == types.ETInt {
		decimal = 0
	}
	fieldTp.SetDecimal(decimal)
	fieldTp.SetFlen(flen)
	types.TryToFixFlenOfDatetime(fieldTp)
	if isBinaryFlag {
		fieldTp.AddFlag(mysql.BinaryFlag)
	}
	// Set retType to BINARY(0) if all arguments are of type NULL.
	if fieldTp.GetType() == mysql.TypeNull {
		fieldTp.SetFlen(0)
		fieldTp.SetDecimal(types.UnspecifiedLength)
		types.SetBinChsClnFlag(fieldTp)
	}
	argTps := make([]types.EvalType, 0, l)
	for i := 0; i < l-1; i += 2 {
		if args[i], err = wrapWithIsTrue(ctx, true, args[i], false); err != nil {
			return nil, err
		}
		argTps = append(argTps, types.ETInt, tp)
	}
	if l%2 == 1 {
		argTps = append(argTps, tp)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, tp, argTps...)
	if err != nil {
		return nil, err
	}
	fieldTp.SetCharset(bf.tp.GetCharset())
	fieldTp.SetCollate(bf.tp.GetCollate())
	bf.tp = fieldTp
	if fieldTp.GetType() == mysql.TypeEnum || fieldTp.GetType() == mysql.TypeSet {
		switch tp {
		case types.ETInt:
			fieldTp.SetType(mysql.TypeLonglong)
		case types.ETString:
			fieldTp.SetType(mysql.TypeVarchar)
		}
	}

	switch tp {
	case types.ETInt:
		bf.tp.SetDecimal(0)
		sig = &builtinCaseWhenIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenInt)
	case types.ETReal:
		sig = &builtinCaseWhenRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenReal)
	case types.ETDecimal:
		sig = &builtinCaseWhenDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenDecimal)
	case types.ETString:
		bf.tp.SetDecimal(types.UnspecifiedLength)
		sig = &builtinCaseWhenStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCaseWhenTimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenTime)
	case types.ETDuration:
		sig = &builtinCaseWhenDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenDuration)
	case types.ETJson:
		sig = &builtinCaseWhenJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CaseWhenJson)
	}
	return sig, nil
}

type builtinCaseWhenIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenIntSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCaseWhenIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenIntSig) evalInt(row chunk.Row) (ret int64, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			return 0, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalInt(b.ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalInt(b.ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenRealSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCaseWhenRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenRealSig) evalReal(row chunk.Row) (ret float64, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			return 0, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalReal(b.ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalReal(b.ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenDecimalSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCaseWhenDecimalSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenDecimalSig) evalDecimal(row chunk.Row) (ret *types.MyDecimal, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			return nil, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalDecimal(b.ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDecimal(b.ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenStringSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinCaseWhenStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenStringSig) evalString(row chunk.Row) (ret string, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			return "", isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalString(b.ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalString(b.ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenTimeSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinCaseWhenTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenTimeSig) evalTime(row chunk.Row) (ret types.Time, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			return ret, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalTime(b.ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalTime(b.ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenDurationSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinCaseWhenDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenDurationSig) evalDuration(row chunk.Row) (ret types.Duration, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			return ret, true, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalDuration(b.ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDuration(b.ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenJSONSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalJSON evals a builtinCaseWhenJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenJSONSig) evalJSON(row chunk.Row) (ret json.BinaryJSON, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, row)
		if err != nil {
			return
		}
		if isNull || condition == 0 {
			continue
		}
		return args[i+1].EvalJSON(b.ctx, row)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		return args[l-1].EvalJSON(b.ctx, row)
	}
	return ret, true, nil
}

type ifFunctionClass struct {
	baseFunctionClass
}

// getFunction see https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_if
func (c *ifFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	retTp, err := InferType4ControlFuncs(ctx, c.funcName, args[1], args[2])
	if err != nil {
		return nil, err
	}
	evalTps := retTp.EvalType()
	args[0], err = wrapWithIsTrue(ctx, true, args[0], false)
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, evalTps, types.ETInt, evalTps, evalTps)
	if err != nil {
		return nil, err
	}

	retTp.AddFlag(bf.tp.GetFlag())
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

func (b *builtinIfIntSig) Clone() builtinFunc {
	newSig := &builtinIfIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfIntSig) evalInt(row chunk.Row) (ret int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalInt(b.ctx, row)
	}
	return b.args[2].EvalInt(b.ctx, row)
}

type builtinIfRealSig struct {
	baseBuiltinFunc
}

func (b *builtinIfRealSig) Clone() builtinFunc {
	newSig := &builtinIfRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfRealSig) evalReal(row chunk.Row) (ret float64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalReal(b.ctx, row)
	}
	return b.args[2].EvalReal(b.ctx, row)
}

type builtinIfDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinIfDecimalSig) Clone() builtinFunc {
	newSig := &builtinIfDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfDecimalSig) evalDecimal(row chunk.Row) (ret *types.MyDecimal, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return nil, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalDecimal(b.ctx, row)
	}
	return b.args[2].EvalDecimal(b.ctx, row)
}

type builtinIfStringSig struct {
	baseBuiltinFunc
}

func (b *builtinIfStringSig) Clone() builtinFunc {
	newSig := &builtinIfStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfStringSig) evalString(row chunk.Row) (ret string, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return "", true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalString(b.ctx, row)
	}
	return b.args[2].EvalString(b.ctx, row)
}

type builtinIfTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinIfTimeSig) Clone() builtinFunc {
	newSig := &builtinIfTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfTimeSig) evalTime(row chunk.Row) (ret types.Time, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalTime(b.ctx, row)
	}
	return b.args[2].EvalTime(b.ctx, row)
}

type builtinIfDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinIfDurationSig) Clone() builtinFunc {
	newSig := &builtinIfDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfDurationSig) evalDuration(row chunk.Row) (ret types.Duration, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalDuration(b.ctx, row)
	}
	return b.args[2].EvalDuration(b.ctx, row)
}

type builtinIfJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinIfJSONSig) Clone() builtinFunc {
	newSig := &builtinIfJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfJSONSig) evalJSON(row chunk.Row) (ret json.BinaryJSON, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalJSON(b.ctx, row)
	}
	return b.args[2].EvalJSON(b.ctx, row)
}

type ifNullFunctionClass struct {
	baseFunctionClass
}

func (c *ifNullFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhs, rhs := args[0].GetType(), args[1].GetType()
	retTp, err := InferType4ControlFuncs(ctx, c.funcName, args[0], args[1])
	if err != nil {
		return nil, err
	}

	retTp.AddFlag((lhs.GetFlag() & mysql.NotNullFlag) | (rhs.GetFlag() & mysql.NotNullFlag))
	if lhs.GetType() == mysql.TypeNull && rhs.GetType() == mysql.TypeNull {
		retTp.SetType(mysql.TypeNull)
		retTp.SetFlen(0)
		retTp.SetDecimal(-1)
		types.SetBinChsClnFlag(retTp)
	}
	evalTps := retTp.EvalType()
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, evalTps, evalTps, evalTps)
	if err != nil {
		return nil, err
	}
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

func (b *builtinIfNullIntSig) Clone() builtinFunc {
	newSig := &builtinIfNullIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalInt(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullRealSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullRealSig) Clone() builtinFunc {
	newSig := &builtinIfNullRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	arg0, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalReal(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullDecimalSig) Clone() builtinFunc {
	newSig := &builtinIfNullDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	arg0, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalDecimal(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullStringSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullStringSig) Clone() builtinFunc {
	newSig := &builtinIfNullStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullStringSig) evalString(row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalString(b.ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalString(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullTimeSig) Clone() builtinFunc {
	newSig := &builtinIfNullTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullTimeSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalTime(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullDurationSig) Clone() builtinFunc {
	newSig := &builtinIfNullDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullJSONSig) Clone() builtinFunc {
	newSig := &builtinIfNullJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullJSONSig) evalJSON(row chunk.Row) (json.BinaryJSON, bool, error) {
	arg0, isNull, err := b.args[0].EvalJSON(b.ctx, row)
	if !isNull {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalJSON(b.ctx, row)
	return arg1, isNull || err != nil, err
}
