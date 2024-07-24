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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mathutil"
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

func setFlenFromArgs(evalType types.EvalType, resultFieldType *types.FieldType, argTps ...*types.FieldType) {
	if evalType == types.ETDecimal || evalType == types.ETInt {
		maxArgFlen := 0
		for i := range argTps {
			flagLen := 0
			if !mysql.HasUnsignedFlag(argTps[i].GetFlag()) {
				flagLen = 1
			}
			flen := argTps[i].GetFlen() - flagLen
			if argTps[i].GetDecimal() != types.UnspecifiedLength {
				flen -= argTps[i].GetDecimal()
			}
			maxArgFlen = maxlen(maxArgFlen, flen)
		}
		// For a decimal field, the `length` and `flen` are not the same.
		// `length` only holds the binary data, while `flen` represents the number of digits required to display the field, including the negative sign.
		// In the current implementation of TiDB, `flen` and `length` are treated as the same, so the `length` of a decimal may be inconsistent with that of MySQL.
		resultFlen := maxArgFlen + resultFieldType.GetDecimal() + 1 // account for -1 len fields
		resultFieldType.SetFlenUnderLimit(resultFlen)
	} else if evalType == types.ETString {
		maxLen := 0
		for i := range argTps {
			argFlen := argTps[i].GetFlen()
			if argFlen == types.UnspecifiedLength {
				resultFieldType.SetFlen(types.UnspecifiedLength)
				return
			}
			maxLen = maxlen(argFlen, maxLen)
		}
		resultFieldType.SetFlen(maxLen)
	} else {
		maxLen := 0
		for i := range argTps {
			maxLen = maxlen(argTps[i].GetFlen(), maxLen)
		}
		resultFieldType.SetFlen(maxLen)
	}
}

func setDecimalFromArgs(evalType types.EvalType, resultFieldType *types.FieldType, argTps ...*types.FieldType) {
	if evalType == types.ETInt {
		resultFieldType.SetDecimal(0)
	} else {
		maxDecimal := 0
		for i := range argTps {
			if argTps[i].GetDecimal() == types.UnspecifiedLength {
				resultFieldType.SetDecimal(types.UnspecifiedLength)
				return
			}
			maxDecimal = mathutil.Max(argTps[i].GetDecimal(), maxDecimal)
		}
		resultFieldType.SetDecimalUnderLimit(maxDecimal)
	}
}

// NonBinaryStr means the arg is a string but not binary string
func hasNonBinaryStr(args []*types.FieldType) bool {
	for _, arg := range args {
		if types.IsNonBinaryStr(arg) {
			return true
		}
	}
	return false
}

func hasBinaryStr(args []*types.FieldType) bool {
	for _, arg := range args {
		if types.IsBinaryStr(arg) {
			return true
		}
	}
	return false
}

func addCollateAndCharsetAndFlagFromArgs(ctx BuildContext, funcName string, evalType types.EvalType, resultFieldType *types.FieldType, args ...Expression) error {
	switch funcName {
	case ast.If, ast.Ifnull, ast.WindowFuncLead, ast.WindowFuncLag:
		if len(args) != 2 {
			panic("unexpected length of args for if/ifnull/lead/lag")
		}
		lexp, rexp := args[0], args[1]
		lhs, rhs := lexp.GetType(ctx.GetEvalCtx()), rexp.GetType(ctx.GetEvalCtx())
		if types.IsNonBinaryStr(lhs) && !types.IsBinaryStr(rhs) {
			ec, err := CheckAndDeriveCollationFromExprs(ctx, funcName, evalType, lexp, rexp)
			if err != nil {
				return err
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
				return err
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
	case ast.Case:
		if len(args) == 0 {
			panic("unexpected length 0 of args for casewhen")
		}
		ec, err := CheckAndDeriveCollationFromExprs(ctx, funcName, evalType, args...)
		if err != nil {
			return err
		}
		resultFieldType.SetCollate(ec.Collation)
		resultFieldType.SetCharset(ec.Charset)
		for i := range args {
			if mysql.HasBinaryFlag(args[i].GetType(ctx.GetEvalCtx()).GetFlag()) || !types.IsNonBinaryStr(args[i].GetType(ctx.GetEvalCtx())) {
				resultFieldType.AddFlag(mysql.BinaryFlag)
				break
			}
		}
	case ast.Coalesce: // TODO ast.Case and ast.Coalesce should be merged into the same branch
		argTypes := make([]*types.FieldType, 0)
		for _, arg := range args {
			argTypes = append(argTypes, arg.GetType(ctx.GetEvalCtx()))
		}

		nonBinaryStrExist := hasNonBinaryStr(argTypes)
		binaryStrExist := hasBinaryStr(argTypes)
		if !binaryStrExist && nonBinaryStrExist {
			ec, err := CheckAndDeriveCollationFromExprs(ctx, funcName, evalType, args...)
			if err != nil {
				return err
			}
			resultFieldType.SetCollate(ec.Collation)
			resultFieldType.SetCharset(ec.Charset)
			resultFieldType.SetFlag(0)

			// hasNonStringType means that there is a type that is not string
			hasNonStringType := false
			for _, argType := range argTypes {
				if !types.IsString(argType.GetType()) {
					hasNonStringType = true
					break
				}
			}

			if hasNonStringType {
				resultFieldType.AddFlag(mysql.BinaryFlag)
			}
		} else if binaryStrExist || !evalType.IsStringKind() {
			types.SetBinChsClnFlag(resultFieldType)
		} else {
			resultFieldType.SetCharset(mysql.DefaultCharset)
			resultFieldType.SetCollate(mysql.DefaultCollationName)
			resultFieldType.SetFlag(0)
		}
	default:
		panic("unexpected function: " + funcName)
	}
	return nil
}

// InferType4ControlFuncs infer result type for builtin IF, IFNULL, NULLIF, CASEWHEN, COALESCE, LEAD and LAG.
func InferType4ControlFuncs(ctx BuildContext, funcName string, args ...Expression) (*types.FieldType, error) {
	argsNum := len(args)
	if argsNum == 0 {
		panic("unexpected length 0 of args")
	}
	nullFields := make([]*types.FieldType, 0, argsNum)
	notNullFields := make([]*types.FieldType, 0, argsNum)
	for i := range args {
		if args[i].GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeNull {
			nullFields = append(nullFields, args[i].GetType(ctx.GetEvalCtx()))
		} else {
			notNullFields = append(notNullFields, args[i].GetType(ctx.GetEvalCtx()))
		}
	}
	resultFieldType := &types.FieldType{}
	if len(nullFields) == argsNum { // all field is TypeNull
		*resultFieldType = *nullFields[0]
		// If any of arg is NULL, result type need unset NotNullFlag.
		tempFlag := resultFieldType.GetFlag()
		types.SetTypeFlag(&tempFlag, mysql.NotNullFlag, false)
		resultFieldType.SetFlag(tempFlag)

		resultFieldType.SetType(mysql.TypeNull)
		resultFieldType.SetFlen(0)
		resultFieldType.SetDecimal(0)
		types.SetBinChsClnFlag(resultFieldType)
	} else {
		if len(notNullFields) == 1 {
			*resultFieldType = *notNullFields[0]
		} else {
			resultFieldType = types.AggFieldType(notNullFields)
			var tempFlag uint
			evalType := types.AggregateEvalType(notNullFields, &tempFlag)
			resultFieldType.SetFlag(tempFlag)
			setDecimalFromArgs(evalType, resultFieldType, notNullFields...)
			err := addCollateAndCharsetAndFlagFromArgs(ctx, funcName, evalType, resultFieldType, args...)
			if err != nil {
				return nil, err
			}
			setFlenFromArgs(evalType, resultFieldType, notNullFields...)
		}

		// If any of arg is NULL, result type need unset NotNullFlag.
		if len(nullFields) > 0 {
			tempFlag := resultFieldType.GetFlag()
			types.SetTypeFlag(&tempFlag, mysql.NotNullFlag, false)
			resultFieldType.SetFlag(tempFlag)
		}

		resultEvalType := resultFieldType.EvalType()
		// fix decimal for int and string.
		if resultEvalType == types.ETInt {
			resultFieldType.SetDecimal(0)
		} else if resultEvalType == types.ETString {
			resultFieldType.SetDecimal(types.UnspecifiedLength)
		}
		// fix type for enum and set
		if resultFieldType.GetType() == mysql.TypeEnum || resultFieldType.GetType() == mysql.TypeSet {
			switch resultEvalType {
			case types.ETInt:
				resultFieldType.SetType(mysql.TypeLonglong)
			case types.ETString:
				resultFieldType.SetType(mysql.TypeVarchar)
			}
		}
		// fix flen for datetime
		types.TryToFixFlenOfDatetime(resultFieldType)
	}
	return resultFieldType, nil
}

type caseWhenFunctionClass struct {
	baseFunctionClass
}

func (c *caseWhenFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	l := len(args)
	// Fill in each 'THEN' clause parameter type.
	thenArgs := make([]Expression, 0, (l+1)/2)
	for i := 1; i < l; i += 2 {
		thenArgs = append(thenArgs, args[i])
	}
	if l%2 == 1 {
		thenArgs = append(thenArgs, args[l-1])
	}
	fieldTp, err := InferType4ControlFuncs(ctx, c.funcName, thenArgs...)
	if err != nil {
		return nil, err
	}
	// Here we turn off NotNullFlag. Because if all when-clauses are false,
	// the result of case-when expr is NULL.
	tempFlag := fieldTp.GetFlag()
	types.SetTypeFlag(&tempFlag, mysql.NotNullFlag, false)
	fieldTp.SetFlag(tempFlag)
	tp := fieldTp.EvalType()

	argTps := make([]*types.FieldType, 0, l)
	for i := 0; i < l-1; i += 2 {
		if args[i], err = wrapWithIsTrue(ctx, true, args[i], false); err != nil {
			return nil, err
		}
		argTps = append(argTps, args[i].GetType(ctx.GetEvalCtx()), fieldTp.Clone())
	}
	if l%2 == 1 {
		argTps = append(argTps, fieldTp.Clone())
	}
	bf, err := newBaseBuiltinFuncWithFieldTypes(ctx, c.funcName, args, tp, argTps...)
	if err != nil {
		return nil, err
	}
	fieldTp.SetCharset(bf.tp.GetCharset())
	fieldTp.SetCollate(bf.tp.GetCollate())
	bf.tp = fieldTp

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
func (b *builtinCaseWhenIntSig) evalInt(ctx EvalContext, row chunk.Row) (ret int64, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return 0, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalInt(ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalInt(ctx, row)
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
func (b *builtinCaseWhenRealSig) evalReal(ctx EvalContext, row chunk.Row) (ret float64, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return 0, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalReal(ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalReal(ctx, row)
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
func (b *builtinCaseWhenDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (ret *types.MyDecimal, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return nil, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalDecimal(ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDecimal(ctx, row)
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
func (b *builtinCaseWhenStringSig) evalString(ctx EvalContext, row chunk.Row) (ret string, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return "", isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalString(ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalString(ctx, row)
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
func (b *builtinCaseWhenTimeSig) evalTime(ctx EvalContext, row chunk.Row) (ret types.Time, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return ret, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalTime(ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalTime(ctx, row)
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
func (b *builtinCaseWhenDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (ret types.Duration, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return ret, true, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalDuration(ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDuration(ctx, row)
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
func (b *builtinCaseWhenJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (ret types.BinaryJSON, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(ctx, row)
		if err != nil {
			return
		}
		if isNull || condition == 0 {
			continue
		}
		return args[i+1].EvalJSON(ctx, row)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		return args[l-1].EvalJSON(ctx, row)
	}
	return ret, true, nil
}

type ifFunctionClass struct {
	baseFunctionClass
}

// getFunction see https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_if
func (c *ifFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
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

	bf, err := newBaseBuiltinFuncWithFieldTypes(ctx, c.funcName, args, evalTps, args[0].GetType(ctx.GetEvalCtx()).Clone(), retTp.Clone(), retTp.Clone())
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

func (b *builtinIfIntSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalInt(ctx, row)
	}
	return b.args[2].EvalInt(ctx, row)
}

type builtinIfRealSig struct {
	baseBuiltinFunc
}

func (b *builtinIfRealSig) Clone() builtinFunc {
	newSig := &builtinIfRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfRealSig) evalReal(ctx EvalContext, row chunk.Row) (val float64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalReal(ctx, row)
	}
	return b.args[2].EvalReal(ctx, row)
}

type builtinIfDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinIfDecimalSig) Clone() builtinFunc {
	newSig := &builtinIfDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (val *types.MyDecimal, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return nil, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalDecimal(ctx, row)
	}
	return b.args[2].EvalDecimal(ctx, row)
}

type builtinIfStringSig struct {
	baseBuiltinFunc
}

func (b *builtinIfStringSig) Clone() builtinFunc {
	newSig := &builtinIfStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfStringSig) evalString(ctx EvalContext, row chunk.Row) (val string, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return "", true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalString(ctx, row)
	}
	return b.args[2].EvalString(ctx, row)
}

type builtinIfTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinIfTimeSig) Clone() builtinFunc {
	newSig := &builtinIfTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfTimeSig) evalTime(ctx EvalContext, row chunk.Row) (ret types.Time, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalTime(ctx, row)
	}
	return b.args[2].EvalTime(ctx, row)
}

type builtinIfDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinIfDurationSig) Clone() builtinFunc {
	newSig := &builtinIfDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (ret types.Duration, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalDuration(ctx, row)
	}
	return b.args[2].EvalDuration(ctx, row)
}

type builtinIfJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinIfJSONSig) Clone() builtinFunc {
	newSig := &builtinIfJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (ret types.BinaryJSON, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalJSON(ctx, row)
	}
	return b.args[2].EvalJSON(ctx, row)
}

type ifNullFunctionClass struct {
	baseFunctionClass
}

func (c *ifNullFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhs, rhs := args[0].GetType(ctx.GetEvalCtx()), args[1].GetType(ctx.GetEvalCtx())
	retTp, err := InferType4ControlFuncs(ctx, c.funcName, args[0], args[1])
	if err != nil {
		return nil, err
	}

	retTp.AddFlag((lhs.GetFlag() & mysql.NotNullFlag) | (rhs.GetFlag() & mysql.NotNullFlag))
	if lhs.GetType() == mysql.TypeNull && rhs.GetType() == mysql.TypeNull {
		retTp.SetType(mysql.TypeNull)
		retTp.SetFlen(0)
		retTp.SetDecimal(0)
		types.SetBinChsClnFlag(retTp)
	}
	evalTps := retTp.EvalType()
	bf, err := newBaseBuiltinFuncWithFieldTypes(ctx, c.funcName, args, evalTps, retTp.Clone(), retTp.Clone())
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

func (b *builtinIfNullIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalInt(ctx, row)
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

func (b *builtinIfNullRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	arg0, isNull, err := b.args[0].EvalReal(ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalReal(ctx, row)
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

func (b *builtinIfNullDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	arg0, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalDecimal(ctx, row)
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

func (b *builtinIfNullStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalString(ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalString(ctx, row)
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

func (b *builtinIfNullTimeSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalTime(ctx, row)
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

func (b *builtinIfNullDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(ctx, row)
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

func (b *builtinIfNullJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	arg0, isNull, err := b.args[0].EvalJSON(ctx, row)
	if !isNull {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalJSON(ctx, row)
	return arg1, isNull || err != nil, err
}
