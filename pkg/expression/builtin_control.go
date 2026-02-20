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
	"slices"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
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
	_ builtinFunc = &builtinCaseWhenVectorFloat32Sig{}
	_ builtinFunc = &builtinIfNullIntSig{}
	_ builtinFunc = &builtinIfNullRealSig{}
	_ builtinFunc = &builtinIfNullDecimalSig{}
	_ builtinFunc = &builtinIfNullStringSig{}
	_ builtinFunc = &builtinIfNullTimeSig{}
	_ builtinFunc = &builtinIfNullDurationSig{}
	_ builtinFunc = &builtinIfNullJSONSig{}
	_ builtinFunc = &builtinIfNullVectorFloat32Sig{}
	_ builtinFunc = &builtinIfIntSig{}
	_ builtinFunc = &builtinIfRealSig{}
	_ builtinFunc = &builtinIfDecimalSig{}
	_ builtinFunc = &builtinIfStringSig{}
	_ builtinFunc = &builtinIfTimeSig{}
	_ builtinFunc = &builtinIfDurationSig{}
	_ builtinFunc = &builtinIfJSONSig{}
	_ builtinFunc = &builtinIfVectorFloat32Sig{}
)

func maxlen(lhsFlen, rhsFlen int) int {
	// -1 indicates that the length is unknown, such as the case for expressions.
	if lhsFlen < 0 || rhsFlen < 0 {
		return mysql.MaxRealWidth
	}
	return max(lhsFlen, rhsFlen)
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
			switch argTps[i].GetType() {
			case mysql.TypeTiny:
				maxLen = maxlen(4, maxLen)
			case mysql.TypeShort:
				maxLen = maxlen(6, maxLen)
			case mysql.TypeInt24:
				maxLen = maxlen(9, maxLen)
			case mysql.TypeLong:
				maxLen = maxlen(11, maxLen)
			case mysql.TypeLonglong:
				maxLen = maxlen(20, maxLen)
			default:
				argFlen := argTps[i].GetFlen()
				if argFlen == types.UnspecifiedLength {
					resultFieldType.SetFlen(types.UnspecifiedLength)
					return
				}
				maxLen = maxlen(argFlen, maxLen)
			}
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
			maxDecimal = max(argTps[i].GetDecimal(), maxDecimal)
		}
		resultFieldType.SetDecimalUnderLimit(maxDecimal)
	}
}

// NonBinaryStr means the arg is a string but not binary string
func hasNonBinaryStr(args []*types.FieldType) bool {
	return slices.ContainsFunc(args, types.IsNonBinaryStr)
}

func hasBinaryStr(args []*types.FieldType) bool {
	return slices.ContainsFunc(args, types.IsBinaryStr)
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

