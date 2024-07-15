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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/contextopt"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &inFunctionClass{}
	_ functionClass = &rowFunctionClass{}
	_ functionClass = &setVarFunctionClass{}
	_ functionClass = &getIntVarFunctionClass{}
	_ functionClass = &getRealVarFunctionClass{}
	_ functionClass = &getDecimalVarFunctionClass{}
	_ functionClass = &getTimeVarFunctionClass{}
	_ functionClass = &getStringVarFunctionClass{}
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
	_ builtinFunc = &builtinSetStringVarSig{}
	_ builtinFunc = &builtinSetIntVarSig{}
	_ builtinFunc = &builtinSetRealVarSig{}
	_ builtinFunc = &builtinSetDecimalVarSig{}
	_ builtinFunc = &builtinGetStringVarSig{}
	_ builtinFunc = &builtinGetIntVarSig{}
	_ builtinFunc = &builtinGetRealVarSig{}
	_ builtinFunc = &builtinGetDecimalVarSig{}
	_ builtinFunc = &builtinGetTimeVarSig{}
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

func (c *inFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	args, err = c.verifyArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = args[0].GetType(ctx.GetEvalCtx()).EvalType()
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(args); i++ {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	bf.tp.SetFlen(1)
	switch args[0].GetType(ctx.GetEvalCtx()).EvalType() {
	case types.ETInt:
		inInt := builtinInIntSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inInt.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inInt, err
		}
		sig = &inInt
		sig.setPbCode(tipb.ScalarFuncSig_InInt)
	case types.ETString:
		inStr := builtinInStringSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inStr.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inStr, err
		}
		sig = &inStr
		sig.setPbCode(tipb.ScalarFuncSig_InString)
	case types.ETReal:
		inReal := builtinInRealSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inReal.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inReal, err
		}
		sig = &inReal
		sig.setPbCode(tipb.ScalarFuncSig_InReal)
	case types.ETDecimal:
		inDecimal := builtinInDecimalSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inDecimal.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inDecimal, err
		}
		sig = &inDecimal
		sig.setPbCode(tipb.ScalarFuncSig_InDecimal)
	case types.ETDatetime, types.ETTimestamp:
		inTime := builtinInTimeSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inTime.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inTime, err
		}
		sig = &inTime
		sig.setPbCode(tipb.ScalarFuncSig_InTime)
	case types.ETDuration:
		inDuration := builtinInDurationSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inDuration.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inDuration, err
		}
		sig = &inDuration
		sig.setPbCode(tipb.ScalarFuncSig_InDuration)
	case types.ETJson:
		sig = &builtinInJSONSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_InJson)
	}
	return sig, nil
}

func (c *inFunctionClass) verifyArgs(ctx BuildContext, args []Expression) ([]Expression, error) {
	columnType := args[0].GetType(ctx.GetEvalCtx())
	validatedArgs := make([]Expression, 0, len(args))
	for _, arg := range args {
		if constant, ok := arg.(*Constant); ok {
			switch {
			case columnType.GetType() == mysql.TypeBit && constant.Value.Kind() == types.KindInt64:
				if constant.Value.GetInt64() < 0 {
					if MaybeOverOptimized4PlanCache(ctx, args) {
						ctx.SetSkipPlanCache(fmt.Sprintf("Bit Column in (%v)", constant.Value.GetInt64()))
					}
					continue
				}
			}
		}
		validatedArgs = append(validatedArgs, arg)
	}
	err := c.baseFunctionClass.verifyArgs(validatedArgs)
	return validatedArgs, err
}

// nolint:structcheck
type baseInSig struct {
	baseBuiltinFunc
	// nonConstArgsIdx stores the indices of non-constant args in the baseBuiltinFunc.args (the first arg is not included).
	// It works with builtinInXXXSig.hashset to accelerate 'eval'.
	nonConstArgsIdx []int
	hasNull         bool
}

// builtinInIntSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInIntSig struct {
	baseInSig
	// the bool value in the map is used to identify whether the constant stored in key is signed or unsigned
	hashSet map[int64]bool
}

func (b *builtinInIntSig) buildHashMapForConstArgs(ctx BuildContext) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = make(map[int64]bool, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstLevel() == ConstStrict {
			val, isNull, err := b.args[i].EvalInt(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet[val] = mysql.HasUnsignedFlag(b.args[i].GetType(ctx.GetEvalCtx()).GetFlag())
		} else {
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, i)
		}
	}
	return nil
}

func (b *builtinInIntSig) Clone() builtinFunc {
	newSig := &builtinInIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	isUnsigned0 := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag())

	args := b.args[1:]
	if len(b.hashSet) != 0 {
		if isUnsigned, ok := b.hashSet[arg0]; ok {
			if (isUnsigned0 && isUnsigned) || (!isUnsigned0 && !isUnsigned) {
				return 1, false, nil
			}
			if arg0 >= 0 {
				return 1, false, nil
			}
		}
		args = make([]Expression, 0, len(b.nonConstArgsIdx))
		for _, i := range b.nonConstArgsIdx {
			args = append(args, b.args[i])
		}
	}

	hasNull := b.hasNull
	for _, arg := range args {
		evaledArg, isNull, err := arg.EvalInt(ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		isUnsigned := mysql.HasUnsignedFlag(arg.GetType(ctx).GetFlag())
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

// builtinInStringSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInStringSig struct {
	baseInSig
	hashSet set.StringSet
}

func (b *builtinInStringSig) buildHashMapForConstArgs(ctx BuildContext) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = set.NewStringSet()
	collator := collate.GetCollator(b.collation)
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstLevel() == ConstStrict {
			val, isNull, err := b.args[i].EvalString(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet.Insert(string(collator.Key(val))) // should do memory copy here
		} else {
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, i)
		}
	}

	return nil
}

func (b *builtinInStringSig) Clone() builtinFunc {
	newSig := &builtinInStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInStringSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalString(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}

	args := b.args[1:]
	collator := collate.GetCollator(b.collation)
	if len(b.hashSet) != 0 {
		if b.hashSet.Exist(string(collator.Key(arg0))) {
			return 1, false, nil
		}
		args = make([]Expression, 0, len(b.nonConstArgsIdx))
		for _, i := range b.nonConstArgsIdx {
			args = append(args, b.args[i])
		}
	}

	hasNull := b.hasNull
	for _, arg := range args {
		evaledArg, isNull, err := arg.EvalString(ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if types.CompareString(arg0, evaledArg, b.collation) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInRealSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInRealSig struct {
	baseInSig
	hashSet set.Float64Set
}

func (b *builtinInRealSig) buildHashMapForConstArgs(ctx BuildContext) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = set.NewFloat64Set()
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstLevel() == ConstStrict {
			val, isNull, err := b.args[i].EvalReal(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet.Insert(val)
		} else {
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, i)
		}
	}

	return nil
}

func (b *builtinInRealSig) Clone() builtinFunc {
	newSig := &builtinInRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInRealSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalReal(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	args := b.args[1:]
	if len(b.hashSet) != 0 {
		if b.hashSet.Exist(arg0) {
			return 1, false, nil
		}
		args = make([]Expression, 0, len(b.nonConstArgsIdx))
		for _, i := range b.nonConstArgsIdx {
			args = append(args, b.args[i])
		}
	}

	hasNull := b.hasNull
	for _, arg := range args {
		evaledArg, isNull, err := arg.EvalReal(ctx, row)
		if err != nil {
			return 0, true, err
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

// builtinInDecimalSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInDecimalSig struct {
	baseInSig
	hashSet set.StringSet
}

func (b *builtinInDecimalSig) buildHashMapForConstArgs(ctx BuildContext) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = set.NewStringSet()
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstLevel() == ConstStrict {
			val, isNull, err := b.args[i].EvalDecimal(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			key, err := val.ToHashKey()
			if err != nil {
				return err
			}
			b.hashSet.Insert(string(key))
		} else {
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, i)
		}
	}

	return nil
}

func (b *builtinInDecimalSig) Clone() builtinFunc {
	newSig := &builtinInDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInDecimalSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalDecimal(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}

	args := b.args[1:]
	key, err := arg0.ToHashKey()
	if err != nil {
		return 0, true, err
	}
	if len(b.hashSet) != 0 {
		if b.hashSet.Exist(string(key)) {
			return 1, false, nil
		}
		args = make([]Expression, 0, len(b.nonConstArgsIdx))
		for _, i := range b.nonConstArgsIdx {
			args = append(args, b.args[i])
		}
	}

	hasNull := b.hasNull
	for _, arg := range args {
		evaledArg, isNull, err := arg.EvalDecimal(ctx, row)
		if err != nil {
			return 0, true, err
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

// builtinInTimeSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInTimeSig struct {
	baseInSig
	hashSet map[types.CoreTime]struct{}
}

func (b *builtinInTimeSig) buildHashMapForConstArgs(ctx BuildContext) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = make(map[types.CoreTime]struct{}, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstLevel() == ConstStrict {
			val, isNull, err := b.args[i].EvalTime(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet[val.CoreTime()] = struct{}{}
		} else {
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, i)
		}
	}

	return nil
}

func (b *builtinInTimeSig) Clone() builtinFunc {
	newSig := &builtinInTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInTimeSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalTime(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	args := b.args[1:]
	if len(b.hashSet) != 0 {
		if _, ok := b.hashSet[arg0.CoreTime()]; ok {
			return 1, false, nil
		}
		args = make([]Expression, 0, len(b.nonConstArgsIdx))
		for _, i := range b.nonConstArgsIdx {
			args = append(args, b.args[i])
		}
	}

	hasNull := b.hasNull
	for _, arg := range args {
		evaledArg, isNull, err := arg.EvalTime(ctx, row)
		if err != nil {
			return 0, true, err
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

// builtinInDurationSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInDurationSig struct {
	baseInSig
	hashSet map[time.Duration]struct{}
}

func (b *builtinInDurationSig) buildHashMapForConstArgs(ctx BuildContext) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = make(map[time.Duration]struct{}, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstLevel() == ConstStrict {
			val, isNull, err := b.args[i].EvalDuration(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet[val.Duration] = struct{}{}
		} else {
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, i)
		}
	}

	return nil
}

func (b *builtinInDurationSig) Clone() builtinFunc {
	newSig := &builtinInDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = make([]int, len(b.nonConstArgsIdx))
	copy(newSig.nonConstArgsIdx, b.nonConstArgsIdx)
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInDurationSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalDuration(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	args := b.args[1:]
	if len(b.hashSet) != 0 {
		if _, ok := b.hashSet[arg0.Duration]; ok {
			return 1, false, nil
		}
		args = make([]Expression, 0, len(b.nonConstArgsIdx))
		for _, i := range b.nonConstArgsIdx {
			args = append(args, b.args[i])
		}
	}

	hasNull := b.hasNull
	for _, arg := range args {
		evaledArg, isNull, err := arg.EvalDuration(ctx, row)
		if err != nil {
			return 0, true, err
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

// builtinInJSONSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinInJSONSig) Clone() builtinFunc {
	newSig := &builtinInJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInJSONSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalJSON(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	var hasNull bool
	for _, arg := range b.args[1:] {
		evaledArg, isNull, err := arg.EvalJSON(ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		result := types.CompareBinaryJSON(evaledArg, arg0)
		if result == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

type rowFunctionClass struct {
	baseFunctionClass
}

func (c *rowFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	for i := range argTps {
		argTps[i] = args[i].GetType(ctx.GetEvalCtx()).EvalType()
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	sig = &builtinRowSig{bf}
	return sig, nil
}

type builtinRowSig struct {
	baseBuiltinFunc
}

func (b *builtinRowSig) Clone() builtinFunc {
	newSig := &builtinRowSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString rowFunc should always be flattened in expression rewrite phrase.
func (b *builtinRowSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	panic("builtinRowSig.evalString() should never be called.")
}

type setVarFunctionClass struct {
	baseFunctionClass
}

func (c *setVarFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := args[1].GetType(ctx.GetEvalCtx()).EvalType()
	if argTp == types.ETTimestamp || argTp == types.ETDuration || argTp == types.ETJson {
		argTp = types.ETString
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, argTp, types.ETString, argTp)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlenUnderLimit(args[1].GetType(ctx.GetEvalCtx()).GetFlen())
	switch argTp {
	case types.ETString:
		sig = &builtinSetStringVarSig{baseBuiltinFunc: bf}
	case types.ETReal:
		sig = &builtinSetRealVarSig{baseBuiltinFunc: bf}
	case types.ETDecimal:
		sig = &builtinSetDecimalVarSig{baseBuiltinFunc: bf}
	case types.ETInt:
		sig = &builtinSetIntVarSig{baseBuiltinFunc: bf}
	case types.ETDatetime:
		sig = &builtinSetTimeVarSig{baseBuiltinFunc: bf}
	default:
		return nil, errors.Errorf("unexpected types.EvalType %v", argTp)
	}
	return sig, nil
}

type builtinSetStringVarSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinSetStringVarSig) Clone() builtinFunc {
	newSig := &builtinSetStringVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetStringVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSetStringVarSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	var varName string
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return "", true, err
	}
	varName, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	datum, err := b.args[1].Eval(ctx, row)
	isNull = datum.IsNull()
	if isNull || err != nil {
		return "", isNull, err
	}
	res, err = datum.ToString()
	if err != nil {
		return "", isNull, err
	}
	sessionVars.SetStringUserVar(varName, stringutil.Copy(res), datum.Collation())
	return res, false, nil
}

type builtinSetRealVarSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinSetRealVarSig) Clone() builtinFunc {
	newSig := &builtinSetRealVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetRealVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSetRealVarSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	var varName string
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	varName, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	datum, err := b.args[1].Eval(ctx, row)
	isNull = datum.IsNull()
	if isNull || err != nil {
		return 0, isNull, err
	}
	res = datum.GetFloat64()
	varName = strings.ToLower(varName)
	sessionVars.SetUserVarVal(varName, datum)
	return res, false, nil
}

type builtinSetDecimalVarSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinSetDecimalVarSig) Clone() builtinFunc {
	newSig := &builtinSetDecimalVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetDecimalVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSetDecimalVarSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return nil, true, err
	}
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	datum, err := b.args[1].Eval(ctx, row)
	isNull = datum.IsNull()
	if isNull || err != nil {
		return nil, isNull, err
	}
	res := datum.GetMysqlDecimal()
	varName = strings.ToLower(varName)
	sessionVars.SetUserVarVal(varName, datum)
	return res, false, nil
}

type builtinSetIntVarSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinSetIntVarSig) Clone() builtinFunc {
	newSig := &builtinSetIntVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetIntVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSetIntVarSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	datum, err := b.args[1].Eval(ctx, row)
	isNull = datum.IsNull()
	if isNull || err != nil {
		return 0, isNull, err
	}
	res := datum.GetInt64()
	varName = strings.ToLower(varName)
	sessionVars.SetUserVarVal(varName, datum)
	return res, false, nil
}

type builtinSetTimeVarSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinSetTimeVarSig) Clone() builtinFunc {
	newSig := &builtinSetTimeVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetTimeVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinSetTimeVarSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	datum, err := b.args[1].Eval(ctx, row)
	if err != nil || datum.IsNull() {
		return types.ZeroTime, datum.IsNull(), handleInvalidTimeError(ctx, err)
	}
	res := datum.GetMysqlTime()
	varName = strings.ToLower(varName)
	sessionVars.SetUserVarVal(varName, datum)
	return res, false, nil
}

// BuildGetVarFunction builds a GetVar ScalarFunction from the Expression.
func BuildGetVarFunction(ctx BuildContext, expr Expression, retType *types.FieldType) (Expression, error) {
	var fc functionClass
	switch retType.EvalType() {
	case types.ETInt:
		fc = &getIntVarFunctionClass{getVarFunctionClass{baseFunctionClass{ast.GetVar, 1, 1}, retType}}
	case types.ETDecimal:
		fc = &getDecimalVarFunctionClass{getVarFunctionClass{baseFunctionClass{ast.GetVar, 1, 1}, retType}}
	case types.ETReal:
		fc = &getRealVarFunctionClass{getVarFunctionClass{baseFunctionClass{ast.GetVar, 1, 1}, retType}}
	case types.ETDatetime:
		fc = &getTimeVarFunctionClass{getVarFunctionClass{baseFunctionClass{ast.GetVar, 1, 1}, retType}}
	default:
		fc = &getStringVarFunctionClass{getVarFunctionClass{baseFunctionClass{ast.GetVar, 1, 1}, retType}}
	}
	f, err := fc.getFunction(ctx, []Expression{expr})
	if err != nil {
		return nil, err
	}
	if builtinRetTp := f.getRetTp(); builtinRetTp.GetType() != mysql.TypeUnspecified || retType.GetType() == mysql.TypeUnspecified {
		retType = builtinRetTp
	}
	return &ScalarFunction{
		FuncName: model.NewCIStr(ast.GetVar),
		RetType:  retType,
		Function: f,
	}, nil
}

type getVarFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

type getStringVarFunctionClass struct {
	getVarFunctionClass
}

func (c *getStringVarFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(c.tp.GetFlen())
	if len(c.tp.GetCharset()) > 0 {
		bf.tp.SetCharset(c.tp.GetCharset())
		bf.tp.SetCollate(c.tp.GetCollate())
	}
	sig = &builtinGetStringVarSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinGetStringVarSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinGetStringVarSig) Clone() builtinFunc {
	newSig := &builtinGetStringVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetStringVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinGetStringVarSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return "", true, err
	}
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	varName = strings.ToLower(varName)
	if v, ok := sessionVars.GetUserVarVal(varName); ok {
		// We cannot use v.GetString() here, because the datum may be in KindMysqlTime, which
		// stores the data in datum.x.
		// This seems controversial with https://dev.mysql.com/doc/refman/8.0/en/user-variables.html:
		// > User variables can be assigned a value from a limited set of data types: integer, decimal,
		// > floating-point, binary or nonbinary string, or NULL value.
		// However, MySQL actually does support query like `set @p = now()`, so we should not assume the datum stored
		// must be of one of the following types: string, decimal, int, float.
		res, err := v.ToString()
		if err != nil {
			return "", false, err
		}
		return res, false, nil
	}
	return "", true, nil
}

type getIntVarFunctionClass struct {
	getVarFunctionClass
}

func (c *getIntVarFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(c.tp.GetFlen())
	bf.tp.SetFlag(c.tp.GetFlag())
	sig = &builtinGetIntVarSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinGetIntVarSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinGetIntVarSig) Clone() builtinFunc {
	newSig := &builtinGetIntVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetIntVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinGetIntVarSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	varName = strings.ToLower(varName)
	if v, ok := sessionVars.GetUserVarVal(varName); ok {
		return v.GetInt64(), false, nil
	}
	return 0, true, nil
}

type getRealVarFunctionClass struct {
	getVarFunctionClass
}

func (c *getRealVarFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(c.tp.GetFlen())
	sig = &builtinGetRealVarSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinGetRealVarSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinGetRealVarSig) Clone() builtinFunc {
	newSig := &builtinGetRealVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetRealVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinGetRealVarSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	varName = strings.ToLower(varName)
	if v, ok := sessionVars.GetUserVarVal(varName); ok {
		d, err := v.ToFloat64(typeCtx(ctx))
		if err != nil {
			return 0, false, err
		}
		return d, false, nil
	}
	return 0, true, nil
}

type getDecimalVarFunctionClass struct {
	getVarFunctionClass
}

func (c *getDecimalVarFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDecimal, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlenUnderLimit(c.tp.GetFlen())
	sig = &builtinGetDecimalVarSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinGetDecimalVarSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinGetDecimalVarSig) Clone() builtinFunc {
	newSig := &builtinGetDecimalVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetDecimalVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinGetDecimalVarSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return nil, true, err
	}
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	varName = strings.ToLower(varName)
	if v, ok := sessionVars.GetUserVarVal(varName); ok {
		d, err := v.ToDecimal(typeCtx(ctx))
		if err != nil {
			return nil, false, err
		}
		return d, false, nil
	}
	return nil, true, nil
}

type getTimeVarFunctionClass struct {
	getVarFunctionClass
}

func (c *getTimeVarFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETString)
	if err != nil {
		return nil, err
	}
	if c.tp.GetType() == mysql.TypeDatetime {
		fsp := c.tp.GetFlen() - mysql.MaxDatetimeWidthNoFsp
		if fsp > 0 {
			fsp--
		}
		bf.setDecimalAndFlenForDatetime(fsp)
	} else {
		bf.setDecimalAndFlenForDate()
	}
	sig = &builtinGetTimeVarSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinGetTimeVarSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinGetTimeVarSig) Clone() builtinFunc {
	newSig := &builtinGetTimeVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetTimeVarSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinGetTimeVarSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}
	varName, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	varName = strings.ToLower(varName)
	if v, ok := sessionVars.GetUserVarVal(varName); ok {
		return v.GetMysqlTime(), false, nil
	}
	return types.ZeroTime, true, nil
}

type valuesFunctionClass struct {
	baseFunctionClass

	offset int
	tp     *types.FieldType
}

func (c *valuesFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp)
	if err != nil {
		return nil, err
	}
	switch c.tp.EvalType() {
	case types.ETInt:
		sig = &builtinValuesIntSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETReal:
		sig = &builtinValuesRealSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETDecimal:
		sig = &builtinValuesDecimalSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETString:
		sig = &builtinValuesStringSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinValuesTimeSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETDuration:
		sig = &builtinValuesDurationSig{baseBuiltinFunc: bf, offset: c.offset}
	case types.ETJson:
		sig = &builtinValuesJSONSig{baseBuiltinFunc: bf, offset: c.offset}
	}
	return sig, nil
}

type builtinValuesIntSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesIntSig) Clone() builtinFunc {
	newSig := &builtinValuesIntSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesIntSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalInt evals a builtinValuesIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesIntSig) evalInt(ctx EvalContext, _ chunk.Row) (int64, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return 0, true, nil
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return 0, true, nil
		}
		// For BinaryLiteral, see issue #15310
		val := row.GetRaw(b.offset)
		if len(val) > 8 {
			return 0, true, errors.New("Session current insert values is too long")
		}
		if len(val) < 8 {
			var binary types.BinaryLiteral = val
			v, err := binary.ToInt(typeCtx(ctx))
			if err != nil {
				return 0, true, errors.Trace(err)
			}
			return int64(v), false, nil
		}
		return row.GetInt64(b.offset), false, nil
	}
	return 0, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesRealSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesRealSig) Clone() builtinFunc {
	newSig := &builtinValuesRealSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesRealSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalReal evals a builtinValuesRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesRealSig) evalReal(ctx EvalContext, _ chunk.Row) (float64, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return 0, true, nil
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return 0, true, nil
		}
		if b.getRetTp().GetType() == mysql.TypeFloat {
			return float64(row.GetFloat32(b.offset)), false, nil
		}
		return row.GetFloat64(b.offset), false, nil
	}
	return 0, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesDecimalSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesDecimalSig) Clone() builtinFunc {
	newSig := &builtinValuesDecimalSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesDecimalSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalDecimal evals a builtinValuesDecimalSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDecimalSig) evalDecimal(ctx EvalContext, _ chunk.Row) (*types.MyDecimal, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return nil, true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return &types.MyDecimal{}, true, nil
	}
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
	contextopt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesStringSig) Clone() builtinFunc {
	newSig := &builtinValuesStringSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesStringSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalString evals a builtinValuesStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesStringSig) evalString(ctx EvalContext, _ chunk.Row) (string, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return "", true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return "", true, nil
	}
	if b.offset >= row.Len() {
		return "", true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
	}

	if row.IsNull(b.offset) {
		return "", true, nil
	}

	// Specially handle the ENUM/SET/BIT input value.
	if retType := b.getRetTp(); retType.Hybrid() {
		val := row.GetDatum(b.offset, retType)
		res, err := val.ToString()
		return res, err != nil, err
	}

	return row.GetString(b.offset), false, nil
}

type builtinValuesTimeSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesTimeSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinValuesTimeSig) Clone() builtinFunc {
	newSig := &builtinValuesTimeSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinValuesTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesTimeSig) evalTime(ctx EvalContext, _ chunk.Row) (types.Time, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return types.ZeroTime, true, nil
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return types.ZeroTime, true, nil
		}
		return row.GetTime(b.offset), false, nil
	}
	return types.ZeroTime, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesDurationSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesDurationSig) Clone() builtinFunc {
	newSig := &builtinValuesDurationSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesDurationSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalDuration evals a builtinValuesDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDurationSig) evalDuration(ctx EvalContext, _ chunk.Row) (types.Duration, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return types.Duration{}, true, nil
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return types.Duration{}, true, nil
		}
		duration := row.GetDuration(b.offset, b.getRetTp().GetDecimal())
		return duration, false, nil
	}
	return types.Duration{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesJSONSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader

	offset int
}

func (b *builtinValuesJSONSig) Clone() builtinFunc {
	newSig := &builtinValuesJSONSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinValuesJSONSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalJSON evals a builtinValuesJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesJSONSig) evalJSON(ctx EvalContext, _ chunk.Row) (types.BinaryJSON, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.BinaryJSON{}, true, err
	}
	row := sessionVars.CurrInsertValues
	if row.IsEmpty() {
		return types.BinaryJSON{}, true, nil
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return types.BinaryJSON{}, true, nil
		}
		return row.GetJSON(b.offset), false, nil
	}
	return types.BinaryJSON{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type bitCountFunctionClass struct {
	baseFunctionClass
}

func (c *bitCountFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(2)
	sig := &builtinBitCountSig{bf}
	return sig, nil
}

type builtinBitCountSig struct {
	baseBuiltinFunc
}

func (b *builtinBitCountSig) Clone() builtinFunc {
	newSig := &builtinBitCountSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals BIT_COUNT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/bit-functions.html#function_bit-count
func (b *builtinBitCountSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	n, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil || isNull {
		if err != nil && types.ErrOverflow.Equal(err) {
			return 64, false, nil
		}
		return 0, true, err
	}
	return bitCount(n), false, nil
}

// getParamFunctionClass for plan cache of prepared statements
type getParamFunctionClass struct {
	baseFunctionClass
}

// getFunction gets function
// TODO: more typed functions will be added when typed parameters are supported.
func (c *getParamFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.MaxFieldVarCharLength)
	sig := &builtinGetParamStringSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinGetParamStringSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinGetParamStringSig) Clone() builtinFunc {
	newSig := &builtinGetParamStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetParamStringSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func (b *builtinGetParamStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return "", true, err
	}
	idx, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	v := sessionVars.PlanCacheParams.GetParamValue(int(idx))

	str, err := v.ToString()
	if err != nil {
		return "", true, nil
	}
	return str, false, nil
}
