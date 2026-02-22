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
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tipb/go-tipb"
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
	case types.ETVectorFloat32:
		sig = &builtinInVectorFloat32Sig{baseBuiltinFunc: bf}
		// sig.setPbCode(tipb.ScalarFuncSig_InVectorFloat32)
	default:
		return nil, errors.Errorf("%s is not supported for IN()", args[0].GetType(ctx.GetEvalCtx()).EvalType())
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
					if MaybeOverOptimized4PlanCache(ctx, args...) {
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

// builtinInIntSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInIntSig struct {
	baseInSig
	// the bool value in the map is used to identify whether the constant stored in key is signed or unsigned
	hashSet map[int64]bool

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinInIntSig) buildHashMapForConstArgs(ctx BuildContext) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = make(map[int64]bool, len(b.args)-1)

	// Keep track of unique args count for in-place modification
	uniqueArgCount := 1 // Start with 1 for the first arg (value to check)

	// TODO: ConstOnlyInContext and default branch should also prune duplicate ones after expression are managed by memo.
	for i := 1; i < len(b.args); i++ {
		switch b.args[i].ConstLevel() {
		case ConstStrict:
			val, isNull, err := b.args[i].EvalInt(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return err
			}

			if isNull {
				// Only keep one NULL value
				if !b.hasNull {
					b.hasNull = true
					b.args[uniqueArgCount] = b.args[i]
					uniqueArgCount++
				}
				continue
			}

			// Only keep this arg if value wasn't seen before
			if _, exists := b.hashSet[val]; !exists {
				b.hashSet[val] = mysql.HasUnsignedFlag(b.args[i].GetType(ctx.GetEvalCtx()).GetFlag())
				b.args[uniqueArgCount] = b.args[i]
				uniqueArgCount++
			}
		case ConstOnlyInContext:
			// Avoid build plans for wrong type.
			if _, _, err := b.args[i].EvalInt(ctx.GetEvalCtx(), chunk.Row{}); err != nil {
				return err
			}
			b.args[uniqueArgCount] = b.args[i]
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, uniqueArgCount)
			uniqueArgCount++
		default:
			b.args[uniqueArgCount] = b.args[i]
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, uniqueArgCount)
			uniqueArgCount++
		}
	}

	// Truncate args to only include unique values
	b.args = b.args[:uniqueArgCount]

	return nil
}

func (b *builtinInIntSig) Clone() builtinFunc {
	newSig := &builtinInIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = slices.Clone(b.nonConstArgsIdx)
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinInStringSig) buildHashMapForConstArgs(ctx BuildContext) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = set.NewStringSet()
	collator := collate.GetCollator(b.collation)

	// Keep track of unique args count for in-place modification
	uniqueArgCount := 1 // Start with 1 for the first arg (value to check)

	// TODO: ConstOnlyInContext and default branch should also prune duplicate ones after expression are managed by memo.
	for i := 1; i < len(b.args); i++ {
		switch b.args[i].ConstLevel() {
		case ConstStrict:
			val, isNull, err := b.args[i].EvalString(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return err
			}

			if isNull {
				// Only keep one NULL value
				if !b.hasNull {
					b.hasNull = true
					b.args[uniqueArgCount] = b.args[i]
					uniqueArgCount++
				}
				continue
			}

			key := string(collator.Key(val)) // should do memory copy here
			// Only keep this arg if value wasn't seen before
			if !b.hashSet.Exist(key) {
				b.hashSet.Insert(key)
				b.args[uniqueArgCount] = b.args[i]
				uniqueArgCount++
			}
		case ConstOnlyInContext:
			// Avoid build plans for wrong type.
			if _, _, err := b.args[i].EvalString(ctx.GetEvalCtx(), chunk.Row{}); err != nil {
				return err
			}
			b.args[uniqueArgCount] = b.args[i]
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, uniqueArgCount)
			uniqueArgCount++
		default:
			b.args[uniqueArgCount] = b.args[i]
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, uniqueArgCount)
			uniqueArgCount++
		}
	}

	// Truncate args to only include unique values
	b.args = b.args[:uniqueArgCount]

	return nil
}

func (b *builtinInStringSig) Clone() builtinFunc {
	newSig := &builtinInStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = slices.Clone(b.nonConstArgsIdx)
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinInRealSig) buildHashMapForConstArgs(ctx BuildContext) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = set.NewFloat64Set()

	// Keep track of unique args count for in-place modification
	uniqueArgCount := 1 // Start with 1 for the first arg (value to check)

	// TODO: ConstOnlyInContext and default branch should also prune duplicate ones after expression are managed by memo.
	for i := 1; i < len(b.args); i++ {
		switch b.args[i].ConstLevel() {
		case ConstStrict:
			val, isNull, err := b.args[i].EvalReal(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return err
			}

			if isNull {
				// Only keep one NULL value
				if !b.hasNull {
					b.hasNull = true
					b.args[uniqueArgCount] = b.args[i]
					uniqueArgCount++
				}
				continue
			}

			// Only keep this arg if value wasn't seen before
			if !b.hashSet.Exist(val) {
				b.hashSet.Insert(val)
				b.args[uniqueArgCount] = b.args[i]
				uniqueArgCount++
			}
		case ConstOnlyInContext:
			// Avoid build plans for wrong type.
			if _, _, err := b.args[i].EvalReal(ctx.GetEvalCtx(), chunk.Row{}); err != nil {
				return err
			}
			b.args[uniqueArgCount] = b.args[i]
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, uniqueArgCount)
			uniqueArgCount++
		default:
			b.args[uniqueArgCount] = b.args[i]
			b.nonConstArgsIdx = append(b.nonConstArgsIdx, uniqueArgCount)
			uniqueArgCount++
		}
	}

	// Truncate args to only include unique values
	b.args = b.args[:uniqueArgCount]

	return nil
}

func (b *builtinInRealSig) Clone() builtinFunc {
	newSig := &builtinInRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = slices.Clone(b.nonConstArgsIdx)
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
