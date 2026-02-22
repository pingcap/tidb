// Copyright 2017 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/set"
)

type builtinInDecimalSig struct {
	baseInSig
	hashSet set.StringSet

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinInDecimalSig) buildHashMapForConstArgs(ctx BuildContext) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = set.NewStringSet()

	// Keep track of unique args count for in-place modification
	uniqueArgCount := 1 // Start with 1 for the first arg (value to check)

	// TODO: ConstOnlyInContext and default branch should also prune duplicate ones after expression are managed by memo.
	for i := 1; i < len(b.args); i++ {
		switch b.args[i].ConstLevel() {
		case ConstStrict:
			val, isNull, err := b.args[i].EvalDecimal(ctx.GetEvalCtx(), chunk.Row{})
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

			key, err := val.ToHashKey()
			if err != nil {
				return err
			}

			hashKey := string(key)
			// Only keep this arg if value wasn't seen before
			if !b.hashSet.Exist(hashKey) {
				b.hashSet.Insert(hashKey)
				b.args[uniqueArgCount] = b.args[i]
				uniqueArgCount++
			}
		case ConstOnlyInContext:
			// Avoid build plans for wrong type.
			if _, _, err := b.args[i].EvalDecimal(ctx.GetEvalCtx(), chunk.Row{}); err != nil {
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

func (b *builtinInDecimalSig) Clone() builtinFunc {
	newSig := &builtinInDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = slices.Clone(b.nonConstArgsIdx)
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinInTimeSig) buildHashMapForConstArgs(ctx BuildContext) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = make(map[types.CoreTime]struct{}, len(b.args)-1)

	// Keep track of unique args count for in-place modification
	uniqueArgCount := 1 // Start with 1 for the first arg (value to check)

	// TODO: ConstOnlyInContext and default branch should also prune duplicate ones after expression are managed by memo.
	for i := 1; i < len(b.args); i++ {
		switch b.args[i].ConstLevel() {
		case ConstStrict:
			val, isNull, err := b.args[i].EvalTime(ctx.GetEvalCtx(), chunk.Row{})
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

			coreTime := val.CoreTime()
			// Only keep this arg if value wasn't seen before
			if _, exists := b.hashSet[coreTime]; !exists {
				b.hashSet[coreTime] = struct{}{}
				b.args[uniqueArgCount] = b.args[i]
				uniqueArgCount++
			}
		case ConstOnlyInContext:
			// Avoid build plans for wrong type.
			if _, _, err := b.args[i].EvalTime(ctx.GetEvalCtx(), chunk.Row{}); err != nil {
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

func (b *builtinInTimeSig) Clone() builtinFunc {
	newSig := &builtinInTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = slices.Clone(b.nonConstArgsIdx)
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinInDurationSig) buildHashMapForConstArgs(ctx BuildContext) error {
	b.nonConstArgsIdx = make([]int, 0)
	b.hashSet = make(map[time.Duration]struct{}, len(b.args)-1)

	// Keep track of unique args count for in-place modification
	uniqueArgCount := 1 // Start with 1 for the first arg (value to check)

	// TODO: ConstOnlyInContext and default branch should also prune duplicate ones after expression are managed by memo.
	for i := 1; i < len(b.args); i++ {
		switch b.args[i].ConstLevel() {
		case ConstStrict:
			val, isNull, err := b.args[i].EvalDuration(ctx.GetEvalCtx(), chunk.Row{})
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
			if _, exists := b.hashSet[val.Duration]; !exists {
				b.hashSet[val.Duration] = struct{}{}
				b.args[uniqueArgCount] = b.args[i]
				uniqueArgCount++
			}
		case ConstOnlyInContext:
			// Avoid build plans for wrong type.
			if _, _, err := b.args[i].EvalDuration(ctx.GetEvalCtx(), chunk.Row{}); err != nil {
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

func (b *builtinInDurationSig) Clone() builtinFunc {
	newSig := &builtinInDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgsIdx = slices.Clone(b.nonConstArgsIdx)
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

type builtinInVectorFloat32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinInVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinInVectorFloat32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInVectorFloat32Sig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalVectorFloat32(ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	var hasNull bool
	for _, arg := range b.args[1:] {
		evaledArg, isNull, err := arg.EvalVectorFloat32(ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		result := arg0.Compare(evaledArg)
		if result == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}
