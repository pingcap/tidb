// Copyright 2019 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func vecResOfLT(res []int64) {
	n := len(res)
	for i := range n {
		if res[i] < 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfNE(res []int64) {
	n := len(res)
	for i := range n {
		if res[i] != 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfEQ(res []int64) {
	n := len(res)
	for i := range n {
		if res[i] == 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfLE(res []int64) {
	n := len(res)
	for i := range n {
		if res[i] <= 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfGT(res []int64) {
	n := len(res)
	for i := range n {
		if res[i] > 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfGE(res []int64) {
	n := len(res)
	for i := range n {
		if res[i] >= 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

// vecCompareInt is vectorized CompareInt()
func vecCompareInt(isUnsigned0, isUnsigned1 bool, largs, rargs, result *chunk.Column) {
	switch {
	case isUnsigned0 && isUnsigned1:
		types.VecCompareUU(largs.Uint64s(), rargs.Uint64s(), result.Int64s())
	case isUnsigned0 && !isUnsigned1:
		types.VecCompareUI(largs.Uint64s(), rargs.Int64s(), result.Int64s())
	case !isUnsigned0 && isUnsigned1:
		types.VecCompareIU(largs.Int64s(), rargs.Uint64s(), result.Int64s())
	case !isUnsigned0 && !isUnsigned1:
		types.VecCompareII(largs.Int64s(), rargs.Int64s(), result.Int64s())
	}
}

func (b *builtinGreatestCmpStringAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinGreatestCmpStringAsTimeSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	dstStrings := make([]string, n)
	// TODO: use Column.MergeNulls instead, however, it doesn't support var-length type currently.
	dstNullMap := make([]bool, n)

	for j := range b.args {
		if err := b.args[j].VecEvalString(ctx, input, result); err != nil {
			return err
		}
		for i := range n {
			if dstNullMap[i] = dstNullMap[i] || result.IsNull(i); dstNullMap[i] {
				continue
			}

			// NOTE: can't use Column.GetString because it returns an unsafe string, copy the row instead.
			argTimeStr := string(result.GetBytes(i))
			var err error
			argTimeStr, err = doTimeConversionForGL(b.cmpAsDate, ctx, argTimeStr)
			if err != nil {
				return err
			}
			if j == 0 || strings.Compare(argTimeStr, dstStrings[i]) > 0 {
				dstStrings[i] = argTimeStr
			}
		}
	}

	// Aggregate the NULL and String value into result
	result.ReserveString(n)
	for i := range n {
		if dstNullMap[i] {
			result.AppendNull()
		} else {
			result.AppendString(dstStrings[i])
		}
	}
	return nil
}

func (b *builtinGreatestRealSig) vectorized() bool {
	return true
}

func (b *builtinGreatestRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}

	f64s := result.Float64s()
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalReal(ctx, input, buf); err != nil {
			return err
		}

		result.MergeNulls(buf)
		v := buf.Float64s()
		for i := range n {
			if result.IsNull(i) {
				continue
			}
			if v[i] > f64s[i] {
				f64s[i] = v[i]
			}
		}
	}
	return nil
}

func (b *builtinLeastCmpStringAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinLeastCmpStringAsTimeSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	dstStrings := make([]string, n)
	// TODO: use Column.MergeNulls instead, however, it doesn't support var-length type currently.
	dstNullMap := make([]bool, n)

	for j := range b.args {
		if err := b.args[j].VecEvalString(ctx, input, result); err != nil {
			return err
		}
		for i := range n {
			if dstNullMap[i] = dstNullMap[i] || result.IsNull(i); dstNullMap[i] {
				continue
			}

			// NOTE: can't use Column.GetString because it returns an unsafe string, copy the row instead.
			argTimeStr := string(result.GetBytes(i))
			var err error
			argTimeStr, err = doTimeConversionForGL(b.cmpAsDate, ctx, argTimeStr)
			if err != nil {
				return err
			}
			if j == 0 || strings.Compare(argTimeStr, dstStrings[i]) < 0 {
				dstStrings[i] = argTimeStr
			}
		}
	}

	// Aggregate the NULL and String value into result
	result.ReserveString(n)
	for i := range n {
		if dstNullMap[i] {
			result.AppendNull()
		} else {
			result.AppendString(dstStrings[i])
		}
	}
	return nil
}

func (b *builtinGreatestStringSig) vectorized() bool {
	return true
}

func (b *builtinGreatestStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)

	src := result
	arg := buf1
	dst := buf2
	dst.ReserveString(n)
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalString(ctx, input, arg); err != nil {
			return err
		}
		for i := range n {
			if src.IsNull(i) || arg.IsNull(i) {
				dst.AppendNull()
				continue
			}
			srcStr := src.GetString(i)
			argStr := arg.GetString(i)
			if types.CompareString(srcStr, argStr, b.collation) > 0 {
				dst.AppendString(srcStr)
			} else {
				dst.AppendString(argStr)
			}
		}
		src, dst = dst, src
		arg.ReserveString(n)
		dst.ReserveString(n)
	}
	if len(b.args)%2 == 0 {
		src.CopyConstruct(result)
	}
	return nil
}

func (b *builtinGreatestTimeSig) vectorized() bool {
	return true
}

func (b *builtinGreatestTimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	result.ResizeTime(n, false)
	for argIdx := range b.args {
		if err := b.args[argIdx].VecEvalTime(ctx, input, buf); err != nil {
			return err
		}
		result.MergeNulls(buf)
		resTimes := result.Times()
		argTimes := buf.Times()
		for rowIdx := range n {
			if result.IsNull(rowIdx) {
				continue
			}
			if argIdx == 0 || argTimes[rowIdx].Compare(resTimes[rowIdx]) > 0 {
				resTimes[rowIdx] = argTimes[rowIdx]
			}
		}
	}
	tc := typeCtx(ctx)
	resTimeTp := getAccurateTimeTypeForGLRet(b.cmpAsDate)
	for rowIdx := range n {
		resTimes := result.Times()
		resTimes[rowIdx], err = resTimes[rowIdx].Convert(tc, resTimeTp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinLeastTimeSig) vectorized() bool {
	return true
}

func (b *builtinLeastTimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	result.ResizeTime(n, false)
	for argIdx := range b.args {
		if err := b.args[argIdx].VecEvalTime(ctx, input, buf); err != nil {
			return err
		}
		result.MergeNulls(buf)
		resTimes := result.Times()
		argTimes := buf.Times()
		for rowIdx := range n {
			if result.IsNull(rowIdx) {
				continue
			}
			if argIdx == 0 || argTimes[rowIdx].Compare(resTimes[rowIdx]) < 0 {
				resTimes[rowIdx] = argTimes[rowIdx]
			}
		}
	}
	tc := typeCtx(ctx)
	resTimeTp := getAccurateTimeTypeForGLRet(b.cmpAsDate)
	for rowIdx := range n {
		resTimes := result.Times()
		resTimes[rowIdx], err = resTimes[rowIdx].Convert(tc, resTimeTp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinGreatestDurationSig) vectorized() bool {
	return true
}

func (b *builtinGreatestDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	result.ResizeGoDuration(n, false)
	for argIdx := range b.args {
		if err := b.args[argIdx].VecEvalDuration(ctx, input, buf); err != nil {
			return err
		}
		result.MergeNulls(buf)
		resDurations := result.GoDurations()
		argDurations := buf.GoDurations()
		for rowIdx := range n {
			if result.IsNull(rowIdx) {
				continue
			}
			if argIdx == 0 || argDurations[rowIdx] > resDurations[rowIdx] {
				resDurations[rowIdx] = argDurations[rowIdx]
			}
		}
	}
	return nil
}

func (b *builtinLeastDurationSig) vectorized() bool {
	return true
}

func (b *builtinLeastDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	result.ResizeGoDuration(n, false)
	for argIdx := range b.args {
		if err := b.args[argIdx].VecEvalDuration(ctx, input, buf); err != nil {
			return err
		}
		result.MergeNulls(buf)
		resDurations := result.GoDurations()
		argDurations := buf.GoDurations()
		for rowIdx := range n {
			if result.IsNull(rowIdx) {
				continue
			}
			if argIdx == 0 || argDurations[rowIdx] < resDurations[rowIdx] {
				resDurations[rowIdx] = argDurations[rowIdx]
			}
		}
	}
	return nil
}
