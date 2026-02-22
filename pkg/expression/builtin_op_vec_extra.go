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
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func (b *builtinLogicXorSig) vectorized() bool {
	return true
}

func (b *builtinLogicXorSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	i64s := result.Int64s()
	arg1s := buf.Int64s()
	// Returns NULL if either operand is NULL.
	// See https://dev.mysql.com/doc/refman/5.7/en/logical-operators.html#operator_xor
	result.MergeNulls(buf)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		arg0 := i64s[i]
		arg1 := arg1s[i]
		if (arg0 != 0 && arg1 != 0) || (arg0 == 0 && arg1 == 0) {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinBitAndSig) vectorized() bool {
	return true
}

func (b *builtinBitAndSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	arg0s := result.Int64s()
	arg1s := buf.Int64s()
	result.MergeNulls(buf)
	for i := range numRows {
		arg0s[i] &= arg1s[i]
	}
	return nil
}

func (b *builtinRealIsFalseSig) vectorized() bool {
	return true
}

func (b *builtinRealIsFalseSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(numRows, false)
	i64s := result.Int64s()
	bufF64s := buf.Float64s()
	for i := range numRows {
		isNull := buf.IsNull(i)
		if b.keepNull && isNull {
			result.SetNull(i, true)
			continue
		}
		if isNull || bufF64s[i] != 0 {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinUnaryMinusIntSig) vectorized() bool {
	return true
}

func (b *builtinUnaryMinusIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	args := result.Int64s()
	if mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()) {
		for i := range n {
			if result.IsNull(i) {
				continue
			}
			if uint64(args[i]) > uint64(-math.MinInt64) {
				return types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("-%v", uint64(args[i])))
			}
			args[i] = -args[i]
		}
	} else {
		for i := range n {
			if result.IsNull(i) {
				continue
			}
			if args[i] == math.MinInt64 {
				return types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("-%v", args[i]))
			}
			args[i] = -args[i]
		}
	}
	return nil
}

func (b *builtinUnaryNotDecimalSig) vectorized() bool {
	return true
}

func (b *builtinUnaryNotDecimalSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}
	decs := buf.Decimals()

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if decs[i].IsZero() {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinUnaryNotIntSig) vectorized() bool {
	return true
}

func (b *builtinUnaryNotIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}

	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if i64s[i] == 0 {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinDecimalIsNullSig) vectorized() bool {
	return true
}

func (b *builtinDecimalIsNullSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	if err := b.args[0].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(numRows, false)
	i64s := result.Int64s()
	for i := range numRows {
		if buf.IsNull(i) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinLeftShiftSig) vectorized() bool {
	return true
}

func (b *builtinLeftShiftSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	arg0s := result.Int64s()
	arg1s := buf.Int64s()
	result.MergeNulls(buf)
	for i := range numRows {
		arg0s[i] = int64(uint64(arg0s[i]) << uint64(arg1s[i]))
	}
	return nil
}

func (b *builtinRightShiftSig) vectorized() bool {
	return true
}

func (b *builtinRightShiftSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	arg0s := result.Int64s()
	arg1s := buf.Int64s()
	result.MergeNulls(buf)
	for i := range numRows {
		arg0s[i] = int64(uint64(arg0s[i]) >> uint64(arg1s[i]))
	}
	return nil
}

func (b *builtinRealIsTrueSig) vectorized() bool {
	return true
}

func (b *builtinRealIsTrueSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	if err := b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(numRows, false)
	f64s := buf.Float64s()
	i64s := result.Int64s()
	for i := range numRows {
		isNull := buf.IsNull(i)
		if b.keepNull && isNull {
			result.SetNull(i, true)
			continue
		}
		if isNull || f64s[i] == 0 {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinDecimalIsTrueSig) vectorized() bool {
	return true
}

func (b *builtinDecimalIsTrueSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}

	decs := buf.Decimals()
	result.ResizeInt64(numRows, false)
	i64s := result.Int64s()

	for i := range numRows {
		isNull := buf.IsNull(i)
		if b.keepNull && isNull {
			result.SetNull(i, true)
			continue
		}
		if isNull || decs[i].IsZero() {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinIntIsTrueSig) vectorized() bool {
	return true
}

func (b *builtinIntIsTrueSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}
	i64s := result.Int64s()
	for i := range numRows {
		isNull := result.IsNull(i)
		if b.keepNull && isNull {
			continue
		}
		if isNull {
			i64s[i] = 0
			result.SetNull(i, false)
		} else if i64s[i] != 0 {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinDurationIsNullSig) vectorized() bool {
	return true
}

func (b *builtinDurationIsNullSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	if err := b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(numRows, false)
	i64s := result.Int64s()
	for i := range numRows {
		if buf.IsNull(i) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}
