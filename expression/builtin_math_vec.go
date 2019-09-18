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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"math"
	"strconv"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinLog10Sig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] <= 0 {
			result.SetNull(i, true)
		} else {
			f64s[i] = math.Log10(f64s[i])
		}
	}
	return nil
}

func (b *builtinLog10Sig) vectorized() bool {
	return true
}

func (b *builtinSqrtSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] < 0 {
			result.SetNull(i, true)
		} else {
			f64s[i] = math.Sqrt(f64s[i])
		}
	}
	return nil
}

func (b *builtinSqrtSig) vectorized() bool {
	return true
}

func (b *builtinAcosSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] < -1 || f64s[i] > 1 {
			result.SetNull(i, true)
		} else {
			f64s[i] = math.Acos(f64s[i])
		}
	}
	return nil
}

func (b *builtinAcosSig) vectorized() bool {
	return true
}

func (b *builtinAsinSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] < -1 || f64s[i] > 1 {
			result.SetNull(i, true)
		} else {
			f64s[i] = math.Asin(f64s[i])
		}
	}
	return nil
}

func (b *builtinAsinSig) vectorized() bool {
	return true
}

func (b *builtinAtan1ArgSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Atan(f64s[i])
	}
	return nil
}

func (b *builtinAtan1ArgSig) vectorized() bool {
	return true
}

func (b *builtinAtan2ArgsSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}

	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	f64s := result.Float64s()
	arg := buf.Float64s()

	result.MergeNulls(buf)

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Atan2(f64s[i], arg[i])
	}
	return nil
}

func (b *builtinAtan2ArgsSig) vectorized() bool {
	return true
}

func (b *builtinCosSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Cos(f64s[i])
	}
	return nil
}

func (b *builtinCosSig) vectorized() bool {
	return true
}

func (b *builtinAbsDecSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}
	zero := new(types.MyDecimal)
	buf := new(types.MyDecimal)
	d64s := result.Decimals()
	for i := 0; i < len(d64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if d64s[i].IsNegative() {
			if err := types.DecimalSub(zero, &d64s[i], buf); err != nil {
				return err
			}
			d64s[i] = *buf
		}
	}
	return nil
}

func (b *builtinAbsDecSig) vectorized() bool {
	return true
}

func (b *builtinRoundDecSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}
	d64s := result.Decimals()
	buf := new(types.MyDecimal)
	for i := 0; i < len(d64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if err := d64s[i].Round(buf, 0, types.ModeHalfEven); err != nil {
			return err
		}
		d64s[i] = *buf
	}
	return nil
}

func (b *builtinRoundDecSig) vectorized() bool {
	return true
}

func (b *builtinPowSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[0].VecEvalReal(b.ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalReal(b.ctx, input, buf2); err != nil {
		return err
	}
	x := buf1.Float64s()
	y := buf2.Float64s()
	result.ResizeFloat64(n, false)
	f64s := result.Float64s()
	for i := 0; i < n; i++ {
		if buf1.IsNull(i) || buf2.IsNull(i) {
			result.SetNull(i, true)
			continue
		}
		power := math.Pow(x[i], y[i])
		if math.IsInf(power, -1) || math.IsInf(power, 1) || math.IsNaN(power) {
			err := types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("pow(%s, %s)", strconv.FormatFloat(x[i], 'f', -1, 64), strconv.FormatFloat(y[i], 'f', -1, 64)))
			b.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, err)
			f64s[i] = 0
		} else {
			f64s[i] = power
		}
	}
	return nil
}

func (b *builtinPowSig) vectorized() bool {
	return true
}
