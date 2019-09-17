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

func (b *builtinCotSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		tan := math.Tan(f64s[i])
		if tan != 0 {
			cot := 1 / tan
			if !math.IsInf(cot, 0) && !math.IsNaN(cot) {
				f64s[i] = cot
			}
			continue
		}
		if err := types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("cot(%s)", strconv.FormatFloat(f64s[i], 'f', -1, 64))); err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinCotSig) vectorized() bool {
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
