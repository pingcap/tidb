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
	"fmt"
	"math"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func (b *builtinLog1ArgSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] <= 0 {
			tc := typeCtx(ctx)
			tc.AppendWarning(ErrInvalidArgumentForLogarithm)
			result.SetNull(i, true)
		} else {
			f64s[i] = math.Log(f64s[i])
		}
	}
	return nil
}

func (b *builtinLog1ArgSig) vectorized() bool {
	return true
}

func (b *builtinLog2Sig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] <= 0 {
			tc := typeCtx(ctx)
			tc.AppendWarning(ErrInvalidArgumentForLogarithm)
			result.SetNull(i, true)
		} else {
			f64s[i] = math.Log2(f64s[i])
		}
	}
	return nil
}

func (b *builtinLog2Sig) vectorized() bool {
	return true
}

func (b *builtinLog10Sig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] <= 0 {
			tc := typeCtx(ctx)
			tc.AppendWarning(ErrInvalidArgumentForLogarithm)
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

func (b *builtinSqrtSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
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

func (b *builtinAcosSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
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

func (b *builtinAsinSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
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

func (b *builtinAtan1ArgSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
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

func (b *builtinAtan2ArgsSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}

	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}

	f64s := result.Float64s()
	arg := buf.Float64s()

	result.MergeNulls(buf)

	for i := range n {
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

func (b *builtinCosSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
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

func (b *builtinCotSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
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

func (b *builtinDegreesSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = f64s[i] * 180 / math.Pi
	}
	return nil
}

func (b *builtinDegreesSig) vectorized() bool {
	return true
}

func (b *builtinExpSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		if result.IsNull(i) {
			continue
		}
		exp := math.Exp(f64s[i])
		if math.IsInf(exp, 0) || math.IsNaN(exp) {
			s := fmt.Sprintf("exp(%s)", b.args[0].StringWithCtx(ctx, errors.RedactLogDisable))
			if err := types.ErrOverflow.GenWithStackByArgs("DOUBLE", s); err != nil {
				return err
			}
		}
		f64s[i] = exp
	}
	return nil
}

func (b *builtinExpSig) vectorized() bool {
	return true
}

func (b *builtinRadiansSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = f64s[i] * (math.Pi / 180)
	}
	return nil
}

func (b *builtinRadiansSig) vectorized() bool {
	return true
}

func (b *builtinSinSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Sin(f64s[i])
	}
	return nil
}

func (b *builtinSinSig) vectorized() bool {
	return true
}

func (b *builtinTanSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Tan(f64s[i])
	}
	return nil
}

func (b *builtinTanSig) vectorized() bool {
	return true
}

func (b *builtinAbsDecSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalDecimal(ctx, input, result); err != nil {
		return err
	}
	zero := new(types.MyDecimal)
	buf := new(types.MyDecimal)
	d64s := result.Decimals()
	for i := range d64s {
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

func (b *builtinRoundDecSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalDecimal(ctx, input, result); err != nil {
		return err
	}
	d64s := result.Decimals()
	buf := new(types.MyDecimal)
	for i := range d64s {
		if result.IsNull(i) {
			continue
		}
		if err := d64s[i].Round(buf, 0, types.ModeHalfUp); err != nil {
			return err
		}
		d64s[i] = *buf
	}
	return nil
}

func (b *builtinRoundDecSig) vectorized() bool {
	return true
}

func (b *builtinPowSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[0].VecEvalReal(ctx, input, buf1); err != nil {
		return err
	}

	if err := b.args[1].VecEvalReal(ctx, input, result); err != nil {
		return err
	}

	x := buf1.Float64s()
	y := result.Float64s()
	result.MergeNulls(buf1)
	f64s := result.Float64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		power := math.Pow(x[i], y[i])
		if math.IsInf(power, -1) || math.IsInf(power, 1) || math.IsNaN(power) {
			return types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("pow(%s, %s)", strconv.FormatFloat(x[i], 'f', -1, 64), strconv.FormatFloat(y[i], 'f', -1, 64)))
		}
		f64s[i] = power
	}
	return nil
}

func (b *builtinPowSig) vectorized() bool {
	return true
}

func (b *builtinFloorRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Floor(f64s[i])
	}
	return nil
}

func (b *builtinFloorRealSig) vectorized() bool {
	return true
}

func (b *builtinLog2ArgsSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalReal(ctx, input, buf1); err != nil {
		return err
	}

	d := result.Float64s()
	x := buf1.Float64s()
	result.MergeNulls(buf1)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if d[i] <= 0 || d[i] == 1 || x[i] <= 0 {
			tc := typeCtx(ctx)
			tc.AppendWarning(ErrInvalidArgumentForLogarithm)
			result.SetNull(i, true)
		}
		d[i] = math.Log(x[i]) / math.Log(d[i])
	}
	return nil
}

func (b *builtinLog2ArgsSig) vectorized() bool {
	return true
}

func (b *builtinCeilRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Ceil(f64s[i])
	}
	return nil
}

func (b *builtinCeilRealSig) vectorized() bool {
	return true
}

func (b *builtinRoundRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = types.Round(f64s[i], 0)
	}
	return nil
}

func (b *builtinRoundRealSig) vectorized() bool {
	return true
}

func (b *builtinRoundWithFracRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	x := result.Float64s()
	d := buf1.Int64s()
	result.MergeNulls(buf1)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		x[i] = types.Round(x[i], int(d[i]))
	}
	return nil
}

func (b *builtinRoundWithFracRealSig) vectorized() bool {
	return true
}

func (b *builtinTruncateRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf1)
	x := result.Float64s()
	d := buf1.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		x[i] = types.Truncate(x[i], int(d[i]))
	}
	return nil
}

func (b *builtinTruncateRealSig) vectorized() bool {
	return true
}

func (b *builtinAbsRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := range f64s {
		f64s[i] = math.Abs(f64s[i])
	}
	return nil
}

func (b *builtinAbsRealSig) vectorized() bool {
	return true
}

