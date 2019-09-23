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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinMonthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < input.NumRows(); i++ {
		if result.IsNull(i) {
			continue
		}
		if ds[i].IsZero() {
			if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
				if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(ds[i].String())); err != nil {
					return err
				}
				result.SetNull(i, true)
				continue
			}
			i64s[i] = 0
			continue
		}
		i64s[i] = int64(ds[i].Time.Month())
	}
	return nil
}

func (b *builtinMonthSig) vectorized() bool {
	return true
}

func (b *builtinYearSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < input.NumRows(); i++ {
		if result.IsNull(i) {
			continue
		}
		if ds[i].IsZero() {
			if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
				if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(ds[i].String())); err != nil {
					return err
				}
				result.SetNull(i, true)
				continue
			}
			i64s[i] = 0
			continue
		}
		i64s[i] = int64(ds[i].Time.Year())
	}
	return nil
}

func (b *builtinYearSig) vectorized() bool {
	return true
}

func (b *builtinDateSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalTime(b.ctx, input, result); err != nil {
		return err
	}
	times := result.Times()
	for i := 0; i < len(times); i++ {
		if result.IsNull(i) {
			continue
		}
		if times[i].IsZero() {
			if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(times[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
		} else {
			times[i].Time = types.FromDate(times[i].Time.Year(), times[i].Time.Month(), times[i].Time.Day(), 0, 0, 0, 0)
			times[i].Type = mysql.TypeDate
		}
	}
	return nil
}

func (b *builtinDateSig) vectorized() bool {
	return true
}
