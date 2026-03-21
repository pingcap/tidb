// Copyright 2024 PingCAP, Inc.
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
	"strconv"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func (b *builtinCaseWhenIntSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.vecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf.Int64s()
	tp := b.tp
	isUnsigned := mysql.HasUnsignedFlag(tp.GetFlag())
	isBitType := tp.GetType() == mysql.TypeBit
	isYearType := tp.GetType() == mysql.TypeYear

	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		var str string
		if isBitType {
			// For BIT type, convert to binary string representation
			byteSize := (tp.GetFlen() + 7) / 8
			bl := types.NewBinaryLiteralFromUint(uint64(i64s[i]), byteSize)
			str = bl.ToString()
		} else if !isUnsigned {
			str = strconv.FormatInt(i64s[i], 10)
		} else {
			str = strconv.FormatUint(uint64(i64s[i]), 10)
		}

		if isYearType && str == "0" {
			str = "0000"
		}

		result.AppendString(str)
	}
	return nil
}

func (b *builtinCaseWhenRealSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.vecEvalReal(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	f64s := buf.Float64s()

	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := strconv.FormatFloat(f64s[i], 'f', -1, 64)
		result.AppendString(str)
	}
	return nil
}

func (b *builtinCaseWhenDecimalSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.vecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)

	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetDecimal(i).String()
		result.AppendString(str)
	}
	return nil
}
