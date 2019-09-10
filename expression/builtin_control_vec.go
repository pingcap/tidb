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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinIfJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalJSON(b.ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalJSON(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveJSON(n)
	arg0 := buf0.Int64s()
	for i := 0; i < n; i++ {
		arg := arg0[i]
		isNull0 := buf0.IsNull(i)
		switch {
		case isNull0 || arg == 0:
			if buf2.IsNull(i) {
				result.AppendNull()
			} else {
				result.AppendJSON(buf2.GetJSON(i))
			}
		case arg != 0:
			if buf1.IsNull(i) {
				result.AppendNull()
			} else {
				result.AppendJSON(buf1.GetJSON(i))
			}
		}
	}
	return nil
}

func (b *builtinIfJSONSig) vectorized() bool {
	return true
}
