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

func (b *builtinLogicAndSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	i64s := result.Int64s()
	arg1 := buf1.Int64s()

	for i := 0; i < n; i++ {
		isNull0 := result.IsNull(i)
		if !isNull0 && i64s[i] == 0 {
			result.SetNull(i, false)
			continue
		}

		isNull1 := buf1.IsNull(i)
		if !isNull1 && arg1[i] == 0 {
			i64s[i] = 0
			result.SetNull(i, false)
			continue
		}

		if isNull0 || isNull1 {
			result.SetNull(i, true)
			continue
		}

		i64s[i] = 1
	}

	return nil
}

func (b *builtinLogicAndSig) vectorized() bool {
	return true
}
