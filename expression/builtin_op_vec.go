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
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	i64s := result.Int64s()
	arg1s := buf.Int64s()

	for i := 0; i < n; i++ {
		isNull0 := result.IsNull(i)
		isNull1 := buf.IsNull(i)
		// Because buf is used to store the conversion of args[0] in place,
		// it could be that args[0] is null, yet args[1] is 0, in which case
		// the result is 0. In this case we need to reset the null bit mask
		// of the corresponding row in result.
		// See https://dev.mysql.com/doc/refman/5.7/en/logical-operators.html#operator_and
		isNull := false
		if (!isNull0 && i64s[i] == 0) || (!isNull1 && arg1s[i] == 0) {
			i64s[i] = 0
		} else if isNull0 || isNull1 {
			isNull = true
		} else {
			i64s[i] = 1
		}
		result.SetNull(i, isNull)
	}
	return nil
}

func (b *builtinLogicAndSig) vectorized() bool {
	return true
}
