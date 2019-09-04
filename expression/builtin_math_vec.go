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
	"math"

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
