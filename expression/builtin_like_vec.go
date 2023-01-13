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
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinLikeSig) vectorized() bool {
	return true
}

func (b *builtinLikeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufVal, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufVal)
	if err = b.args[0].VecEvalString(b.ctx, input, bufVal); err != nil {
		return err
	}
	bufPattern, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufPattern)
	if err = b.args[1].VecEvalString(b.ctx, input, bufPattern); err != nil {
		return err
	}

	bufEscape, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufEscape)
	if err = b.args[2].VecEvalInt(b.ctx, input, bufEscape); err != nil {
		return err
	}
	escapes := bufEscape.Int64s()

	// Must not use b.pattern to avoid data race
	pattern := b.collator().Pattern()

	result.ResizeInt64(n, false)
	result.MergeNulls(bufVal, bufPattern, bufEscape)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		pattern.Compile(bufPattern.GetString(i), byte(escapes[i]))
		match := pattern.DoMatch(bufVal.GetString(i))
		i64s[i] = boolToInt64(match)
	}

	return nil
}
