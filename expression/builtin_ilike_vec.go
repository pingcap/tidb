// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/stringutil"
)

func LowerAlphaAscii(lowered_col *chunk.Column, row_num int) {
	for i := 0; i < row_num; i++ {
		str := lowered_col.GetString(i)
		str_bytes := hack.Slice(str)

		stringutil.LowerOneString(str_bytes)
	}
}

func LowerAlphaAsciiExcludeEscapeChar(lowered_col *chunk.Column, row_num int, excluded_char []int64) {
	for i := 0; i < row_num; i++ {
		str := lowered_col.GetString(i)
		str_bytes := hack.Slice(str)
		escape_char := excluded_char[i]

		stringutil.LowerOneStringExcludeEscapeChar(str_bytes, byte(escape_char))
	}
}

func (b *builtinIlikeSig) vectorized() bool {
	return true
}

func (b *builtinIlikeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	row_num := input.NumRows()
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
	pattern := collate.ConvertAndGetBinCollation(b.collation).Pattern()

	tmp_val_col := bufVal.CopyConstruct(nil)
	tmp_pattern_col := bufPattern.CopyConstruct(nil)
	LowerAlphaAscii(tmp_val_col, row_num)
	LowerAlphaAsciiExcludeEscapeChar(tmp_pattern_col, row_num, escapes)
	bufVal = tmp_val_col
	bufPattern = tmp_pattern_col

	result.ResizeInt64(row_num, false)
	result.MergeNulls(bufVal, bufPattern, bufEscape)
	i64s := result.Int64s()
	for i := 0; i < row_num; i++ {
		if result.IsNull(i) {
			continue
		}
		pattern.Compile(bufPattern.GetString(i), byte(escapes[i]))
		match := pattern.DoMatch(bufVal.GetString(i))
		i64s[i] = boolToInt64(match)
	}

	return nil
}
