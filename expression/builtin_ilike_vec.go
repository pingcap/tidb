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
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/stringutil"
)

func LowerAlphaAsciiAndStoreInNewColumn(src_col *chunk.Column, lowered_col *chunk.Column, elem_num int) {
	lowered_col.ReserveString(elem_num)
	for i := 0; i < elem_num; i++ {
		if src_col.IsNull(i) {
			lowered_col.AppendString("")
			lowered_col.SetNull(i, true)
			continue
		}

		src_str := src_col.GetString(i)
		lowered_col.AppendString(stringutil.LowerOneString(src_str))
		lowered_col.SetNull(i, false)
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
	pattern := b.collator().Pattern()

	if !(collate.IsCICollation(b.collation)) {
		tmp_val_col := chunk.NewColumn(types.NewFieldType(mysql.TypeVarString), 0)
		tmp_pattern_col := chunk.NewColumn(types.NewFieldType(mysql.TypeVarString), 0)
		LowerAlphaAsciiAndStoreInNewColumn(bufVal, tmp_val_col, row_num)
		LowerAlphaAsciiAndStoreInNewColumn(bufPattern, tmp_pattern_col, row_num)
		bufVal = tmp_val_col
		bufPattern = tmp_pattern_col
	}

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
