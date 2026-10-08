// Copyright 2026 PingCAP, Inc.
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

package util

import (
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// MarkTouchedRowsByColumn compares old/new values and sets the bit for rows whose value changed.
// Values are compared using their binary representation; this matches the mlog touched-column
// contract, which must not apply the session's SQL collation rules.
func MarkTouchedRowsByColumn(
	updateRows []int,
	updateTouchedBitmap []uint8,
	updateTouchedStride int,
	updateBitPos int,
	oldCol *chunk.Column,
	newCol *chunk.Column,
	fieldType *types.FieldType,
	notNull bool,
	comparisonContext string,
) error {
	if fieldType == nil {
		return errors.Errorf("field type is nil when comparing %s", comparisonContext)
	}
	if oldCol == nil || newCol == nil {
		return errors.Errorf("column is nil when comparing %s", comparisonContext)
	}
	if updateTouchedStride <= 0 || updateBitPos < 0 {
		return errors.Errorf("invalid touched bitmap layout when comparing %s", comparisonContext)
	}
	byteOffset := updateBitPos >> 3
	mask := uint8(1 << uint(updateBitPos&7))
	for ordinal, rowIdx := range updateRows {
		if rowIdx < 0 || rowIdx >= oldCol.Rows() || rowIdx >= newCol.Rows() {
			return errors.Errorf("row index %d out of range when comparing %s", rowIdx, comparisonContext)
		}
		oldNull, newNull := oldCol.IsNull(rowIdx), newCol.IsNull(rowIdx)
		changed := oldNull != newNull
		if !changed && !oldNull && !newNull {
			switch fieldType.GetType() {
			case mysql.TypeEnum:
				changed = oldCol.GetEnum(rowIdx).Name != newCol.GetEnum(rowIdx).Name
			case mysql.TypeSet:
				changed = oldCol.GetSet(rowIdx).Name != newCol.GetSet(rowIdx).Name
			default:
				changed = !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx))
			}
		}
		if notNull && (oldNull || newNull) {
			return errors.Errorf("unexpected null value when comparing %s", comparisonContext)
		}
		if changed {
			pos := ordinal*updateTouchedStride + byteOffset
			if pos >= len(updateTouchedBitmap) {
				return errors.Errorf("touched bitmap index out of range when comparing %s", comparisonContext)
			}
			updateTouchedBitmap[pos] |= mask
		}
	}
	return nil
}
