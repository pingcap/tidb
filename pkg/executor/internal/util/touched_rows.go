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
	"github.com/pingcap/tidb/pkg/util/collate"
)

// MarkTouchedRowsByColumn compares old/new chunk columns and calls markTouched for each changed update row.
// The comparison deliberately uses binary semantics for string-like values to match row-update touched-column
// detection instead of SQL collation equality.
func MarkTouchedRowsByColumn(
	updateRows []int,
	oldCol *chunk.Column,
	newCol *chunk.Column,
	fieldType *types.FieldType,
	notNull bool,
	markTouched func(updateOrdinal int),
	comparisonContext string,
) error {
	if fieldType == nil {
		return errors.Errorf("field type is nil when comparing %s", comparisonContext)
	}
	if len(updateRows) == 0 {
		return nil
	}

	switch fieldType.EvalType() {
	case types.ETInt:
		if mysql.HasUnsignedFlag(fieldType.GetFlag()) {
			oldVals := oldCol.Uint64s()
			newVals := newCol.Uint64s()
			if notNull {
				for updateOrdinal, rowIdx := range updateRows {
					if oldVals[rowIdx] != newVals[rowIdx] {
						markTouched(updateOrdinal)
					}
				}
				return nil
			}
			for updateOrdinal, rowIdx := range updateRows {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						markTouched(updateOrdinal)
					}
					continue
				}
				if oldVals[rowIdx] != newVals[rowIdx] {
					markTouched(updateOrdinal)
				}
			}
			return nil
		}
		oldVals := oldCol.Int64s()
		newVals := newCol.Int64s()
		if notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if oldVals[rowIdx] != newVals[rowIdx] {
					markTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					markTouched(updateOrdinal)
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				markTouched(updateOrdinal)
			}
		}
		return nil
	case types.ETReal:
		if fieldType.GetType() == mysql.TypeFloat {
			oldVals := oldCol.Float32s()
			newVals := newCol.Float32s()
			if notNull {
				for updateOrdinal, rowIdx := range updateRows {
					if oldVals[rowIdx] != newVals[rowIdx] {
						markTouched(updateOrdinal)
					}
				}
				return nil
			}
			for updateOrdinal, rowIdx := range updateRows {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						markTouched(updateOrdinal)
					}
					continue
				}
				if oldVals[rowIdx] != newVals[rowIdx] {
					markTouched(updateOrdinal)
				}
			}
			return nil
		}
		oldVals := oldCol.Float64s()
		newVals := newCol.Float64s()
		if notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if oldVals[rowIdx] != newVals[rowIdx] {
					markTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					markTouched(updateOrdinal)
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				markTouched(updateOrdinal)
			}
		}
		return nil
	case types.ETDecimal:
		oldVals := oldCol.Decimals()
		newVals := newCol.Decimals()
		if notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if oldVals[rowIdx].Compare(&newVals[rowIdx]) != 0 {
					markTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					markTouched(updateOrdinal)
				}
				continue
			}
			if oldVals[rowIdx].Compare(&newVals[rowIdx]) != 0 {
				markTouched(updateOrdinal)
			}
		}
		return nil
	case types.ETString:
		binaryCollator := collate.GetBinaryCollator()
		switch fieldType.GetType() {
		case mysql.TypeEnum:
			if notNull {
				for updateOrdinal, rowIdx := range updateRows {
					if binaryCollator.Compare(oldCol.GetEnum(rowIdx).Name, newCol.GetEnum(rowIdx).Name) != 0 {
						markTouched(updateOrdinal)
					}
				}
				return nil
			}
			for updateOrdinal, rowIdx := range updateRows {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						markTouched(updateOrdinal)
					}
					continue
				}
				if binaryCollator.Compare(oldCol.GetEnum(rowIdx).Name, newCol.GetEnum(rowIdx).Name) != 0 {
					markTouched(updateOrdinal)
				}
			}
			return nil
		case mysql.TypeSet:
			if notNull {
				for updateOrdinal, rowIdx := range updateRows {
					if binaryCollator.Compare(oldCol.GetSet(rowIdx).Name, newCol.GetSet(rowIdx).Name) != 0 {
						markTouched(updateOrdinal)
					}
				}
				return nil
			}
			for updateOrdinal, rowIdx := range updateRows {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						markTouched(updateOrdinal)
					}
					continue
				}
				if binaryCollator.Compare(oldCol.GetSet(rowIdx).Name, newCol.GetSet(rowIdx).Name) != 0 {
					markTouched(updateOrdinal)
				}
			}
			return nil
		}
		if notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if binaryCollator.Compare(oldCol.GetString(rowIdx), newCol.GetString(rowIdx)) != 0 {
					markTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					markTouched(updateOrdinal)
				}
				continue
			}
			if binaryCollator.Compare(oldCol.GetString(rowIdx), newCol.GetString(rowIdx)) != 0 {
				markTouched(updateOrdinal)
			}
		}
		return nil
	case types.ETDatetime, types.ETTimestamp:
		oldVals := oldCol.Times()
		newVals := newCol.Times()
		if notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if oldVals[rowIdx] != newVals[rowIdx] {
					markTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					markTouched(updateOrdinal)
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				markTouched(updateOrdinal)
			}
		}
		return nil
	case types.ETDuration:
		oldVals := oldCol.GoDurations()
		newVals := newCol.GoDurations()
		if notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if oldVals[rowIdx] != newVals[rowIdx] {
					markTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					markTouched(updateOrdinal)
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				markTouched(updateOrdinal)
			}
		}
		return nil
	case types.ETJson, types.ETVectorFloat32:
		if notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx)) {
					markTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					markTouched(updateOrdinal)
				}
				continue
			}
			if !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx)) {
				markTouched(updateOrdinal)
			}
		}
		return nil
	default:
		return errors.Errorf("unsupported eval type %d in %s comparison", fieldType.EvalType(), comparisonContext)
	}
}
