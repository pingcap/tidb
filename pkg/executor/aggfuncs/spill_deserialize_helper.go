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

package aggfuncs

import (
	"bytes"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/spill"
)

const byteLen = int64(1)
const intLen = int64(unsafe.Sizeof(int(0)))
const boolLen = int64(unsafe.Sizeof(true))
const uint64Len = int64(unsafe.Sizeof(uint64(0)))
const int64Len = int64(unsafe.Sizeof(int64(0)))
const float32Len = int64(unsafe.Sizeof(float32(0)))
const float64Len = int64(unsafe.Sizeof(float64(0)))
const timeLen = int64(unsafe.Sizeof(types.Time{}))
const durationLen = int64(unsafe.Sizeof(time.Duration(0)))

type spillDeserializeHelper struct {
	column       *chunk.Column
	readRowIndex int
	totalRowCnt  int
}

func newDeserializeHelper(column *chunk.Column, rowNum int) spillDeserializeHelper {
	return spillDeserializeHelper{
		column:       column,
		readRowIndex: 0,
		totalRowCnt:  rowNum,
	}
}

func (s *spillDeserializeHelper) deserializePartialResult4Count(dst *partialResult4Count) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		*dst = spill.DeserializeInt64(bytes, 0)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinInt(dst *partialResult4MaxMinInt) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = spill.DeserializeBool(bytes, 0)
		dst.val = spill.DeserializeInt64(bytes, boolLen)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinUint(dst *partialResult4MaxMinUint) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = spill.DeserializeBool(bytes, 0)
		dst.val = spill.DeserializeUint64(bytes, boolLen)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinDecimal(dst *partialResult4MaxMinDecimal) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = spill.DeserializeBool(bytes, 0)
		dst.val = spill.DeserializeMyDecimal(bytes, boolLen)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinFloat32(dst *partialResult4MaxMinFloat32) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = spill.DeserializeBool(bytes, 0)
		dst.val = spill.DeserializeFloat32(bytes, boolLen)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinFloat64(dst *partialResult4MaxMinFloat64) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = spill.DeserializeBool(bytes, 0)
		dst.val = spill.DeserializeFloat64(bytes, boolLen)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinTime(dst *partialResult4MaxMinTime) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = spill.DeserializeBool(bytes, 0)
		dst.val = spill.DeserializeTime(bytes, boolLen)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinDuration(dst *partialResult4MaxMinDuration) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = spill.DeserializeBool(bytes, 0)
		dst.val.Duration = spill.DeserializeDuration(bytes, boolLen)
		dst.val.Fsp = spill.DeserializeInt(bytes, durationLen+boolLen)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinString(dst *partialResult4MaxMinString) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = spill.DeserializeBool(bytes, 0)
		dst.val = string(bytes[boolLen:])
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinJSON(dst *partialResult4MaxMinJSON) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = spill.DeserializeBool(bytes, 0)
		dst.val, _ = spill.DeserializeBinaryJSON(bytes, byteLen)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinEnum(dst *partialResult4MaxMinEnum) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = spill.DeserializeBool(bytes, 0)
		dst.val = spill.DeserializeEnum(bytes, boolLen)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4MaxMinSet(dst *partialResult4MaxMinSet) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = spill.DeserializeBool(bytes, 0)
		dst.val = spill.DeserializeSet(bytes, boolLen)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4AvgDecimal(dst *partialResult4AvgDecimal) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.sum = spill.DeserializeMyDecimal(bytes, 0)
		dst.count = spill.DeserializeInt64(bytes, types.MyDecimalStructSize)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4AvgFloat64(dst *partialResult4AvgFloat64) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.sum = spill.DeserializeFloat64(bytes, 0)
		dst.count = spill.DeserializeInt64(bytes, float64Len)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4SumDecimal(dst *partialResult4SumDecimal) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val = spill.DeserializeMyDecimal(bytes, 0)
		dst.notNullRowCount = spill.DeserializeInt64(bytes, types.MyDecimalStructSize)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4SumFloat64(dst *partialResult4SumFloat64) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val = spill.DeserializeFloat64(bytes, 0)
		dst.notNullRowCount = spill.DeserializeInt64(bytes, float64Len)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializeBasePartialResult4GroupConcat(dst *basePartialResult4GroupConcat) bool {
	if s.readRowIndex < s.totalRowCnt {
		restoredBytes := s.column.GetBytes(s.readRowIndex)
		valsBufLen := spill.DeserializeInt64(restoredBytes, 0)
		dst.valsBuf = bytes.NewBuffer(restoredBytes[int64Len : int64Len+valsBufLen])
		dst.buffer = bytes.NewBuffer(restoredBytes[int64Len+valsBufLen:])
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4GroupConcat(dst *partialResult4GroupConcat) bool {
	base := basePartialResult4GroupConcat{}
	success := s.deserializeBasePartialResult4GroupConcat(&base)
	dst.valsBuf = base.valsBuf
	dst.buffer = base.buffer
	return success
}

func (s *spillDeserializeHelper) deserializePartialResult4BitFunc(dst *partialResult4BitFunc) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		*dst = spill.DeserializeUint64(bytes, 0)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4JsonArrayagg(dst *partialResult4JsonArrayagg) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		byteNum := int64(len(bytes))
		readPos := int64(0)
		for readPos < byteNum {
			value := spill.DeserializeInterface(bytes, &readPos)
			dst.entries = append(dst.entries, value)
		}
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4JsonObjectAgg(dst *partialResult4JsonObjectAgg) (bool, int64) {
	memDelta := int64(0)
	dst.bInMap = 0
	dst.entries = make(map[string]interface{})
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		byteNum := int64(len(bytes))
		readPos := int64(0)
		for readPos < byteNum {
			keyLen := spill.DeserializeInt64(bytes, readPos)
			readPos += int64Len
			key := string(bytes[readPos : readPos+keyLen])
			readPos += keyLen
			realVal := spill.DeserializeInterface(bytes, &readPos)
			if _, ok := dst.entries[key]; !ok {
				memDelta += int64(len(key)) + getValMemDelta(realVal)
				if len(dst.entries)+1 > (1<<dst.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
					memDelta += (1 << dst.bInMap) * hack.DefBucketMemoryUsageForMapStringToAny
					dst.bInMap++
				}
			}
			dst.entries[key] = realVal
		}
		s.readRowIndex++
		return true, memDelta
	}
	return false, memDelta
}

func (*spillDeserializeHelper) deserializeBasePartialResult4FirstRow(dst *basePartialResult4FirstRow, bytes []byte, readPos int64) int64 {
	dst.isNull = spill.DeserializeBool(bytes, readPos)
	readPos += boolLen
	dst.gotFirstRow = spill.DeserializeBool(bytes, readPos)
	readPos += boolLen
	return readPos
}

func (s *spillDeserializeHelper) deserializePartialResult4FirstRowInt(dst *partialResult4FirstRowInt) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		readPos := int64(0)
		readPos = s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, readPos)
		dst.val = spill.DeserializeInt64(bytes, readPos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4FirstRowFloat32(dst *partialResult4FirstRowFloat32) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		readPos := int64(0)
		readPos = s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, readPos)
		dst.val = spill.DeserializeFloat32(bytes, readPos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4FirstRowFloat64(dst *partialResult4FirstRowFloat64) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		readPos := int64(0)
		readPos = s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, readPos)
		dst.val = spill.DeserializeFloat64(bytes, readPos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4FirstRowDecimal(dst *partialResult4FirstRowDecimal) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		readPos := int64(0)
		readPos = s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, readPos)
		dst.val = spill.DeserializeMyDecimal(bytes, readPos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4FirstRowString(dst *partialResult4FirstRowString) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		readPos := int64(0)
		readPos = s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, readPos)
		dst.val = string(bytes[readPos:])
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4FirstRowTime(dst *partialResult4FirstRowTime) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		readPos := int64(0)
		readPos = s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, readPos)
		dst.val = spill.DeserializeTime(bytes, readPos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4FirstRowDuration(dst *partialResult4FirstRowDuration) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		readPos := int64(0)
		readPos = s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, readPos)
		dst.val.Duration = *(*time.Duration)(unsafe.Pointer(&bytes[readPos]))
		readPos += durationLen
		dst.val.Fsp = spill.DeserializeInt(bytes, readPos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4FirstRowJSON(dst *partialResult4FirstRowJSON) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		readPos := s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, 0)
		dst.val, _ = spill.DeserializeBinaryJSON(bytes, readPos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4FirstRowEnum(dst *partialResult4FirstRowEnum) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		readPos := s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, 0)
		dst.val = spill.DeserializeEnum(bytes, readPos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *spillDeserializeHelper) deserializePartialResult4FirstRowSet(dst *partialResult4FirstRowSet) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		readPos := int64(0)
		readPos = s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, readPos)
		dst.val = spill.DeserializeSet(bytes, readPos)
		s.readRowIndex++
		return true
	}
	return false
}
