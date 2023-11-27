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
	"unsafe"

	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	util "github.com/pingcap/tidb/pkg/util/serialization"
)

const boolLen = int64(unsafe.Sizeof(true))

type deserializeHelper struct {
	column       *chunk.Column
	readRowIndex int
	totalRowCnt  int
}

func newDeserializeHelper(column *chunk.Column, rowNum int) deserializeHelper {
	return deserializeHelper{
		column:       column,
		readRowIndex: 0,
		totalRowCnt:  rowNum,
	}
}

func (s *deserializeHelper) deserializePartialResult4Count(dst *partialResult4Count) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		*dst = util.DeserializeInt64(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinInt(dst *partialResult4MaxMinInt) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = util.DeserializeBool(bytes, &pos)
		dst.val = util.DeserializeInt64(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinUint(dst *partialResult4MaxMinUint) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = util.DeserializeBool(bytes, &pos)
		dst.val = util.DeserializeUint64(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinDecimal(dst *partialResult4MaxMinDecimal) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = util.DeserializeBool(bytes, &pos)
		dst.val = util.DeserializeMyDecimal(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinFloat32(dst *partialResult4MaxMinFloat32) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = util.DeserializeBool(bytes, &pos)
		dst.val = util.DeserializeFloat32(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinFloat64(dst *partialResult4MaxMinFloat64) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = util.DeserializeBool(bytes, &pos)
		dst.val = util.DeserializeFloat64(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinTime(dst *partialResult4MaxMinTime) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = util.DeserializeBool(bytes, &pos)
		dst.val = util.DeserializeTime(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinDuration(dst *partialResult4MaxMinDuration) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = util.DeserializeBool(bytes, &pos)
		dst.val = util.DeserializeTypesDuration(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinString(dst *partialResult4MaxMinString) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = util.DeserializeBool(bytes, &pos)
		dst.val = util.DeserializeString(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinJSON(dst *partialResult4MaxMinJSON) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = util.DeserializeBool(bytes, &pos)
		dst.val = util.DeserializeBinaryJSON(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinEnum(dst *partialResult4MaxMinEnum) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = util.DeserializeBool(bytes, &pos)
		dst.val = util.DeserializeEnum(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinSet(dst *partialResult4MaxMinSet) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.isNull = util.DeserializeBool(bytes, &pos)
		dst.val = util.DeserializeSet(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4AvgDecimal(dst *partialResult4AvgDecimal) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.sum = util.DeserializeMyDecimal(bytes, &pos)
		dst.count = util.DeserializeInt64(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4AvgFloat64(dst *partialResult4AvgFloat64) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.sum = util.DeserializeFloat64(bytes, &pos)
		dst.count = util.DeserializeInt64(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4SumDecimal(dst *partialResult4SumDecimal) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val = util.DeserializeMyDecimal(bytes, &pos)
		dst.notNullRowCount = util.DeserializeInt64(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4SumFloat64(dst *partialResult4SumFloat64) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		dst.val = util.DeserializeFloat64(bytes, &pos)
		dst.notNullRowCount = util.DeserializeInt64(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializeBasePartialResult4GroupConcat(dst *basePartialResult4GroupConcat) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		restoredBytes := s.column.GetBytes(s.readRowIndex)
		dst.valsBuf = util.DeserializeBytesBuffer(restoredBytes, &pos)
		dst.buffer = util.DeserializeBytesBuffer(restoredBytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4GroupConcat(dst *partialResult4GroupConcat) bool {
	base := basePartialResult4GroupConcat{}
	success := s.deserializeBasePartialResult4GroupConcat(&base)
	dst.valsBuf = base.valsBuf
	dst.buffer = base.buffer
	return success
}

func (s *deserializeHelper) deserializePartialResult4BitFunc(dst *partialResult4BitFunc) bool {
	if s.readRowIndex < s.totalRowCnt {
		pos := int64(0)
		bytes := s.column.GetBytes(s.readRowIndex)
		*dst = util.DeserializeUint64(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4JsonArrayagg(dst *partialResult4JsonArrayagg) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		byteNum := int64(len(bytes))
		readPos := int64(0)
		for readPos < byteNum {
			value := util.DeserializeInterface(bytes, &readPos)
			dst.entries = append(dst.entries, value)
		}
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4JsonObjectAgg(dst *partialResult4JsonObjectAgg) (bool, int64) {
	memDelta := int64(0)
	dst.bInMap = 0
	dst.entries = make(map[string]interface{})
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		byteNum := int64(len(bytes))
		readPos := int64(0)
		for readPos < byteNum {
			key := util.DeserializeString(bytes, &readPos)
			realVal := util.DeserializeInterface(bytes, &readPos)
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

func (*deserializeHelper) deserializeBasePartialResult4FirstRow(dst *basePartialResult4FirstRow, bytes []byte, pos *int64) {
	dst.isNull = util.DeserializeBool(bytes, pos)
	dst.gotFirstRow = util.DeserializeBool(bytes, pos)
}

func (s *deserializeHelper) deserializePartialResult4FirstRowInt(dst *partialResult4FirstRowInt) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		pos := int64(0)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, &pos)
		dst.val = util.DeserializeInt64(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowFloat32(dst *partialResult4FirstRowFloat32) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		pos := int64(0)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, &pos)
		dst.val = util.DeserializeFloat32(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowFloat64(dst *partialResult4FirstRowFloat64) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		pos := int64(0)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, &pos)
		dst.val = util.DeserializeFloat64(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowDecimal(dst *partialResult4FirstRowDecimal) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		pos := int64(0)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, &pos)
		dst.val = util.DeserializeMyDecimal(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowString(dst *partialResult4FirstRowString) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		pos := int64(0)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, &pos)
		dst.val = util.DeserializeString(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowTime(dst *partialResult4FirstRowTime) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		pos := int64(0)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, &pos)
		dst.val = util.DeserializeTime(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowDuration(dst *partialResult4FirstRowDuration) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		pos := int64(0)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, &pos)
		dst.val = util.DeserializeTypesDuration(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowJSON(dst *partialResult4FirstRowJSON) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		pos := int64(0)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, &pos)
		dst.val = util.DeserializeBinaryJSON(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowEnum(dst *partialResult4FirstRowEnum) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		pos := int64(0)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, &pos)
		dst.val = util.DeserializeEnum(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowSet(dst *partialResult4FirstRowSet) bool {
	if s.readRowIndex < s.totalRowCnt {
		bytes := s.column.GetBytes(s.readRowIndex)
		pos := int64(0)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, bytes, &pos)
		dst.val = util.DeserializeSet(bytes, &pos)
		s.readRowIndex++
		return true
	}
	return false
}
