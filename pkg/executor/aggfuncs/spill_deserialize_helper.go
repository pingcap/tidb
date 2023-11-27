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
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	util "github.com/pingcap/tidb/pkg/util/serialization"
)

type posAndBytes struct {
	bytes []byte
	pos   *int64
}

func newPosAndBytes() *posAndBytes {
	pos := int64(0)
	return &posAndBytes{pos: &pos}
}

func (p *posAndBytes) prepare(col *chunk.Column, idx int) {
	p.bytes = col.GetBytes(idx)
	*(p.pos) = 0
}

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
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		*dst = util.DeserializeInt64(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinInt(dst *partialResult4MaxMinInt) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.isNull = util.DeserializeBool(helper.bytes, helper.pos)
		dst.val = util.DeserializeInt64(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinUint(dst *partialResult4MaxMinUint) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.isNull = util.DeserializeBool(helper.bytes, helper.pos)
		dst.val = util.DeserializeUint64(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinDecimal(dst *partialResult4MaxMinDecimal) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.isNull = util.DeserializeBool(helper.bytes, helper.pos)
		dst.val = util.DeserializeMyDecimal(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinFloat32(dst *partialResult4MaxMinFloat32) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.isNull = util.DeserializeBool(helper.bytes, helper.pos)
		dst.val = util.DeserializeFloat32(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinFloat64(dst *partialResult4MaxMinFloat64) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.isNull = util.DeserializeBool(helper.bytes, helper.pos)
		dst.val = util.DeserializeFloat64(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinTime(dst *partialResult4MaxMinTime) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.isNull = util.DeserializeBool(helper.bytes, helper.pos)
		dst.val = util.DeserializeTime(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinDuration(dst *partialResult4MaxMinDuration) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.isNull = util.DeserializeBool(helper.bytes, helper.pos)
		dst.val = util.DeserializeTypesDuration(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinString(dst *partialResult4MaxMinString) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.isNull = util.DeserializeBool(helper.bytes, helper.pos)
		dst.val = util.DeserializeString(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinJSON(dst *partialResult4MaxMinJSON) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.isNull = util.DeserializeBool(helper.bytes, helper.pos)
		dst.val = util.DeserializeBinaryJSON(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinEnum(dst *partialResult4MaxMinEnum) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.isNull = util.DeserializeBool(helper.bytes, helper.pos)
		dst.val = util.DeserializeEnum(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4MaxMinSet(dst *partialResult4MaxMinSet) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.isNull = util.DeserializeBool(helper.bytes, helper.pos)
		dst.val = util.DeserializeSet(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4AvgDecimal(dst *partialResult4AvgDecimal) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.sum = util.DeserializeMyDecimal(helper.bytes, helper.pos)
		dst.count = util.DeserializeInt64(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4AvgFloat64(dst *partialResult4AvgFloat64) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.sum = util.DeserializeFloat64(helper.bytes, helper.pos)
		dst.count = util.DeserializeInt64(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4SumDecimal(dst *partialResult4SumDecimal) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.val = util.DeserializeMyDecimal(helper.bytes, helper.pos)
		dst.notNullRowCount = util.DeserializeInt64(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4SumFloat64(dst *partialResult4SumFloat64) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.val = util.DeserializeFloat64(helper.bytes, helper.pos)
		dst.notNullRowCount = util.DeserializeInt64(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializeBasePartialResult4GroupConcat(dst *basePartialResult4GroupConcat) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		dst.valsBuf = util.DeserializeBytesBuffer(helper.bytes, helper.pos)
		dst.buffer = util.DeserializeBytesBuffer(helper.bytes, helper.pos)
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
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		*dst = util.DeserializeUint64(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4JsonArrayagg(dst *partialResult4JsonArrayagg) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		byteNum := int64(len(helper.bytes))
		for *helper.pos < byteNum {
			value := util.DeserializeInterface(helper.bytes, helper.pos)
			dst.entries = append(dst.entries, value)
		}
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4JsonObjectAgg(dst *partialResult4JsonObjectAgg) (bool, int64) {
	helper := newPosAndBytes()
	memDelta := int64(0)
	dst.bInMap = 0
	dst.entries = make(map[string]interface{})
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		byteNum := int64(len(helper.bytes))
		for *helper.pos < byteNum {
			key := util.DeserializeString(helper.bytes, helper.pos)
			realVal := util.DeserializeInterface(helper.bytes, helper.pos)
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
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, helper.bytes, helper.pos)
		dst.val = util.DeserializeInt64(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowFloat32(dst *partialResult4FirstRowFloat32) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, helper.bytes, helper.pos)
		dst.val = util.DeserializeFloat32(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowFloat64(dst *partialResult4FirstRowFloat64) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, helper.bytes, helper.pos)
		dst.val = util.DeserializeFloat64(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowDecimal(dst *partialResult4FirstRowDecimal) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, helper.bytes, helper.pos)
		dst.val = util.DeserializeMyDecimal(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowString(dst *partialResult4FirstRowString) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, helper.bytes, helper.pos)
		dst.val = util.DeserializeString(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowTime(dst *partialResult4FirstRowTime) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, helper.bytes, helper.pos)
		dst.val = util.DeserializeTime(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowDuration(dst *partialResult4FirstRowDuration) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, helper.bytes, helper.pos)
		dst.val = util.DeserializeTypesDuration(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowJSON(dst *partialResult4FirstRowJSON) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, helper.bytes, helper.pos)
		dst.val = util.DeserializeBinaryJSON(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowEnum(dst *partialResult4FirstRowEnum) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, helper.bytes, helper.pos)
		dst.val = util.DeserializeEnum(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}

func (s *deserializeHelper) deserializePartialResult4FirstRowSet(dst *partialResult4FirstRowSet) bool {
	helper := newPosAndBytes()
	if s.readRowIndex < s.totalRowCnt {
		helper.prepare(s.column, s.readRowIndex)
		s.deserializeBasePartialResult4FirstRow(&dst.basePartialResult4FirstRow, helper.bytes, helper.pos)
		dst.val = util.DeserializeSet(helper.bytes, helper.pos)
		s.readRowIndex++
		return true
	}
	return false
}
