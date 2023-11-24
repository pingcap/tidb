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
	"github.com/pingcap/tidb/pkg/util/spill"
)

// SpillSerializeHelper can only be used for one aggregator function.
// It may cause error if agg func1 and agg func2 use the same SpillSerializeHelper.
type SpillSerializeHelper struct {
	buf []byte
}

// NewSpillSerializeHelper creates a new SpillSerializeHelper
func NewSpillSerializeHelper() *SpillSerializeHelper {
	return &SpillSerializeHelper{
		buf: make([]byte, 1024),
	}
}

func (s *SpillSerializeHelper) serializePartialResult4Count(value partialResult4Count) []byte {
	s.buf = s.buf[:0]
	return spill.SerializeInt64(value, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinInt(value partialResult4MaxMinInt) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeBool(value.isNull, s.buf)
	return spill.SerializeInt64(value.val, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinUint(value partialResult4MaxMinUint) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeBool(value.isNull, s.buf)
	return spill.SerializeUint64(value.val, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinDecimal(value partialResult4MaxMinDecimal) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeBool(value.isNull, s.buf)
	return spill.SerializeMyDecimal(&value.val, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinFloat32(value partialResult4MaxMinFloat32) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeBool(value.isNull, s.buf)
	return spill.SerializeFloat32(value.val, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinFloat64(value partialResult4MaxMinFloat64) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeBool(value.isNull, s.buf)
	return spill.SerializeFloat64(value.val, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinTime(value partialResult4MaxMinTime) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeBool(value.isNull, s.buf)
	return spill.SerializeTime(value.val, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinDuration(value partialResult4MaxMinDuration) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeBool(value.isNull, s.buf)
	s.buf = spill.SerializeDuration(value.val.Duration, s.buf)
	return spill.SerializeInt(value.val.Fsp, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinString(value partialResult4MaxMinString) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeBool(value.isNull, s.buf)
	s.buf = append(s.buf, value.val...)
	return s.buf
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinJSON(value partialResult4MaxMinJSON) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeBool(value.isNull, s.buf)
	s.buf = spill.SerializeBinaryJSON(&value.val, s.buf)
	return s.buf
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinEnum(value partialResult4MaxMinEnum) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeBool(value.isNull, s.buf)
	s.buf = spill.SerializeEnum(&value.val, s.buf)
	return s.buf
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinSet(value partialResult4MaxMinSet) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeBool(value.isNull, s.buf)
	s.buf = spill.SerializeSet(&value.val, s.buf)
	return s.buf
}

func (s *SpillSerializeHelper) serializePartialResult4AvgDecimal(value partialResult4AvgDecimal) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeMyDecimal(&value.sum, s.buf)
	return spill.SerializeInt64(value.count, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4AvgFloat64(value partialResult4AvgFloat64) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeFloat64(value.sum, s.buf)
	return spill.SerializeInt64(value.count, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4SumDecimal(value partialResult4SumDecimal) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeMyDecimal(&value.val, s.buf)
	return spill.SerializeInt64(value.notNullRowCount, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4SumFloat64(value partialResult4SumFloat64) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeFloat64(value.val, s.buf)
	return spill.SerializeInt64(value.notNullRowCount, s.buf)
}

func (s *SpillSerializeHelper) serializeBasePartialResult4GroupConcat(value basePartialResult4GroupConcat) []byte {
	valsBuf := value.valsBuf.Bytes()
	valsBufLen := len(valsBuf)
	buffer := value.buffer.Bytes()
	bufferLen := len(buffer)

	s.buf = s.buf[:0]
	s.buf = spill.SerializeInt(valsBufLen, s.buf)
	s.buf = append(s.buf, valsBuf...)
	s.buf = spill.SerializeInt(bufferLen, s.buf)
	s.buf = append(s.buf, buffer...)
	return s.buf
}

func (s *SpillSerializeHelper) serializePartialResult4GroupConcat(value partialResult4GroupConcat) []byte {
	return s.serializeBasePartialResult4GroupConcat(basePartialResult4GroupConcat{
		valsBuf: value.valsBuf,
		buffer:  value.buffer,
	})
}

func (s *SpillSerializeHelper) serializePartialResult4BitFunc(value partialResult4BitFunc) []byte {
	return spill.SerializeUint64(value, s.buf[:0])
}

func (s *SpillSerializeHelper) serializePartialResult4JsonArrayagg(value partialResult4JsonArrayagg) []byte {
	s.buf = s.buf[:0]
	for _, value := range value.entries {
		s.buf = spill.SerializeInterface(value, s.buf)
	}
	return s.buf
}

func (s *SpillSerializeHelper) serializePartialResult4JsonObjectAgg(value partialResult4JsonObjectAgg) []byte {
	s.buf = s.buf[:0]
	for key, value := range value.entries {
		s.buf = spill.SerializeInt64(int64(len(key)), s.buf)
		s.buf = append(s.buf, key...)
		s.buf = spill.SerializeInterface(value, s.buf)
	}
	return s.buf
}

func (s *SpillSerializeHelper) serializeBasePartialResult4FirstRow(value basePartialResult4FirstRow) []byte {
	s.buf = s.buf[:0]
	s.buf = spill.SerializeBool(value.isNull, s.buf)
	return spill.SerializeBool(value.gotFirstRow, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowDecimal(value partialResult4FirstRowDecimal) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	return spill.SerializeMyDecimal(&value.val, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowInt(value partialResult4FirstRowInt) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	return spill.SerializeInt64(value.val, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowTime(value partialResult4FirstRowTime) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	return spill.SerializeTime(value.val, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowString(value partialResult4FirstRowString) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.buf = append(s.buf, value.val...)
	return s.buf
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowFloat32(value partialResult4FirstRowFloat32) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	return spill.SerializeFloat32(value.val, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowFloat64(value partialResult4FirstRowFloat64) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	return spill.SerializeFloat64(value.val, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowDuration(value partialResult4FirstRowDuration) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.buf = spill.SerializeInt64(int64(value.val.Duration), s.buf)
	return spill.SerializeInt(value.val.Fsp, s.buf)
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowJSON(value partialResult4FirstRowJSON) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.buf = spill.SerializeBinaryJSON(&value.val, s.buf)
	return s.buf
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowEnum(value partialResult4FirstRowEnum) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.buf = spill.SerializeEnum(&value.val, s.buf)
	return s.buf
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowSet(value partialResult4FirstRowSet) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.buf = spill.SerializeSet(&value.val, s.buf)
	return s.buf
}
