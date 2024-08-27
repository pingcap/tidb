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
	util "github.com/pingcap/tidb/pkg/util/serialization"
)

// SerializeHelper is the helper for serializing agg function meta data.
type SerializeHelper struct {
	buf []byte
}

// NewSerializeHelper creates a new SerializeHelper
func NewSerializeHelper() *SerializeHelper {
	return &SerializeHelper{
		buf: make([]byte, 0, 64),
	}
}

func (s *SerializeHelper) serializePartialResult4Count(value partialResult4Count) []byte {
	s.buf = s.buf[:0]
	return util.SerializeInt64(value, s.buf)
}

func (s *SerializeHelper) serializePartialResult4MaxMinInt(value partialResult4MaxMinInt) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBool(value.isNull, s.buf)
	return util.SerializeInt64(value.val, s.buf)
}

func (s *SerializeHelper) serializePartialResult4MaxMinUint(value partialResult4MaxMinUint) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBool(value.isNull, s.buf)
	return util.SerializeUint64(value.val, s.buf)
}

func (s *SerializeHelper) serializePartialResult4MaxMinDecimal(value partialResult4MaxMinDecimal) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBool(value.isNull, s.buf)
	return util.SerializeMyDecimal(&value.val, s.buf)
}

func (s *SerializeHelper) serializePartialResult4MaxMinFloat32(value partialResult4MaxMinFloat32) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBool(value.isNull, s.buf)
	return util.SerializeFloat32(value.val, s.buf)
}

func (s *SerializeHelper) serializePartialResult4MaxMinFloat64(value partialResult4MaxMinFloat64) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBool(value.isNull, s.buf)
	return util.SerializeFloat64(value.val, s.buf)
}

func (s *SerializeHelper) serializePartialResult4MaxMinTime(value partialResult4MaxMinTime) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBool(value.isNull, s.buf)
	return util.SerializeTime(value.val, s.buf)
}

func (s *SerializeHelper) serializePartialResult4MaxMinDuration(value partialResult4MaxMinDuration) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBool(value.isNull, s.buf)
	return util.SerializeTypesDuration(value.val, s.buf)
}

func (s *SerializeHelper) serializePartialResult4MaxMinString(value partialResult4MaxMinString) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBool(value.isNull, s.buf)
	s.buf = util.SerializeString(value.val, s.buf)
	return s.buf
}

func (s *SerializeHelper) serializePartialResult4MaxMinJSON(value partialResult4MaxMinJSON) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBool(value.isNull, s.buf)
	s.buf = util.SerializeBinaryJSON(&value.val, s.buf)
	return s.buf
}

func (s *SerializeHelper) serializePartialResult4MaxMinEnum(value partialResult4MaxMinEnum) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBool(value.isNull, s.buf)
	s.buf = util.SerializeEnum(&value.val, s.buf)
	return s.buf
}

func (s *SerializeHelper) serializePartialResult4MaxMinSet(value partialResult4MaxMinSet) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBool(value.isNull, s.buf)
	s.buf = util.SerializeSet(&value.val, s.buf)
	return s.buf
}

func (s *SerializeHelper) serializePartialResult4AvgDecimal(value partialResult4AvgDecimal) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeMyDecimal(&value.sum, s.buf)
	return util.SerializeInt64(value.count, s.buf)
}

func (s *SerializeHelper) serializePartialResult4AvgFloat64(value partialResult4AvgFloat64) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeFloat64(value.sum, s.buf)
	return util.SerializeInt64(value.count, s.buf)
}

func (s *SerializeHelper) serializePartialResult4SumDecimal(value partialResult4SumDecimal) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeMyDecimal(&value.val, s.buf)
	return util.SerializeInt64(value.notNullRowCount, s.buf)
}

func (s *SerializeHelper) serializePartialResult4SumFloat64(value partialResult4SumFloat64) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeFloat64(value.val, s.buf)
	return util.SerializeInt64(value.notNullRowCount, s.buf)
}

func (s *SerializeHelper) serializeBasePartialResult4GroupConcat(value basePartialResult4GroupConcat) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBytesBuffer(value.valsBuf, s.buf)
	s.buf = util.SerializeBytesBuffer(value.buffer, s.buf)
	return s.buf
}

func (s *SerializeHelper) serializePartialResult4GroupConcat(value partialResult4GroupConcat) []byte {
	return s.serializeBasePartialResult4GroupConcat(basePartialResult4GroupConcat{
		valsBuf: value.valsBuf,
		buffer:  value.buffer,
	})
}

func (s *SerializeHelper) serializePartialResult4BitFunc(value partialResult4BitFunc) []byte {
	return util.SerializeUint64(value, s.buf[:0])
}

func (s *SerializeHelper) serializePartialResult4JsonArrayagg(value partialResult4JsonArrayagg) []byte {
	s.buf = s.buf[:0]
	for _, value := range value.entries {
		s.buf = util.SerializeInterface(value, s.buf)
	}
	return s.buf
}

func (s *SerializeHelper) serializePartialResult4JsonObjectAgg(value partialResult4JsonObjectAgg) []byte {
	s.buf = s.buf[:0]
	for key, value := range value.entries {
		s.buf = util.SerializeString(key, s.buf)
		s.buf = util.SerializeInterface(value, s.buf)
	}
	return s.buf
}

func (s *SerializeHelper) serializeBasePartialResult4FirstRow(value basePartialResult4FirstRow) []byte {
	s.buf = s.buf[:0]
	s.buf = util.SerializeBool(value.isNull, s.buf)
	return util.SerializeBool(value.gotFirstRow, s.buf)
}

func (s *SerializeHelper) serializePartialResult4FirstRowDecimal(value partialResult4FirstRowDecimal) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	return util.SerializeMyDecimal(&value.val, s.buf)
}

func (s *SerializeHelper) serializePartialResult4FirstRowInt(value partialResult4FirstRowInt) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	return util.SerializeInt64(value.val, s.buf)
}

func (s *SerializeHelper) serializePartialResult4FirstRowTime(value partialResult4FirstRowTime) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	return util.SerializeTime(value.val, s.buf)
}

func (s *SerializeHelper) serializePartialResult4FirstRowString(value partialResult4FirstRowString) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.buf = util.SerializeString(value.val, s.buf)
	return s.buf
}

func (s *SerializeHelper) serializePartialResult4FirstRowFloat32(value partialResult4FirstRowFloat32) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	return util.SerializeFloat32(value.val, s.buf)
}

func (s *SerializeHelper) serializePartialResult4FirstRowFloat64(value partialResult4FirstRowFloat64) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	return util.SerializeFloat64(value.val, s.buf)
}

func (s *SerializeHelper) serializePartialResult4FirstRowDuration(value partialResult4FirstRowDuration) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.buf = util.SerializeInt64(int64(value.val.Duration), s.buf)
	return util.SerializeInt(value.val.Fsp, s.buf)
}

func (s *SerializeHelper) serializePartialResult4FirstRowJSON(value partialResult4FirstRowJSON) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.buf = util.SerializeBinaryJSON(&value.val, s.buf)
	return s.buf
}

func (s *SerializeHelper) serializePartialResult4FirstRowEnum(value partialResult4FirstRowEnum) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.buf = util.SerializeEnum(&value.val, s.buf)
	return s.buf
}

func (s *SerializeHelper) serializePartialResult4FirstRowSet(value partialResult4FirstRowSet) []byte {
	s.buf = s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.buf = util.SerializeSet(&value.val, s.buf)
	return s.buf
}
