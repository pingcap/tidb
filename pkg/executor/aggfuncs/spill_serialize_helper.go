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

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/spill"
)

// spillSerializeHelper can only be used for one aggregator function.
// It may cause error if agg func1 and agg func2 use the same spillSerializeHelper.
type spillSerializeHelper struct {
	buf []byte
}

// newSpillSerializeHelper creates a new SpillSerializeHelper
func newSpillSerializeHelper() *spillSerializeHelper {
	return &spillSerializeHelper{
		buf: make([]byte, 1024),
	}
}

func (s *spillSerializeHelper) serializePartialResult4Count(value partialResult4Count) []byte {
	spill.SerializeInt64(value, s.buf[0:])
	return s.buf[:int64Len]
}

func (s *spillSerializeHelper) serializePartialResult4MaxMinInt(value partialResult4MaxMinInt) []byte {
	spill.SerializeInt64(value.val, s.buf[0:int64Len])
	end := int64Len + boolLen
	spill.SerializeBool(value.isNull, s.buf[int64Len:end])
	return s.buf[0:end]
}

func (s *spillSerializeHelper) serializePartialResult4MaxMinUint(value partialResult4MaxMinUint) []byte {
	spill.SerializeUint64(value.val, s.buf[0:uint64Len])
	end := uint64Len + boolLen
	spill.SerializeBool(value.isNull, s.buf[uint64Len:end])
	return s.buf[0:end]
}

func (s *spillSerializeHelper) serializePartialResult4MaxMinDecimal(value partialResult4MaxMinDecimal) []byte {
	*(*types.MyDecimal)(unsafe.Pointer(&s.buf[0])) = value.val
	end := types.MyDecimalStructSize + boolLen
	spill.SerializeBool(value.isNull, s.buf[types.MyDecimalStructSize:end])
	return s.buf[0:end]
}

func (s *spillSerializeHelper) serializePartialResult4MaxMinFloat32(value partialResult4MaxMinFloat32) []byte {
	spill.SerializeFloat32(value.val, s.buf[0:float32Len])
	end := float32Len + boolLen
	spill.SerializeBool(value.isNull, s.buf[float32Len:end])
	return s.buf[0:end]
}

func (s *spillSerializeHelper) serializePartialResult4MaxMinFloat64(value partialResult4MaxMinFloat64) []byte {
	spill.SerializeFloat64(value.val, s.buf[0:float64Len])
	end := float64Len + boolLen
	spill.SerializeBool(value.isNull, s.buf[float64Len:end])
	return s.buf[0:end]
}

func (s *spillSerializeHelper) serializePartialResult4MaxMinTime(value partialResult4MaxMinTime) []byte {
	*(*types.Time)(unsafe.Pointer(&s.buf[0])) = value.val
	end := timeLen + boolLen
	spill.SerializeBool(value.isNull, s.buf[timeLen:end])
	return s.buf[0:end]
}

func (s *spillSerializeHelper) serializePartialResult4MaxMinDuration(value partialResult4MaxMinDuration) []byte {
	spill.SerializeInt64(int64(value.val.Duration), s.buf[0:int64Len])
	spill.SerializeInt(value.val.Fsp, s.buf[int64Len:int64Len+intLen])
	end := int64Len + intLen + boolLen
	spill.SerializeBool(value.isNull, s.buf[int64Len+intLen:end])
	return s.buf[0:end]
}

func (s *spillSerializeHelper) serializePartialResult4MaxMinString(value partialResult4MaxMinString) []byte {
	spill.SerializeBool(value.isNull, s.buf[0:boolLen])
	s.buf = s.buf[:boolLen]
	s.buf = append(s.buf, value.val...)
	return s.buf
}

func (s *spillSerializeHelper) serializePartialResult4MaxMinJSON(value partialResult4MaxMinJSON) []byte {
	spill.SerializeBool(value.isNull, s.buf[0:])

	// Assign the return value to `s.buf` is necessary, so that the `s.buf` could expand it's capacity.
	s.buf = spill.SerializeBinaryJSON(&value.val, s.buf, boolLen)
	return s.buf
}

func (s *spillSerializeHelper) serializePartialResult4MaxMinEnum(value partialResult4MaxMinEnum) []byte {
	spill.SerializeBool(value.isNull, s.buf[0:])

	// Assign the return value to `s.buf` is necessary, so that the `s.buf` could expand it's capacity.
	s.buf = spill.SerializeEnum(&value.val, s.buf, boolLen)
	return s.buf
}

func (s *spillSerializeHelper) serializePartialResult4MaxMinSet(value partialResult4MaxMinSet) []byte {
	spill.SerializeBool(value.isNull, s.buf)
	s.buf = spill.SerializeSet(&value.val, s.buf, boolLen)
	return s.buf
}

func (s *spillSerializeHelper) serializePartialResult4AvgDecimal(value partialResult4AvgDecimal) []byte {
	*(*types.MyDecimal)(unsafe.Pointer(&s.buf[0])) = value.sum
	spill.SerializeInt64(value.count, s.buf[types.MyDecimalStructSize:])
	return s.buf[0 : types.MyDecimalStructSize+int64Len]
}

func (s *spillSerializeHelper) serializePartialResult4AvgFloat64(value partialResult4AvgFloat64) []byte {
	spill.SerializeFloat64(value.sum, s.buf[:])
	spill.SerializeInt64(value.count, s.buf[float64Len:])
	return s.buf[0 : float64Len+int64Len]
}

func (s *spillSerializeHelper) serializePartialResult4SumDecimal(value partialResult4SumDecimal) []byte {
	*(*types.MyDecimal)(unsafe.Pointer(&s.buf[0])) = value.val
	spill.SerializeInt64(value.notNullRowCount, s.buf[types.MyDecimalStructSize:])
	return s.buf[0 : types.MyDecimalStructSize+int64Len]
}

func (s *spillSerializeHelper) serializePartialResult4SumFloat64(value partialResult4SumFloat64) []byte {
	spill.SerializeFloat64(value.val, s.buf[:])
	spill.SerializeInt64(value.notNullRowCount, s.buf[float64Len:])
	return s.buf[0 : float64Len+int64Len]
}

func (s *spillSerializeHelper) serializeBasePartialResult4GroupConcat(value basePartialResult4GroupConcat) []byte {
	valsBuf := value.valsBuf.Bytes()
	valsBufLen := int64(len(valsBuf))
	buffer := value.buffer.Bytes()

	spill.SerializeInt64(valsBufLen, s.buf)
	s.buf = s.buf[:int64Len]
	s.buf = append(s.buf, valsBuf...)
	s.buf = append(s.buf, buffer...)
	return s.buf
}

func (s *spillSerializeHelper) serializePartialResult4GroupConcat(value partialResult4GroupConcat) []byte {
	return s.serializeBasePartialResult4GroupConcat(basePartialResult4GroupConcat{
		valsBuf: value.valsBuf,
		buffer:  value.buffer,
	})
}

func (s *spillSerializeHelper) serializePartialResult4BitFunc(value partialResult4BitFunc) []byte {
	spill.SerializeUint64(value, s.buf[:])
	return s.buf[0:uint64Len]
}

func (s *spillSerializeHelper) serializePartialResult4JsonArrayagg(value partialResult4JsonArrayagg) []byte {
	varBuf := make([]byte, 0)
	for _, value := range value.entries {
		spill.SerializeInterface(value, &varBuf, s.buf[:])
	}
	return varBuf
}

func (s *spillSerializeHelper) serializePartialResult4JsonObjectAgg(value partialResult4JsonObjectAgg) []byte {
	resBuf := make([]byte, 0)
	for key, value := range value.entries {
		spill.SerializeInt64(int64(len(key)), s.buf)
		resBuf = append(resBuf, s.buf[:int64Len]...)
		resBuf = append(resBuf, key...)
		spill.SerializeInterface(value, &resBuf, s.buf[:])
	}
	return resBuf
}

func (s *spillSerializeHelper) serializeBasePartialResult4FirstRow(value basePartialResult4FirstRow) ([]byte, int64) {
	spill.SerializeBool(value.isNull, s.buf[:])
	spill.SerializeBool(value.gotFirstRow, s.buf[1:])
	return s.buf[:2*boolLen], 2 * boolLen
}

func (s *spillSerializeHelper) serializePartialResult4FirstRowDecimal(value partialResult4FirstRowDecimal) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	*(*types.MyDecimal)(unsafe.Pointer(&s.buf[baseBytesNum])) = value.val
	return s.buf[:types.MyDecimalStructSize+baseBytesNum]
}

func (s *spillSerializeHelper) serializePartialResult4FirstRowInt(value partialResult4FirstRowInt) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	spill.SerializeInt64(value.val, s.buf[baseBytesNum:])
	return s.buf[:int64Len+baseBytesNum]
}

func (s *spillSerializeHelper) serializePartialResult4FirstRowTime(value partialResult4FirstRowTime) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	*(*types.Time)(unsafe.Pointer(&s.buf[baseBytesNum])) = value.val
	return s.buf[:timeLen+baseBytesNum]
}

func (s *spillSerializeHelper) serializePartialResult4FirstRowString(value partialResult4FirstRowString) []byte {
	resBuf, _ := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	resBuf = append(resBuf, value.val...)
	return resBuf
}

func (s *spillSerializeHelper) serializePartialResult4FirstRowFloat32(value partialResult4FirstRowFloat32) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	spill.SerializeFloat32(value.val, s.buf[baseBytesNum:])
	return s.buf[:float32Len+baseBytesNum]
}

func (s *spillSerializeHelper) serializePartialResult4FirstRowFloat64(value partialResult4FirstRowFloat64) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	spill.SerializeFloat64(value.val, s.buf[baseBytesNum:])
	return s.buf[:float64Len+baseBytesNum]
}

func (s *spillSerializeHelper) serializePartialResult4FirstRowDuration(value partialResult4FirstRowDuration) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	spill.SerializeInt64(int64(value.val.Duration), s.buf[baseBytesNum:])
	spill.SerializeInt(value.val.Fsp, s.buf[baseBytesNum+int64Len:int64Len+intLen])
	return s.buf[:int64Len+intLen+baseBytesNum]
}

func (s *spillSerializeHelper) serializePartialResult4FirstRowJSON(value partialResult4FirstRowJSON) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)

	// Assign the return value to `s.buf` is necessary, so that the `s.buf` could expand it's capacity.
	s.buf = spill.SerializeBinaryJSON(&value.val, s.buf, baseBytesNum)
	return s.buf
}

func (s *spillSerializeHelper) serializePartialResult4FirstRowEnum(value partialResult4FirstRowEnum) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.buf = spill.SerializeEnum(&value.val, s.buf, baseBytesNum)
	return s.buf
}

func (s *spillSerializeHelper) serializePartialResult4FirstRowSet(value partialResult4FirstRowSet) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.buf = spill.SerializeSet(&value.val, s.buf, baseBytesNum)
	return s.buf
}
