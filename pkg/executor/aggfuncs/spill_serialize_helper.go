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

type SpillSerializeHelper struct {
	// tmpBuf is an auxiliary data struct that used for encoding bytes.
	// 1024 is large enough for all fixed length data struct.
	tmpBuf [1024]byte

	// varBuf is used for storing variable length data struct
	varBuf []byte
}

func (s *SpillSerializeHelper) serializePartialResult4Count(value partialResult4Count) []byte {
	return spill.SerializeInt64(int64(value), s.tmpBuf[0:int64Len])
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinInt(value partialResult4MaxMinInt) []byte {
	spill.SerializeInt64(value.val, s.tmpBuf[0:int64Len])
	end := int64Len + boolLen
	spill.SerializeBool(value.isNull, s.tmpBuf[int64Len:end])
	return s.tmpBuf[0:end]
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinUint(value partialResult4MaxMinUint) []byte {
	spill.SerializeUint64(value.val, s.tmpBuf[0:uint64Len])
	end := uint64Len + boolLen
	spill.SerializeBool(value.isNull, s.tmpBuf[uint64Len:end])
	return s.tmpBuf[0:end]
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinDecimal(value partialResult4MaxMinDecimal) []byte {
	*(*types.MyDecimal)(unsafe.Pointer(&s.tmpBuf[0])) = value.val
	end := types.MyDecimalStructSize + boolLen
	spill.SerializeBool(value.isNull, s.tmpBuf[types.MyDecimalStructSize:end])
	return s.tmpBuf[0:end]
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinFloat32(value partialResult4MaxMinFloat32) []byte {
	spill.SerializeFloat32(value.val, s.tmpBuf[0:float32Len])
	end := float32Len + boolLen
	spill.SerializeBool(value.isNull, s.tmpBuf[float32Len:end])
	return s.tmpBuf[0:end]
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinFloat64(value partialResult4MaxMinFloat64) []byte {
	spill.SerializeFloat64(value.val, s.tmpBuf[0:float64Len])
	end := float64Len + boolLen
	spill.SerializeBool(value.isNull, s.tmpBuf[float64Len:end])
	return s.tmpBuf[0:end]
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinTime(value partialResult4MaxMinTime) []byte {
	*(*types.Time)(unsafe.Pointer(&s.tmpBuf[0])) = value.val
	end := timeLen + boolLen
	spill.SerializeBool(value.isNull, s.tmpBuf[timeLen:end])
	return s.tmpBuf[0:end]
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinDuration(value partialResult4MaxMinDuration) []byte {
	spill.SerializeInt64(int64(value.val.Duration), s.tmpBuf[0:int64Len])
	spill.SerializeInt(value.val.Fsp, s.tmpBuf[int64Len:int64Len+intLen])
	end := int64Len + intLen + boolLen
	spill.SerializeBool(value.isNull, s.tmpBuf[int64Len+intLen:end])
	return s.tmpBuf[0:end]
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinString(value partialResult4MaxMinString) []byte {
	spill.SerializeBool(value.isNull, s.tmpBuf[0:boolLen])
	resBuf := s.tmpBuf[:1]
	resBuf = append(resBuf, value.val...)
	return resBuf
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinJSON(value partialResult4MaxMinJSON) []byte {
	s.tmpBuf[0] = value.val.TypeCode
	spill.SerializeBool(value.isNull, s.tmpBuf[1:])
	resBuf := s.tmpBuf[:2]
	resBuf = append(resBuf, value.val.Value...)
	return resBuf
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinEnum(value partialResult4MaxMinEnum) []byte {
	spill.SerializeUint64(value.val.Value, s.tmpBuf[0:])
	spill.SerializeBool(value.isNull, s.tmpBuf[uint64Len:])
	resBuf := s.tmpBuf[:uint64Len+boolLen]
	resBuf = append(resBuf, value.val.Name...)
	return resBuf
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinSet(value partialResult4MaxMinSet) []byte {
	spill.SerializeUint64(value.val.Value, s.tmpBuf[0:])
	spill.SerializeBool(value.isNull, s.tmpBuf[uint64Len:])
	resBuf := s.tmpBuf[:uint64Len+boolLen]
	resBuf = append(resBuf, value.val.Name...)
	return resBuf
}

func (s *SpillSerializeHelper) serializePartialResult4AvgDecimal(value partialResult4AvgDecimal) []byte {
	*(*types.MyDecimal)(unsafe.Pointer(&s.tmpBuf[0])) = value.sum
	spill.SerializeInt64(value.count, s.tmpBuf[types.MyDecimalStructSize:])
	return s.tmpBuf[0 : types.MyDecimalStructSize+int64Len]
}

func (s *SpillSerializeHelper) serializePartialResult4AvgFloat64(value partialResult4AvgFloat64) []byte {
	spill.SerializeFloat64(value.sum, s.tmpBuf[:])
	spill.SerializeInt64(value.count, s.tmpBuf[float64Len:])
	return s.tmpBuf[0 : float64Len+int64Len]
}

func (s *SpillSerializeHelper) serializePartialResult4SumDecimal(value partialResult4SumDecimal) []byte {
	*(*types.MyDecimal)(unsafe.Pointer(&s.tmpBuf[0])) = value.val
	spill.SerializeInt64(value.notNullRowCount, s.tmpBuf[types.MyDecimalStructSize:])
	return s.tmpBuf[0 : types.MyDecimalStructSize+int64Len]
}

func (s *SpillSerializeHelper) serializePartialResult4SumFloat64(value partialResult4SumFloat64) []byte {
	spill.SerializeFloat64(value.val, s.tmpBuf[:])
	spill.SerializeInt64(value.notNullRowCount, s.tmpBuf[float64Len:])
	return s.tmpBuf[0 : float64Len+int64Len]
}

func (s *SpillSerializeHelper) serializeBasePartialResult4GroupConcat(value basePartialResult4GroupConcat) []byte {
	valsBuf := value.valsBuf.Bytes()
	valsBufLen := int64(len(valsBuf))
	buffer := value.buffer.Bytes()
	bufferLen := int64(len(buffer))
	dataLen := valsBufLen + bufferLen + int64Len
	if dataLen > int64(len(s.varBuf)) {
		s.varBuf = make([]byte, dataLen)
	}

	spill.SerializeInt64(valsBufLen, s.varBuf)
	copy(s.varBuf[int64Len:], valsBuf)
	copy(s.varBuf[int64Len+valsBufLen:], buffer)
	return s.varBuf[:dataLen]
}

func (s *SpillSerializeHelper) serializePartialResult4GroupConcat(value partialResult4GroupConcat) []byte {
	return s.serializeBasePartialResult4GroupConcat(basePartialResult4GroupConcat{
		valsBuf: value.valsBuf,
		buffer:  value.buffer,
	})
}

func (s *SpillSerializeHelper) serializePartialResult4BitFunc(value partialResult4BitFunc) []byte {
	spill.SerializeUint64(value, s.tmpBuf[:])
	return s.tmpBuf[0:uint64Len]
}

func (s *SpillSerializeHelper) serializePartialResult4JsonArrayagg(value partialResult4JsonArrayagg) []byte {
	varBuf := make([]byte, 0)
	for _, value := range value.entries {
		spill.SerializeInterface(value, &varBuf, s.tmpBuf[:])
	}
	return varBuf
}

func (s *SpillSerializeHelper) serializePartialResult4JsonObjectAgg(value partialResult4JsonObjectAgg) []byte {
	resBuf := make([]byte, 0)
	for key, value := range value.entries {
		tmpBuf := spill.SerializeInt64(int64(len(key)), s.tmpBuf[:])
		resBuf = append(resBuf, tmpBuf...)
		resBuf = append(resBuf, key...)
		spill.SerializeInterface(value, &resBuf, s.tmpBuf[:])
	}
	return resBuf
}

func (s *SpillSerializeHelper) serializeBasePartialResult4FirstRow(value basePartialResult4FirstRow) ([]byte, int64) {
	spill.SerializeBool(value.isNull, s.tmpBuf[:])
	spill.SerializeBool(value.gotFirstRow, s.tmpBuf[1:])
	return s.tmpBuf[:2], 2
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowDecimal(value partialResult4FirstRowDecimal) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	*(*types.MyDecimal)(unsafe.Pointer(&s.tmpBuf[baseBytesNum])) = value.val
	return s.tmpBuf[:types.MyDecimalStructSize+baseBytesNum]
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowInt(value partialResult4FirstRowInt) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	spill.SerializeInt64(value.val, s.tmpBuf[baseBytesNum:])
	return s.tmpBuf[:int64Len+baseBytesNum]
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowTime(value partialResult4FirstRowTime) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	*(*types.Time)(unsafe.Pointer(&s.tmpBuf[baseBytesNum])) = value.val
	return s.tmpBuf[:timeLen+baseBytesNum]
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowString(value partialResult4FirstRowString) []byte {
	resBuf, _ := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	resBuf = append(resBuf, value.val...)
	return resBuf
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowFloat32(value partialResult4FirstRowFloat32) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	spill.SerializeFloat32(value.val, s.tmpBuf[baseBytesNum:])
	return s.tmpBuf[:float32Len+baseBytesNum]
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowFloat64(value partialResult4FirstRowFloat64) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	spill.SerializeFloat64(value.val, s.tmpBuf[baseBytesNum:])
	return s.tmpBuf[:float64Len+baseBytesNum]
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowDuration(value partialResult4FirstRowDuration) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	spill.SerializeInt64(int64(value.val.Duration), s.tmpBuf[baseBytesNum:])
	spill.SerializeInt(value.val.Fsp, s.tmpBuf[baseBytesNum+int64Len:int64Len+intLen])
	return s.tmpBuf[:int64Len+intLen+baseBytesNum]
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowJSON(value partialResult4FirstRowJSON) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	s.tmpBuf[baseBytesNum] = value.val.TypeCode
	totalLen := baseBytesNum + 1 + int64(len(value.val.Value))
	if int64(len(s.varBuf)) < totalLen {
		s.varBuf = make([]byte, totalLen)
	}
	copy(s.varBuf, s.tmpBuf[:baseBytesNum+2])
	copy(s.varBuf[baseBytesNum+1:], value.val.Value)
	return s.varBuf[:totalLen]
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowEnum(value partialResult4FirstRowEnum) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	spill.SerializeUint64(value.val.Value, s.tmpBuf[baseBytesNum:])
	resBuf := s.tmpBuf[:8+baseBytesNum]
	resBuf = append(resBuf, value.val.Name...)
	return resBuf
}

func (s *SpillSerializeHelper) serializePartialResult4FirstRowSet(value partialResult4FirstRowSet) []byte {
	_, baseBytesNum := s.serializeBasePartialResult4FirstRow(value.basePartialResult4FirstRow)
	spill.SerializeUint64(value.val.Value, s.tmpBuf[baseBytesNum:])
	resBuf := s.tmpBuf[:8+baseBytesNum]
	resBuf = append(resBuf, value.val.Name...)
	return resBuf
}
