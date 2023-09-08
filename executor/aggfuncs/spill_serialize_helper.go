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

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/spill"
)

type SpillSerializeHelper struct {
	// tmpBuf is an auxiliary data struct that used for encoding bytes.
	// 1024 is large enough for all fixed length data struct.
	tmpBuf [1024]byte
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
	end := int64Len + boolLen
	spill.SerializeBool(value.isNull, s.tmpBuf[int64Len:end])
	return s.tmpBuf[0:end]
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinString(value partialResult4MaxMinString) []byte {
	spill.SerializeBool(value.isNull, s.tmpBuf[0:boolLen])
	bytes := []byte(value.val)
	copy(s.tmpBuf[boolLen:], bytes)
	return s.tmpBuf[0 : boolLen+int64(len(bytes))]
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinJSON(value partialResult4MaxMinJSON) []byte {
	s.tmpBuf[0] = value.val.TypeCode
	spill.SerializeBool(value.isNull, s.tmpBuf[1:])
	copy(s.tmpBuf[1+boolLen:], value.val.Value)
	return s.tmpBuf[0 : 1+boolLen+int64(len(value.val.Value))]
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinEnum(value partialResult4MaxMinEnum) []byte {
	spill.SerializeUint64(value.val.Value, s.tmpBuf[0:])
	spill.SerializeBool(value.isNull, s.tmpBuf[uint64Len:])
	copy(s.tmpBuf[uint64Len+boolLen:], []byte(value.val.Name))
	return s.tmpBuf[0 : uint64Len+boolLen+int64(len(value.val.Name))]
}

func (s *SpillSerializeHelper) serializePartialResult4MaxMinSet(value partialResult4MaxMinSet) []byte {
	spill.SerializeUint64(value.val.Value, s.tmpBuf[0:])
	spill.SerializeBool(value.isNull, s.tmpBuf[uint64Len:])
	copy(s.tmpBuf[uint64Len+boolLen:], []byte(value.val.Name))
	return s.tmpBuf[0 : uint64Len+boolLen+int64(len(value.val.Name))]
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

// basePartialResult4GroupConcat
// partialResult4GroupConcat
// type basePartialResult4GroupConcat struct {
// 	valsBuf *bytes.Buffer
// 	buffer  *bytes.Buffer
// }
// type Buffer struct {
// 	buf      []byte // contents are the bytes buf[off : len(buf)]
// 	off      int    // read at &buf[off], write at &buf[len(buf)]
// 	lastRead readOp // last read operation, so that Unread* can work correctly. int8
// }

func (s *SpillSerializeHelper) serializeBasePartialResult4GroupConcat(value basePartialResult4GroupConcat) []byte {
	// totalMemLen := 2 * intLen + 2 * int8Len + len(value.valsBuf.)
}
