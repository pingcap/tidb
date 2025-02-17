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

package serialization

import (
	"bytes"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/types"
)

func serializeBuffer(value []byte, buf []byte) []byte {
	length := len(value)
	buf = SerializeInt(length, buf)
	return append(buf, value...)
}

// SerializeByte serializes byte type
func SerializeByte(value byte, buf []byte) []byte {
	return append(buf, value)
}

// SerializeBool serializes bool type
func SerializeBool(value bool, buf []byte) []byte {
	var tmp [BoolLen]byte
	*(*bool)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeInt serializes int type
func SerializeInt(value int, buf []byte) []byte {
	var tmp [IntLen]byte
	*(*int)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeInt8 serializes int8 type
func SerializeInt8(value int8, buf []byte) []byte {
	var tmp [Int8Len]byte
	*(*int8)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeUint8 serializes uint8 type
func SerializeUint8(value uint8, buf []byte) []byte {
	var tmp [Uint8Len]byte
	*(*uint8)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeInt32 serializes int32 type
func SerializeInt32(value int32, buf []byte) []byte {
	var tmp [Int32Len]byte
	*(*int32)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeUint32 serializes uint32 type
func SerializeUint32(value uint32, buf []byte) []byte {
	var tmp [Uint32Len]byte
	*(*uint32)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeUint64 serializes uint64 type
func SerializeUint64(value uint64, buf []byte) []byte {
	var tmp [Uint64Len]byte
	*(*uint64)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeInt64 serializes int64 type
func SerializeInt64(value int64, buf []byte) []byte {
	var tmp [Int64Len]byte
	*(*int64)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeFloat32 serializes float32 type
func SerializeFloat32(value float32, buf []byte) []byte {
	var tmp [Float32Len]byte
	*(*float32)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeFloat64 serializes float64 type
func SerializeFloat64(value float64, buf []byte) []byte {
	var tmp [Float64Len]byte
	*(*float64)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeMyDecimal serializes MyDecimal type
func SerializeMyDecimal(value *types.MyDecimal, buf []byte) []byte {
	var tmp [types.MyDecimalStructSize]byte
	*(*types.MyDecimal)(unsafe.Pointer(&tmp[0])) = *value
	return append(buf, tmp[:]...)
}

// SerializeTime serializes Time type
func SerializeTime(value types.Time, buf []byte) []byte {
	var tmp [TimeLen]byte
	*(*types.Time)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeGoTimeDuration serializes time.Duration type
func SerializeGoTimeDuration(value time.Duration, buf []byte) []byte {
	var tmp [TimeDurationLen]byte
	*(*time.Duration)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeTypesDuration serializes types.Duration type
func SerializeTypesDuration(value types.Duration, buf []byte) []byte {
	buf = SerializeGoTimeDuration(value.Duration, buf)
	return SerializeInt(value.Fsp, buf)
}

// SerializeJSONTypeCode serializes JSONTypeCode type
func SerializeJSONTypeCode(value types.JSONTypeCode, buf []byte) []byte {
	var tmp [JSONTypeCodeLen]byte
	*(*types.JSONTypeCode)(unsafe.Pointer(&tmp[0])) = value
	return append(buf, tmp[:]...)
}

// SerializeBinaryJSON serializes BinaryJSON type
func SerializeBinaryJSON(value *types.BinaryJSON, buf []byte) []byte {
	buf = SerializeByte(value.TypeCode, buf)
	return serializeBuffer(value.Value, buf)
}

// SerializeSet serializes Set type
func SerializeSet(value *types.Set, buf []byte) []byte {
	buf = SerializeUint64(value.Value, buf)
	return serializeBuffer([]byte(value.Name), buf)
}

// SerializeEnum serializes Enum type
func SerializeEnum(value *types.Enum, buf []byte) []byte {
	buf = SerializeUint64(value.Value, buf)
	return serializeBuffer([]byte(value.Name), buf)
}

// SerializeOpaque serializes Opaque type
func SerializeOpaque(value types.Opaque, buf []byte) []byte {
	buf = SerializeByte(value.TypeCode, buf)
	return serializeBuffer(value.Buf, buf)
}

// SerializeString serializes String type
func SerializeString(value string, buf []byte) []byte {
	return serializeBuffer([]byte(value), buf)
}

// SerializeBytesBuffer serializes bytes.Buffer type
func SerializeBytesBuffer(value *bytes.Buffer, buf []byte) []byte {
	bufferBytes := value.Bytes()
	return serializeBuffer(bufferBytes, buf)
}

// SerializeInterface serialize interface type
func SerializeInterface(value any, buf []byte) []byte {
	switch v := value.(type) {
	case bool:
		buf = append(buf, BoolType)
		return SerializeBool(v, buf)
	case int64:
		buf = append(buf, Int64Type)
		return SerializeInt64(v, buf)
	case uint64:
		buf = append(buf, Uint64Type)
		return SerializeUint64(v, buf)
	case float64:
		buf = append(buf, FloatType)
		return SerializeFloat64(v, buf)
	case string:
		buf = append(buf, StringType)
		return SerializeString(v, buf)
	case types.BinaryJSON:
		buf = append(buf, BinaryJSONType)
		return SerializeBinaryJSON(&v, buf)
	case types.Opaque:
		buf = append(buf, OpaqueType)
		return SerializeOpaque(v, buf)
	case types.Time:
		buf = append(buf, TimeType)
		return SerializeTime(v, buf)
	case types.Duration:
		buf = append(buf, DurationType)
		return SerializeTypesDuration(v, buf)
	default:
		panic("Agg spill encounters an unexpected interface type!")
	}
}
