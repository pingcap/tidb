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

package spill

import (
	gotime "time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

var (
	// ErrInternal is an error for spill
	ErrInternal = dbterror.ClassOptimizer.NewStd(mysql.ErrInternal)
)

// These types are used for serializing or deserializing interface type
const (
	BoolType = iota
	Int64Type
	Uint64Type
	FloatType
	StringType
	BinaryJSONType
	OpaqueType
	TimeType
	DurationType

	typeLen    = int64(1)
	boolLen    = int64(unsafe.Sizeof(true))
	byteLen    = int64(unsafe.Sizeof(byte(0)))
	uint8Len   = int64(unsafe.Sizeof(uint8(0)))
	intLen     = int64(unsafe.Sizeof(int(0)))
	int64Len   = int64(unsafe.Sizeof(int64(0)))
	uint64Len  = int64(unsafe.Sizeof(uint64(0)))
	float64Len = int64(unsafe.Sizeof(float64(0)))
)

// DeserializeBool deserializes bool type
func DeserializeBool(buf []byte, pos int64) bool {
	return *(*bool)(unsafe.Pointer(&buf[pos]))
}

// DeserializeInt deserializes int type
func DeserializeInt(buf []byte, pos int64) int {
	return *(*int)(unsafe.Pointer(&buf[pos]))
}

// DeserializeInt8 deserializes int8 type
func DeserializeInt8(buf []byte, pos int64) int8 {
	return *(*int8)(unsafe.Pointer(&buf[pos]))
}

// DeserializeUint8 deserializes int8 type
func DeserializeUint8(buf []byte, pos int64) uint8 {
	return *(*uint8)(unsafe.Pointer(&buf[pos]))
}

// DeserializeInt32 deserializes int32 type
func DeserializeInt32(buf []byte, pos int64) int32 {
	return *(*int32)(unsafe.Pointer(&buf[pos]))
}

// DeserializeUint32 deserializes uint32 type
func DeserializeUint32(buf []byte, pos int64) uint32 {
	return *(*uint32)(unsafe.Pointer(&buf[pos]))
}

// DeserializeUint64 deserializes uint64 type
func DeserializeUint64(buf []byte, pos int64) uint64 {
	return *(*uint64)(unsafe.Pointer(&buf[pos]))
}

// DeserializeInt64 deserializes int64 type
func DeserializeInt64(buf []byte, pos int64) int64 {
	return *(*int64)(unsafe.Pointer(&buf[pos]))
}

// DeserializeFloat32 deserializes float32 type
func DeserializeFloat32(buf []byte, pos int64) float32 {
	return *(*float32)(unsafe.Pointer(&buf[pos]))
}

// DeserializeFloat64 deserializes float64 type
func DeserializeFloat64(buf []byte, pos int64) float64 {
	return *(*float64)(unsafe.Pointer(&buf[pos]))
}

// DeserializeInterface deserializes interface type and return the new readPos
func DeserializeInterface(buf []byte, readPos int64) (interface{}, int64) {
	// Get type
	dataType := int(buf[readPos])
	readPos += typeLen

	switch dataType {
	case BoolType:
		res := int(buf[readPos])
		readPos += boolLen
		if res == 0 {
			return false, readPos
		} else if res == 1 {
			return true, readPos
		} else {
			panic("Invalid value happens when deserializing agg spill data!")
		}
	case Int64Type:
		res := DeserializeInt64(buf, readPos)
		readPos += int64Len
		return res, readPos
	case Uint64Type:
		res := DeserializeUint64(buf, readPos)
		readPos += uint64Len
		return res, readPos
	case FloatType:
		res := DeserializeFloat64(buf, readPos)
		readPos += float64Len
		return res, readPos
	case StringType:
		strLen := DeserializeInt64(buf, readPos)
		readPos += int64Len
		res := string(buf[readPos : readPos+strLen])
		readPos += strLen
		return res, readPos
	case BinaryJSONType:
		retValue, deserializedByteNum := DeserializeBinaryJSON(buf, readPos)
		readPos += deserializedByteNum
		return retValue, readPos
	case OpaqueType:
		typeCode := buf[readPos]
		readPos++
		valueLen := DeserializeInt64(buf, readPos)
		readPos += int64Len
		return types.Opaque{
			TypeCode: typeCode,
			Buf:      buf[readPos : readPos+valueLen],
		}, readPos + valueLen
	case TimeType:
		coreTime := DeserializeUint64(buf, readPos)
		readPos += uint64Len
		t := DeserializeUint8(buf, readPos)
		readPos += uint8Len
		fsp := DeserializeInt(buf, readPos)
		readPos += intLen
		return types.NewTime(types.CoreTime(coreTime), t, fsp), readPos
	case DurationType:
		value := DeserializeInt64(buf, readPos)
		readPos += int64Len
		fsp := DeserializeInt(buf, readPos)
		readPos += intLen
		return types.Duration{
			Duration: gotime.Duration(value),
			Fsp:      fsp,
		}, readPos
	default:
		panic("Invalid data type happens in agg spill deserializing!")
	}

}

// DeserializeBinaryJSON deserializes Set type
func DeserializeBinaryJSON(buf []byte, pos int64) (types.BinaryJSON, int64) {
	retValue := types.BinaryJSON{}
	retValue.TypeCode = buf[pos]
	pos += byteLen
	jsonValueLen := DeserializeInt(buf, pos)
	pos += intLen
	retValue.Value = make([]byte, jsonValueLen)
	copy(retValue.Value, buf[pos:pos+int64(jsonValueLen)])
	return retValue, byteLen + intLen + int64(jsonValueLen)
}

// DeserializeSet deserializes Set type
//
// Commonly, function should return the deserialized bytes for variable length type.
// However, `Set` type is always deserialized with fix length types.
// So, there is no need to return deserialized bytes so far.
func DeserializeSet(buf []byte, pos int64) types.Set {
	retValue := types.Set{}
	retValue.Value = DeserializeUint64(buf, pos)
	retValue.Name = string(buf[pos+uint64Len:])
	return retValue
}

// DeserializeEnum deserializes Set type
//
// Commonly, function should return the deserialized bytes for variable length type.
// However, `Enum` type is always deserialized with fix length types.
// So, there is no need to return deserialized bytes so far.
func DeserializeEnum(buf []byte, pos int64) types.Enum {
	retValue := types.Enum{}
	retValue.Value = DeserializeUint64(buf, pos)
	retValue.Name = string(buf[pos+uint64Len:])
	return retValue
}

// SerializeBool serializes bool type
func SerializeBool(value bool, tmpBuf []byte) {
	*(*bool)(unsafe.Pointer(&tmpBuf[0])) = value
}

// SerializeInt serializes int type
func SerializeInt(value int, tmpBuf []byte) {
	*(*int)(unsafe.Pointer(&tmpBuf[0])) = value
}

// SerializeInt8 serializes int8 type
func SerializeInt8(value int8, tmpBuf []byte) {
	*(*int8)(unsafe.Pointer(&tmpBuf[0])) = value
}

// SerializeUint8 serializes uint8 type
func SerializeUint8(value uint8, tmpBuf []byte) {
	*(*uint8)(unsafe.Pointer(&tmpBuf[0])) = value
}

// SerializeInt32 serializes int32 type
func SerializeInt32(value int32, tmpBuf []byte) {
	*(*int32)(unsafe.Pointer(&tmpBuf[0])) = value
}

// SerializeUint32 serializes uint32 type
func SerializeUint32(value uint32, tmpBuf []byte) {
	*(*uint32)(unsafe.Pointer(&tmpBuf[0])) = value
}

// SerializeUint64 serializes uint64 type
func SerializeUint64(value uint64, tmpBuf []byte) {
	*(*uint64)(unsafe.Pointer(&tmpBuf[0])) = value
}

// SerializeInt64 serializes int64 type
func SerializeInt64(value int64, tmpBuf []byte) {
	*(*int64)(unsafe.Pointer(&tmpBuf[0])) = value
}

// SerializeFloat32 serializes float32 type
func SerializeFloat32(value float32, tmpBuf []byte) {
	*(*float32)(unsafe.Pointer(&tmpBuf[0])) = value
}

// SerializeFloat64 serializes float64 type
func SerializeFloat64(value float64, tmpBuf []byte) {
	*(*float64)(unsafe.Pointer(&tmpBuf[0])) = value
}

// SerializeInterface serialize interface type and return the number of bytes serialized
func SerializeInterface(value interface{}, varBuf *[]byte, tmpBuf []byte) {
	encodedBytesNum := typeLen

	switch v := value.(type) {
	case bool:
		*varBuf = append(*varBuf, BoolType)
		if v {
			*varBuf = append(*varBuf, byte(1))
		} else {
			*varBuf = append(*varBuf, byte(0))
		}
		encodedBytesNum++
	case int64:
		*varBuf = append(*varBuf, Int64Type)
		SerializeInt64(v, tmpBuf)
		*varBuf = append(*varBuf, tmpBuf[:int64Len]...)
		encodedBytesNum += int64Len
	case uint64:
		*varBuf = append(*varBuf, Uint64Type)
		SerializeUint64(v, tmpBuf)
		*varBuf = append(*varBuf, tmpBuf[:uint64Len]...)
		encodedBytesNum += uint64Len
	case float64:
		*varBuf = append(*varBuf, FloatType)
		SerializeFloat64(v, tmpBuf)
		*varBuf = append(*varBuf, tmpBuf[:float64Len]...)
		encodedBytesNum += float64Len
	case string:
		*varBuf = append(*varBuf, StringType)
		vLen := int64(len(v))
		SerializeInt64(vLen, tmpBuf)
		*varBuf = append(*varBuf, tmpBuf[:int64Len]...)
		*varBuf = append(*varBuf, v...)
		encodedBytesNum += vLen + int64Len
	case types.BinaryJSON:
		*varBuf = append(*varBuf, BinaryJSONType)
		varBufLenBeforeSerializeJSON := int64(len(*varBuf))

		// Add padding for seialization
		*varBuf = append(*varBuf, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
		*varBuf = SerializeBinaryJSON(&v, *varBuf, varBufLenBeforeSerializeJSON)

		encodedBytesNum += int64(len(*varBuf)) - varBufLenBeforeSerializeJSON
	case types.Opaque:
		*varBuf = append(*varBuf, OpaqueType)
		bufLen := int64(len(v.Buf))
		*varBuf = append(*varBuf, v.TypeCode)
		SerializeInt64(int64(len(v.Buf)), tmpBuf)
		*varBuf = append(*varBuf, tmpBuf[:int64Len]...)
		*varBuf = append(*varBuf, v.Buf...)
		encodedBytesNum += bufLen + typeLen + int64Len
	case types.Time:
		*varBuf = append(*varBuf, TimeType)
		SerializeUint64(uint64(v.CoreTime()), tmpBuf)
		*varBuf = append(*varBuf, tmpBuf[:uint64Len]...)
		SerializeUint8(v.Type(), tmpBuf)
		*varBuf = append(*varBuf, tmpBuf[:uint8Len]...)
		SerializeInt(v.Fsp(), tmpBuf)
		*varBuf = append(*varBuf, tmpBuf[:intLen]...)
		encodedBytesNum += uint64Len + uint8Len + intLen
	case types.Duration:
		*varBuf = append(*varBuf, DurationType)
		SerializeInt64(int64(v.Duration), tmpBuf)
		*varBuf = append(*varBuf, tmpBuf[:int64Len]...)
		SerializeInt(v.Fsp, tmpBuf)
		*varBuf = append(*varBuf, tmpBuf[:intLen]...)
		encodedBytesNum += int64Len + intLen
	default:
		panic("Agg spill encounters an unexpected interface type!")
	}
}

// SerializeBinaryJSON serializes Set type
func SerializeBinaryJSON(json *types.BinaryJSON, varBuf []byte, startPos int64) []byte {
	varBuf[startPos] = json.TypeCode
	valueLen := len(json.Value)
	SerializeInt(valueLen, varBuf[startPos+byteLen:])
	varBuf = varBuf[:startPos+byteLen+intLen]
	return append(varBuf, json.Value...)
}

// SerializeSet serializes Set type
//
// Commonly, function should return the serialized bytes for variable length type.
// However, `Set` type is always serialized with fix length types.
// So, there is no need to return serialized bytes so far.
func SerializeSet(value *types.Set, varBuf []byte, startPos int64) []byte {
	SerializeUint64(value.Value, varBuf[startPos:])
	varBuf = varBuf[:startPos+uint64Len]
	return append(varBuf, value.Name...)
}

// SerializeEnum serializes Set type
//
// Commonly, function should return the serialized bytes for variable length type.
// However, `Enum` type is always serialized with fix length types.
// So, there is no need to return serialized bytes so far.
func SerializeEnum(value *types.Enum, varBuf []byte, startPos int64) []byte {
	SerializeUint64(value.Value, varBuf[startPos:])
	varBuf = varBuf[:startPos+uint64Len]
	return append(varBuf, value.Name...)
}
