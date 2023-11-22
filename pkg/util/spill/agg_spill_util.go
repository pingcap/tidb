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
	"time"
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

	TypeLen     = int64(1)
	BoolLen     = int64(unsafe.Sizeof(true))
	ByteLen     = int64(unsafe.Sizeof(byte(0)))
	Int8Len     = int64(unsafe.Sizeof(int8(0)))
	Uint8Len    = int64(unsafe.Sizeof(uint8(0)))
	IntLen      = int64(unsafe.Sizeof(int(0)))
	Int32Len    = int64(unsafe.Sizeof(int32(0)))
	Uint32Len   = int64(unsafe.Sizeof(uint32(0)))
	Int64Len    = int64(unsafe.Sizeof(int64(0)))
	Uint64Len   = int64(unsafe.Sizeof(uint64(0)))
	Float32Len  = int64(unsafe.Sizeof(float32(0)))
	Float64Len  = int64(unsafe.Sizeof(float64(0)))
	TimeLen     = int64(unsafe.Sizeof(types.Time{}))
	DurationLen = int64(unsafe.Sizeof(time.Duration(0)))
)

// DeserializeBool deserializes bool type
func DeserializeBool(buf []byte, pos *int64) bool {
	retVal := *(*bool)(unsafe.Pointer(&buf[*pos]))
	*pos += BoolLen
	return retVal
}

// DeserializeInt deserializes int type
func DeserializeInt(buf []byte, pos *int64) int {
	retVal := *(*int)(unsafe.Pointer(&buf[*pos]))
	*pos += IntLen
	return retVal
}

// DeserializeInt8 deserializes int8 type
func DeserializeInt8(buf []byte, pos *int64) int8 {
	retVal := *(*int8)(unsafe.Pointer(&buf[*pos]))
	*pos += Int8Len
	return retVal
}

// DeserializeUint8 deserializes int8 type
func DeserializeUint8(buf []byte, pos *int64) uint8 {
	retVal := *(*uint8)(unsafe.Pointer(&buf[*pos]))
	*pos += Uint8Len
	return retVal
}

// DeserializeInt32 deserializes int32 type
func DeserializeInt32(buf []byte, pos *int64) int32 {
	retVal := *(*int32)(unsafe.Pointer(&buf[*pos]))
	*pos += Int32Len
	return retVal
}

// DeserializeUint32 deserializes uint32 type
func DeserializeUint32(buf []byte, pos *int64) uint32 {
	retVal := *(*uint32)(unsafe.Pointer(&buf[*pos]))
	*pos += Uint32Len
	return retVal
}

// DeserializeUint64 deserializes uint64 type
func DeserializeUint64(buf []byte, pos *int64) uint64 {
	retVal := *(*uint64)(unsafe.Pointer(&buf[*pos]))
	*pos += Uint64Len
	return retVal
}

// DeserializeInt64 deserializes int64 type
func DeserializeInt64(buf []byte, pos *int64) int64 {
	retVal := *(*int64)(unsafe.Pointer(&buf[*pos]))
	*pos += Int64Len
	return retVal
}

// DeserializeFloat32 deserializes float32 type
func DeserializeFloat32(buf []byte, pos *int64) float32 {
	retVal := *(*float32)(unsafe.Pointer(&buf[*pos]))
	*pos += Float32Len
	return retVal
}

// DeserializeFloat64 deserializes float64 type
func DeserializeFloat64(buf []byte, pos *int64) float64 {
	retVal := *(*float64)(unsafe.Pointer(&buf[*pos]))
	*pos += Float64Len
	return retVal
}

// DeserializeMyDecimal deserializes float64 type
func DeserializeMyDecimal(buf []byte, pos *int64) types.MyDecimal {
	retVal := *(*types.MyDecimal)(unsafe.Pointer(&buf[*pos]))
	*pos += types.MyDecimalStructSize
	return retVal
}

// DeserializeTime deserializes Time type
func DeserializeTime(buf []byte, pos *int64) types.Time {
	retVal := *(*types.Time)(unsafe.Pointer(&buf[*pos]))
	*pos += TimeLen
	return retVal
}

// DeserializeDuration deserializes Duration type
func DeserializeDuration(buf []byte, pos *int64) gotime.Duration {
	retVal := *(*gotime.Duration)(unsafe.Pointer(&buf[*pos]))
	*pos += DurationLen
	return retVal
}

// DeserializeInterface deserializes interface type
func DeserializeInterface(buf []byte, readPos *int64) interface{} {
	// Get type
	dataType := int(buf[*readPos])
	*readPos += TypeLen

	switch dataType {
	case BoolType:
		res := DeserializeBool(buf, readPos)
		return res
	case Int64Type:
		res := DeserializeInt64(buf, readPos)
		return res
	case Uint64Type:
		res := DeserializeUint64(buf, readPos)
		return res
	case FloatType:
		res := DeserializeFloat64(buf, readPos)
		return res
	case StringType:
		strLen := DeserializeInt64(buf, readPos)
		res := string(buf[*readPos : *readPos+strLen])
		*readPos += strLen
		return res
	case BinaryJSONType:
		retValue := DeserializeBinaryJSON(buf, readPos)
		return retValue
	case OpaqueType:
		typeCode := buf[*readPos]
		*readPos++
		valueLen := DeserializeInt64(buf, readPos)
		return types.Opaque{
			TypeCode: typeCode,
			Buf:      buf[*readPos : *readPos+valueLen],
		}
	case TimeType:
		coreTime := DeserializeUint64(buf, readPos)
		t := DeserializeUint8(buf, readPos)
		fsp := DeserializeInt(buf, readPos)
		return types.NewTime(types.CoreTime(coreTime), t, fsp)
	case DurationType:
		value := DeserializeInt64(buf, readPos)
		fsp := DeserializeInt(buf, readPos)
		return types.Duration{
			Duration: gotime.Duration(value),
			Fsp:      fsp,
		}
	default:
		panic("Invalid data type happens in agg spill deserializing!")
	}
}

// DeserializeBinaryJSON deserializes Set type and return the size of deserialized object
func DeserializeBinaryJSON(buf []byte, pos *int64) types.BinaryJSON {
	retValue := types.BinaryJSON{}
	retValue.TypeCode = buf[*pos]
	*pos += ByteLen
	jsonValueLen := DeserializeInt(buf, pos)
	retValue.Value = make([]byte, jsonValueLen)
	copy(retValue.Value, buf[*pos:*pos+int64(jsonValueLen)])
	*pos += int64(jsonValueLen)
	return retValue
}

// DeserializeSet deserializes Set type
func DeserializeSet(buf []byte, pos *int64) types.Set {
	retValue := types.Set{}
	retValue.Value = DeserializeUint64(buf, pos)
	retValue.Name = string(buf[*pos:])
	*pos += int64(len(retValue.Name))
	return retValue
}

// DeserializeEnum deserializes Set type
func DeserializeEnum(buf []byte, pos *int64) types.Enum {
	retValue := types.Enum{}
	retValue.Value = DeserializeUint64(buf, pos)
	retValue.Name = string(buf[*pos:])
	*pos += int64(len(retValue.Name))
	return retValue
}

// SerializeBool serializes bool type
func SerializeBool(value bool, buf []byte) []byte {
	var tmp [BoolLen]byte
	*(*bool)(unsafe.Pointer(&tmp[0])) = value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeInt serializes int type
func SerializeInt(value int, buf []byte) []byte {
	var tmp [IntLen]byte
	*(*int)(unsafe.Pointer(&tmp[0])) = value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeInt8 serializes int8 type
func SerializeInt8(value int8, buf []byte) []byte {
	var tmp [Int8Len]byte
	*(*int8)(unsafe.Pointer(&tmp[0])) = value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeUint8 serializes uint8 type
func SerializeUint8(value uint8, buf []byte) []byte {
	var tmp [Uint8Len]byte
	*(*uint8)(unsafe.Pointer(&tmp[0])) = value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeInt32 serializes int32 type
func SerializeInt32(value int32, buf []byte) []byte {
	var tmp [Int32Len]byte
	*(*int32)(unsafe.Pointer(&tmp[0])) = value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeUint32 serializes uint32 type
func SerializeUint32(value uint32, buf []byte) []byte {
	var tmp [Uint32Len]byte
	*(*uint32)(unsafe.Pointer(&tmp[0])) = value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeUint64 serializes uint64 type
func SerializeUint64(value uint64, buf []byte) []byte {
	var tmp [Uint64Len]byte
	*(*uint64)(unsafe.Pointer(&tmp[0])) = value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeInt64 serializes int64 type
func SerializeInt64(value int64, buf []byte) []byte {
	var tmp [Int64Len]byte
	*(*int64)(unsafe.Pointer(&tmp[0])) = value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeFloat32 serializes float32 type
func SerializeFloat32(value float32, buf []byte) []byte {
	var tmp [Float32Len]byte
	*(*float32)(unsafe.Pointer(&tmp[0])) = value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeFloat64 serializes float64 type
func SerializeFloat64(value float64, buf []byte) []byte {
	var tmp [Float64Len]byte
	*(*float64)(unsafe.Pointer(&tmp[0])) = value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeMyDecimal serializes MyDecimal type
func SerializeMyDecimal(value *types.MyDecimal, buf []byte) []byte {
	var tmp [types.MyDecimalStructSize]byte
	*(*types.MyDecimal)(unsafe.Pointer(&tmp[0])) = *value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeTime serializes Time type
func SerializeTime(value types.Time, buf []byte) []byte {
	var tmp [TimeLen]byte
	*(*types.Time)(unsafe.Pointer(&tmp[0])) = value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeDuration serializes Duration type
func SerializeDuration(value gotime.Duration, buf []byte) []byte {
	var tmp [DurationLen]byte
	*(*gotime.Duration)(unsafe.Pointer(&tmp[0])) = value
	buf = append(buf, tmp[:]...)
	return buf
}

// SerializeInterface serialize interface type and return the number of bytes serialized
func SerializeInterface(value interface{}, buf []byte) []byte {
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
		buf = SerializeInt64(int64(len(v)), buf)
		return append(buf, v...)
	case types.BinaryJSON:
		buf = append(buf, BinaryJSONType)
		return SerializeBinaryJSON(&v, buf)
	case types.Opaque:
		buf = append(buf, OpaqueType)
		buf = append(buf, v.TypeCode)
		buf = SerializeInt64(int64(len(v.Buf)), buf)
		return append(buf, v.Buf...)
	case types.Time:
		buf = append(buf, TimeType)
		buf = SerializeUint64(uint64(v.CoreTime()), buf)
		buf = SerializeUint8(v.Type(), buf)
		return SerializeInt(v.Fsp(), buf)
	case types.Duration:
		buf = append(buf, DurationType)
		buf = SerializeInt64(int64(v.Duration), buf)
		return SerializeInt(v.Fsp, buf)
	default:
		panic("Agg spill encounters an unexpected interface type!")
	}
}

// SerializeBinaryJSON serializes Set type
func SerializeBinaryJSON(value *types.BinaryJSON, buf []byte) []byte {
	buf = append(buf, value.TypeCode)
	buf = SerializeInt(len(value.Value), buf)
	return append(buf, value.Value...)
}

// SerializeSet serializes Set type
func SerializeSet(value *types.Set, buf []byte) []byte {
	buf = SerializeUint64(value.Value, buf)
	return append(buf, value.Name...)
}

// SerializeEnum serializes Set type
func SerializeEnum(value *types.Enum, buf []byte) []byte {
	buf = SerializeUint64(value.Value, buf)
	return append(buf, value.Name...)
}
