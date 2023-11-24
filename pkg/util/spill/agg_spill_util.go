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

	InterfaceTypeCodeLen = int64(1)
	JSONTypeCodeLen      = int64(types.JSONTypeCode(1))
	BoolLen              = int64(unsafe.Sizeof(true))
	ByteLen              = int64(unsafe.Sizeof(byte(0)))
	Int8Len              = int64(unsafe.Sizeof(int8(0)))
	Uint8Len             = int64(unsafe.Sizeof(uint8(0)))
	IntLen               = int64(unsafe.Sizeof(int(0)))
	Int32Len             = int64(unsafe.Sizeof(int32(0)))
	Uint32Len            = int64(unsafe.Sizeof(uint32(0)))
	Int64Len             = int64(unsafe.Sizeof(int64(0)))
	Uint64Len            = int64(unsafe.Sizeof(uint64(0)))
	Float32Len           = int64(unsafe.Sizeof(float32(0)))
	Float64Len           = int64(unsafe.Sizeof(float64(0)))
	TimeLen              = int64(unsafe.Sizeof(types.Time{}))
	TimeDurationLen      = int64(unsafe.Sizeof(time.Duration(0)))
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

// DeserializeMyDecimal deserializes MyDecimal type
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

// DeserializeTimeDuration deserializes time.Duration type
func DeserializeTimeDuration(buf []byte, pos *int64) time.Duration {
	retVal := *(*time.Duration)(unsafe.Pointer(&buf[*pos]))
	*pos += TimeDurationLen
	return retVal
}

// DeserializeTypesDuration deserializes types.Duration type
func DeserializeTypesDuration(buf []byte, pos *int64) types.Duration {
	retVal := types.Duration{}
	retVal.Duration = DeserializeTimeDuration(buf, pos)
	retVal.Fsp = DeserializeInt(buf, pos)
	return retVal
}

// DeserializeJSONTypeCode deserializes JSONTypeCode type
func DeserializeJSONTypeCode(buf []byte, pos *int64) types.JSONTypeCode {
	retVal := *(*types.JSONTypeCode)(unsafe.Pointer(&buf[*pos]))
	*pos += JSONTypeCodeLen
	return retVal
}

// DeserializeInterface deserializes interface type
func DeserializeInterface(buf []byte, pos *int64) interface{} {
	// Get type
	dataType := int(buf[*pos])
	*pos += InterfaceTypeCodeLen

	switch dataType {
	case BoolType:
		return DeserializeBool(buf, pos)
	case Int64Type:
		return DeserializeInt64(buf, pos)
	case Uint64Type:
		return DeserializeUint64(buf, pos)
	case FloatType:
		return DeserializeFloat64(buf, pos)
	case StringType:
		return DeserializeString(buf, pos)
	case BinaryJSONType:
		return DeserializeBinaryJSON(buf, pos)
	case OpaqueType:
		return DeserializeOpaque(buf, pos)
	case TimeType:
		return DeserializeTime(buf, pos)
	case DurationType:
		return DeserializeTypesDuration(buf, pos)
	default:
		panic("Invalid data type happens in agg spill deserializing!")
	}
}

// DeserializeBinaryJSON deserializes BinaryJSON type
func DeserializeBinaryJSON(buf []byte, pos *int64) types.BinaryJSON {
	retValue := types.BinaryJSON{}
	retValue.TypeCode = DeserializeJSONTypeCode(buf, pos)
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
	retValue.Name = DeserializeString(buf, pos)
	return retValue
}

// DeserializeEnum deserializes Enum type
func DeserializeEnum(buf []byte, pos *int64) types.Enum {
	retValue := types.Enum{}
	retValue.Value = DeserializeUint64(buf, pos)
	retValue.Name = DeserializeString(buf, pos)
	return retValue
}

// DeserializeOpaque deserializes Opaque type
func DeserializeOpaque(buf []byte, pos *int64) types.Opaque {
	retVal := types.Opaque{}
	retVal.TypeCode = buf[*pos]
	*pos++
	retValBufLen := DeserializeInt(buf, pos)
	retVal.Buf = make([]byte, retValBufLen)
	copy(retVal.Buf, buf[*pos:*pos+int64(retValBufLen)])
	*pos += int64(retValBufLen)
	return retVal
}

// DeserializeString deserializes String type
func DeserializeString(buf []byte, pos *int64) string {
	strLen := DeserializeInt(buf, pos)
	retVal := string(buf[*pos : *pos+int64(strLen)])
	*pos += int64(strLen)
	return retVal
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

// SerializeInterface serialize interface type
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

// SerializeBinaryJSON serializes BinaryJSON type
func SerializeBinaryJSON(value *types.BinaryJSON, buf []byte) []byte {
	buf = append(buf, value.TypeCode)
	buf = SerializeInt(len(value.Value), buf)
	return append(buf, value.Value...)
}

// SerializeSet serializes Set type
func SerializeSet(value *types.Set, buf []byte) []byte {
	buf = SerializeUint64(value.Value, buf)
	buf = SerializeInt(len(value.Name), buf)
	return append(buf, value.Name...)
}

// SerializeEnum serializes Enum type
func SerializeEnum(value *types.Enum, buf []byte) []byte {
	buf = SerializeUint64(value.Value, buf)
	buf = SerializeInt(len(value.Name), buf)
	return append(buf, value.Name...)
}

// SerializeOpaque serializes Opaque type
func SerializeOpaque(value types.Opaque, buf []byte) []byte {
	buf = append(buf, value.TypeCode)
	buf = SerializeInt(len(value.Buf), buf)
	return append(buf, value.Buf...)
}

// SerializeString serializes String type
func SerializeString(value string, buf []byte) []byte {
	strLen := len(value)
	buf = SerializeInt(strLen, buf)
	return append(buf, value...)
}
