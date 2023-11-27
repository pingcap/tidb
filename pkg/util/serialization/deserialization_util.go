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

func deserializeLength(buf []byte, pos *int64) int {
	return DeserializeInt(buf, pos)
}

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

// DeserializeBinaryJSON deserializes BinaryJSON type
func DeserializeBinaryJSON(buf []byte, pos *int64) types.BinaryJSON {
	retValue := types.BinaryJSON{}
	retValue.TypeCode = DeserializeJSONTypeCode(buf, pos)
	jsonValueLen := deserializeLength(buf, pos)
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
	retValBufLen := deserializeLength(buf, pos)
	retVal.Buf = make([]byte, retValBufLen)
	copy(retVal.Buf, buf[*pos:*pos+int64(retValBufLen)])
	*pos += int64(retValBufLen)
	return retVal
}

// DeserializeString deserializes String type
func DeserializeString(buf []byte, pos *int64) string {
	strLen := deserializeLength(buf, pos)
	retVal := string(buf[*pos : *pos+int64(strLen)])
	*pos += int64(strLen)
	return retVal
}

// DeserializeBytesBuffer deserializes bytes.Buffer type
func DeserializeBytesBuffer(buf []byte, pos *int64) *bytes.Buffer {
	bufLen := deserializeLength(buf, pos)
	tmp := make([]byte, bufLen)
	copy(tmp, buf[*pos:*pos+int64(bufLen)])
	*pos += int64(bufLen)
	return bytes.NewBuffer(tmp)
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
