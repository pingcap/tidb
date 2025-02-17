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
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// PosAndBuf is the parameter of all DeserializeXXX functions
type PosAndBuf struct {
	Buf []byte
	Pos int64
}

// Reset resets data in PosAndBuf
func (p *PosAndBuf) Reset(col *chunk.Column, idx int) {
	p.Buf = col.GetBytes(idx)
	p.Pos = 0
}

func deserializeBuffer(posAndBuf *PosAndBuf) []byte {
	bufLen := DeserializeInt(posAndBuf) // Get buffer length
	retVal := posAndBuf.Buf[posAndBuf.Pos : posAndBuf.Pos+int64(bufLen)]
	posAndBuf.Pos += int64(bufLen)
	return retVal
}

// DeserializeByte deserializes byte type
func DeserializeByte(posAndBuf *PosAndBuf) byte {
	retVal := posAndBuf.Buf[posAndBuf.Pos]
	posAndBuf.Pos++
	return retVal
}

// DeserializeBool deserializes bool type
func DeserializeBool(posAndBuf *PosAndBuf) bool {
	retVal := *(*bool)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += BoolLen
	return retVal
}

// DeserializeInt deserializes int type
func DeserializeInt(posAndBuf *PosAndBuf) int {
	retVal := *(*int)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += IntLen
	return retVal
}

// DeserializeInt8 deserializes int8 type
func DeserializeInt8(posAndBuf *PosAndBuf) int8 {
	retVal := *(*int8)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += Int8Len
	return retVal
}

// DeserializeUint8 deserializes int8 type
func DeserializeUint8(posAndBuf *PosAndBuf) uint8 {
	retVal := *(*uint8)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += Uint8Len
	return retVal
}

// DeserializeInt32 deserializes int32 type
func DeserializeInt32(posAndBuf *PosAndBuf) int32 {
	retVal := *(*int32)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += Int32Len
	return retVal
}

// DeserializeUint32 deserializes uint32 type
func DeserializeUint32(posAndBuf *PosAndBuf) uint32 {
	retVal := *(*uint32)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += Uint32Len
	return retVal
}

// DeserializeUint64 deserializes uint64 type
func DeserializeUint64(posAndBuf *PosAndBuf) uint64 {
	retVal := *(*uint64)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += Uint64Len
	return retVal
}

// DeserializeInt64 deserializes int64 type
func DeserializeInt64(posAndBuf *PosAndBuf) int64 {
	retVal := *(*int64)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += Int64Len
	return retVal
}

// DeserializeFloat32 deserializes float32 type
func DeserializeFloat32(posAndBuf *PosAndBuf) float32 {
	retVal := *(*float32)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += Float32Len
	return retVal
}

// DeserializeFloat64 deserializes float64 type
func DeserializeFloat64(posAndBuf *PosAndBuf) float64 {
	retVal := *(*float64)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += Float64Len
	return retVal
}

// DeserializeMyDecimal deserializes MyDecimal type
func DeserializeMyDecimal(posAndBuf *PosAndBuf) types.MyDecimal {
	retVal := *(*types.MyDecimal)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += types.MyDecimalStructSize
	return retVal
}

// DeserializeTime deserializes Time type
func DeserializeTime(posAndBuf *PosAndBuf) types.Time {
	retVal := *(*types.Time)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += TimeLen
	return retVal
}

// DeserializeTimeDuration deserializes time.Duration type
func DeserializeTimeDuration(posAndBuf *PosAndBuf) time.Duration {
	retVal := *(*time.Duration)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += TimeDurationLen
	return retVal
}

// DeserializeTypesDuration deserializes types.Duration type
func DeserializeTypesDuration(posAndBuf *PosAndBuf) types.Duration {
	retVal := types.Duration{}
	retVal.Duration = DeserializeTimeDuration(posAndBuf)
	retVal.Fsp = DeserializeInt(posAndBuf)
	return retVal
}

// DeserializeJSONTypeCode deserializes JSONTypeCode type
func DeserializeJSONTypeCode(posAndBuf *PosAndBuf) types.JSONTypeCode {
	retVal := *(*types.JSONTypeCode)(unsafe.Pointer(&posAndBuf.Buf[posAndBuf.Pos]))
	posAndBuf.Pos += JSONTypeCodeLen
	return retVal
}

// DeserializeBinaryJSON deserializes BinaryJSON type
func DeserializeBinaryJSON(posAndBuf *PosAndBuf) types.BinaryJSON {
	retValue := types.BinaryJSON{}
	retValue.TypeCode = DeserializeJSONTypeCode(posAndBuf)
	buf := deserializeBuffer(posAndBuf)
	retValue.Value = make([]byte, len(buf))
	copy(retValue.Value, buf)
	return retValue
}

// DeserializeSet deserializes Set type
func DeserializeSet(posAndBuf *PosAndBuf) types.Set {
	retValue := types.Set{}
	retValue.Value = DeserializeUint64(posAndBuf)
	retValue.Name = DeserializeString(posAndBuf)
	return retValue
}

// DeserializeEnum deserializes Enum type
func DeserializeEnum(posAndBuf *PosAndBuf) types.Enum {
	retValue := types.Enum{}
	retValue.Value = DeserializeUint64(posAndBuf)
	retValue.Name = DeserializeString(posAndBuf)
	return retValue
}

// DeserializeOpaque deserializes Opaque type
func DeserializeOpaque(posAndBuf *PosAndBuf) types.Opaque {
	retVal := types.Opaque{}
	retVal.TypeCode = DeserializeByte(posAndBuf)
	buf := deserializeBuffer(posAndBuf)
	retVal.Buf = make([]byte, len(buf))
	copy(retVal.Buf, buf)
	return retVal
}

// DeserializeString deserializes String type
func DeserializeString(posAndBuf *PosAndBuf) string {
	buf := deserializeBuffer(posAndBuf)
	return string(buf)
}

// DeserializeBytesBuffer deserializes bytes.Buffer type
func DeserializeBytesBuffer(posAndBuf *PosAndBuf) *bytes.Buffer {
	buf := deserializeBuffer(posAndBuf)
	tmp := make([]byte, len(buf))
	copy(tmp, buf)
	return bytes.NewBuffer(tmp)
}

// DeserializeInterface deserializes interface type
func DeserializeInterface(posAndBuf *PosAndBuf) any {
	// Get type
	dataType := int(posAndBuf.Buf[posAndBuf.Pos])
	posAndBuf.Pos += InterfaceTypeCodeLen

	switch dataType {
	case BoolType:
		return DeserializeBool(posAndBuf)
	case Int64Type:
		return DeserializeInt64(posAndBuf)
	case Uint64Type:
		return DeserializeUint64(posAndBuf)
	case FloatType:
		return DeserializeFloat64(posAndBuf)
	case StringType:
		return DeserializeString(posAndBuf)
	case BinaryJSONType:
		return DeserializeBinaryJSON(posAndBuf)
	case OpaqueType:
		return DeserializeOpaque(posAndBuf)
	case TimeType:
		return DeserializeTime(posAndBuf)
	case DurationType:
		return DeserializeTypesDuration(posAndBuf)
	default:
		panic("Invalid data type happens in agg spill deserializing!")
	}
}
