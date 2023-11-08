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
	"github.com/pingcap/tidb/pkg/util/hack"
)

var (
	// ErrInternal is an error for spill
	ErrInternal = dbterror.ClassOptimizer.NewStd(mysql.ErrInternal)
)

// These types are used for serializing or deserializing interface type
const (
	BoolType       = 0
	Int64Type      = 1
	Uint64Type     = 2
	FloatType      = 3
	StringType     = 4
	BinaryJSONType = 5
	OpaqueType     = 6
	TimeType       = 7
	DurationType   = 8

	intLen = int64(unsafe.Sizeof(int(0)))
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
	readPos++

	switch dataType {
	case BoolType:
		res := int(buf[readPos])
		readPos++
		if res == 0 {
			return false, readPos
		} else if res == 1 {
			return true, readPos
		} else {
			panic("Invalid value happens when deserializing agg spill data!")
		}
	case Int64Type:
		res := DeserializeInt64(buf, readPos)
		readPos += 8
		return res, readPos
	case Uint64Type:
		res := DeserializeUint64(buf, readPos)
		readPos += 8
		return res, readPos
	case FloatType:
		res := DeserializeFloat64(buf, readPos)
		readPos += 8
		return res, readPos
	case StringType:
		strLen := DeserializeInt64(buf, readPos)
		readPos += 8
		res := string(hack.String(buf[readPos : readPos+strLen]))
		readPos += strLen
		return res, readPos
	case BinaryJSONType:
		typeCode := buf[readPos]
		readPos++
		valueLen := DeserializeInt64(buf, readPos)
		readPos += 8
		return types.BinaryJSON{
			TypeCode: typeCode,
			Value:    buf[readPos : readPos+valueLen],
		}, readPos + valueLen
	case OpaqueType:
		typeCode := buf[readPos]
		readPos++
		valueLen := DeserializeInt64(buf, readPos)
		readPos += 8
		return types.Opaque{
			TypeCode: typeCode,
			Buf:      buf[readPos : readPos+valueLen],
		}, readPos + valueLen
	case TimeType:
		coreTime := DeserializeUint64(buf, readPos)
		readPos += 8
		t := DeserializeUint8(buf, readPos)
		readPos++
		fsp := DeserializeInt(buf, readPos)
		readPos += intLen
		return types.NewTime(types.CoreTime(coreTime), t, fsp), readPos
	case DurationType:
		value := DeserializeInt64(buf, readPos)
		readPos += 8
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

// SerializeBool serializes bool type
func SerializeBool(value bool, tmpBuf []byte) []byte {
	*(*bool)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf[:1]
}

// SerializeInt serializes int type
func SerializeInt(value int, tmpBuf []byte) []byte {
	*(*int)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf[:intLen]
}

// SerializeInt8 serializes int8 type
func SerializeInt8(value int8, tmpBuf []byte) []byte {
	*(*int8)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf[:1]
}

// SerializeUint8 serializes uint8 type
func SerializeUint8(value uint8, tmpBuf []byte) []byte {
	*(*uint8)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf[:1]
}

// SerializeInt32 serializes int32 type
func SerializeInt32(value int32, tmpBuf []byte) []byte {
	*(*int32)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf[:4]
}

// SerializeUint32 serializes uint32 type
func SerializeUint32(value uint32, tmpBuf []byte) []byte {
	*(*uint32)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf[:4]
}

// SerializeUint64 serializes uint64 type
func SerializeUint64(value uint64, tmpBuf []byte) []byte {
	*(*uint64)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf[:8]
}

// SerializeInt64 serializes int64 type
func SerializeInt64(value int64, tmpBuf []byte) []byte {
	*(*int64)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf[:8]
}

// SerializeFloat32 serializes float32 type
func SerializeFloat32(value float32, tmpBuf []byte) []byte {
	*(*float32)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf[:4]
}

// SerializeFloat64 serializes float64 type
func SerializeFloat64(value float64, tmpBuf []byte) []byte {
	*(*float64)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf[:8]
}

// SerializeInterface serialize interface type and return the number of bytes serialized
func SerializeInterface(value interface{}, varBuf *[]byte, tmpBuf []byte) int64 {
	// Data type always occupies 1 byte
	encodedBytesNum := int64(1)

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
		*varBuf = append(*varBuf, SerializeInt64(v, tmpBuf)...)
		encodedBytesNum += 8
	case uint64:
		*varBuf = append(*varBuf, Uint64Type)
		*varBuf = append(*varBuf, SerializeUint64(v, tmpBuf)...)
		encodedBytesNum += 8
	case float64:
		*varBuf = append(*varBuf, FloatType)
		*varBuf = append(*varBuf, SerializeFloat64(v, tmpBuf)...)
		encodedBytesNum += 8
	case string:
		*varBuf = append(*varBuf, StringType)
		vLen := int64(len(v))
		*varBuf = append(*varBuf, SerializeInt64(vLen, tmpBuf)...)
		*varBuf = append(*varBuf, v...)
		encodedBytesNum += vLen + 8
	case types.BinaryJSON:
		*varBuf = append(*varBuf, BinaryJSONType)
		valueLen := int64(len(v.Value))
		*varBuf = append(*varBuf, v.TypeCode)
		*varBuf = append(*varBuf, SerializeInt64(int64(len(v.Value)), tmpBuf)...)
		*varBuf = append(*varBuf, v.Value...)
		encodedBytesNum += valueLen + 1 + 8
	case types.Opaque:
		*varBuf = append(*varBuf, OpaqueType)
		bufLen := int64(len(v.Buf))
		*varBuf = append(*varBuf, v.TypeCode)
		*varBuf = append(*varBuf, SerializeInt64(int64(len(v.Buf)), tmpBuf)...)
		*varBuf = append(*varBuf, v.Buf...)
		encodedBytesNum += bufLen + 1 + 8
	case types.Time:
		*varBuf = append(*varBuf, TimeType)
		*varBuf = append(*varBuf, SerializeUint64(uint64(v.CoreTime()), tmpBuf)...)
		*varBuf = append(*varBuf, SerializeUint8(v.Type(), tmpBuf)...)
		*varBuf = append(*varBuf, SerializeInt(v.Fsp(), tmpBuf)...)
		encodedBytesNum += 8 + 1 + intLen
	case types.Duration:
		*varBuf = append(*varBuf, DurationType)
		*varBuf = append(*varBuf, SerializeInt64(int64(v.Duration), tmpBuf)...)
		*varBuf = append(*varBuf, SerializeInt(v.Fsp, tmpBuf)...)
		encodedBytesNum += 8 + intLen
	default:
		panic("Agg spill encounters an unexpected interface type!")
	}

	return encodedBytesNum
}
