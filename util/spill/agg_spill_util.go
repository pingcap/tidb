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
	"unsafe"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/dbterror"
)

var (
	ErrInternal = dbterror.ClassOptimizer.NewStd(mysql.ErrInternal)
)

// DeserializeBool deserializes bool type
func DeserializeBool(buf []byte, pos int64) bool {
	return *(*bool)(unsafe.Pointer(&buf[pos]))
}

// DeserializeBool deserializes int8 type
func DeserializeInt8(buf []byte, pos int64) int8 {
	return *(*int8)(unsafe.Pointer(&buf[pos]))
}

// DeserializeBool deserializes int32 type
func DeserializeInt32(buf []byte, pos int64) int32 {
	return *(*int32)(unsafe.Pointer(&buf[pos]))
}

// DeserializeBool deserializes uint32 type
func DeserializeUint32(buf []byte, pos int64) uint32 {
	return *(*uint32)(unsafe.Pointer(&buf[pos]))
}

// DeserializeBool deserializes uint64 type
func DeserializeUint64(buf []byte, pos int64) uint64 {
	return *(*uint64)(unsafe.Pointer(&buf[pos]))
}

// DeserializeBool deserializes int64 type
func DeserializeInt64(buf []byte, pos int64) int64 {
	return *(*int64)(unsafe.Pointer(&buf[pos]))
}

// DeserializeBool deserializes float32 type
func DeserializeFloat32(buf []byte, pos int64) float32 {
	return *(*float32)(unsafe.Pointer(&buf[pos]))
}

// DeserializeBool deserializes float64 type
func DeserializeFloat64(buf []byte, pos int64) float64 {
	return *(*float64)(unsafe.Pointer(&buf[pos]))
}

// SerializeBool serializes bool type
func SerializeBool(value bool, tmpBuf []byte) []byte {
	*(*bool)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf
}

// SerializeBool serializes int8 type
func SerializeInt8(value int8, tmpBuf []byte) []byte {
	*(*int8)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf
}

// SerializeBool serializes int32 type
func SerializeInt32(value int32, tmpBuf []byte) []byte {
	*(*int32)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf
}

// SerializeBool serializes uint32 type
func SerializeUint32(value uint32, tmpBuf []byte) []byte {
	*(*uint32)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf
}

// SerializeBool serializes uint64 type
func SerializeUint64(value uint64, tmpBuf []byte) []byte {
	*(*uint64)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf
}

// SerializeBool serializes int64 type
func SerializeInt64(value int64, tmpBuf []byte) []byte {
	*(*int64)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf
}

// SerializeBool serializes float32 type
func SerializeFloat32(value float32, tmpBuf []byte) []byte {
	*(*float32)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf
}

// SerializeBool serializes float64 type
func SerializeFloat64(value float64, tmpBuf []byte) []byte {
	*(*float64)(unsafe.Pointer(&tmpBuf[0])) = value
	return tmpBuf
}
