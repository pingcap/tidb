// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"unsafe"
)

// EncodeBool append bool to []byte.
func EncodeBool(result []byte, value bool) []byte {
	if value {
		result = append(result, uint8(1))
	} else {
		result = append(result, uint8(0))
	}
	return result
}

// EncodeUintptr append uintptr to []byte.
func EncodeUintptr(result []byte, value uintptr) []byte {
	size := unsafe.Sizeof(value)
	switch size {
	case 4:
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(value))
		return append(result, buf[:]...)
	case 8:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(value))
		return append(result, buf[:]...)
	default:
		panic(fmt.Sprintf("unknown uintptr size: %v", size))
	}
}

// Encode append array to []byte.
// size = Sizeof(len(array)) + len(array) * (Sizeof(len(array[i])) + Sizeof(array[i]))
//		= 4 + len(array) * (4 + Sizeof(array[i]))
func Encode(result []byte, hashCode func(index int) []byte, size int) []byte {
	result = EncodeIntAsUint32(result, size)
	for i := 0; i < size; i++ {
		hashCode := hashCode(i)
		result = EncodeIntAsUint32(result, len(hashCode))
		result = append(result, hashCode...)
	}
	return result
}

// EncodeAndSort sort and append array to []byte.
// size = Sizeof(len(array)) + len(array) * (Sizeof(len(array[i])) + Sizeof(array[i]))
//		= 4 + len(array) * (4 + Sizeof(array[i]))
func EncodeAndSort(result []byte, hashCode func(index int) []byte, size int) []byte {
	hashCodes := make([][]byte, size)
	for i := 0; i < size; i++ {
		hashCodes[i] = hashCode(i)
	}
	sort.Slice(hashCodes, func(i, j int) bool { return bytes.Compare(hashCodes[i], hashCodes[j]) < 0 })
	result = EncodeIntAsUint32(result, size)
	for _, hashCode := range hashCodes {
		result = EncodeIntAsUint32(result, len(hashCode))
		result = append(result, hashCode...)
	}
	return result
}
