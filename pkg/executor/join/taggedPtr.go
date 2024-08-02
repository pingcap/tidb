// Copyright 2024 PingCAP, Inc.
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

package join

import (
	"unsafe"
)

// taggedPtr is a struct to save unsafe.Pointer with tagged value
// the value of unsafe.Pointer is actually an uint64 and in most
// cases, the n MSB in unsafe.Pointer is all zeros, which means we
// can save information in the n-MSB of a unsafe.Pointer.
// for example, if the 16 MSB in unsafe.Pointer is all zeros,
// then the value of unsafe.Pointer is like 0x0000xxxxxxxxxxxx
// we can save up to 16 bit information inside the unsafe.Pointer
// Assuming the tagged value is 0x1010, then the taggedPtr will
// be 0x1010xxxxxxxxxxxx
// However, we can not save the tagged value into unsafe.Pointer
// directly since go gc will check the validation of unsafe.Pointer,
// so in implementation, we save both the tagged value and the unsafe.Pointer
// into an uintptr.
type taggedPtr uintptr

type tagPtrHelper struct {
	// mask to get the tagged value from hashValue
	hashValueTaggedMask uint64
	// offset to convert the tagged value from hashValue to the taggedValue in taggedPtr
	hashValueTaggedOffset uint8
	// mask to get the tagged value in taggedPtr
	taggedMask uint64
}

func (th *tagPtrHelper) init(taggedBits uint8) {
	th.hashValueTaggedMask = (1 << taggedBits) - 1
	th.hashValueTaggedOffset = 64 - taggedBits
	th.taggedMask = th.hashValueTaggedMask << th.hashValueTaggedOffset
}

func (th *tagPtrHelper) getTaggedValueFromHashValue(hashValue uint64) uint64 {
	return (hashValue >> 32) & th.hashValueTaggedMask << uint64(th.hashValueTaggedOffset)
}

func (th *tagPtrHelper) getTaggedValueFromTaggedPtr(taggedPtr taggedPtr) uint64 {
	return uint64(taggedPtr) & th.taggedMask
}

func (th *tagPtrHelper) toTaggedPtr(taggedValue uint64, ptr unsafe.Pointer) taggedPtr {
	ret := taggedPtr(0)
	// first save ptr into taggedPtr
	*(*unsafe.Pointer)(unsafe.Pointer(&ret)) = ptr
	// tag the value
	ret = taggedPtr(uint64(ret) | taggedValue)
	return ret
}

func (th *tagPtrHelper) toUnsafePointer(tPtr taggedPtr) unsafe.Pointer {
	// clear the tagged value
	tPtr = taggedPtr(uint64(tPtr) & ^th.taggedMask)
	return *(*unsafe.Pointer)(unsafe.Pointer(&tPtr))
}

func getTaggedBitsFromUnsafePointer(up unsafe.Pointer) uint8 {
	if sizeOfUintptr != 8 {
		return 0
	}
	p := taggedPtr(0)
	*(*unsafe.Pointer)(unsafe.Pointer(&p)) = up
	initMask := uint64(0xffffffffffff)
	taggedBits := int8(16)
	pValue := uint64(p)
	for taggedBits > 0 {
		if pValue & ^initMask == 0 {
			return uint8(taggedBits)
		}
		taggedBits--
		initMask = (initMask << 1) + 1
	}
	return 0
}
