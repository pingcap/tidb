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
	"math/bits"
	"unsafe"
)

const maxTaggedBits = int8(24)
const maxTaggedMask = uint64(0xffffffffff)

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
	// mask to get the tagged value in taggedPtr
	taggedMask uint64
}

func (th *tagPtrHelper) init(taggedBits uint8) {
	hashValueTaggedMask := uint64(1<<taggedBits) - 1
	hashValueTaggedOffset := 64 - taggedBits
	th.taggedMask = hashValueTaggedMask << hashValueTaggedOffset
}

func (th *tagPtrHelper) getTaggedValue(hashValue uint64) uint64 {
	return hashValue & th.taggedMask
}

func (*tagPtrHelper) toTaggedPtr(taggedValue uint64, ptr unsafe.Pointer) taggedPtr {
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

func getTaggedBitsFromUintptr(ptr uintptr) uint8 {
	if sizeOfUintptr != 8 {
		return 0
	}
	// count leading zeros to determine the number of tagged bits
	return uint8(min(int8(bits.LeadingZeros64(uint64(ptr))), maxTaggedBits))
}
