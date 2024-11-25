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
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestTaggedBits(t *testing.T) {
	p := uintptr(0)
	for i := 0; i <= 64; i++ {
		taggedBits := getTaggedBitsFromUintptr(p)
		require.Equal(t, min(int8(64-i), maxTaggedBits), int8(taggedBits))
		p = (p << 1) + 1
	}
}

func TestTagHelperInit(t *testing.T) {
	mask := ^maxTaggedMask
	for taggedBits := maxTaggedBits; taggedBits >= 0; taggedBits-- {
		tagHelper := &tagPtrHelper{}
		tagHelper.init(uint8(taggedBits))
		require.Equal(t, mask, tagHelper.taggedMask)
		mask <<= 1
	}
}

func TestTagHelper(t *testing.T) {
	rawData := make([]byte, 10*1024*1024)
	startPtr := unsafe.Pointer(&rawData[0])
	endPtr := unsafe.Pointer(&rawData[len(rawData)-1])
	startUintptr := uintptr(0)
	endUintptr := uintptr(0)
	*(*unsafe.Pointer)(unsafe.Pointer(&startUintptr)) = startPtr
	*(*unsafe.Pointer)(unsafe.Pointer(&endUintptr)) = endPtr
	taggedBits := getTaggedBitsFromUintptr(startUintptr | endUintptr)
	tagHelper := &tagPtrHelper{}
	tagHelper.init(taggedBits)
	taggedValue := uint64(0x1234) << (64 - maxTaggedBits)
	for {
		if taggedValue&tagHelper.taggedMask == taggedValue {
			break
		}
		taggedValue <<= 1
	}
	require.True(t, taggedValue != 0, "tagged value should not be zero")
	//
	testPtrs := []unsafe.Pointer{startPtr, endPtr}
	for _, testPtr := range testPtrs {
		taggedPtr := tagHelper.toTaggedPtr(taggedValue, testPtr)
		require.Equal(t, taggedValue, tagHelper.getTaggedValue(uint64(taggedPtr)))
		require.Equal(t, testPtr, tagHelper.toUnsafePointer(taggedPtr))
	}
}
