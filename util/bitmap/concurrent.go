// Copyright 2019 PingCAP, Inc.
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

package bitmap

import (
	"sync/atomic"
	"unsafe"
)

const (
	segmentWidth             = 32
	segmentWidthPower        = 5
	bitMask           uint32 = 0x80000000
)

var bytesConcurrentBitmap = int64(int(unsafe.Sizeof(ConcurrentBitmap{})))

// ConcurrentBitmap is a static-length bitmap which is thread-safe on setting.
// It is implemented using CAS, as atomic bitwise operation is not supported by
// golang yet. (See https://github.com/golang/go/issues/24244)
// CAS operation is narrowed down to uint32 instead of longer types like uint64,
// to reduce probability of racing.
type ConcurrentBitmap struct {
	segments []uint32
	bitLen   int
}

// Clone clones a new bitmap with the old bit set.
func (cb *ConcurrentBitmap) Clone() *ConcurrentBitmap {
	cp := NewConcurrentBitmap(cb.bitLen)
	needLen := len(cp.segments)
	for i := 0; i < needLen; i++ {
		cp.segments[i] = cb.segments[i]
	}
	return cp
}

// NewConcurrentBitmap initializes a ConcurrentBitmap which can store
// bitLen of bits.
func NewConcurrentBitmap(bitLen int) *ConcurrentBitmap {
	segmentLen := (bitLen + segmentWidth - 1) >> segmentWidthPower
	return &ConcurrentBitmap{
		segments: make([]uint32, segmentLen),
		bitLen:   bitLen,
	}
}

// Reset clean the bitmap if the length is suitable, otherwise renewing one.
func (cb *ConcurrentBitmap) Reset(bitLen int) {
	segmentLen := (bitLen + segmentWidth - 1) >> segmentWidthPower
	if segmentLen <= len(cb.segments) {
		for i := range cb.segments {
			cb.segments[i] = 0
		}
		cb.bitLen = bitLen
	} else {
		cb.segments = make([]uint32, segmentLen)
		cb.bitLen = bitLen
	}
}

// BytesConsumed returns size of this bitmap in bytes.
func (cb *ConcurrentBitmap) BytesConsumed() int64 {
	return bytesConcurrentBitmap + int64(segmentWidth/8*cap(cb.segments))
}

// Set sets the bit on bitIndex to be 1 (bitIndex starts from 0).
// isSetter indicates whether the function call this time triggers the bit from 0 to 1.
// bitIndex bigger than bitLen initialized will be ignored.
func (cb *ConcurrentBitmap) Set(bitIndex int) (isSetter bool) {
	if bitIndex < 0 || bitIndex >= cb.bitLen {
		return
	}

	var oldValue, newValue uint32
	segmentPointer := &(cb.segments[bitIndex>>segmentWidthPower])
	mask := bitMask >> uint32(bitIndex%segmentWidth)
	// Repeatedly observe whether bit is already set, and try to set
	// it based on observation.
	for {
		// Observe.
		oldValue = atomic.LoadUint32(segmentPointer)
		if (oldValue & mask) != 0 {
			return
		}

		// Set.
		newValue = oldValue | mask
		isSetter = atomic.CompareAndSwapUint32(segmentPointer, oldValue, newValue)
		if isSetter {
			return
		}
	}
}

// UnsafeSet sets the bit on bitIndex to be 1 (bitIndex starts from 0).
// isSetter indicates whether the function call this time triggers the bit from 0 to 1.
// bitIndex bigger than bitLen initialized will be ignored.
// (this version is concurrent unsafe if the caller can make sure write is in single thread)
func (cb *ConcurrentBitmap) UnsafeSet(bitIndex int) {
	if bitIndex < 0 || bitIndex >= cb.bitLen {
		return
	}

	mask := bitMask >> uint32(bitIndex%segmentWidth)
	cb.segments[bitIndex>>segmentWidthPower] = cb.segments[bitIndex>>segmentWidthPower] | mask
}

// UnsafeIsSet returns if a bit on bitIndex is set (bitIndex starts from 0).
// bitIndex bigger than bitLen initialized will return false.
// This method is not thread-safe as it does not use atomic load.
func (cb *ConcurrentBitmap) UnsafeIsSet(bitIndex int) (isSet bool) {
	if bitIndex < 0 || bitIndex >= cb.bitLen {
		return
	}

	mask := bitMask >> uint32(bitIndex%segmentWidth)
	isSet = ((cb.segments[bitIndex>>segmentWidthPower] & mask) != 0)
	return
}
