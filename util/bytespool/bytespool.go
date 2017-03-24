// Copyright 2017 PingCAP, Inc.
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

package bytespool

import (
	"sync"
)

// BytesPool maintains large bytes pools, used for reducing memory allocation.
// It has a slice of pools which handle different size of bytes.
// Can be safely used concurrently.
type BytesPool struct {
	buckets []sync.Pool
}

const (
	kilo       = 1024
	mega       = kilo * kilo
	baseSize   = kilo
	numBuckets = 18
	maxSize    = 128 * mega
)

// DefaultPool is a default BytesBool instance.
var DefaultPool = NewBytesPool()

// NewBytesPool creates a new bytes pool.
func NewBytesPool() *BytesPool {
	bp := new(BytesPool)
	bp.buckets = make([]sync.Pool, numBuckets)
	for i := uint(0); i < numBuckets; i++ {
		bp.buckets[i].New = makeNewFunc(i)
	}
	return bp
}

func makeNewFunc(shift uint) func() interface{} {
	return func() interface{} {
		return make([]byte, baseSize<<shift)
	}
}

// Alloc allocates a bytes which has the size of power of two.
// The caller should keep the origin bytes and use the returned data.
// When finished using, the origin bytes should be freed to the pool.
// The allocated data may not have zero value.
func (bp *BytesPool) Alloc(size int) (origin, data []byte) {
	if size > maxSize {
		return nil, make([]byte, size)
	}
	i := bucketIdx(size)
	origin = bp.buckets[i].Get().([]byte)
	data = origin[:size]
	return
}

// Free frees the data which should be the original bytes return by Alloc.
// It returns the bucket index of the data. returns -1 means the data is not returned to the pool.
func (bp *BytesPool) Free(origin []byte) int {
	originLen := len(origin)
	if originLen > maxSize || originLen < baseSize || !isPowerOfTwo(originLen) {
		return -1
	}
	i := bucketIdx(originLen)
	bp.buckets[i].Put(origin)
	return i
}

func isPowerOfTwo(x int) bool {
	return x&(x-1) == 0
}

func bucketIdx(size int) (i int) {
	for size > baseSize {
		size = (size + 1) >> 1
		i++
	}
	return
}
