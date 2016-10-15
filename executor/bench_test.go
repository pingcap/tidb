// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/arena"
)

func prepareKVRanges(count int) ([]int64, int) {
	handles := make([]int64, count)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < len(handles); i++ {
		handles[i] = rand.Int63()
	}
	return handles, count*tablecodec.RowKeyWithHandleLen + 20
}

func BenchmarkToKVRanges(b *testing.B) {
	handles, _ := prepareKVRanges(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tableHandlesToKVRanges(42, handles, arena.StdAllocator)
	}
}

func BenchmarkToKVRangesAllocator(b *testing.B) {
	handles, size := prepareKVRanges(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		allocator := arena.NewAllocator(size)
		tableHandlesToKVRanges(42, handles, allocator)
	}
}

func BenchmarkToKVRangesAllocatorPool(b *testing.B) {
	handles, size := prepareKVRanges(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		allocPool := getAllocPool(size)
		allocator := allocPool.Get().(arena.Allocator)

		tableHandlesToKVRanges(42, handles, allocator)

		allocator.Reset()
		allocPool.Put(allocator)
	}
}
