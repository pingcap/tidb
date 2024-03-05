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

package globalconn_test

import (
	"fmt"
	"math"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/pkg/util/globalconn"
	"github.com/stretchr/testify/assert"
)

func TestToConnID(t *testing.T) {
	assert := assert.New(t)

	type Case struct {
		gcid        globalconn.GCID
		shouldPanic bool
		expected    uint64
	}

	cases := []Case{
		{
			gcid: globalconn.GCID{
				Is64bits:    true,
				ServerID:    1001,
				LocalConnID: 123,
			},
			shouldPanic: false,
			expected:    (uint64(1001) << 41) | (uint64(123) << 1) | 1,
		},
		{
			gcid: globalconn.GCID{
				Is64bits:    true,
				ServerID:    1 << 22,
				LocalConnID: 123,
			},
			shouldPanic: true,
			expected:    0,
		},
		{
			gcid: globalconn.GCID{
				Is64bits:    true,
				ServerID:    1001,
				LocalConnID: 1 << 40,
			},
			shouldPanic: true,
			expected:    0,
		},
		{
			gcid: globalconn.GCID{
				Is64bits:    false,
				ServerID:    1001,
				LocalConnID: 123,
			},
			shouldPanic: false,
			expected:    (uint64(1001) << 21) | (uint64(123) << 1),
		},
		{
			gcid: globalconn.GCID{
				Is64bits:    false,
				ServerID:    1 << 11,
				LocalConnID: 123,
			},
			shouldPanic: true,
			expected:    0,
		},
		{
			gcid: globalconn.GCID{
				Is64bits:    false,
				ServerID:    1001,
				LocalConnID: 1 << 20,
			},
			shouldPanic: true,
			expected:    0,
		},
	}

	for _, c := range cases {
		if c.shouldPanic {
			assert.Panics(func() {
				c.gcid.ToConnID()
			})
		} else {
			assert.Equal(c.expected, c.gcid.ToConnID())
		}
	}
}

func TestGlobalConnID(t *testing.T) {
	assert := assert.New(t)
	var (
		err         error
		isTruncated bool
	)

	// exceeds int64
	_, _, err = globalconn.ParseConnID(0x80000000_00000321)
	assert.NotNil(err)

	// 64bits truncated
	_, isTruncated, err = globalconn.ParseConnID(101)
	assert.Nil(err)
	assert.True(isTruncated)

	// 64bits
	id1 := (uint64(1001) << 41) | (uint64(123) << 1) | 1
	gcid1, isTruncated, err := globalconn.ParseConnID(id1)
	assert.Nil(err)
	assert.False(isTruncated)
	assert.Equal(uint64(1001), gcid1.ServerID)
	assert.Equal(uint64(123), gcid1.LocalConnID)
	assert.True(gcid1.Is64bits)

	// exceeds uint32
	_, _, err = globalconn.ParseConnID(0x1_00000320)
	assert.NotNil(err)

	// 32bits
	id2 := (uint64(2002) << 21) | (uint64(321) << 1)
	gcid2, isTruncated, err := globalconn.ParseConnID(id2)
	assert.Nil(err)
	assert.False(isTruncated)
	assert.Equal(uint64(2002), gcid2.ServerID)
	assert.Equal(uint64(321), gcid2.LocalConnID)
	assert.False(gcid2.Is64bits)
	assert.Equal(gcid2.ToConnID(), id2)
}

func TestGetReservedConnID(t *testing.T) {
	assert := assert.New(t)

	simpleAlloc := globalconn.NewSimpleAllocator()
	assert.Equal(math.MaxUint64-uint64(0), simpleAlloc.GetReservedConnID(0))
	assert.Equal(math.MaxUint64-uint64(1), simpleAlloc.GetReservedConnID(1))

	serverID := func() uint64 {
		return 1001
	}

	globalAlloc := globalconn.NewGlobalAllocator(serverID, true)
	var maxLocalConnID uint64 = 1<<40 - 1
	assert.Equal(uint64(1001)<<41|(maxLocalConnID)<<1|1, globalAlloc.GetReservedConnID(0))
	assert.Equal(uint64(1001)<<41|(maxLocalConnID-1)<<1|1, globalAlloc.GetReservedConnID(1))
}

func benchmarkLocalConnIDAllocator32(b *testing.B, pool globalconn.IDPool) {
	var (
		id uint64
		ok bool
	)

	// allocate local conn ID.
	for {
		if id, ok = pool.Get(); ok {
			break
		}
		runtime.Gosched()
	}

	// deallocate local conn ID.
	if ok = pool.Put(id); !ok {
		b.Fatal("pool unexpected full")
	}
}

func BenchmarkLocalConnIDAllocator(b *testing.B) {
	b.ReportAllocs()

	concurrencyCases := []int{1, 3, 10, 20, 100}
	for _, concurrency := range concurrencyCases {
		b.Run(fmt.Sprintf("Allocator 64 x%v", concurrency), func(b *testing.B) {
			pool := globalconn.AutoIncPool{}
			pool.InitExt(1<<globalconn.LocalConnIDBits64, true, globalconn.LocalConnIDAllocator64TryCount)

			b.SetParallelism(concurrency)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id, ok := pool.Get()
					if !ok {
						b.Fatal("AutoIncPool.Get() failed.")
					}
					pool.Put(id)
				}
			})
		})

		b.Run(fmt.Sprintf("Allocator 32(LockBased) x%v", concurrency), func(b *testing.B) {
			pool := LockBasedCircularPool{}
			pool.InitExt(1<<globalconn.LocalConnIDBits32, math.MaxUint32)

			b.SetParallelism(concurrency)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					benchmarkLocalConnIDAllocator32(b, &pool)
				}
			})
		})

		b.Run(fmt.Sprintf("Allocator 32(LockFreeCircularPool) x%v", concurrency), func(b *testing.B) {
			pool := globalconn.LockFreeCircularPool{}
			pool.InitExt(1<<globalconn.LocalConnIDBits32, math.MaxUint32)

			b.SetParallelism(concurrency)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					benchmarkLocalConnIDAllocator32(b, &pool)
				}
			})
		})
	}
}
