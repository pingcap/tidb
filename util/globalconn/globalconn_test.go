package globalconn_test

import (
	"fmt"
	"math"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/util/globalconn"
	"github.com/stretchr/testify/assert"
)

func TestGlobalConnID(t *testing.T) {
	assert := assert.New(t)

	gcid := globalconn.GCID{
		Is64bits:    true,
		ServerID:    1001,
		LocalConnID: 123,
	}
	assert.Equal((uint64(1001)<<41)|(uint64(123)<<1)|1, gcid.ToConnID())

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
			pool.InitExt(globalconn.LocalConnIDBits64, true, globalconn.LocalConnIDAllocator64TryCount)

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
			pool.InitExt(globalconn.LocalConnIDBits32, math.MaxUint32)

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
			pool.InitExt(globalconn.LocalConnIDBits32, math.MaxUint32)

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
