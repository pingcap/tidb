// Copyright 2021 PingCAP, Inc.
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

package membuf

import (
	"bytes"
	"crypto/rand"
	rand2 "math/rand"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testAllocator struct {
	allocs int
	frees  int
}

func (t *testAllocator) Alloc(n int) []byte {
	t.allocs++
	return make([]byte, n)
}

func (t *testAllocator) Free(_ []byte) {
	t.frees++
}

func TestBufferPool(t *testing.T) {
	allocator := &testAllocator{}
	pool := NewPool(
		WithBlockNum(2),
		WithAllocator(allocator),
		WithBlockSize(1024),
	)
	defer pool.Destroy()

	bytesBuf := pool.NewBuffer()
	bytesBuf.AllocBytes(256)
	require.Equal(t, 1, allocator.allocs)
	bytesBuf.AllocBytes(512)
	require.Equal(t, 1, allocator.allocs)
	bytesBuf.AllocBytes(257)
	require.Equal(t, 2, allocator.allocs)
	bytesBuf.AllocBytes(767)
	require.Equal(t, 2, allocator.allocs)

	largeBytes := bytesBuf.AllocBytes(1025)
	require.Equal(t, 1025, len(largeBytes))
	require.Equal(t, 2, allocator.allocs)

	require.Equal(t, 0, allocator.frees)
	bytesBuf.Destroy()
	require.Equal(t, 0, allocator.frees)

	bytesBuf = pool.NewBuffer()
	for i := 0; i < 6; i++ {
		bytesBuf.AllocBytes(512)
	}
	bytesBuf.Destroy()
	require.Equal(t, 3, allocator.allocs)
	require.Equal(t, 1, allocator.frees)
}

func TestPoolMemLimit(t *testing.T) {
	limiter := NewLimiter(2*1024*1024 + 2*smallObjOverheadBatch)
	// only allow to allocate one block
	pool := NewPool(
		WithBlockSize(2*1024*1024),
		WithPoolMemoryLimiter(limiter),
	)
	defer pool.Destroy()
	buf := pool.NewBuffer()
	buf.AllocBytes(1024 * 1024)
	buf.AllocBytes(1024 * 1024)

	buf2 := pool.NewBuffer()
	done := make(chan struct{}, 1)
	go func() {
		buf2.AllocBytes(1024 * 1024)
		buf2.Destroy()
		done <- struct{}{}
	}()

	// sleep a while to make sure the goroutine is started
	time.Sleep(50 * time.Millisecond)
	require.Len(t, done, 0)
	// reset will not release memory to pool
	buf.Reset()
	buf.AllocBytes(1024 * 1024)
	buf.AllocBytes(1024 * 1024)
	require.Len(t, done, 0)
	// destroy will release memory to pool
	buf.Destroy()
	// wait buf2 to finish
	require.Eventually(t, func() bool {
		return len(done) > 0
	}, time.Second, 10*time.Millisecond)
	// after buf2 is finished, still can allocate memory from pool
	buf.AllocBytes(2 * 1024 * 1024)
	buf.Destroy()
}

func TestBufferIsolation(t *testing.T) {
	pool := NewPool(WithBlockSize(1024))
	defer pool.Destroy()
	bytesBuf := pool.NewBuffer()
	defer bytesBuf.Destroy()

	b1 := bytesBuf.AllocBytes(16)
	b2 := bytesBuf.AllocBytes(16)
	require.Len(t, b1, cap(b1))
	require.Len(t, b2, cap(b2))

	_, err := rand.Read(b2)
	require.NoError(t, err)
	b3 := append([]byte(nil), b2...)
	b1 = append(b1, 0, 1, 2, 3)
	require.Equal(t, b3, b2)
	require.NotEqual(t, b2, b1)
}

func TestBufferMemLimit(t *testing.T) {
	pool := NewPool(WithBlockSize(10))
	defer pool.Destroy()
	// the actual memory limit is 10 bytes.
	bytesBuf := pool.NewBuffer(WithBufferMemoryLimit(5))

	got, _ := bytesBuf.AllocBytesWithSliceLocation(9)
	require.NotNil(t, got)
	got, _ = bytesBuf.AllocBytesWithSliceLocation(3)
	require.Nil(t, got)

	bytesBuf.Destroy()
	// test the buffer is still usable after destroy.
	got, _ = bytesBuf.AllocBytesWithSliceLocation(3)
	require.NotNil(t, got)

	// exactly 2 block
	bytesBuf = pool.NewBuffer(WithBufferMemoryLimit(20))

	got, _ = bytesBuf.AllocBytesWithSliceLocation(9)
	require.NotNil(t, got)
	got, _ = bytesBuf.AllocBytesWithSliceLocation(9)
	require.NotNil(t, got)
	got, _ = bytesBuf.AllocBytesWithSliceLocation(2)
	require.Nil(t, got)

	// after reset, can get same allocation again
	bytesBuf.Reset()

	got, _ = bytesBuf.AllocBytesWithSliceLocation(9)
	require.NotNil(t, got)
	got, _ = bytesBuf.AllocBytesWithSliceLocation(9)
	require.NotNil(t, got)
	got, _ = bytesBuf.AllocBytesWithSliceLocation(2)
	require.Nil(t, got)
}

const dataNum = 100 * 1024 * 1024

func BenchmarkStoreSlice(b *testing.B) {
	data := make([][]byte, dataNum)
	for i := 0; i < b.N; i++ {
		func() {
			pool := NewPool()
			defer pool.Destroy()
			bytesBuf := pool.NewBuffer()
			defer bytesBuf.Destroy()

			for j := range data {
				data[j] = bytesBuf.AllocBytes(10)
			}
		}()
	}
}

func BenchmarkStoreLocation(b *testing.B) {
	data := make([]SliceLocation, dataNum)
	for i := 0; i < b.N; i++ {
		func() {
			pool := NewPool()
			defer pool.Destroy()
			bytesBuf := pool.NewBuffer()
			defer bytesBuf.Destroy()

			for j := range data {
				_, data[j] = bytesBuf.AllocBytesWithSliceLocation(10)
			}
		}()
	}
}

const sortDataNum = 1024 * 1024

func BenchmarkSortSlice(b *testing.B) {
	data := make([][]byte, sortDataNum)
	// fixed seed for benchmark
	rnd := rand2.New(rand2.NewSource(6716))

	for i := 0; i < b.N; i++ {
		func() {
			pool := NewPool()
			defer pool.Destroy()
			bytesBuf := pool.NewBuffer()
			defer bytesBuf.Destroy()

			for j := range data {
				data[j] = bytesBuf.AllocBytes(10)
				rnd.Read(data[j])
			}
			slices.SortFunc(data, func(a, b []byte) int {
				return bytes.Compare(a, b)
			})
		}()
	}
}

func BenchmarkSortLocation(b *testing.B) {
	data := make([]SliceLocation, sortDataNum)
	// fixed seed for benchmark
	rnd := rand2.New(rand2.NewSource(6716))

	for i := 0; i < b.N; i++ {
		func() {
			pool := NewPool()
			defer pool.Destroy()
			bytesBuf := pool.NewBuffer()
			defer bytesBuf.Destroy()

			for j := range data {
				var buf []byte
				buf, data[j] = bytesBuf.AllocBytesWithSliceLocation(10)
				rnd.Read(buf)
			}
			slices.SortFunc(data, func(a, b SliceLocation) int {
				return bytes.Compare(bytesBuf.GetSlice(a), bytesBuf.GetSlice(b))
			})
		}()
	}
}

func BenchmarkSortSliceWithGC(b *testing.B) {
	data := make([][]byte, sortDataNum)
	// fixed seed for benchmark
	rnd := rand2.New(rand2.NewSource(6716))

	for i := 0; i < b.N; i++ {
		func() {
			pool := NewPool()
			defer pool.Destroy()
			bytesBuf := pool.NewBuffer()
			defer bytesBuf.Destroy()

			for j := range data {
				data[j] = bytesBuf.AllocBytes(10)
				rnd.Read(data[j])
			}
			runtime.GC()
			slices.SortFunc(data, func(a, b []byte) int {
				return bytes.Compare(a, b)
			})
		}()
	}
}

func BenchmarkSortLocationWithGC(b *testing.B) {
	data := make([]SliceLocation, sortDataNum)
	// fixed seed for benchmark
	rnd := rand2.New(rand2.NewSource(6716))

	for i := 0; i < b.N; i++ {
		func() {
			pool := NewPool()
			defer pool.Destroy()
			bytesBuf := pool.NewBuffer()
			defer bytesBuf.Destroy()

			for j := range data {
				var buf []byte
				buf, data[j] = bytesBuf.AllocBytesWithSliceLocation(10)
				rnd.Read(buf)
			}
			runtime.GC()
			slices.SortFunc(data, func(a, b SliceLocation) int {
				return bytes.Compare(bytesBuf.GetSlice(a), bytesBuf.GetSlice(b))
			})
		}()
	}
}

func BenchmarkConcurrentAcquire(b *testing.B) {
	for i := 0; i < b.N; i++ {
		limiter := NewLimiter(512 * 1024 * 1024)
		pool := NewPool(WithPoolMemoryLimiter(limiter), WithBlockSize(4*1024))
		// start 1000 clients, each client will acquire 100B for 1000 times.
		wg := sync.WaitGroup{}
		clientNum := 1000
		wg.Add(clientNum)
		for j := 0; j < clientNum; j++ {
			go func() {
				defer wg.Done()
				buf := pool.NewBuffer()
				for k := 0; k < 1000; k++ {
					buf.AllocBytes(100)
				}
				buf.Destroy()
			}()
		}
		wg.Wait()
		pool.Destroy()
	}
}
