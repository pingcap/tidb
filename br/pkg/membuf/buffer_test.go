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
	"crypto/rand"
	"testing"

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
		WithPoolSize(2),
		WithAllocator(allocator),
		WithBlockSize(1024),
		WithLargeAllocThreshold(512),
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

	largeBytes := bytesBuf.AllocBytes(513)
	require.Equal(t, 513, len(largeBytes))
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
