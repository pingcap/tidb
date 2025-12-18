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

package mydump

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAllocator(t *testing.T) {
	arenaSize = 16 << 20

	pool := GetPool(16 << 23)
	a := NewParquetAllocator(pool, 0)

	var (
		lk sync.Mutex
		wg sync.WaitGroup
	)

	allocSize := []int{16 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10, 512 << 10}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	allocFunc := func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				lk.Lock()
				bufSize := allocSize[rand.Intn(len(allocSize))]
				buf := a.Allocate(bufSize)
				lk.Unlock()

				// hold for sometimes
				time.Sleep(time.Millisecond)

				lk.Lock()
				a.Free(buf)
				lk.Unlock()
			}
		}
	}

	numCPU := runtime.NumCPU()
	for i := 0; i < numCPU*2; i++ {
		wg.Add(1)
		go allocFunc(ctx)
	}
	wg.Wait()

	a.Check()
}

func TestArena(t *testing.T) {
	buf := make([]byte, 100)
	s := newArena(buf)

	b1 := s.allocate(20)
	b2 := s.allocate(20)
	b3 := s.allocate(20)
	b4 := s.allocate(20)
	b5 := s.allocate(20)
	require.Nil(t, s.allocate(10))
	require.Len(t, s.allocated, 5)

	s.free(b2)
	s.free(b4)
	s.free(b5)
	// free blocks: [20,40), [60,100)
	require.Equal(t, 40, s.freeByStart[60])
	require.Equal(t, 40, s.freeByEnd[100])
	require.Len(t, s.allocated, 2)

	s.free(b3)
	// free blocks: [20, 100)
	require.Equal(t, 80, s.freeByStart[20])
	require.Equal(t, 80, s.freeByEnd[100])
	require.Len(t, s.allocated, 1)

	s6 := s.allocate(60)
	require.NotNil(t, s6)
	// free blocks: [80, 100)
	require.Equal(t, 20, s.freeByStart[80])
	require.Equal(t, 20, s.freeByEnd[100])
	require.Len(t, s.allocated, 2)

	s.free(b1)
	s.free(s6)
	require.Len(t, s.allocated, 0)
	require.Equal(t, 100, s.freeByStart[0])
	require.Equal(t, 100, s.freeByEnd[100])
}
