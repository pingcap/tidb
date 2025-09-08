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
)

func TestSimpleAllocator(t *testing.T) {
	arenaSize = 16 << 20

	pool := GetPool(16 << 23)
	a := NewAppendOnlyAllocator(pool, 0)

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

	a.check()
}
