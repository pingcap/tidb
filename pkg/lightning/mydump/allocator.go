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

package mydump

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/joechenrh/arrow-go/v18/arrow/memory"
	"github.com/pingcap/tidb/pkg/lightning/log"
	tidbmemory "github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
)

var (
	maxArenaCount    = 0         // maximum arena count
	arenaDefaultSize = 512 << 20 // size of each arena
	leafSize         = 256 << 10 // The smallest block size is 256KB
)

// SetMaxMemoryUsage set the memory used by parquet reader.
func SetMaxMemoryUsage(size int) {
	maxArenaCount = size / arenaDefaultSize
}

func GetArenaSize() int {
	return arenaDefaultSize
}

type arena interface {
	allocate(size int) []byte
	free(bs []byte)
	allocated() int64
	reset()
	freeAll()
}

// Convert slice to an uintptr. This value is used as key in map.
func unsafeGetblkAddr(slice []byte) uintptr {
	return uintptr(unsafe.Pointer(&slice[0]))
}

var (
	arenas    []atomic.Value
	numArenas atomic.Int32
	lock      sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	allocatedOutside    atomic.Int64
	allocatedOutsideNum atomic.Int64
)

func initArenas() {
	arenas = make([]atomic.Value, maxArenaCount)
	initNum := min(8, maxArenaCount)
	for i := 0; i < initNum; i++ {
		a := &buddyAllocator{}
		a.init(arenaDefaultSize)
		arenas[i].Store(a)
	}
	numArenas.Store(int32(initNum))

	ctx, cancel = context.WithCancel(context.Background())
	go getStatus()
}

func freeArenas() {
	cancel()
	for i := 0; i < len(arenas); i++ {
		if v := arenas[i].Load(); v != nil {
			a := v.(arena)
			a.freeAll()
			a.reset()
			a = nil
			// store an empty buddyAllocator to release the memory
			arenas[i].Store(&buddyAllocator{})
		}
	}
	numArenas.Store(0)
	arenas = nil
}

func adjustGCPercent() {
	gogc := os.Getenv("GOGC")
	memTotal, err := tidbmemory.MemTotal()
	if gogc == "" && err == nil {
		if numArenas.Load() == 0 {
			debug.SetGCPercent(100)
			return
		}
		percent := int(memTotal)*90/(int(numArenas.Load())*arenaDefaultSize) - 100
		percent = min(percent, 100) / 10 * 10
		percent = max(percent, 5)

		old := debug.SetGCPercent(percent)
		runtime.GC()
		log.L().Debug("set gc percentage",
			zap.Int("old", old),
			zap.Int("new", percent),
			zap.Int("total memory", int(memTotal)),
			zap.Int("allocated memory", int(numArenas.Load())*arenaDefaultSize),
		)
	}
}

type defaultAllocator struct {
	allocatedBuf sync.Map
}

func (alloc *defaultAllocator) Init(_ int) {

}

func getStatus() {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			l := int(numArenas.Load())
			var totalAllocated int64
			for i := 0; i < l; i++ {
				if a := arenas[i].Load().(arena); a != nil {
					totalAllocated += a.allocated()
				}
			}

			fmt.Printf("[buddyAllocator] Inside the allocator: %d MiB, outside the allocator: %d MiB(%d blocks)\n",
				int(totalAllocated)/1024/1024,
				int(allocatedOutsideNum.Load()),
				int(allocatedOutside.Load())/1024/1024,
			)
		case <-ctx.Done():
			return
		}
	}
}

func (alloc *defaultAllocator) Allocate(size int) []byte {
START:
	// start from a random arena to avoid contention
	l := int(numArenas.Load())
	idx := rand.Intn(l)
	for i := 0; i < l; i++ {
		a := arenas[idx].Load().(arena)
		if buf := a.allocate(size); buf != nil {
			alloc.allocatedBuf.Store(unsafeGetblkAddr(buf), idx)
			return buf
		}
		idx = (idx + 1) % l
	}

	// Can't create new arena, use make to allocate memory
	if l == maxArenaCount {
		allocatedOutside.Add(int64(size))
		allocatedOutsideNum.Add(1)
		return make([]byte, size)
	}

	// Create some new arenas, if someone else has created the arena, just use it.
	lock.Lock()
	defer lock.Unlock()
	for i := 0; i < min(maxArenaCount-l, 2); i++ {
		if arenas[l+i].Load() == nil {
			a := &buddyAllocator{}
			a.init(arenaDefaultSize)
			arenas[l+i].Store(a)
			numArenas.Add(1)
		}
	}
	adjustGCPercent()

	idx = int(numArenas.Load()) - 1
	a := arenas[idx].Load().(arena)
	if buf := a.allocate(size); buf != nil {
		alloc.allocatedBuf.Store(unsafeGetblkAddr(buf), idx)
		return buf
	}

	// This should rarely happen, goto START to try again
	goto START
}

func (alloc *defaultAllocator) Free(buf []byte) {
	if buf == nil || cap(buf) == 0 {
		return
	}
	addr := unsafeGetblkAddr(buf[:1])
	arenaID, ok := alloc.allocatedBuf.Load(addr)
	if !ok {
		return
	}

	arenas[arenaID.(int)].Load().(arena).free(buf)
	alloc.allocatedBuf.Delete(addr)
}

func (alloc *defaultAllocator) Reallocate(size int, buf []byte) []byte {
	alloc.Free(buf)
	return alloc.Allocate(size)
}

var once sync.Once

func GetDefaultAllocator() memory.Allocator {
	once.Do(func() {
		initArenas()
	})
	return &defaultAllocator{}
}

// FreeMemory free all the memory allocated for arenas.
func FreeMemory() {
	freeArenas()
}
