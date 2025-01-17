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
)

/*
 * There are two usage modes for the memory allocation:
 * 1. Call `GetDefaultAllocator` directly to get an allocator.
 * 2. Call `InitializeGlobalArena` to initialize the global arena pool,
 *    so the arena allocated in this node can be reused by subsequent allocation.
 *    User should remember to call `FreeMemory` after the execution is completed.
 */

var (
	maxArenaCount    = 0         // maximum arena count
	defaultArenaSize = 256 << 20 // size of each arena

	// AllocSize returns actual allocated size in arena
	AllocSize func(int) int

	// GetArena creates a new arena
	GetArena func(int) arena
)

func init() {
	AllocSize = simpleGetAllocationSize
	GetArena = getSimpleAllocator
}

// Get the address of a buffer, return 0 if the buffer is nil
func addressOf(buf []byte) uintptr {
	if buf == nil || cap(buf) == 0 {
		return 0
	}
	buf = buf[:1]
	return uintptr(unsafe.Pointer(&buf[0]))
}

// GetArenaSize return the default arena size
func GetArenaSize() int {
	return defaultArenaSize
}

// arena is the interface of single allocator
type arena interface {
	allocate(size int) []byte
	free(bs []byte)
	allocated() int64
	reset()
	freeAll()
}

type globalArenaPool struct {
	arenas    chan arena
	allocated int
	lock      sync.Mutex
}

func (ga *globalArenaPool) adjustGCPercent() {
	gogc := os.Getenv("GOGC")
	memTotal, err := tidbmemory.MemTotal()
	if gogc == "" && err == nil {
		if ga.allocated == 0 {
			debug.SetGCPercent(100)
			return
		}
		percent := int(memTotal)*90/(ga.allocated*defaultArenaSize) - 100
		percent = min(percent, 100) / 10 * 10
		percent = max(percent, 5)

		old := debug.SetGCPercent(percent)
		//nolint: all_revive,revive
		runtime.GC()
		log.L().Debug("set gc percentage",
			zap.Int("old", old),
			zap.Int("new", percent),
			zap.Int("total memory", int(memTotal)),
			zap.Int("allocated memory", ga.allocated*defaultArenaSize),
		)
	}
}

func (ga *globalArenaPool) get() arena {
	// First try to get cached arena
	select {
	case a := <-ga.arenas:
		return a
	default:
	}

	ga.lock.Lock()
	defer ga.lock.Unlock()

	// Create a new one and return
	if ga.allocated < maxArenaCount {
		ga.allocated++
		bd := GetArena(defaultArenaSize)
		ga.adjustGCPercent()
		return bd
	}

	// We can't create new arena, return nil
	return nil
}

func (ga *globalArenaPool) put(a arena) {
	ga.lock.Lock()
	defer ga.lock.Unlock()

	// discard it if necessary
	if ga.allocated > maxArenaCount {
		a.freeAll()
		a.reset()
		ga.adjustGCPercent()
		return
	}

	ga.arenas <- a
}

func (ga *globalArenaPool) free() {
	ga.lock.Lock()
	defer ga.lock.Unlock()

	ga.allocated = 0
	for len(ga.arenas) > 0 {
		a := <-ga.arenas
		a.freeAll()
		a.reset()
	}
	ga.adjustGCPercent()
}

var globalPool *globalArenaPool

type defaultAllocator struct {
	arenas       []arena
	allocatedBuf map[uintptr]int

	allocatedOutside    atomic.Int64
	allocatedOutsideNum atomic.Int64

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (alloc *defaultAllocator) init() {
	alloc.allocatedBuf = make(map[uintptr]int, 8)
	ctx, cancel := context.WithCancel(context.Background())
	alloc.wg.Add(1)
	go func() {
		defer alloc.wg.Done()
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)

				num := 0
				for _, a := range alloc.arenas {
					num += int(a.allocated())
				}

				fmt.Printf("[Allocator] num arenas = %d, num blocks = %d, outside the allocator: %d MiB(%d blocks)\n",
					len(alloc.arenas), num,
					int(alloc.allocatedOutsideNum.Load()),
					int(alloc.allocatedOutside.Load())/1024/1024,
				)
			case <-ctx.Done():
				return
			}
		}
	}()
	alloc.cancel = cancel
}

func (alloc *defaultAllocator) Allocate(size int) []byte {
	for i, a := range alloc.arenas {
		if buf := a.allocate(size); buf != nil {
			alloc.allocatedBuf[addressOf(buf)] = i
			return buf
		}
	}

	// If global pool is initialized, get arena from the pool.
	// Otherwise, we just create a new one.
	var na arena
	if globalPool != nil {
		if na = globalPool.get(); na == nil {
			return make([]byte, size)
		}
	} else {
		na = GetArena(defaultArenaSize)
	}

	buf := na.allocate(size)
	alloc.allocatedBuf[addressOf(buf)] = len(alloc.arenas)
	alloc.arenas = append(alloc.arenas, na)
	return buf
}

func (alloc *defaultAllocator) Free(buf []byte) {
	addr := addressOf(buf[:1])
	if arenaID, ok := alloc.allocatedBuf[addr]; ok {
		alloc.arenas[arenaID].free(buf)
		delete(alloc.allocatedBuf, addr)
	}
}

func (alloc *defaultAllocator) Reallocate(size int, buf []byte) []byte {
	alloc.Free(buf)
	return alloc.Allocate(size)
}

func (alloc *defaultAllocator) Close() {
	alloc.cancel()
	alloc.wg.Wait()

	// If global pool is initialized, return allocated arena to the pool.
	if globalPool != nil {
		for _, a := range alloc.arenas {
			a.freeAll()
			a.reset()
			globalPool.put(a)
		}
	}

	alloc.arenas = nil
}

// GetDefaultAllocator get a default allocator
func GetDefaultAllocator() memory.Allocator {
	a := &defaultAllocator{}
	a.init()
	return a
}

// InitializeGlobalArena initialize a global arena pool.
// If you call this function, remember to call FreeMemory.
func InitializeGlobalArena(size int) {
	maxArenaCount = size / defaultArenaSize
	globalPool = &globalArenaPool{}
	globalPool.arenas = make(chan arena, maxArenaCount)
}

// FreeMemory free all the memory allocated for arenas.
func FreeMemory() {
	if globalPool != nil {
		globalPool.free()
	}
}
