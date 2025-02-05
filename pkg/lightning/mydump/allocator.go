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
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
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
	allocate(int) []byte
	free([]byte)
	allocated() int64
	reset()
}

type arenaPool struct {
	arenas    chan arena
	allocated int
	lock      sync.Mutex
}

func (ap *arenaPool) adjustGCPercent() {
	gogc := os.Getenv("GOGC")
	memTotal, err := tidbmemory.MemTotal()
	if gogc == "" && err == nil {
		if ap.allocated == 0 {
			debug.SetGCPercent(100)
			return
		}
		percent := int(memTotal)*90/(ap.allocated*defaultArenaSize) - 100
		percent = min(percent, 50) / 10 * 10
		percent = max(percent, 5)

		old := debug.SetGCPercent(percent)
		//nolint: all_revive,revive
		runtime.GC()
		log.L().Debug("set gc percentage",
			zap.Int("old", old),
			zap.Int("new", percent),
			zap.Int("total memory", int(memTotal)),
			zap.Int("allocated memory", ap.allocated*defaultArenaSize),
		)
	}
}

func (ap *arenaPool) get() arena {
	// First try to get cached arena
	select {
	case a := <-ap.arenas:
		return a
	default:
	}

	ap.lock.Lock()
	defer ap.lock.Unlock()

	// Create a new one and return
	if ap.allocated < maxArenaCount {
		ap.allocated++
		bd := GetArena(defaultArenaSize)
		ap.adjustGCPercent()
		return bd
	}

	// We can't create new arena, return nil
	return nil
}

func (ap *arenaPool) put(a arena) {
	ap.lock.Lock()
	defer ap.lock.Unlock()

	// discard it if necessary
	if ap.allocated > maxArenaCount {
		a.reset()
		ap.adjustGCPercent()
		return
	}

	ap.arenas <- a
}

func (ap *arenaPool) free() {
	ap.lock.Lock()
	defer ap.lock.Unlock()

	ap.allocated = 0
	for len(ap.arenas) > 0 {
		a := <-ap.arenas
		a.reset()
	}
	ap.adjustGCPercent()
}

var globalPool *arenaPool

type defaultAllocator struct {
	arenas       []arena
	allocatedBuf map[uintptr]int

	allocatedOutside    atomic.Int64
	allocatedOutsideNum atomic.Int64
}

func (alloc *defaultAllocator) init() {
	alloc.allocatedBuf = make(map[uintptr]int, 8)
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
	// If global pool is initialized, return allocated arena to the pool.
	if globalPool != nil {
		for _, a := range alloc.arenas {
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
	globalPool = &arenaPool{}
	globalPool.arenas = make(chan arena, maxArenaCount)
}

// FreeMemory free all the memory allocated for arenas.
func FreeMemory() {
	if globalPool != nil {
		globalPool.free()
	}
}
