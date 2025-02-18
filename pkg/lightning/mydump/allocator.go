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
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"unsafe"

	"github.com/joechenrh/arrow-go/v18/arrow/memory"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	tidbmemory "github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

var (
	// size of each arena
	defaultArenaSize = 256 << 20

	// memory limit for parquet reader
	readerMemoryLimit   int
	readerMemoryLimiter *membuf.Limiter

	// AllocSize returns actual allocated size from arena
	AllocSize func(int) int

	// GetArena creates a new arena
	GetArena func(int) arena
)

// InitializeGlobalArena initialize a global arena pool.
func InitializeGlobalArena(size int, reuse bool) {
	maxArenaCount := size / defaultArenaSize
	if globalPool == nil {
		globalPool = &arenaPool{
			maxArenaCount: maxArenaCount,
			reuse:         reuse,
			arenas:        make(chan arena, 1024),
		}
		return
	}

	globalPool.adjustMaxArenaCount(maxArenaCount)
}

// FreeMemory free all the memory allocated for arenas.
func FreeMemory() {
	if globalPool != nil {
		globalPool.free()
	}
}

// SetMemoryLimitForParquet set the memory limit for parquet reader.
// If reuse = true, remember to call FreeMemory to free the memory.
func SetMemoryLimitForParquet(percent int, reuse bool) {
	memTotal, err := tidbmemory.MemTotal()
	if err != nil {
		// Set limit to int max, which means no limiter
		memTotal = math.MaxInt32
	}
	readerMemoryLimit = int(memTotal) * min(percent, 90) / 100
	readerMemoryLimiter = membuf.NewLimiter(readerMemoryLimit)
	InitializeGlobalArena(readerMemoryLimit, reuse)

	log.L().Info("set memory limit",
		zap.Int("total memory", int(memTotal)),
		zap.Int("memory limit", readerMemoryLimit),
	)
}

// GetMemoryQuota get the memory quota for non-streaming mode read.
// TODO(joechenrh): set a more proper memory quota
func GetMemoryQuota(concurrency int) int {
	quotaPerTask := readerMemoryLimit / concurrency

	// Because other part like encoder also need memory,
	// we assume that the reader can use up to 80% of the memroy.
	// Maybe we can have a more accurate estimation later.
	quotaPerReader := quotaPerTask * 8 / 10
	quotaPerReader = quotaPerReader / defaultArenaSize * defaultArenaSize
	return quotaPerReader
}

func init() {
	AllocSize = simpleGetAllocationSize
	GetArena = getSimpleAllocator

	// This is used for `IMPORT INTO`.
	// We set the default memory usage to 40% and don't reuse arenas.
	SetMemoryLimitForParquet(40, false)
}

// Get the address of a buffer, return 0 if the buffer is nil
func addressOf(buf []byte) uintptr {
	if buf == nil || cap(buf) == 0 {
		return 0
	}
	buf = buf[:1]
	return uintptr(unsafe.Pointer(&buf[0]))
}

// arena is the interface of single allocator
type arena interface {
	allocate(int) []byte
	free([]byte)
	allocated() int64
	reset()
}

type arenaPool struct {
	arenas        chan arena
	maxArenaCount int
	allocated     int
	reuse         bool
	lock          sync.Mutex
}

func (ap *arenaPool) adjustGCPercent() {
	gogc := os.Getenv("GOGC")
	memTotal, err := tidbmemory.MemTotal()
	if gogc == "" && err == nil {
		if ap.allocated == 0 {
			debug.SetGCPercent(100)
			return
		}
		percent := int(memTotal)*100/(ap.allocated*defaultArenaSize) - 100
		percent = min(percent, 50)
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

func (ap *arenaPool) adjustMaxArenaCount(newCount int) {
	ap.lock.Lock()
	defer ap.lock.Unlock()

	ap.maxArenaCount = newCount
	for ap.allocated > newCount && len(ap.arenas) > 0 {
		a := <-ap.arenas
		a.reset()
		ap.allocated--
	}

	ap.adjustGCPercent()
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
	if ap.allocated < ap.maxArenaCount {
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

	if ap.reuse && ap.allocated <= ap.maxArenaCount {
		ap.arenas <- a
		return
	}

	a.reset()
	ap.allocated--
	ap.adjustGCPercent()
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
}

func (alloc *defaultAllocator) init() {
	alloc.allocatedBuf = make(map[uintptr]int, 8)
}

func (alloc *defaultAllocator) Allocate(size int, _ memory.BufferType) []byte {
	for i, a := range alloc.arenas {
		if buf := a.allocate(size); buf != nil {
			alloc.allocatedBuf[addressOf(buf)] = i
			return buf
		}
	}

	na := globalPool.get()
	if na == nil {
		return make([]byte, size)
	}

	buf := na.allocate(size)
	alloc.allocatedBuf[addressOf(buf)] = len(alloc.arenas)
	alloc.arenas = append(alloc.arenas, na)
	return buf
}

func (alloc *defaultAllocator) Free(buf []byte) {
	addr := addressOf(buf)
	if arenaID, ok := alloc.allocatedBuf[addr]; ok {
		alloc.arenas[arenaID].free(buf)
		delete(alloc.allocatedBuf, addr)
	}
}

func (alloc *defaultAllocator) Reallocate(size int, buf []byte, tp memory.BufferType) []byte {
	alloc.Free(buf)
	return alloc.Allocate(size, tp)
}

func (alloc *defaultAllocator) Close() {
	for _, a := range alloc.arenas {
		a.reset()
		globalPool.put(a)
	}

	alloc.arenas = nil
}

// GetAllocator get a default allocator
func GetAllocator() memory.Allocator {
	a := &defaultAllocator{}
	a.init()
	return a
}
