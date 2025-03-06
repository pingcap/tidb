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
	"runtime"
	"runtime/debug"
	"sync"
	"unsafe"

	"github.com/joechenrh/arrow-go/v18/arrow/memory"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	tidbmemory "github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

const ImportIntoReaderUsage = 40

var (
	// size of each arena
	defaultArenaSize = 256 << 20

	// memory limit for parquet reader
	readerMemoryLimit   int
	readerMemoryLimiter *membuf.Limiter

	// globalPool is used for all parquet import tasks.
	// We use importCount to track and release memory.
	lk          sync.Mutex
	globalPool  *membuf.Pool
	importCount int

	// GetArena creates a new arena
	GetArena func(*membuf.Buffer) arena
)

// ConfigureReaderLimitForParquet set the memory limit for parquet reader.
// Remember to call ReleaseMemoryForParquet to free the memory.
func ConfigureReaderLimitForParquet(percent int) {
	lk.Lock()
	defer lk.Unlock()

	importCount++
	if importCount > 1 {
		return
	}

	memTotal, err := tidbmemory.MemTotal()
	if err != nil {
		log.L().Warn("Fail to get total memory")
		// Set limit to int max, which means no limiter
		memTotal = math.MaxInt32
	}
	readerMemoryLimit = int(memTotal) * min(percent, 75) / 100
	readerMemoryLimiter = membuf.NewLimiter(readerMemoryLimit)

	gcPercent := (10000/percent - 100) / 10 * 10
	gcPercent = max(gcPercent, 10)
	gcPercent = min(gcPercent, 50)
	debug.SetGCPercent(gcPercent)

	globalPool = membuf.NewPool(
		membuf.WithBlockNum(readerMemoryLimit/defaultArenaSize),
		membuf.WithBlockSize(defaultArenaSize),
	)

	log.L().Info("set memory limit",
		zap.Int("total memory", int(memTotal)),
		zap.Int("memory limit", readerMemoryLimit),
		zap.Int("GC Percentage", gcPercent),
	)
}

// ReleaseMemoryForParquet releases memory allocated for parquet readers.
func ReleaseMemoryForParquet() {
	lk.Lock()
	defer lk.Unlock()

	importCount--
	if importCount == 0 {
		globalPool.Destroy()
		globalPool = nil
		debug.SetGCPercent(100)
		//nolint: all_revive,revive
		runtime.GC()
	}
}

// GetMemoryQuota get the memory quota for non-streaming mode read.
func GetMemoryQuota(concurrency int) int {
	quotaPerTask := readerMemoryLimit / concurrency

	// Because other parts like encoder also consume memory,
	// we assume that the reader can use up to 80% of the memroy.
	quotaPerReader := quotaPerTask * 8 / 10
	quotaPerReader = quotaPerReader / defaultArenaSize * defaultArenaSize
	return quotaPerReader
}

// AdjustEncodeThreadCnt adjust the concurrency in encode&sort step for parquet file.
func AdjustEncodeThreadCnt(memoryUsage, threadCnt int) int {
	memTotal, err := tidbmemory.MemTotal()
	if err != nil {
		return threadCnt
	}

	adjustedThreadCnt := int(memTotal) * ImportIntoReaderUsage / 100 / memoryUsage
	return max(min(adjustedThreadCnt, threadCnt), 1)
}

func init() {
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

// arena is the interface of single allocator
type arena interface {
	allocate(int) []byte
	free([]byte)
	reset()
}

type defaultAllocator struct {
	arenas []arena
	mbufs  []*membuf.Buffer

	allocatedBuf map[uintptr]int
}

func (alloc *defaultAllocator) Allocate(size int) []byte {
	for i, a := range alloc.arenas {
		if buf := a.allocate(size); buf != nil {
			alloc.allocatedBuf[addressOf(buf)] = i
			return buf
		}
	}

	var mbuf *membuf.Buffer
	if globalPool != nil {
		mbuf = globalPool.NewBuffer()
		alloc.mbufs = append(alloc.mbufs, mbuf)
	}

	na := GetArena(mbuf)
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

func (alloc *defaultAllocator) Reallocate(size int, buf []byte) []byte {
	alloc.Free(buf)
	return alloc.Allocate(size)
}

func (alloc *defaultAllocator) Close() {
	for _, a := range alloc.arenas {
		a.reset()
	}
	for _, mbuf := range alloc.mbufs {
		mbuf.Destroy()
	}
	alloc.arenas = nil
}

func (alloc *defaultAllocator) Allocated() int {
	return defaultArenaSize * len(alloc.arenas)
}

// GetAllocator get a default allocator
func GetAllocator() memory.Allocator {
	return &defaultAllocator{
		allocatedBuf: make(map[uintptr]int, 32),
	}
}
