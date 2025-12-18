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
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/util/intest"
)

type arena struct {
	buf  []byte
	base uintptr
	mu   sync.Mutex

	allocated   map[uintptr]int
	freeByStart map[int]int
	freeByEnd   map[int]int
}

func newArena(buf []byte) *arena {
	s := &arena{
		buf:         buf,
		base:        addressOf(buf),
		allocated:   make(map[uintptr]int),
		freeByStart: make(map[int]int),
		freeByEnd:   make(map[int]int),
	}

	s.freeByStart[0] = len(buf)
	s.freeByEnd[len(buf)] = len(buf)
	return s
}

func (s *arena) allocate(size int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		chosenSize  int = -1
		chosenStart int
	)
	for start, freeSize := range s.freeByStart {
		if freeSize >= size {
			chosenStart = start
			chosenSize = freeSize
			break
		}
	}
	if chosenSize < 0 {
		return nil
	}

	delete(s.freeByStart, chosenStart)
	delete(s.freeByEnd, chosenStart+chosenSize)

	start := chosenStart
	end := start + size
	if rem := chosenSize - size; rem > 0 {
		remainderStart := end
		remainderEnd := end + rem
		s.freeByStart[remainderStart] = rem
		s.freeByEnd[remainderEnd] = rem
	}

	buf := s.buf[start:end]
	s.allocated[addressOf(buf)] = size
	return buf
}

func (s *arena) free(buf []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	addr := addressOf(buf)
	sz, ok := s.allocated[addr]
	if !ok {
		return
	}

	delete(s.allocated, addr)
	newStart := int(addr - s.base)
	newEnd := newStart + sz

	if leftSize, ok := s.freeByEnd[newStart]; ok {
		leftStart := newStart - leftSize
		delete(s.freeByStart, leftStart)
		delete(s.freeByEnd, newStart)
		newStart = leftStart
	}

	if rightSize, ok := s.freeByStart[newEnd]; ok {
		rightEnd := newEnd + rightSize
		delete(s.freeByStart, newEnd)
		delete(s.freeByEnd, rightEnd)
		newEnd = rightEnd
	}

	mergedSize := newEnd - newStart
	s.freeByStart[newStart] = mergedSize
	s.freeByEnd[newEnd] = mergedSize
}

type parquetAllocator struct {
	pool *Pool

	arenasMutex  sync.RWMutex
	arenas       []*arena
	arenaIndex   sync.Map
	nextAllocIdx atomic.Int32

	memUsage              int
	externalMemoryCurrent atomic.Int64
	externalMemoryMax     atomic.Int64
}

// NewParquetAllocator creates a new parquetAllocator with the given memory pool
func NewParquetAllocator(pool *Pool, memUsage int) *parquetAllocator {
	memUsage = min(memUsage, pool.limit)
	pool.Acquire(memUsage)
	alloc := &parquetAllocator{
		pool:     pool,
		memUsage: memUsage,
	}

	initialCount := max(memUsage/arenaSize, 1)
	for range initialCount {
		alloc.arenas = append(alloc.arenas, newArena(pool.Get()))
	}
	return alloc
}

func (a *parquetAllocator) Allocate(size int) []byte {
	if size >= arenaSize {
		buf := make([]byte, size)
		a.arenaIndex.Store(addressOf(buf), -1)

		// Not accurate but enough for estimation
		current := a.externalMemoryCurrent.Add(int64(size))
		if current > a.externalMemoryMax.Load() {
			a.externalMemoryMax.Store(current)
		}
		return buf
	}

	if buf := a.allocate(size); buf != nil {
		return buf
	}
	return a.getAndAllocate(size)
}

func (a *parquetAllocator) allocate(size int) []byte {
	a.arenasMutex.RLock()
	defer a.arenasMutex.RUnlock()

	arenaLen := len(a.arenas)
	idx := int(a.nextAllocIdx.Add(1)) % arenaLen
	for i := range arenaLen {
		sel := (idx + i) % arenaLen
		if buf := a.arenas[sel].allocate(size); buf != nil {
			a.arenaIndex.Store(addressOf(buf), sel)
			return buf
		}
	}

	return nil
}

func (a *parquetAllocator) getAndAllocate(size int) []byte {
	a.arenasMutex.Lock()
	defer a.arenasMutex.Unlock()

	arena := newArena(a.pool.Get())
	buf := arena.allocate(size)
	a.arenas = append(a.arenas, arena)
	a.arenaIndex.Store(addressOf(buf), len(a.arenas)-1)
	return buf
}

func (a *parquetAllocator) Free(buf []byte) {
	addr := addressOf(buf)
	v, ok := a.arenaIndex.Load(addr)
	if !ok {
		return
	}

	a.arenasMutex.RLock()
	defer a.arenasMutex.RUnlock()

	id, _ := v.(int)
	if id == -1 {
		a.externalMemoryCurrent.Add(-int64(len(buf)))
	} else {
		a.arenas[id].free(buf)
	}

	a.arenaIndex.Delete(addr)
}

func (a *parquetAllocator) Reallocate(size int, buf []byte) []byte {
	a.Free(buf)
	return a.Allocate(size)
}

func (a *parquetAllocator) Allocated() int {
	return arenaSize*len(a.arenas) + int(a.externalMemoryMax.Load())
}

func (a *parquetAllocator) Close() {
	for _, s := range a.arenas {
		a.pool.Put(s.buf)
	}
	a.pool.Release(a.memUsage)
}

func (a *parquetAllocator) Check() {
	for _, s := range a.arenas {
		intest.Assert(len(s.freeByEnd) == 1)
		intest.Assert(len(s.freeByStart) == 1)
		intest.Assert(len(s.allocated) == 0)
		intest.Assert(s.freeByEnd[arenaSize] == arenaSize)
		intest.Assert(s.freeByStart[0] == arenaSize)
	}
}
