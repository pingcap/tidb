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
)

type appendOnlySlice struct {
	buf     []byte
	offset  int
	counter int
	mu      sync.Mutex
}

func (s *appendOnlySlice) allocate(size int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.offset+size <= len(s.buf) {
		start := s.offset
		s.offset += size
		s.counter++
		return s.buf[start : start+size]
	}
	return nil
}

func (s *appendOnlySlice) free() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.counter > 0 {
		s.counter--
		if s.counter == 0 {
			s.offset = 0
		}
	}
}

type appendOnlyAllocator struct {
	pool        *Pool
	slices      []*appendOnlySlice
	slicesMutex sync.RWMutex
	mapper      sync.Map

	memUsage int

	nextAllocIdx atomic.Int32

	externalMemoryCurrent atomic.Int64
	externalMemoryMax     atomic.Int64
}

// NewAppendOnlyAllocator creates a new appendOnlyAllocator with the given memory pool
func NewAppendOnlyAllocator(pool *Pool, memUsage int) *appendOnlyAllocator {
	memUsage = min(memUsage, pool.limit)
	pool.Acquire(memUsage)
	alloc := &appendOnlyAllocator{
		pool:     pool,
		memUsage: memUsage,
	}
	for range 1 {
		alloc.slices = append(alloc.slices, &appendOnlySlice{buf: pool.Get()})
	}
	return alloc
}

func (a *appendOnlyAllocator) Allocate(size int) []byte {
	if size >= arenaSize {
		buf := make([]byte, size)
		a.mapper.Store(addressOf(buf), -1)

		// Many not accurate but enough for estimation
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

func (a *appendOnlyAllocator) allocate(size int) []byte {
	a.slicesMutex.RLock()
	defer a.slicesMutex.RUnlock()

	sliceLen := len(a.slices)
	idx := int(a.nextAllocIdx.Add(1)) % sliceLen
	for i := range sliceLen {
		sel := (idx + i) % sliceLen
		if buf := a.slices[sel].allocate(size); buf != nil {
			a.mapper.Store(addressOf(buf), sel)
			return buf
		}
	}

	return nil
}

func (a *appendOnlyAllocator) getAndAllocate(size int) []byte {
	a.slicesMutex.Lock()
	defer a.slicesMutex.Unlock()

	newSlice := &appendOnlySlice{buf: a.pool.Get()}
	buf := newSlice.allocate(size)
	a.slices = append(a.slices, newSlice)
	a.mapper.Store(addressOf(buf), len(a.slices)-1)
	return buf
}

func (a *appendOnlyAllocator) Free(buf []byte) {
	addr := addressOf(buf)
	v, ok := a.mapper.Load(addr)
	if !ok {
		return
	}

	a.slicesMutex.RLock()
	defer a.slicesMutex.RUnlock()

	id, _ := v.(int)
	if id == -1 {
		a.externalMemoryCurrent.Add(-int64(len(buf)))
	} else {
		a.slices[id].free()
	}

	a.mapper.Delete(addr)
}

func (a *appendOnlyAllocator) Reallocate(size int, buf []byte) []byte {
	a.Free(buf)
	return a.Allocate(size)
}

func (a *appendOnlyAllocator) Allocated() int {
	return arenaSize*len(a.slices) + int(a.externalMemoryMax.Load())
}

func (a *appendOnlyAllocator) Close() {
	for _, s := range a.slices {
		a.pool.Put(s.buf)
	}
	a.pool.Release(a.memUsage)
}

func (a *appendOnlyAllocator) check() {
	a.mapper.Range(func(key, value any) bool {
		panic("memory leak detected in appendOnlyAllocator")
	})
	for _, s := range a.slices {
		if s.counter != 0 {
			panic("memory leak detected in appendOnlyAllocator")
		}
	}
}
