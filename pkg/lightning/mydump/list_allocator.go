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
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pingcap/tidb/pkg/util/intest"
)

const (
	// metaSize is the size of metadata for each allocated block (64 bytes for alignment)
	metaSize = 64

	// invalid represents an invalid offset/pointer in the linked list
	invalid = math.MaxInt32

	// alignSize defines the alignment boundary for memory allocation.
	alignSize = 1 << 10
)

func roundUp(n, sz int) int {
	return (n + sz - 1) / sz * sz
}

func storeInt(value int, buf []byte) {
	buf[0] = byte(value >> 24)
	buf[1] = byte(value >> 16)
	buf[2] = byte(value >> 8)
	buf[3] = byte(value)
}

func readInt(buf []byte) int {
	return int(buf[0])<<24 | int(buf[1])<<16 | int(buf[2])<<8 | int(buf[3])
}

/*
simpleAllocator is a memory allocator that manages allocated memory using a linked list structure.
It provides basic memory allocation and deallocation with automatic merging of adjacent free blocks
to reduce fragmentation.

While this allocator has relatively low allocation efficiency due to its linear search algorithm,
it is sufficient for parquet reader scenarios where memory allocation is not the primary bottleneck.

Memory Layout:
Each block has the following structure:
- Size (4 bytes): Size of the block including metadata
- Previous offset (4 bytes): Offset to the previous block in the free list
- Next offset (4 bytes): Offset to the next block in the free list
- Reserved (52 bytes): Reserved space for alignment
- Data: The actual allocated data follows the metadata

The allocator maintains a linked list of free blocks:

	            ┌───────────────────────────┐
	            │                           ▼
	┌─────────────────────────────────────────────────────────────────┐
	│         │ s │ p │ n │ xxxx │          │ s │ p │ n │ xxxx │      │
	└─────────────────────────────────────────────────────────────────┘
	          ▲                                   │
	          └───────────────────────────────────┘

Where:
- s = size of block
- p = previous block offset
- n = next block offset
- xxxx = reserved space
*/
type simpleAllocator struct {
	buf  []byte
	base int

	// Number of blocks and bytes allocated
	blocksAlloc int
	bytesAloc   int
}

func getSimpleAllocator(buf []byte) arena {
	a := &simpleAllocator{
		buf:  buf,
		base: int(addressOf(buf)),
	}
	a.reset()
	return a
}

func (sa *simpleAllocator) getOffset(buf []byte) int {
	return int(addressOf(buf)) - sa.base - metaSize
}

func (sa *simpleAllocator) setBlk(offset, prev, next, blkSize int) {
	if blkSize >= 0 {
		storeInt(blkSize, sa.buf[offset:offset+4])
	}
	if prev >= 0 {
		storeInt(prev, sa.buf[offset+4:offset+8])
	}
	if next >= 0 {
		storeInt(next, sa.buf[offset+8:offset+12])
	}
}

func (sa *simpleAllocator) getBlk(offset int) (prev, next, blkSize int) {
	blkSize = readInt(sa.buf[offset : offset+4])
	prev = readInt(sa.buf[offset+4 : offset+8])
	next = readInt(sa.buf[offset+8 : offset+12])
	return
}

func (sa *simpleAllocator) insertFree(free int) {
	_, _, freeSize := sa.getBlk(free)

	for offset := 0; offset != invalid; {
		if free > offset {
			_, next, _ := sa.getBlk(offset)
			sa.setBlk(offset, -1, free, -1)
			sa.setBlk(free, offset, next, -1)
			sa.setBlk(next, free, -1, -1)
			sa.bytesAloc -= freeSize
			return
		}
	}
	panic("Error insertFree")
}

// merge coalesces adjacent free blocks into larger blocks to reduce fragmentation.
func (sa *simpleAllocator) merge() {
	for offset := 0; offset != invalid; {
		_, next, blkSize := sa.getBlk(offset)
		if offset+blkSize == next {
			_, nextnext, nextBlkSize := sa.getBlk(next)
			sa.setBlk(offset, -1, nextnext, blkSize+nextBlkSize)
			sa.setBlk(nextnext, offset, -1, -1)
		} else {
			offset = next
		}
	}
}

func (sa *simpleAllocator) allocate(size int) []byte {
	sa.merge()

	allocSize := roundUp(size+metaSize, alignSize)

	bestOffset := -1
	minRemain := math.MaxInt32

	for offset := 0; offset != invalid; {
		_, next, blkSize := sa.getBlk(offset)
		if offset+blkSize >= len(sa.buf) {
			panic("Error blk size")
		}
		if blkSize >= allocSize && blkSize-allocSize < minRemain {
			bestOffset = offset
			minRemain = blkSize - allocSize
			if minRemain == 0 {
				break
			}
		}
		offset = next
	}

	if bestOffset == -1 {
		return nil
	}

	if minRemain == 0 {
		prev, next, _ := sa.getBlk(bestOffset)
		sa.setBlk(prev, -1, next, -1)
		sa.setBlk(next, prev, -1, -1)
	} else {
		sa.setBlk(bestOffset, -1, -1, minRemain)
	}

	sa.blocksAlloc++
	sa.bytesAloc += allocSize
	bufStart := bestOffset + minRemain
	sa.setBlk(bufStart, -1, -1, allocSize)
	sa.sanityCheck()
	return sa.buf[bufStart+metaSize : bufStart+metaSize+size]
}

func (sa *simpleAllocator) free(buf []byte) {
	offset := sa.getOffset(buf)
	if offset < 0 || offset >= len(sa.buf) {
		return
	}

	sa.blocksAlloc--
	if sa.blocksAlloc == 0 {
		sa.reset()
		return
	}

	sa.insertFree(offset)
	sa.sanityCheck()
}

func (sa *simpleAllocator) sanityCheck() {
	if !intest.InTest {
		return
	}

	mem := sa.bytesAloc
	for offset := 0; offset != invalid; {
		_, next, blkSize := sa.getBlk(offset)
		mem += blkSize
		offset = next
	}
	if mem != (len(sa.buf) - 3*alignSize) {
		panic("sanity check failed: memory accounting mismatch")
	}
}

func (sa *simpleAllocator) reset() {
	sa.blocksAlloc = 0
	sa.bytesAloc = 0

	total := len(sa.buf)
	sa.setBlk(0, invalid, alignSize, 0)
	sa.setBlk(alignSize, 0, total-alignSize, total-alignSize*3)
	sa.setBlk(total-alignSize, alignSize, invalid, 0)
}

// listAllocator implements memory.Allocator interface using multiple arenas.
type listAllocator struct {
	mu sync.RWMutex

	arenas []arena
	mbufs  [][]byte
	pool   *Pool

	allocatedBuf map[uintptr]int
}

func (alloc *listAllocator) Allocate(size int) []byte {
	if size >= arenaSize {
		return make([]byte, size)
	}

	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	for i, a := range alloc.arenas {
		if buf := a.allocate(size); buf != nil {
			alloc.allocatedBuf[addressOf(buf)] = i
			return buf
		}
	}

	mbuf := alloc.pool.Get()
	alloc.mbufs = append(alloc.mbufs, mbuf)

	na := getSimpleAllocator(mbuf)
	alloc.arenas = append(alloc.arenas, na)

	buf := na.allocate(size)
	arenaIndex := len(alloc.arenas)
	alloc.allocatedBuf[addressOf(buf)] = arenaIndex

	return buf
}

func (alloc *listAllocator) Free(buf []byte) {
	addr := addressOf(buf)
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	if arenaID, ok := alloc.allocatedBuf[addr]; ok {
		alloc.arenas[arenaID].free(buf)
		delete(alloc.allocatedBuf, addr)
	}
}

func (alloc *listAllocator) Reallocate(size int, buf []byte) []byte {
	alloc.Free(buf)
	return alloc.Allocate(size)
}

func (alloc *listAllocator) Close() {
	for _, mbuf := range alloc.mbufs {
		alloc.pool.Put(mbuf)
	}
}

func (alloc *listAllocator) Allocated() int {
	return arenaSize * len(alloc.arenas)
}

// NewAllocator creates a new default allocator with the given pool.
func NewAllocator(pool *Pool) memory.Allocator {
	return &listAllocator{
		pool:         pool,
		allocatedBuf: make(map[uintptr]int, 32),
	}
}
