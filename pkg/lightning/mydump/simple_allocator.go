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

	"github.com/pingcap/tidb/pkg/util/intest"
)

const (
	// Size of metadata of each block
	metaSize = 64
	invalid  = math.MaxInt32

	// The allocated memory size will be aligned to the nearest multiple of alignSize.
	// This value will be modifed in test
	alignSize = 16 << 10
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

func simpleGetAllocationSize(size int) int {
	return roundUp(size+metaSize, alignSize) * 2
}

/*
simpleAllocator is a very simple allocator with low allocation efficiency
which manages allocated memory using a linked list structure.

It is used in parquet reader and it's sufficient for our scenario
as memory allocation will not be a bottleneck.

The memory layout is as follows:

								  --------------------|
	                              |                   v
	  -------------------------------------------------------------------------
	  |                 | s | p | n | xxxx |          | s | p | n | xxxx |    |
	  -------------------------------------------------------------------------
	                    ^                                   |
						|___________________________________|
*/
type simpleAllocator struct {
	buf  []byte
	base int

	// Number of blocks and bytes allocated
	blocksAlloc int
	bytesAloc   int
}

func getSimpleAllocator(size int) arena {
	a := &simpleAllocator{}
	a.init(size)
	return a
}

func (sa *simpleAllocator) init(bufSize int) {
	sa.buf = make([]byte, bufSize)
	sa.base = int(addressOf(sa.buf))
	sa.reset()
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
	for offset := 0; offset != invalid; {
		if free > offset {
			_, _, blkSize := sa.getBlk(free)
			_, next, _ := sa.getBlk(offset)
			sa.setBlk(offset, -1, free, -1)
			sa.setBlk(free, offset, next, -1)
			sa.setBlk(next, free, -1, -1)
			sa.bytesAloc -= blkSize
			return
		}
	}
	panic("Error insertFree")
}

// Merge adjacent free blocks into one big free block to reduce fragmentation.
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
	if offset < 0 || offset > len(sa.buf) {
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

func (sa *simpleAllocator) reallocate(buf []byte, size int) []byte {
	sa.free(buf)
	return sa.allocate(size)
}

func (sa *simpleAllocator) allocated() int64 {
	return int64(sa.blocksAlloc)
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
		panic("sanity check failed")
	}
}

func (sa *simpleAllocator) reset() {
	sa.bytesAloc = 0

	// Add dummy head and tail block to simplify the allocation logic
	total := len(sa.buf)
	sa.setBlk(0, invalid, alignSize, 0)
	sa.setBlk(alignSize, 0, total-alignSize, total-alignSize*3)
	sa.setBlk(total-alignSize, alignSize, invalid, 0)
}
