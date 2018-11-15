// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package latch

import (
	"sync"
	"unsafe"
)

const blockSize = 1 * 1024 * 1024

type block [blockSize]byte

const metaSize = unsafe.Sizeof(meta{})

// meta is allocated from block.
type meta struct {
	offset int
	minTS  uint64
}

func newBlock(currentTS uint64) *block {
	b := new(block)
	m := b.meta()
	m.offset = int(metaSize)
	m.minTS = currentTS
	return b
}

func (b *block) meta() *meta {
	return (*meta)(unsafe.Pointer(b))
}

func (b *block) alloc(size int) []byte {
	m := b.meta()
	if m.offset+size >= blockSize {
		return nil
	}

	start := m.offset
	m.offset += size
	return (*b)[start:m.offset]
}

// allocator is a customized allocator for latch.
// The allocated memory is volatile.
// The allocator can not allocate data larger than the block size.
type allocator struct {
	mu        sync.Mutex
	blocks    []*block
	currentTS uint64
}

func newAllocator() *allocator {
	a := &allocator{}
	b := newBlock(a.currentTS)
	a.blocks = append(a.blocks, b)
	return a
}

var gAlloc = newAllocator()

// Alloc returns the allocated buffer with a timestamp.
func (a *allocator) Alloc(size int) ([]byte, uint64) {
	const dataSize = blockSize - metaSize
	if size >= int(dataSize) {
		panic("alloc fail")
	}

	// It's a pity this is not a lock-free allocator.
	a.mu.Lock()
	defer a.mu.Unlock()

	a.currentTS++
	b := a.blocks[len(a.blocks)-1]
	ret := b.alloc(size)
	if ret == nil {
		b = newBlock(a.currentTS)
		a.blocks = append(a.blocks, b)
		ret = b.alloc(size)
	}

	return ret, a.currentTS
}

// GC releases objects that are created before safeTS.
// After this function returns, objects create during [safeTS, MaxUint64) is safe.
func (a *allocator) GC(safeTS uint64) {
	i := 0
	a.currentTS++
	for ; i < len(a.blocks)-1; i++ {
		b := a.blocks[i+1]
		m := b.meta()
		if safeTS < m.minTS {
			break
		}
		// Delete block to free memory.
		a.blocks[i] = nil
	}
	a.blocks = a.blocks[i:]
}
