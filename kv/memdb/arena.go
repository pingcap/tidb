// Copyright 2019 PingCAP, Inc.
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

package memdb

import "math"

type arenaAddr uint64

const (
	alignMask                 = 1<<32 - 8 // 29 bit 1 and 3 bit 0.
	nullBlockOffset           = math.MaxUint32
	maxBlockSize              = 128 << 20
	nullArenaAddr   arenaAddr = 0
)

func (addr arenaAddr) blockIdx() int {
	return int(addr>>32 - 1)
}

func (addr arenaAddr) blockOffset() uint32 {
	return uint32(addr)
}

func newArenaAddr(blockIdx int, blockOffset uint32) arenaAddr {
	return arenaAddr(uint64(blockIdx+1)<<32 | uint64(blockOffset))
}

type arena struct {
	blockSize int
	availIdx  int
	blocks    []*arenaBlock
}

func newArenaLocator(initBlockSize int) *arena {
	return &arena{
		blockSize: initBlockSize,
		blocks:    []*arenaBlock{newArenaBlock(initBlockSize)},
	}
}

func (a *arena) getFrom(addr arenaAddr) []byte {
	return a.blocks[addr.blockIdx()].getFrom(addr.blockOffset())
}

func (a *arena) alloc(size int) (arenaAddr, []byte) {
	if size > a.blockSize {
		// Use a separate block to store entry which size larger than specified block size.
		blk := newArenaBlock(size)
		addr := newArenaAddr(len(a.blocks), 0)
		a.blocks = append(a.blocks, blk)
		return addr, blk.buf
	}

	for {
		block := a.blocks[a.availIdx]
		blockOffset := block.alloc(size)
		if blockOffset != nullBlockOffset {
			return newArenaAddr(a.availIdx, blockOffset), block.buf[blockOffset : int(blockOffset)+size]
		}

		blockSize := a.blockSize << 1
		if blockSize <= maxBlockSize {
			a.blockSize = blockSize
		}
		a.blocks = append(a.blocks, newArenaBlock(a.blockSize))
		a.availIdx = len(a.blocks) - 1
	}
}

func (a *arena) reset() {
	a.availIdx = 0
	a.blockSize = len(a.blocks[0].buf)
	a.blocks = a.blocks[:1]
	a.blocks[0].reset()
}

type arenaBlock struct {
	buf    []byte
	ref    uint64
	length int
}

func newArenaBlock(blockSize int) *arenaBlock {
	return &arenaBlock{
		buf: make([]byte, blockSize),
	}
}

func (a *arenaBlock) getFrom(offset uint32) []byte {
	return a.buf[offset:]
}

func (a *arenaBlock) alloc(size int) uint32 {
	// The returned addr should be aligned in 8 bytes.
	offset := (a.length + 7) & alignMask
	a.length = offset + size
	if a.length > len(a.buf) {
		return nullBlockOffset
	}
	a.ref++
	return uint32(offset)
}

func (a *arenaBlock) reset() {
	a.buf = a.buf[:0]
	a.ref = 0
	a.length = 0
}
