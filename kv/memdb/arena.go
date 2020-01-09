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

type arenaAddr struct {
	blockIdx    uint32
	blockOffset uint32
}

func (addr arenaAddr) isNull() bool {
	return addr.blockIdx == 0 && addr.blockOffset == 0
}

func newArenaAddr(idx int, offset uint32) arenaAddr {
	return arenaAddr{
		blockIdx:    uint32(idx) + 1,
		blockOffset: offset,
	}
}

const (
	nullBlockOffset = math.MaxUint32
	maxBlockSize    = 128 << 20
)

type arena struct {
	blockSize int
	availIdx  int
	blocks    []arenaBlock
}

func newArenaLocator(initBlockSize int) *arena {
	return &arena{
		blockSize: initBlockSize,
		blocks:    []arenaBlock{newArenaBlock(initBlockSize)},
	}
}

func (a *arena) getFrom(addr arenaAddr) []byte {
	return a.blocks[addr.blockIdx-1].getFrom(addr.blockOffset)
}

func (a *arena) alloc(size int) (arenaAddr, []byte) {
	if size >= maxBlockSize {
		// Use a separate block to store entry which size larger than specified block size.
		blk := newArenaBlock(size)
		blk.length = size
		a.blocks = append(a.blocks, blk)

		addr := newArenaAddr(len(a.blocks)-1, 0)
		return addr, blk.buf
	}

	addr, data := a.allocInBlock(a.availIdx, size)
	if !addr.isNull() {
		return addr, data
	}

	a.enlarge(size)
	return a.allocInBlock(a.availIdx, size)
}

func (a *arena) enlarge(size int) {
	a.blockSize <<= 1
	for a.blockSize <= size {
		a.blockSize <<= 1
	}
	// Size always less than maxBlockSize.
	if a.blockSize > maxBlockSize {
		a.blockSize = maxBlockSize
	}
	a.blocks = append(a.blocks, newArenaBlock(a.blockSize))
	a.availIdx = int(uint32(len(a.blocks) - 1))
}

func (a *arena) allocInBlock(idx, size int) (arenaAddr, []byte) {
	offset, data := a.blocks[idx].alloc(size)
	if offset == nullBlockOffset {
		return arenaAddr{}, nil
	}
	return newArenaAddr(idx, offset), data
}

func (a *arena) reset() {
	a.availIdx = 0
	a.blockSize = len(a.blocks[0].buf)
	a.blocks = []arenaBlock{a.blocks[0]}
	a.blocks[0].reset()
}

type arenaBlock struct {
	buf    []byte
	length int
}

func newArenaBlock(blockSize int) arenaBlock {
	return arenaBlock{
		buf: make([]byte, blockSize),
	}
}

func (a *arenaBlock) getFrom(offset uint32) []byte {
	return a.buf[offset:]
}

func (a *arenaBlock) alloc(size int) (uint32, []byte) {
	offset := a.length
	newLen := offset + size
	if newLen > len(a.buf) {
		return nullBlockOffset, nil
	}
	a.length = newLen
	return uint32(offset), a.buf[offset : offset+size]
}

func (a *arenaBlock) reset() {
	a.length = 0
}
