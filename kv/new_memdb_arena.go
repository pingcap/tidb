// Copyright 2020 PingCAP, Inc.
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

package kv

import (
	"encoding/binary"
	"math"
)

var (
	nullAddr = memdbArenaAddr{math.MaxUint32, math.MaxUint32}
	endian   = binary.LittleEndian
)

type memdbArenaAddr struct {
	idx uint32
	off uint32
}

func (addr memdbArenaAddr) isNull() bool {
	return addr == nullAddr
}

// store and load is used by vlog, due to pointer in vlog is not aligned.

func (addr memdbArenaAddr) store(dst []byte) {
	panic("todo")
}

func (addr *memdbArenaAddr) load(src []byte) {
	panic("todo")
}

type memdbArena struct {
	blockSize int
	blocks    []memdbArenaBlock
}

func (a *memdbArena) alloc(size int, align bool) (memdbArenaAddr, []byte) {
	panic("todo")
}

func (a *memdbArena) enlarge(allocSize, blockSize int) {
	panic("todo")
}

func (a *memdbArena) allocInLastBlock(size int, align bool) (memdbArenaAddr, []byte) {
	panic("todo")
}

func (a *memdbArena) reset() {
	panic("todo")
}

type memdbArenaBlock struct {
	buf    []byte
	length int
}

func (a *memdbArenaBlock) alloc(size int, align bool) (uint32, []byte) {
	panic("todo")
}

func (a *memdbArenaBlock) reset() {
	panic("todo")
}

type memdbCheckpoint struct {
	blockSize     int
	blocks        int
	offsetInBlock int
}

func (cp *memdbCheckpoint) isSamePosition(other *memdbCheckpoint) bool {
	panic("todo")
}

func (a *memdbArena) checkpoint() memdbCheckpoint {
	panic("todo")
}

func (a *memdbArena) truncate(snap *memdbCheckpoint) {
	panic("todo")
}

type nodeAllocator struct {
	memdbArena

	// Dummy node, so that we can make X.left.up = X.
	// We then use this instead of NULL to mean the top or bottom
	// end of the rb tree. It is a black node.
	nullNode memdbNode
}

func (a *nodeAllocator) init() {
	panic("todo")
}

func (a *nodeAllocator) getNode(addr memdbArenaAddr) *memdbNode {
	if addr.isNull() {
		return &a.nullNode
	}

	panic("todo")
}

func (a *nodeAllocator) allocNode(key Key) (memdbArenaAddr, *memdbNode) {
	panic("todo")
}

func (a *nodeAllocator) freeNode(addr memdbArenaAddr) {}

func (a *nodeAllocator) reset() {
	panic("todo")
}

type memdbVlog struct {
	memdbArena
}

const memdbVlogHdrSize = 8 + 8 + 4

type memdbVlogHdr struct {
	nodeAddr memdbArenaAddr
	oldValue memdbArenaAddr
	valueLen uint32
}

func (l *memdbVlog) appendValue(nodeAddr memdbArenaAddr, oldValue memdbArenaAddr, value []byte) memdbArenaAddr {
	panic("todo")
}

func (l *memdbVlog) getValue(addr memdbArenaAddr) []byte {
	panic("todo")
}

func (l *memdbVlog) revertToCheckpoint(db *memdb, cp *memdbCheckpoint) {
	panic("todo")
}

func (l *memdbVlog) inspectKVInLog(db *memdb, head, tail *memdbCheckpoint, f func(Key, NewKeyFlags, []byte)) {
	panic("todo")
}

func (l *memdbVlog) canModify(cp *memdbCheckpoint, addr memdbArenaAddr) bool {
	panic("todo")
}
