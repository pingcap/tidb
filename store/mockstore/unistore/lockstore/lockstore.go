// Copyright 2019-present PingCAP, Inc.
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

package lockstore

import (
	"bytes"
	"math"
	"math/rand"
	"sync/atomic"
	"time"
	"unsafe"
)

// MemStore is a skiplist variant used to store lock.
// Compares to normal skip list, it only supports single thread write.
// But it can reuse the memory, so that the memory usage doesn't keep growing.
type MemStore struct {
	height   int32 // Current height. 1 <= height <= maxHeight.
	head     *node
	arenaPtr unsafe.Pointer

	// We only consume 2 bits for a random height call.
	rand   rand.Source64
	length int
}

const (
	maxHeight      = 16
	nodeHeaderSize = int(unsafe.Sizeof(nodeHeader{addr: 0, height: 0, keyLen: 0, valLen: 0}))
)

type nodeHeader struct {
	addr   arenaAddr
	height uint16
	keyLen uint16
	valLen uint32
}

type node struct {
	nodeHeader
	// Height of the nexts.

	// node is a variable length struct.
	// The nextsBase is the first element of nexts slice,
	// it act as the base pointer we do pointer arithmetic in `next` and `setNext`.
	nextsBase uint64
}

type entry struct {
	*node
	key []byte
}

func (e *entry) getValue(arena *arena) []byte {
	if e.node != nil {
		return e.node.getValue(arena)
	}
	return nil
}

func (n *node) setNexts(level int, val uint64) {
	*n.nextsAddr(level) = val
}

func (n *node) nextsAddr(idx int) *uint64 {
	offset := uintptr(idx) * unsafe.Sizeof(n.nextsBase)
	return (*uint64)(unsafe.Pointer(uintptr(unsafe.Pointer(&n.nextsBase)) + offset))
}

func (n *node) getNextAddr(level int) arenaAddr {
	return arenaAddr(atomic.LoadUint64(n.nextsAddr(level)))
}

func (n *node) setNextAddr(level int, addr arenaAddr) {
	atomic.StoreUint64(n.nextsAddr(level), uint64(addr))
}

func (n *node) nodeLen() int {
	return int(n.height)*8 + nodeHeaderSize
}

func (n *node) getKey(a *arena) []byte {
	nodeLen := n.nodeLen()
	entryData := a.get(n.addr, nodeLen+int(n.keyLen))
	return entryData[nodeLen:]
}

func (n *node) getValue(a *arena) []byte {
	nodeLenKeyLen := n.nodeLen() + int(n.keyLen)
	entryData := a.get(n.addr, nodeLenKeyLen+int(n.valLen))
	return entryData[nodeLenKeyLen:]
}

// NewMemStore returns a new mem store.
func NewMemStore(arenaBlockSize int) *MemStore {
	ls := &MemStore{
		height:   1,
		arenaPtr: unsafe.Pointer(newArenaLocator(arenaBlockSize)),
		rand:     rand.NewSource(time.Now().Unix()).(rand.Source64),
	}
	ls.setHeadNode()
	return ls
}

func (ls *MemStore) setHeadNode() {
	n := ls.newNode(ls.getArena(), nil, nil, maxHeight)
	for i := 0; i < maxHeight; i++ {
		n.setNexts(i, 0)
	}
	ls.head = n
}

func (ls *MemStore) getHeight() int {
	return int(atomic.LoadInt32(&ls.height))
}

func (ls *MemStore) setHeight(height int) {
	atomic.StoreInt32(&ls.height, int32(height))
}

// Get gets a value with the key.
func (ls *MemStore) Get(key, buf []byte) []byte {
	e, match := ls.findGreater(key, true)
	if !match {
		return nil
	}
	e.getValue(ls.getArena())
	return append(buf[:0], e.getValue(ls.getArena())...)
}

func (n *node) getNextNode(arena *arena, level int) *node {
	addr := n.getNextAddr(level)
	if addr == nullArenaAddr {
		return nil
	}
	data := arena.get(addr, nodeHeaderSize)
	return (*node)(unsafe.Pointer(&data[0]))
}

func (ls *MemStore) getNext(n *node, level int) (e entry) {
	addr := n.getNextAddr(level)
	if addr == nullArenaAddr {
		return
	}
	arena := ls.getArena()
	data := arena.get(addr, nodeHeaderSize)
	e.node = (*node)(unsafe.Pointer(&data[0]))
	e.key = e.node.getKey(arena)
	return e
}

func (ls *MemStore) findGreater(key []byte, allowEqual bool) (entry, bool) {
	var prev entry
	prev.node = ls.head
	level := ls.getHeight() - 1
	for {
		var next entry
		addr := prev.getNextAddr(level)
		if addr != nullArenaAddr {
			arena := ls.getArena()
			data := arena.get(addr, nodeHeaderSize)
			next.node = (*node)(unsafe.Pointer(&data[0]))
			next.key = next.node.getKey(arena)
			cmp := bytes.Compare(next.key, key)
			if cmp < 0 {
				// next key is still smaller, keep moving.
				prev = next
				continue
			}
			if cmp == 0 {
				// prev.key < key == next.key.
				if allowEqual {
					return next, true
				}
				level = 0
				prev = next
				continue
			}
		}
		// next is greater than key or next is nil. go to the lower level.
		if level > 0 {
			level--
			continue
		}
		return next, false
	}
}

func (ls *MemStore) findLess(key []byte, allowEqual bool) (entry, bool) {
	var prev entry
	prev.node = ls.head
	level := ls.getHeight() - 1
	for {
		next := ls.getNext(prev.node, level)
		if next.node != nil {
			cmp := bytes.Compare(key, next.key)
			if cmp > 0 {
				// prev.key < next.key < key. We can continue to move right.
				prev = next
				continue
			}
			if cmp == 0 && allowEqual {
				// prev.key < key == next.key.
				return next, true
			}
		}
		// get closer to the key in the lower level.
		if level > 0 {
			level--
			continue
		}
		break
	}
	// We are not going to return head.
	if prev.node == ls.head {
		return entry{}, false
	}
	return prev, false
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key < key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return true.
func (ls *MemStore) findSpliceForLevel(arena *arena, key []byte, before *node, level int) (*node, *node, bool) {
	for {
		// Assume before.key < key.
		nextAddr := before.getNextAddr(level)
		if nextAddr == nullArenaAddr {
			return before, nil, false
		}
		data := arena.get(nextAddr, nodeHeaderSize)
		next := (*node)(unsafe.Pointer(&data[0]))
		nextKey := next.getKey(arena)
		cmp := bytes.Compare(nextKey, key)
		if cmp >= 0 {
			// before.key < key < next.key. We are done for this level.
			return before, next, cmp == 0
		}
		before = next // Keep moving right on this level.
	}
}

// findLast returns the last element. If head (empty ls), we return nil. All the find functions
// will NEVER return the head nodes.
func (ls *MemStore) findLast() entry {
	var e entry
	e.node = ls.head
	level := ls.getHeight() - 1
	for {
		next := ls.getNext(e.node, level)
		if next.node != nil {
			e = next
			continue
		}
		if level == 0 {
			if e.node == ls.head {
				return entry{}
			}
			return e
		}
		level--
	}
}

func (ls *MemStore) getNode(arena *arena, addr arenaAddr) *node {
	data := arena.get(addr, nodeHeaderSize)
	return (*node)(unsafe.Pointer(&data[0]))
}

// Put puts the key-value pair, returns true if the key doesn't exist.
func (ls *MemStore) Put(key []byte, v []byte) bool {
	return ls.PutWithHint(key, v, nil)
}

// PutWithHint puts the key-value pair, returns true if the key doesn't exist.
func (ls *MemStore) PutWithHint(key []byte, v []byte, hint *Hint) bool {
	arena := ls.getArena()
	lsHeight := ls.getHeight()
	if hint == nil {
		hint = new(Hint)
	}
	recomputeHeight := ls.calculateRecomputeHeight(key, hint, lsHeight)
	var old *node
	if recomputeHeight > 0 {
		for i := recomputeHeight - 1; i >= 0; i-- {
			// Use higher level to speed up for current level.
			var exists bool
			hint.prev[i], hint.next[i], exists = ls.findSpliceForLevel(arena, key, hint.prev[i+1], i)
			if exists {
				old = hint.next[i]
			}
		}
	} else {
		if hint.next[0] != nil && bytes.Equal(key, hint.next[0].getKey(arena)) {
			old = hint.next[0]
		}
	}

	if old != nil {
		ls.replace(key, v, hint, old)
		return false
	}
	height := ls.randomHeight()
	x := ls.newNode(arena, key, v, height)
	if height > lsHeight {
		ls.setHeight(height)
	}

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < height; i++ {
		if hint.next[i] != nil {
			x.setNexts(i, uint64(hint.next[i].addr))
		} else {
			x.setNexts(i, uint64(nullArenaAddr))
		}
		if hint.prev[i] == nil {
			hint.prev[i] = ls.head
		}
		hint.prev[i].setNextAddr(i, x.addr)
		hint.prev[i] = x
	}
	ls.length++
	return true
}

func (ls *MemStore) replace(key, v []byte, hint *Hint, old *node) {
	x := ls.newNode(ls.getArena(), key, v, int(old.height))
	arena := ls.getArena()
	for i := 0; i < int(old.height); i++ {
		nextAddr := atomic.LoadUint64(old.nextsAddr(i))
		x.setNexts(i, nextAddr)
		if nextAddr != uint64(nullArenaAddr) {
			hint.next[i] = ls.getNode(arena, arenaAddr(nextAddr))
		} else {
			hint.next[i] = nil
		}
		hint.prev[i].setNextAddr(i, x.addr)
		hint.prev[i] = x
	}
	ls.getArena().free(old.addr)
}

func (ls *MemStore) newNode(arena *arena, key []byte, v []byte, height int) *node {
	// The base level is already allocated in the node struct.
	nodeSize := nodeHeaderSize + height*8 + len(key) + len(v)
	addr := arena.alloc(nodeSize)
	if addr == nullArenaAddr {
		arena = arena.grow()
		ls.setArena(arena)
		// The new arena block must have enough memory to alloc.
		addr = arena.alloc(nodeSize)
	}
	data := arena.get(addr, nodeSize)
	node := (*node)(unsafe.Pointer(&data[0]))
	node.addr = addr
	node.keyLen = uint16(len(key))
	node.height = uint16(height)
	node.valLen = uint32(len(v))
	copy(data[node.nodeLen():], key)
	copy(data[node.nodeLen()+int(node.keyLen):], v)
	return node
}

func (ls *MemStore) getArena() *arena {
	return (*arena)(atomic.LoadPointer(&ls.arenaPtr))
}

func (ls *MemStore) setArena(al *arena) {
	atomic.StorePointer(&ls.arenaPtr, unsafe.Pointer(al))
}

func (ls *MemStore) randomHeight() int {
	h := 1
	for h < maxHeight && ls.rand.Uint64() < uint64(math.MaxUint64)/4 {
		h++
	}
	return h
}

func (ls *MemStore) calculateRecomputeHeight(key []byte, hint *Hint, listHeight int) int {
	recomputeHeight := 0
	arena := ls.getArena()
	if hint.height < int32(listHeight) {
		// Either splice is never used or list height has grown, we recompute all.
		hint.prev[listHeight] = ls.head
		hint.next[listHeight] = nil
		hint.height = int32(listHeight)
		recomputeHeight = listHeight
	} else {
		for recomputeHeight < listHeight {
			prevNode := hint.prev[recomputeHeight]
			nextNode := hint.next[recomputeHeight]
			prevNext := prevNode.getNextNode(arena, recomputeHeight)
			if prevNext != nextNode {
				recomputeHeight++
				continue
			}
			keyBeforePrev := prevNode != ls.head && prevNode != nil && bytes.Compare(key, prevNode.getKey(arena)) <= 0
			if keyBeforePrev {
				for prevNode == hint.prev[recomputeHeight] {
					recomputeHeight++
				}
				continue
			}
			keyAfterNext := nextNode != nil && bytes.Compare(key, nextNode.getKey(arena)) > 0
			if keyAfterNext {
				for nextNode == hint.next[recomputeHeight] {
					recomputeHeight++
				}
				continue
			}
			break
		}
	}
	return recomputeHeight
}

// DeleteWithHint deletes a value with the key and hint.
func (ls *MemStore) DeleteWithHint(key []byte, hint *Hint) bool {
	listHeight := ls.getHeight()
	if hint == nil {
		hint = new(Hint)
	}
	recomputeHeight := ls.calculateRecomputeHeight(key, hint, listHeight)
	arena := ls.getArena()
	var keyNode *node
	if recomputeHeight > 0 {
		for i := recomputeHeight - 1; i >= 0; i-- {
			// Use higher level to speed up for current level.
			var match bool
			hint.prev[i], hint.next[i], match = ls.findSpliceForLevel(arena, key, hint.prev[i+1], i)
			if match {
				keyNode = hint.next[i]
			}
		}
	} else {
		if hint.next[0] != nil && bytes.Equal(key, hint.next[0].getKey(arena)) {
			keyNode = hint.next[0]
		}
	}
	if keyNode == nil {
		return false
	}
	for i := int(keyNode.height) - 1; i >= 0; i-- {
		// Change the nexts from higher to lower, so the data is consistent at any point.
		addr := keyNode.getNextAddr(i)
		if addr != nullArenaAddr {
			hint.next[i] = ls.getNode(arena, addr)
		} else {
			hint.next[i] = nil
		}
		hint.prev[i].setNextAddr(i, keyNode.getNextAddr(i))
	}
	arena.free(keyNode.addr)
	ls.length--
	return true
}

// Delete deletes a value with the key.
func (ls *MemStore) Delete(key []byte) bool {
	return ls.DeleteWithHint(key, nil)
}

// Len returns the length of a mem store.
func (ls *MemStore) Len() int {
	return ls.length
}

// Hint represents a hint.
type Hint struct {
	height int32
	prev   [maxHeight + 1]*node
	next   [maxHeight + 1]*node
}
