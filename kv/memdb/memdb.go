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

import (
	"bytes"
	"math"
	"unsafe"

	"github.com/pingcap/tidb/util/fastrand"
)

const (
	maxHeight      = 16
	nodeHeaderSize = int(unsafe.Sizeof(nodeHeader{}))
)

// DB is an in-memory key/value database.
type DB struct {
	height int
	head   nodeWithAddr
	arena  *arena

	length int
	size   int
}

// New creates a new initialized in-memory key/value DB.
// The initBlockSize is the size of first block.
// This DB is append-only, deleting an entry would remove entry node but not
// reclaim KV buffer.
func New(initBlockSize int) *DB {
	db := &DB{
		height: 1,
		arena:  newArenaLocator(initBlockSize),
	}
	db.setHeadNode()
	return db
}

// Reset resets the DB to initial empty state.
// Release all blocks except the initial one.
func (db *DB) Reset() {
	db.height = 1
	db.length = 0
	db.size = 0
	db.arena.reset()
	db.setHeadNode()
}

func (db *DB) setHeadNode() {
	n, _ := db.newNode(db.arena, nil, nil, maxHeight)
	for i := 0; i < maxHeight; i++ {
		n.setNexts(i, arenaAddr{})
	}
	db.head.node = n
}

// Get gets the value for the given key. It returns nil if the
// DB does not contain the key.
func (db *DB) Get(key []byte) []byte {
	node, data, match := db.findGreaterEqual(key)
	if !match {
		return nil
	}
	return node.getValue(data)
}

// Put sets the value for the given key.
// It overwrites any previous value for that key.
func (db *DB) Put(key []byte, v []byte) bool {
	arena := db.arena
	lsHeight := db.height
	var prev [maxHeight + 1]nodeWithAddr
	var next [maxHeight + 1]nodeWithAddr
	prev[lsHeight] = db.head

	var exists bool
	for i := lsHeight - 1; i >= 0; i-- {
		// Use higher level to speed up for current level.
		prev[i], next[i], exists = db.findSpliceForLevel(db.arena, key, prev[i+1], i)
	}

	var height int
	if !exists {
		height = db.randomHeight()
	} else {
		height = db.prepareOverwrite(next[:])
	}

	x, addr := db.newNode(arena, key, v, height)
	if height > lsHeight {
		db.height = height
	}

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < height; i++ {
		x.setNexts(i, next[i].addr)
		if prev[i].node == nil {
			prev[i] = db.head
		}
		prev[i].setNexts(i, addr)
	}

	x.prev = prev[0].addr
	if next[0].node != nil {
		next[0].prev = addr
	}

	db.length++
	db.size += len(key) + len(v)
	return true
}

// The pointers in findSpliceForLevel may point to the node which going to be overwrite,
// prepareOverwrite update them to point to the next node, so we can link new node with the list correctly.
func (db *DB) prepareOverwrite(next []nodeWithAddr) int {
	old := next[0]

	// Update necessary states.
	db.size -= int(old.valLen) + int(old.keyLen)
	db.length--

	height := int(old.height)
	for i := 0; i < height; i++ {
		if next[i].addr == old.addr {
			next[i].addr = old.nexts(i)
			if !next[i].addr.isNull() {
				data := db.arena.getFrom(next[i].addr)
				next[i].node = (*node)(unsafe.Pointer(&data[0]))
			}
		}
	}
	return height
}

// Delete deletes the value for the given key.
// It returns false if the DB does not contain the key.
func (db *DB) Delete(key []byte) bool {
	listHeight := db.height
	var prev [maxHeight + 1]nodeWithAddr
	prev[listHeight] = db.head

	var keyNode nodeWithAddr
	var match bool
	for i := listHeight - 1; i >= 0; i-- {
		prev[i], keyNode, match = db.findSpliceForLevel(db.arena, key, prev[i+1], i)
	}
	if !match {
		return false
	}

	for i := int(keyNode.height) - 1; i >= 0; i-- {
		prev[i].setNexts(i, keyNode.nexts(i))
	}
	nextAddr := keyNode.nexts(0)
	if !nextAddr.isNull() {
		nextData := db.arena.getFrom(nextAddr)
		next := (*node)(unsafe.Pointer(&nextData[0]))
		next.prev = prev[0].addr
	}

	db.length--
	db.size -= int(keyNode.keyLen) + int(keyNode.valLen)
	return true
}

// Len returns the number of entries in the DB.
func (db *DB) Len() int {
	return db.length
}

// Size returns sum of keys and values length. Note that deleted
// key/value will not be accounted for, but it will still consume
// the buffer, since the buffer is append only.
func (db *DB) Size() int {
	return db.size
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key < key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return true.
func (db *DB) findSpliceForLevel(arena *arena, key []byte, before nodeWithAddr, level int) (nodeWithAddr, nodeWithAddr, bool) {
	for {
		// Assume before.key < key.
		nextAddr := before.nexts(level)
		if nextAddr.isNull() {
			return before, nodeWithAddr{}, false
		}
		data := arena.getFrom(nextAddr)
		next := nodeWithAddr{(*node)(unsafe.Pointer(&data[0])), nextAddr}
		nextKey := next.getKey(data)
		cmp := bytes.Compare(nextKey, key)
		if cmp >= 0 {
			// before.key < key < next.key. We are done for this level.
			return before, next, cmp == 0
		}
		before = next // Keep moving right on this level.
	}
}

func (db *DB) findGreaterEqual(key []byte) (*node, []byte, bool) {
	prev := db.head.node
	level := db.height - 1

	for {
		var nextData []byte
		var next *node
		addr := prev.nexts(level)
		if !addr.isNull() {
			arena := db.arena
			nextData = arena.getFrom(addr)
			next = (*node)(unsafe.Pointer(&nextData[0]))

			nextKey := next.getKey(nextData)
			cmp := bytes.Compare(nextKey, key)
			if cmp < 0 {
				// next key is still smaller, keep moving.
				prev = next
				continue
			}
			if cmp == 0 {
				// prev.key < key == next.key.
				return next, nextData, true
			}
		}
		// next is greater than key or next is nil. go to the lower level.
		if level > 0 {
			level--
			continue
		}
		return next, nextData, false
	}
}

func (db *DB) findLess(key []byte, allowEqual bool) (*node, []byte, bool) {
	var prevData []byte
	prev := db.head.node
	level := db.height - 1

	for {
		next, nextData := db.getNext(prev, level)
		if next != nil {
			cmp := bytes.Compare(key, next.getKey(nextData))
			if cmp > 0 {
				// prev.key < next.key < key. We can continue to move right.
				prev = next
				prevData = nextData
				continue
			}
			if cmp == 0 && allowEqual {
				// prev.key < key == next.key.
				return next, nextData, true
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
	if prev == db.head.node {
		return nil, nil, false
	}
	return prev, prevData, false
}

// findLast returns the last element. If head (empty db), we return nil. All the find functions
// will NEVER return the head nodes.
func (db *DB) findLast() (*node, []byte) {
	var nodeData []byte
	node := db.head.node
	level := db.height - 1

	for {
		next, nextData := db.getNext(node, level)
		if next != nil {
			node = next
			nodeData = nextData
			continue
		}
		if level == 0 {
			if node == db.head.node {
				return nil, nil
			}
			return node, nodeData
		}
		level--
	}
}

func (db *DB) newNode(arena *arena, key []byte, v []byte, height int) (*node, arenaAddr) {
	// The base level is already allocated in the node struct.
	nodeSize := nodeHeaderSize + height*8 + 8 + len(key) + len(v)
	addr, data := arena.alloc(nodeSize)
	node := (*node)(unsafe.Pointer(&data[0]))
	node.keyLen = uint16(len(key))
	node.height = uint16(height)
	node.valLen = uint32(len(v))
	copy(data[node.nodeLen():], key)
	copy(data[node.nodeLen()+int(node.keyLen):], v)
	return node, addr
}

func (db *DB) randomHeight() int {
	h := 1
	for h < maxHeight && fastrand.Uint32() < uint32(math.MaxUint32)/4 {
		h++
	}
	return h
}

type nodeHeader struct {
	height uint16
	keyLen uint16
	valLen uint32
}

type node struct {
	nodeHeader

	// Addr of previous node at base level.
	prev arenaAddr

	// node is a variable length struct.
	// The nextsBase is the first element of nexts slice,
	// it act as the base pointer we do pointer arithmetic in `next` and `setNext`.
	nextsBase arenaAddr
}

type nodeWithAddr struct {
	*node
	addr arenaAddr
}

func (n *node) nodeLen() int {
	return int(n.height)*8 + 8 + nodeHeaderSize
}

func (n *node) getKey(buf []byte) []byte {
	nodeLen := n.nodeLen()
	return buf[nodeLen : nodeLen+int(n.keyLen)]
}

func (n *node) getValue(buf []byte) []byte {
	nodeLenKeyLen := n.nodeLen() + int(n.keyLen)
	return buf[nodeLenKeyLen : nodeLenKeyLen+int(n.valLen)]
}

func (n *node) nexts(level int) arenaAddr {
	return *n.nextsAddr(level)
}

func (n *node) setNexts(level int, val arenaAddr) {
	*n.nextsAddr(level) = val
}

func (n *node) nextsAddr(idx int) *arenaAddr {
	offset := uintptr(idx) * unsafe.Sizeof(n.nextsBase)
	return (*arenaAddr)(unsafe.Pointer(uintptr(unsafe.Pointer(&n.nextsBase)) + offset))
}

func (db *DB) getNext(n *node, level int) (*node, []byte) {
	addr := n.nexts(level)
	if addr.isNull() {
		return nil, nil
	}
	arena := db.arena
	data := arena.getFrom(addr)
	node := (*node)(unsafe.Pointer(&data[0]))
	return node, data
}
