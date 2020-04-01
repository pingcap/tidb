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
	initBlockSize  = 4 * 1024
)

// Sandbox is a space to keep pending kvs.
type Sandbox struct {
	frozen bool
	done   bool
	head   headNode
	parent *Sandbox
	arena  *arena
	height int
	length int
	size   int

	arenaSnap arenaSnapshot
}

// NewSandbox create a new Sandbox.
func NewSandbox() *Sandbox {
	arena := newArenaLocator()
	return &Sandbox{
		height:    1,
		arena:     arena,
		arenaSnap: arena.snapshot(),
	}
}

// Get returns value for key in this sandbox's space.
func (sb *Sandbox) Get(key []byte) []byte {
	node, data, match := sb.findGreaterEqual(key)
	if !match {
		return nil
	}
	return node.getValue(data)
}

// Put inserts kv into this sandbox.
func (sb *Sandbox) Put(key, value []byte) {
	if sb.frozen {
		panic("cannot write to a sandbox when it has forked a new sanbox")
	}

	head := sb.getHead()
	arena := sb.arena
	lsHeight := sb.height
	var prev [maxHeight + 1]nodeWithAddr
	var next [maxHeight + 1]nodeWithAddr
	prev[lsHeight] = head

	var exists bool
	for i := lsHeight - 1; i >= 0; i-- {
		// Use higher level to speed up for current level.
		prev[i], next[i], exists = sb.findSpliceForLevel(key, prev[i+1], i)
	}

	var height int
	if !exists {
		height = sb.randomHeight()
	} else {
		height = sb.prepareOverwrite(next[:])
	}

	x, addr := arena.newNode(key, value, height)
	if height > lsHeight {
		sb.height = height
	}

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < height; i++ {
		x.setNexts(i, next[i].addr)
		if prev[i].node == nil {
			prev[i] = head
		}
		prev[i].setNexts(i, addr)
	}

	x.prev = prev[0].addr
	if next[0].node != nil {
		next[0].prev = addr
	}

	sb.length++
	sb.size += len(key) + len(value)
}

// Derive derive a new sandbox to buffer a batch of modifactions.
func (sb *Sandbox) Derive() *Sandbox {
	if sb.frozen {
		panic("cannot start second sandbox")
	}
	sb.frozen = true
	new := &Sandbox{
		parent:    sb,
		height:    1,
		arena:     sb.arena,
		arenaSnap: sb.arena.snapshot(),
	}
	return new
}

// Flush flushes all kvs into parent sandbox.
func (sb *Sandbox) Flush() int {
	if sb.parent == nil || sb.done {
		return 0
	}
	if !sb.parent.frozen {
		panic("the parent sandbox must be freezed when doing flush")
	}
	sb.parent.frozen = false
	sb.done = true
	return sb.parent.merge(sb)
}

// GetParent returns the parent sandbox.
func (sb *Sandbox) GetParent() *Sandbox {
	return sb.parent
}

// Discard discards all kvs in this sandbox.
// It is safe to discard a flushed sandbox, and it is recommend to
// call discard using defer to maintain correct state of parent.
func (sb *Sandbox) Discard() {
	if sb.done {
		return
	}

	if sb.parent != nil {
		if !sb.parent.frozen {
			panic("the parent sandbox must be freezed when doing discard")
		}
		sb.parent.frozen = false
		sb.done = true
	} else if sb.frozen {
		panic("root sandbox is freezed")
	}

	sb.head = headNode{}
	sb.height = 1
	sb.length = 0
	sb.size = 0
	sb.arena.revert(sb.arenaSnap)
	sb.arena = nil
}

// Len returns the number of entries in the DB.
func (sb *Sandbox) Len() int {
	return sb.length
}

// Size returns sum of keys and values length. Note that deleted
// key/value will not be accounted for, but it will still consume
// the buffer, since the buffer is append only.
func (sb *Sandbox) Size() int {
	return sb.size
}

// The pointers in findSpliceForLevel may point to the node which going to be overwrite,
// prepareOverwrite update them to point to the next node, so we can link new node with the list correctly.
func (sb *Sandbox) prepareOverwrite(next []nodeWithAddr) int {
	old := next[0]

	// Update necessary states.
	sb.size -= int(old.valLen) + int(old.keyLen)
	sb.length--

	height := int(old.height)
	for i := 0; i < height; i++ {
		if next[i].addr == old.addr {
			next[i].addr = old.nexts(i)
			if !next[i].addr.isNull() {
				data := sb.arena.getFrom(next[i].addr)
				next[i].node = (*node)(unsafe.Pointer(&data[0]))
			}
		}
	}
	return height
}

func (sb *Sandbox) getHead() nodeWithAddr {
	head := (*node)(unsafe.Pointer(&sb.head))
	return nodeWithAddr{node: head}
}

func (sb *Sandbox) randomHeight() int {
	h := 1
	for h < maxHeight && fastrand.Uint32() < uint32(math.MaxUint32)/4 {
		h++
	}
	return h
}

// findSpliceForLevel returns (outBefore, outAfter) with outBefore.key < key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return true.
func (sb *Sandbox) findSpliceForLevel(key []byte, before nodeWithAddr, level int) (nodeWithAddr, nodeWithAddr, bool) {
	arena := sb.arena
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

func (sb *Sandbox) findGreaterEqual(key []byte) (*node, []byte, bool) {
	head := sb.getHead()
	prev := head.node
	level := sb.height - 1
	arena := sb.arena

	for {
		var nextData []byte
		var next *node
		addr := prev.nexts(level)
		if !addr.isNull() {
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

func (sb *Sandbox) findLess(key []byte, allowEqual bool) (*node, []byte, bool) {
	var prevData []byte
	head := sb.getHead()
	prev := head.node
	level := sb.height - 1
	arena := sb.arena

	for {
		next, nextData := prev.getNext(arena, level)
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
	if prev == head.node {
		return nil, nil, false
	}
	return prev, prevData, false
}

// findLast returns the last element. If head (empty db), we return nil. All the find functions
// will NEVER return the head nodes.
func (sb *Sandbox) findLast() (*node, []byte) {
	var nodeData []byte
	head := sb.getHead()
	node := head.node
	level := sb.height - 1
	arena := sb.arena

	for {
		next, nextData := node.getNext(arena, level)
		if next != nil {
			node = next
			nodeData = nextData
			continue
		}
		if level == 0 {
			if node == head.node {
				return nil, nil
			}
			return node, nodeData
		}
		level--
	}
}

func (sb *Sandbox) merge(new *Sandbox) int {
	var ms mergeState
	arena := sb.arena

	if sb.head.nexts[0].isNull() {
		// current skip-list is empty, overwite head node using the new list's head.
		sb.head = new.head
		sb.height = new.height
		sb.length = new.length
		sb.size = new.size
		return new.length
	}

	var (
		newNode      *node
		nextNode     *node
		newNodeAddr  arenaAddr
		nextNodeAddr arenaAddr
		newNodeData  []byte
		nextNodeData []byte
	)

	newCnt := new.length
	sb.length += newCnt
	sb.size += new.size

	head := new.getHead()
	if sb.Len() < new.Len() {
		sb.head, new.head = new.head, sb.head
		sb.height = new.height
		sb.length = new.length
		sb.size = new.size
	}

	newNodeAddr = head.nexts(0)
	newNode, newNodeData = head.getNext(arena, 0)

	for newNode != nil {
		key := newNode.getKey(newNodeData)
		recomputeHeight := ms.calculateRecomputeHeight(key, sb)

		nextNodeAddr = newNode.nexts(0)
		nextNode, nextNodeData = newNode.getNext(arena, 0)

		var exists bool
		if recomputeHeight > 0 {
			for i := recomputeHeight - 1; i >= 0; i-- {
				ms.prev[i], ms.next[i], exists = sb.findSpliceForLevel(key, ms.prev[i+1], i)
			}
		}

		height := int(newNode.height)
		if exists {
			height = sb.prepareOverwrite(ms.next[:])
			if height > int(newNode.height) {
				// The space is not enough, we have to create a new node.
				k := newNode.getKey(newNodeData)
				v := newNode.getValue(newNodeData)
				newNode, newNodeAddr = arena.newNode(k, v, height)
			}
		}

		if height > sb.height {
			sb.height = height
		}

		for i := 0; i < height; i++ {
			newNode.setNexts(i, ms.next[i].addr)
			if ms.prev[i].node == nil {
				ms.prev[i] = head
			}
			ms.prev[i].setNexts(i, newNodeAddr)
		}

		newNode.prev = ms.prev[0].addr
		if ms.next[0].node != nil {
			ms.next[0].prev = newNodeAddr
		}

		newNode, newNodeAddr, newNodeData = nextNode, nextNodeAddr, nextNodeData
	}

	return newCnt
}

type mergeState struct {
	height int

	// hitHeight is used to reduce cost of calculateRecomputeHeight.
	// For random workload, comparing hint keys from bottom up is wasted work.
	// So we record the hit height of the last operation, only grow recompute height from near that height.
	hitHeight int
	prev      [maxHeight + 1]nodeWithAddr
	next      [maxHeight + 1]nodeWithAddr
}

func (ms *mergeState) calculateRecomputeHeight(key []byte, sb *Sandbox) int {
	head := sb.getHead()
	listHeight := sb.height
	arena := sb.arena

	if ms.height < listHeight {
		// Either splice is never used or list height has grown, we recompute all.
		ms.prev[listHeight] = head
		ms.next[listHeight] = nodeWithAddr{}
		ms.height = listHeight
		ms.hitHeight = ms.height
		return listHeight
	}
	recomputeHeight := ms.hitHeight - 2
	if recomputeHeight < 0 {
		recomputeHeight = 0
	}
	for recomputeHeight < listHeight {
		prev := ms.prev[recomputeHeight]
		next := ms.next[recomputeHeight]
		prevNext := prev.nexts(recomputeHeight)
		if prevNext != next.addr {
			recomputeHeight++
			continue
		}
		if prev.addr != head.addr &&
			!prev.addr.isNull() &&
			bytes.Compare(key, prev.getKey(arena.getFrom(prev.addr))) <= 0 {
			// Key is before splice.
			for prev.addr == ms.prev[recomputeHeight].addr {
				recomputeHeight++
			}
			continue
		}
		if !next.addr.isNull() && bytes.Compare(key, next.getKey(arena.getFrom(next.addr))) > 0 {
			// Key is after splice.
			for next == ms.next[recomputeHeight] {
				recomputeHeight++
			}
			continue
		}
		break
	}
	ms.hitHeight = recomputeHeight
	return recomputeHeight
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

type headNode struct {
	nodeHeader

	// Addr of previous node at base level.
	prev arenaAddr

	nexts [maxHeight]arenaAddr
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

func (n *node) getNext(arena *arena, level int) (*node, []byte) {
	addr := n.nexts(level)
	if addr.isNull() {
		return nil, nil
	}
	data := arena.getFrom(addr)
	node := (*node)(unsafe.Pointer(&data[0]))
	return node, data
}
