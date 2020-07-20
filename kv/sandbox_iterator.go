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
	"unsafe"
)

// Iterator iterates the entries in the DB.
type sandboxIterator struct {
	sb   *sandbox
	curr *node
	key  []byte
	val  []byte
}

// NewIterator returns a new Iterator for the lock store.
func (sb *sandbox) NewIterator() *sandboxIterator {
	return &sandboxIterator{
		sb: sb,
	}
}

// Valid returns true if the iterator is positioned at a valid node.
func (it *sandboxIterator) Valid() bool { return it.curr != nil }

// Key returns the key at the current position.
func (it *sandboxIterator) Key() []byte {
	return it.key
}

// Value returns value.
func (it *sandboxIterator) Value() []byte {
	return it.val
}

// Next moves the iterator to the next entry.
func (it *sandboxIterator) Next() {
	it.changeToAddr(it.curr.nexts(0))
}

// Prev moves the iterator to the previous entry.
func (it *sandboxIterator) Prev() {
	it.changeToAddr(it.curr.prev)
}

// Seek locates the iterator to the first entry with a key >= seekKey.
func (it *sandboxIterator) Seek(seekKey []byte) {
	node, nodeData, _ := it.sb.findGreaterEqual(seekKey) // find >=.
	it.updateState(node, nodeData)
}

// SeekForPrev locates the iterator to the last entry with key <= target.
func (it *sandboxIterator) SeekForPrev(target []byte) {
	node, nodeData, _ := it.sb.findLess(target, true) // find <=.
	it.updateState(node, nodeData)
}

// SeekForExclusivePrev locates the iterator to the last entry with key < target.
func (it *sandboxIterator) SeekForExclusivePrev(target []byte) {
	node, nodeData, _ := it.sb.findLess(target, false)
	it.updateState(node, nodeData)
}

// SeekToFirst locates the iterator to the first entry.
func (it *sandboxIterator) SeekToFirst() {
	head := it.sb.getHead()
	node, nodeData := head.getNext(it.sb.arena, 0)
	it.updateState(node, nodeData)
}

// SeekToLast locates the iterator to the last entry.
func (it *sandboxIterator) SeekToLast() {
	node, nodeData := it.sb.findLast()
	it.updateState(node, nodeData)
}

func (it *sandboxIterator) updateState(node *node, nodeData []byte) {
	it.curr = node
	if node != nil {
		it.key = node.getKey(nodeData)
		it.val = node.getValue(nodeData)
	}
}

func (it *sandboxIterator) changeToAddr(addr arenaAddr) {
	var data []byte
	var n *node
	if !addr.isNull() {
		data = it.sb.arena.getFrom(addr)
		n = (*node)(unsafe.Pointer(&data[0]))
	}
	it.updateState(n, data)
}
