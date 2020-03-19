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

import "unsafe"

// Iterator iterates the entries in the DB.
type Iterator struct {
	db   *DB
	curr *node
	key  []byte
	val  []byte
}

// NewIterator returns a new Iterator for the lock store.
func (db *DB) NewIterator() Iterator {
	return Iterator{
		db: db,
	}
}

// Valid returns true if the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool { return it.curr != nil }

// Key returns the key at the current position.
func (it *Iterator) Key() []byte {
	return it.key
}

// Value returns value.
func (it *Iterator) Value() []byte {
	return it.val
}

// Next moves the iterator to the next entry.
func (it *Iterator) Next() {
	it.changeToAddr(it.curr.nexts(0))
}

// Prev moves the iterator to the previous entry.
func (it *Iterator) Prev() {
	it.changeToAddr(it.curr.prev)
}

// Seek locates the iterator to the first entry with a key >= seekKey.
func (it *Iterator) Seek(seekKey []byte) {
	node, nodeData, _ := it.db.findGreaterEqual(seekKey) // find >=.
	it.updateState(node, nodeData)
}

// SeekForPrev locates the iterator to the last entry with key <= target.
func (it *Iterator) SeekForPrev(target []byte) {
	node, nodeData, _ := it.db.findLess(target, true) // find <=.
	it.updateState(node, nodeData)
}

// SeekForExclusivePrev locates the iterator to the last entry with key < target.
func (it *Iterator) SeekForExclusivePrev(target []byte) {
	node, nodeData, _ := it.db.findLess(target, false)
	it.updateState(node, nodeData)
}

// SeekToFirst locates the iterator to the first entry.
func (it *Iterator) SeekToFirst() {
	node, nodeData := it.db.getNext(it.db.head.node, 0)
	it.updateState(node, nodeData)
}

// SeekToLast locates the iterator to the last entry.
func (it *Iterator) SeekToLast() {
	node, nodeData := it.db.findLast()
	it.updateState(node, nodeData)
}

func (it *Iterator) updateState(node *node, nodeData []byte) {
	it.curr = node
	if node != nil {
		it.key = node.getKey(nodeData)
		it.val = node.getValue(nodeData)
	}
}

func (it *Iterator) changeToAddr(addr arenaAddr) {
	var data []byte
	var n *node
	if !addr.isNull() {
		data = it.db.arena.getFrom(addr)
		n = (*node)(unsafe.Pointer(&data[0]))
	}
	it.updateState(n, data)
}
