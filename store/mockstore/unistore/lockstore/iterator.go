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

// Iterator iterates the entries in the MemStore.
type Iterator struct {
	ls  *MemStore
	key []byte
	val []byte
}

// NewIterator returns a new Iterator for the lock store.
func (ls *MemStore) NewIterator() *Iterator {
	return &Iterator{
		ls: ls,
	}
}

// Valid returns true iff the iterator is positioned at a valid node.
func (it *Iterator) Valid() bool { return len(it.key) != 0 }

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
	e, _ := it.ls.findGreater(it.key, false)
	it.setKeyValue(e)
}

// Prev moves the iterator to the previous entry.
func (it *Iterator) Prev() {
	e, _ := it.ls.findLess(it.key, false) // find <. No equality allowed.
	it.setKeyValue(e)
}

// Seek locates the iterator to the first entry with a key >= seekKey.
func (it *Iterator) Seek(seekKey []byte) {
	e, _ := it.ls.findGreater(seekKey, true) // find >=.
	it.setKeyValue(e)
}

// SeekForPrev locates the iterator to the last entry with key <= target.
func (it *Iterator) SeekForPrev(target []byte) {
	e, _ := it.ls.findLess(target, true) // find <=.
	it.setKeyValue(e)
}

// SeekForExclusivePrev locates the iterator to the last entry with key < target.
func (it *Iterator) SeekForExclusivePrev(target []byte) {
	e, _ := it.ls.findLess(target, false)
	it.setKeyValue(e)
}

// SeekToFirst locates the iterator to the first entry.
func (it *Iterator) SeekToFirst() {
	e := it.ls.getNext(it.ls.head, 0)
	it.setKeyValue(e)
}

// SeekToLast locates the iterator to the last entry.
func (it *Iterator) SeekToLast() {
	e := it.ls.findLast()
	it.setKeyValue(e)
}

func (it *Iterator) setKeyValue(e entry) {
	it.key = append(it.key[:0], e.key...)
	it.val = append(it.val[:0], e.getValue(it.ls.getArena())...)
}
