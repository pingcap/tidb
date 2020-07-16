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

import "bytes"

type memdbIterator struct {
	db           *memdb
	curr         arenaAddr
	currn        *memdbNode
	start        Key
	end          Key
	reverse      bool
	includeFlags bool
}

func (db *memdb) Iter(k Key, upperBound Key) (Iterator, error) {
	i := &memdbIterator{
		db:    db,
		start: k,
		end:   upperBound,
	}
	if len(i.start) == 0 {
		i.seekToFirst()
	} else {
		i.seek(i.start)
	}
	i.fixPosition()
	return i, nil
}

func (db *memdb) IterReverse(k Key) (Iterator, error) {
	i := &memdbIterator{
		db:      db,
		end:     k,
		reverse: true,
	}
	if len(i.end) == 0 {
		i.seekToLast()
	} else {
		i.seek(i.end)
	}
	i.fixPosition()
	return i, nil
}

func (i *memdbIterator) Valid() bool {
	if !i.reverse {
		return !i.curr.isNull() && (i.end == nil || bytes.Compare(i.Key(), i.end) < 0)
	}
	return !i.curr.isNull()
}

func (i *memdbIterator) Key() Key {
	return i.currn.getKey()
}

func (i *memdbIterator) Value() []byte {
	return i.db.vlog.getValue(i.currn.vptr)
}

func (i *memdbIterator) Next() error {
	for {
		if i.reverse {
			i.curr, i.currn = i.db.predecessor(i.curr, i.currn)
		} else {
			i.curr, i.currn = i.db.successor(i.curr, i.currn)
		}

		// We need to skip persistent flags only nodes.
		if i.includeFlags || i.curr.isNull() || !i.currn.vptr.isNull() {
			break
		}
	}
	return nil
}

func (i *memdbIterator) Close() {}

func (i *memdbIterator) seekToFirst() {
	y := nullAddr
	x := i.db.root
	var yn *memdbNode

	for !x.isNull() {
		y = x
		yn = i.db.allocator.getNode(y)
		x = yn.left
	}

	i.curr = y
	i.currn = yn
}

func (i *memdbIterator) seekToLast() {
	y := nullAddr
	x := i.db.root
	var yn *memdbNode

	for !x.isNull() {
		y = x
		yn = i.db.allocator.getNode(y)
		x = yn.right
	}

	i.curr = y
	i.currn = yn
}

func (i *memdbIterator) seek(key Key) {
	var (
		y   = nullAddr
		x   = i.db.root
		yn  *memdbNode
		cmp int
	)

	for !x.isNull() {
		y = x
		yn = i.db.allocator.getNode(y)
		cmp = bytes.Compare(key, yn.getKey())

		if cmp < 0 {
			x = yn.left
		} else if cmp > 0 {
			x = yn.right
		} else {
			break
		}
	}

	if !i.reverse {
		if cmp > 0 {
			// Move to next
			i.curr, i.currn = i.db.successor(y, yn)
			return
		}
		i.curr = y
		i.currn = yn
		return
	}

	if cmp <= 0 && !y.isNull() {
		i.curr, i.currn = i.db.predecessor(y, yn)
		return
	}
	i.curr = y
	i.currn = yn
}

func (i *memdbIterator) fixPosition() {
	if !i.curr.isNull() && i.currn.vptr.isNull() && !i.includeFlags {
		err := i.Next()
		_ = err // memdbIterator will never fail
	}
}
