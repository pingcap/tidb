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

import "context"

func (db *memdb) SnapshotGetter() Getter {
	return &memdbSnapGetter{
		db: db,
		cp: db.getSnapshot(),
	}
}

func (db *memdb) SnapshotIter(start, end Key) Iterator {
	it := &memdbSnapIter{
		memdbIterator: &memdbIterator{
			db:    db,
			start: start,
			end:   end,
		},
		cp: db.getSnapshot(),
	}
	it.init()
	return it
}

func (db *memdb) getSnapshot() memdbCheckpoint {
	if len(db.stages) > 0 {
		return db.stages[0]
	}
	return db.vlog.checkpoint()
}

type memdbSnapGetter struct {
	db *memdb
	cp memdbCheckpoint
}

func (snap *memdbSnapGetter) Get(_ context.Context, key Key) ([]byte, error) {
	x := snap.db.traverse(key, false)
	if x.isNull() {
		return nil, ErrNotExist
	}
	if x.vptr.isNull() {
		// A flag only key, act as value not exists
		return nil, ErrNotExist
	}
	v, ok := snap.db.vlog.getSnapshotValue(x.vptr, &snap.cp)
	if !ok {
		return nil, ErrNotExist
	}
	return v, nil
}

type memdbSnapIter struct {
	*memdbIterator
	value []byte
	cp    memdbCheckpoint
}

func (i *memdbSnapIter) Value() []byte {
	return i.value
}

func (i *memdbSnapIter) Next() error {
	i.value = nil
	for i.Valid() {
		if err := i.memdbIterator.Next(); err != nil {
			return err
		}
		if i.setValue() {
			return nil
		}
	}
	return nil
}

func (i *memdbSnapIter) setValue() bool {
	if !i.Valid() {
		return false
	}
	if v, ok := i.db.vlog.getSnapshotValue(i.curr.vptr, &i.cp); ok {
		i.value = v
		return true
	}
	return false
}

func (i *memdbSnapIter) init() {
	if len(i.start) == 0 {
		i.seekToFirst()
	} else {
		i.seek(i.start)
	}

	if !i.setValue() {
		err := i.Next()
		_ = err // memdbIterator will never fail
	}
}
