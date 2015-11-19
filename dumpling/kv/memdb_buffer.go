// Copyright 2015 PingCAP, Inc.
//
// Copyright 2015 Wenbin Xiao
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
	"github.com/pingcap/tidb/terror"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type memDbBuffer struct {
	db *memdb.DB
}

type memDbIter struct {
	iter iterator.Iterator
}

// NewMemDbBuffer creates a new memDbBuffer.
func NewMemDbBuffer() MemBuffer {
	return &memDbBuffer{db: memdb.New(comparer.DefaultComparer, 4*1024)}
}

// NewIterator creates an Iterator.
func (m *memDbBuffer) NewIterator(param interface{}) Iterator {
	var i Iterator
	if param == nil {
		i = &memDbIter{iter: m.db.NewIterator(&util.Range{})}
	} else {
		i = &memDbIter{iter: m.db.NewIterator(&util.Range{Start: param.([]byte)})}
	}
	i.Next()
	return i
}

// Get returns the value associated with key.
func (m *memDbBuffer) Get(k Key) ([]byte, error) {
	v, err := m.db.Get(k)
	if terror.ErrorEqual(err, leveldb.ErrNotFound) {
		return nil, ErrNotExist
	}
	return v, nil
}

// Set associates key with value.
func (m *memDbBuffer) Set(k []byte, v []byte) error {
	return m.db.Put(k, v)
}

// Release reset the buffer.
func (m *memDbBuffer) Release() {
	m.db.Reset()
}

// Next implements the Iterator Next.
func (i *memDbIter) Next() error {
	i.iter.Next()
	return nil
}

// Valid implements the Iterator Valid.
func (i *memDbIter) Valid() bool {
	return i.iter.Valid()
}

// Key implements the Iterator Key.
func (i *memDbIter) Key() string {
	return string(i.iter.Key())
}

// Value implements the Iterator Value.
func (i *memDbIter) Value() []byte {
	return i.iter.Value()
}

// Close Implements the Iterator Close.
func (i *memDbIter) Close() {
	i.iter.Release()
}
