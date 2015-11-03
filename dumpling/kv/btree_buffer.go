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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv/memkv"
	"github.com/pingcap/tidb/util/types"
)

type btreeBuffer struct {
	tree *memkv.Tree
}

// NewBTreeBuffer returns a breeBuffer.
func NewBTreeBuffer() MemBuffer {
	return &btreeBuffer{
		tree: memkv.NewTree(types.Collators[true]),
	}
}

// Get returns the value associated with the key; ErrNotExist error if the key does not exist.
func (b *btreeBuffer) Get(k Key) ([]byte, error) {
	v, ok := b.tree.Get(toIfaces(k))
	if !ok {
		return nil, ErrNotExist
	}
	return fromIfaces(v), nil
}

// Set associates the key with the value.
func (b *btreeBuffer) Set(k []byte, v []byte) error {
	b.tree.Set(toIfaces(k), toIfaces(v))
	return nil
}

// Release clear the whole buffer.
func (b *btreeBuffer) Release() {
	b.tree.Clear()
}

type btreeIter struct {
	e  *memkv.Enumerator
	k  string
	v  []byte
	ok bool
}

// NewIterator creates a new Iterator based on the provided param
func (b *btreeBuffer) NewIterator(param interface{}) Iterator {
	var e *memkv.Enumerator
	var err error
	if param == nil {
		e, err = b.tree.SeekFirst()
		if err != nil {
			return &btreeIter{ok: false}
		}
	} else {
		key := toIfaces(param.([]byte))
		e, _ = b.tree.Seek(key)
	}
	iter := &btreeIter{e: e}
	// the initial push...
	err = iter.Next()
	if err != nil {
		log.Error(err)
		return &btreeIter{ok: false}
	}
	return iter
}

// Close implements Iterator Close.
func (i *btreeIter) Close() {
	//noop
}

// Key implements Iterator Key.
func (i *btreeIter) Key() string {
	return i.k
}

// Value implements Iterator Value.
func (i *btreeIter) Value() []byte {
	return i.v
}

// Next implements Iterator Next.
func (i *btreeIter) Next() error {
	k, v, err := i.e.Next()
	if err != nil {
		i.ok = false
		return errors.Trace(err)
	}
	i.k, i.v, i.ok = string(fromIfaces(k)), fromIfaces(v), true
	return nil
}

// Valid implements Iterator Valid.
func (i *btreeIter) Valid() bool {
	return i.ok
}

func toIfaces(v []byte) []interface{} {
	return []interface{}{v}
}

func fromIfaces(v []interface{}) []byte {
	if v == nil {
		return nil
	}
	return v[0].([]byte)
}
