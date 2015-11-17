// Copyright 2015 PingCAP, Inc.
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
	"bytes"

	"github.com/juju/errors"
	"github.com/ngaut/pool"
	"github.com/pingcap/tidb/terror"
)

var (
	p = pool.NewCache("memdb pool", 100, func() interface{} {
		return NewMemDbBuffer()
	})
)

// IsErrNotFound checks if err is a kind of NotFound error.
func IsErrNotFound(err error) bool {
	if terror.ErrorEqual(err, ErrNotExist) {
		return true
	}

	return false
}

// UnionStore is an in-memory Store which contains a buffer for write and a
// snapshot for read.
type UnionStore struct {
	WBuffer            MemBuffer // updates are buffered in memory
	Snapshot           Snapshot  // for read
	lazyConditionPairs MemBuffer // for delay check
}

// NewUnionStore builds a new UnionStore.
func NewUnionStore(snapshot Snapshot, opts Options) UnionStore {
	wbuffer := p.Get().(MemBuffer)
	lazy := p.Get().(MemBuffer)
	return UnionStore{
		WBuffer:            wbuffer,
		Snapshot:           NewCacheSnapshot(snapshot, lazy, opts),
		lazyConditionPairs: lazy,
	}
}

// Get implements the Store Get interface.
func (us *UnionStore) Get(key []byte) (value []byte, err error) {
	// Get from update records frist
	value, err = us.WBuffer.Get(key)
	if IsErrNotFound(err) {
		// Try get from snapshot
		return us.Snapshot.Get(key)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(value) == 0 { // Deleted marker
		return nil, errors.Trace(ErrNotExist)
	}

	return value, nil
}

// Set implements the Store Set interface.
func (us *UnionStore) Set(key []byte, value []byte) error {
	return us.WBuffer.Set(key, value)
}

// Seek implements the Snapshot Seek interface.
func (us *UnionStore) Seek(key []byte, txn Transaction) (Iterator, error) {
	bufferIt := us.WBuffer.NewIterator(key)
	cacheIt := us.Snapshot.NewIterator(key)
	return newUnionIter(bufferIt, cacheIt), nil
}

// Delete implements the Store Delete interface.
func (us *UnionStore) Delete(k []byte) error {
	// Mark as deleted
	val, err := us.WBuffer.Get(k)
	if err != nil {
		if !IsErrNotFound(err) { // something wrong
			return errors.Trace(err)
		}

		// missed in buffer
		val, err = us.Snapshot.Get(k)
		if err != nil {
			if IsErrNotFound(err) {
				return errors.Trace(ErrNotExist)
			}
		}
	}

	if len(val) == 0 { // deleted marker, already deleted
		return errors.Trace(ErrNotExist)
	}

	return us.WBuffer.Set(k, nil)
}

// CheckLazyConditionPairs loads all lazy values from store then checks if all values are matched.
func (us *UnionStore) CheckLazyConditionPairs() error {
	var keys []Key
	for it := us.lazyConditionPairs.NewIterator(nil); it.Valid(); it.Next() {
		keys = append(keys, []byte(it.Key()))
	}
	if len(keys) == 0 {
		return nil
	}
	values, err := us.Snapshot.BatchGet(keys)
	if err != nil {
		return errors.Trace(err)
	}
	for it := us.lazyConditionPairs.NewIterator(nil); it.Valid(); it.Next() {
		if len(it.Value()) == 0 {
			if _, exist := values[it.Key()]; exist {
				return errors.Trace(ErrKeyExists)
			}
		} else {
			if bytes.Compare(values[it.Key()], it.Value()) != 0 {
				return errors.Trace(ErrLazyConditionPairsNotMatch)
			}
		}
	}
	return nil
}

// Close implements the Store Close interface.
func (us *UnionStore) Close() error {
	us.Snapshot.Release()
	us.WBuffer.Release()
	p.Put(us.WBuffer)
	us.lazyConditionPairs.Release()
	p.Put(us.lazyConditionPairs)
	return nil
}
