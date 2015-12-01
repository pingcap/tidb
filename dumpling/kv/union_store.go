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

	"strconv"

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
type unionStore struct {
	wbuffer            MemBuffer // updates are buffered in memory
	snapshot           Snapshot  // for read
	lazyConditionPairs MemBuffer // for delay check
	opts               options
}

// NewUnionStore builds a new UnionStore.
func NewUnionStore(snapshot Snapshot) UnionStore {
	lazy := &lazyMemBuffer{}
	opts := make(map[Option]interface{})
	return &unionStore{
		wbuffer:            &lazyMemBuffer{},
		snapshot:           NewCacheSnapshot(snapshot, lazy, options(opts)),
		lazyConditionPairs: lazy,
		opts:               opts,
	}
}

type lazyMemBuffer struct {
	mb MemBuffer
}

func (lmb *lazyMemBuffer) Get(k Key) ([]byte, error) {
	if lmb.mb == nil {
		return nil, ErrNotExist
	}

	return lmb.mb.Get(k)
}

func (lmb *lazyMemBuffer) Set(key []byte, value []byte) error {
	if lmb.mb == nil {
		lmb.mb = p.Get().(MemBuffer)
	}

	return lmb.mb.Set(key, value)
}

func (lmb *lazyMemBuffer) NewIterator(param interface{}) Iterator {
	if lmb.mb == nil {
		lmb.mb = p.Get().(MemBuffer)
	}

	return lmb.mb.NewIterator(param)
}

func (lmb *lazyMemBuffer) Release() {
	if lmb.mb == nil {
		return
	}

	lmb.mb.Release()

	p.Put(lmb.mb)
	lmb.mb = nil
}

// Get implements the Retriever interface.
func (us *unionStore) Get(key Key) (value []byte, err error) {
	// Get from update records frist
	value, err = us.wbuffer.Get(key)
	if IsErrNotFound(err) {
		// Try get from snapshot
		value, err = us.snapshot.Get(key)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(value) == 0 { // Deleted marker
		return nil, errors.Trace(ErrNotExist)
	}

	return value, nil
}

// Set implements the Updater interface.
func (us *unionStore) Set(key Key, value []byte) error {
	if len(value) == 0 {
		return errors.Trace(ErrCannotSetNilValue)
	}
	return us.wbuffer.Set(key, value)
}

// Inc implements the UnionStore interface.
func (us *unionStore) Inc(k Key, step int64) (int64, error) {
	val, err := us.Get(k)
	if IsErrNotFound(err) {
		err = us.Set(k, []byte(strconv.FormatInt(step, 10)))
		if err != nil {
			return 0, errors.Trace(err)
		}
		return step, nil
	}
	if err != nil {
		return 0, errors.Trace(err)
	}

	intVal, err := strconv.ParseInt(string(val), 10, 0)
	if err != nil {
		return 0, errors.Trace(err)
	}

	intVal += step
	err = us.Set(k, []byte(strconv.FormatInt(intVal, 10)))
	if err != nil {
		return 0, errors.Trace(err)
	}
	return intVal, nil
}

// GetInt64 implements UnionStore interface.
func (us *unionStore) GetInt64(k Key) (int64, error) {
	val, err := us.Get(k)
	if IsErrNotFound(err) {
		return 0, nil
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	intVal, err := strconv.ParseInt(string(val), 10, 0)
	return intVal, errors.Trace(err)
}

// Seek implements the Retriever interface.
func (us *unionStore) Seek(key Key) (Iterator, error) {
	bufferIt := us.wbuffer.NewIterator([]byte(key))
	cacheIt := us.snapshot.NewIterator([]byte(key))
	return newUnionIter(bufferIt, cacheIt), nil
}

// Delete implements the Updater interface.
func (us *unionStore) Delete(k Key) error {
	// Mark as deleted
	val, err := us.wbuffer.Get(k)
	if err != nil {
		if !IsErrNotFound(err) { // something wrong
			return errors.Trace(err)
		}

		// missed in buffer
		val, err = us.snapshot.Get(k)
		if err != nil {
			if IsErrNotFound(err) {
				return errors.Trace(ErrNotExist)
			}
		}
	}

	if len(val) == 0 { // deleted marker, already deleted
		return errors.Trace(ErrNotExist)
	}

	err = us.wbuffer.Set(k, nil)
	return errors.Trace(err)
}

// WalkWriteBuffer implements the UnionStore interface.
func (us *unionStore) WalkWriteBuffer(f func(Iterator) error) error {
	iter := us.wbuffer.NewIterator(nil)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		if err := f(iter); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// BatchPrefetch implements the UnionStore interface.
func (us *unionStore) BatchPrefetch(keys []Key) error {
	_, err := us.snapshot.BatchGet(keys)
	return errors.Trace(err)
}

// RangePrefetch implements the UnionStore interface.
func (us *unionStore) RangePrefetch(start, end Key, limit int) error {
	_, err := us.snapshot.RangeGet(start, end, limit)
	return errors.Trace(err)
}

// CheckLazyConditionPairs implements the UnionStore interface.
func (us *unionStore) CheckLazyConditionPairs() error {
	var keys []Key
	for it := us.lazyConditionPairs.NewIterator(nil); it.Valid(); it.Next() {
		keys = append(keys, []byte(it.Key()))
	}
	if len(keys) == 0 {
		return nil
	}
	values, err := us.snapshot.BatchGet(keys)
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

// SetOption implements the UnionStore SetOption interface.
func (us *unionStore) SetOption(opt Option, val interface{}) {
	us.opts[opt] = val
}

// DelOption implements the UnionStore DelOption interface.
func (us *unionStore) DelOption(opt Option) {
	delete(us.opts, opt)
}

// ReleaseSnapshot implements the UnionStore ReleaseSnapshot interface.
func (us *unionStore) ReleaseSnapshot() {
	us.snapshot.Release()
}

// Close implements the Store Close interface.
func (us *unionStore) Close() {
	us.snapshot.Release()
	us.wbuffer.Release()
	us.lazyConditionPairs.Release()
}

type options map[Option]interface{}

func (opts options) Get(opt Option) (interface{}, bool) {
	v, ok := opts[opt]
	return v, ok
}
