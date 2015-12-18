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

// UnionStore is a store that wraps a snapshot for read and a BufferStore for buffered write.
// Also, it provides some transaction related utilities.
type UnionStore interface {
	MemBuffer
	// Inc increases the value for key k in KV storage by step.
	Inc(k Key, step int64) (int64, error)
	// GetInt64 get int64 which created by Inc method.
	GetInt64(k Key) (int64, error)
	// CheckLazyConditionPairs loads all lazy values from store then checks if all values are matched.
	// Lazy condition pairs should be checked before transaction commit.
	CheckLazyConditionPairs() error
	// BatchPrefetch fetches values from KV storage to cache for later use.
	BatchPrefetch(keys []Key) error
	// RangePrefetch fetches values in the range [start, end] from KV storage
	// to cache for later use. Maximum number of values is up to limit.
	RangePrefetch(start, end Key, limit int) error
	// WalkBuffer iterates all buffered kv pairs.
	WalkBuffer(f func(k Key, v []byte) error) error
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option.
	SetOption(opt Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt Option)
	// ReleaseSnapshot releases underlying snapshot.
	ReleaseSnapshot()
}

// Option is used for customizing kv store's behaviors during a transaction.
type Option int

// Options is an interface of a set of options. Each option is associated with a value.
type Options interface {
	// Get gets an option value.
	Get(opt Option) (v interface{}, ok bool)
}

var (
	p = pool.NewCache("memdb pool", 100, func() interface{} {
		return NewMemDbBuffer()
	})
)

// UnionStore is an in-memory Store which contains a buffer for write and a
// snapshot for read.
type unionStore struct {
	*BufferStore
	snapshot           Snapshot  // for read
	lazyConditionPairs MemBuffer // for delay check
	opts               options
}

// NewUnionStore builds a new UnionStore.
func NewUnionStore(snapshot Snapshot) UnionStore {
	lazy := &lazyMemBuffer{}
	opts := make(map[Option]interface{})
	cacheSnapshot := NewCacheSnapshot(snapshot, lazy, options(opts))
	bufferStore := NewBufferStore(cacheSnapshot)
	return &unionStore{
		BufferStore:        bufferStore,
		snapshot:           cacheSnapshot,
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

func (lmb *lazyMemBuffer) Set(key Key, value []byte) error {
	if lmb.mb == nil {
		lmb.mb = p.Get().(MemBuffer)
	}

	return lmb.mb.Set(key, value)
}

func (lmb *lazyMemBuffer) Delete(k Key) error {
	if lmb.mb == nil {
		lmb.mb = p.Get().(MemBuffer)
	}

	return lmb.mb.Delete(k)
}

func (lmb *lazyMemBuffer) Seek(k Key) (Iterator, error) {
	if lmb.mb == nil {
		lmb.mb = p.Get().(MemBuffer)
	}

	return lmb.mb.Seek(k)
}

func (lmb *lazyMemBuffer) Release() {
	if lmb.mb == nil {
		return
	}

	lmb.mb.Release()

	p.Put(lmb.mb)
	lmb.mb = nil
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
	it, err := us.lazyConditionPairs.Seek(nil)
	if err != nil {
		return errors.Trace(err)
	}
	for ; it.Valid(); it.Next() {
		keys = append(keys, []byte(it.Key()))
	}
	it.Close()

	if len(keys) == 0 {
		return nil
	}
	values, err := us.snapshot.BatchGet(keys)
	if err != nil {
		return errors.Trace(err)
	}
	it, err = us.lazyConditionPairs.Seek(nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		if len(it.Value()) == 0 {
			if _, exist := values[it.Key()]; exist {
				return errors.Trace(terror.ErrKeyExists)
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

// Release implements the UnionStore Release interface.
func (us *unionStore) Release() {
	us.snapshot.Release()
	us.BufferStore.Release()
	us.lazyConditionPairs.Release()
}

type options map[Option]interface{}

func (opts options) Get(opt Option) (interface{}, bool) {
	v, ok := opts[opt]
	return v, ok
}
