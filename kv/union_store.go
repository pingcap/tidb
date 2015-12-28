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
)

// UnionStore is a store that wraps a snapshot for read and a BufferStore for buffered write.
// Also, it provides some transaction related utilities.
type UnionStore interface {
	MemBuffer
	// CheckLazyConditionPairs loads all lazy values from store then checks if all values are matched.
	// Lazy condition pairs should be checked before transaction commit.
	CheckLazyConditionPairs() error
	// WalkBuffer iterates all buffered kv pairs.
	WalkBuffer(f func(k Key, v []byte) error) error
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option.
	SetOption(opt Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt Option)
}

// Option is used for customizing kv store's behaviors during a transaction.
type Option int

// Options is an interface of a set of options. Each option is associated with a value.
type Options interface {
	// Get gets an option value.
	Get(opt Option) (v interface{}, ok bool)
}

var (
	p = newCache("memdb pool", 100, func() MemBuffer {
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
	return &unionStore{
		BufferStore:        NewBufferStore(snapshot),
		snapshot:           snapshot,
		lazyConditionPairs: &lazyMemBuffer{},
		opts:               make(map[Option]interface{}),
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
		lmb.mb = p.get()
	}

	return lmb.mb.Set(key, value)
}

func (lmb *lazyMemBuffer) Delete(k Key) error {
	if lmb.mb == nil {
		lmb.mb = p.get()
	}

	return lmb.mb.Delete(k)
}

func (lmb *lazyMemBuffer) Seek(k Key) (Iterator, error) {
	if lmb.mb == nil {
		lmb.mb = p.get()
	}

	return lmb.mb.Seek(k)
}

func (lmb *lazyMemBuffer) Release() {
	if lmb.mb == nil {
		return
	}

	lmb.mb.Release()

	p.put(lmb.mb)
	lmb.mb = nil
}

// Get implements the Retriever interface.
func (us *unionStore) Get(k Key) ([]byte, error) {
	v, err := us.MemBuffer.Get(k)
	if IsErrNotFound(err) {
		if _, ok := us.opts.Get(PresumeKeyNotExists); ok {
			err = us.markLazyConditionPair(k, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return nil, errors.Trace(ErrNotExist)
		}
	}
	if IsErrNotFound(err) {
		v, err = us.BufferStore.r.Get(k)
	}
	if err != nil {
		return v, errors.Trace(err)
	}
	if len(v) == 0 {
		return nil, errors.Trace(ErrNotExist)
	}
	return v, nil
}

// markLazyConditionPair marks a kv pair for later check.
func (us *unionStore) markLazyConditionPair(k Key, v []byte) error {
	if len(v) == 0 {
		return errors.Trace(us.lazyConditionPairs.Delete(k))
	}
	return errors.Trace(us.lazyConditionPairs.Set(k, v))
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
				return errors.Trace(errors.New("Error: key already exist in " + it.Key()))
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
