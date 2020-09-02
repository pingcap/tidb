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
	"context"
)

// UnionStore is a store that wraps a snapshot for read and a MemBuffer for buffered write.
// Also, it provides some transaction related utilities.
type UnionStore interface {
	Retriever

	// HasPresumeKeyNotExists returns whether the key presumed key not exists error for the lazy check.
	HasPresumeKeyNotExists(k Key) bool
	// DeleteKeyExistErrInfo deletes the key presume key not exists error flag for the lazy check.
	UnmarkPresumeKeyNotExists(k Key)
	// CacheIndexName caches the index name.
	// PresumeKeyNotExists will use this to help decode error message.
	CacheIndexName(tableID, indexID int64, name string)
	// GetIndexName returns the cached index name.
	// If there is no such index already inserted through CacheIndexName, it will return UNKNOWN.
	GetIndexName(tableID, indexID int64) string

	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option.
	SetOption(opt Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt Option)
	// GetOption gets an option.
	GetOption(opt Option) interface{}
	// GetMemBuffer return the MemBuffer binding to this unionStore.
	GetMemBuffer() MemBuffer
}

// AssertionType is the type of a assertion.
type AssertionType int

// The AssertionType constants.
const (
	None AssertionType = iota
	Exist
	NotExist
)

// Option is used for customizing kv store's behaviors during a transaction.
type Option int

// Options is an interface of a set of options. Each option is associated with a value.
type Options interface {
	// Get gets an option value.
	Get(opt Option) (v interface{}, ok bool)
}

type idxNameKey struct {
	tableID, indexID int64
}

// unionStore is an in-memory Store which contains a buffer for write and a
// snapshot for read.
type unionStore struct {
	memBuffer    *memdb
	snapshot     Snapshot
	idxNameCache map[idxNameKey]string
	opts         options
}

// NewUnionStore builds a new unionStore.
func NewUnionStore(snapshot Snapshot) UnionStore {
	return &unionStore{
		snapshot:     snapshot,
		memBuffer:    newMemDB(),
		idxNameCache: make(map[idxNameKey]string),
		opts:         make(map[Option]interface{}),
	}
}

// GetMemBuffer return the MemBuffer binding to this unionStore.
func (us *unionStore) GetMemBuffer() MemBuffer {
	return us.memBuffer
}

// Get implements the Retriever interface.
func (us *unionStore) Get(ctx context.Context, k Key) ([]byte, error) {
	v, err := us.memBuffer.Get(ctx, k)
	if IsErrNotFound(err) {
		v, err = us.snapshot.Get(ctx, k)
	}
	if err != nil {
		return v, err
	}
	if len(v) == 0 {
		return nil, ErrNotExist
	}
	return v, nil
}

// Iter implements the Retriever interface.
func (us *unionStore) Iter(k Key, upperBound Key) (Iterator, error) {
	bufferIt, err := us.memBuffer.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := us.snapshot.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	return NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse implements the Retriever interface.
func (us *unionStore) IterReverse(k Key) (Iterator, error) {
	bufferIt, err := us.memBuffer.IterReverse(k)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := us.snapshot.IterReverse(k)
	if err != nil {
		return nil, err
	}
	return NewUnionIter(bufferIt, retrieverIt, true)
}

// HasPresumeKeyNotExists gets the key exist error info for the lazy check.
func (us *unionStore) HasPresumeKeyNotExists(k Key) bool {
	flags, err := us.memBuffer.GetFlags(k)
	if err != nil {
		return false
	}
	return flags.HasPresumeKeyNotExists()
}

// DeleteKeyExistErrInfo deletes the key exist error info for the lazy check.
func (us *unionStore) UnmarkPresumeKeyNotExists(k Key) {
	us.memBuffer.UpdateFlags(k, DelPresumeKeyNotExists)
}

func (us *unionStore) GetIndexName(tableID, indexID int64) string {
	key := idxNameKey{tableID: tableID, indexID: indexID}
	name, ok := us.idxNameCache[key]
	if !ok {
		return "UNKNOWN"
	}
	return name
}

func (us *unionStore) CacheIndexName(tableID, indexID int64, name string) {
	key := idxNameKey{tableID: tableID, indexID: indexID}
	us.idxNameCache[key] = name
}

// SetOption implements the unionStore SetOption interface.
func (us *unionStore) SetOption(opt Option, val interface{}) {
	us.opts[opt] = val
}

// DelOption implements the unionStore DelOption interface.
func (us *unionStore) DelOption(opt Option) {
	delete(us.opts, opt)
}

// GetOption implements the unionStore GetOption interface.
func (us *unionStore) GetOption(opt Option) interface{} {
	return us.opts[opt]
}

type options map[Option]interface{}

func (opts options) Get(opt Option) (interface{}, bool) {
	v, ok := opts[opt]
	return v, ok
}
