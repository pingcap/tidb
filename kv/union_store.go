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

// UnionStore is a store that wraps a snapshot for read and a BufferStore for buffered write.
// Also, it provides some transaction related utilities.
type UnionStore interface {
	MemBuffer
	// GetKeyExistErrInfo gets the key exist error info for the lazy check.
	GetKeyExistErrInfo(k Key) *existErrInfo
	// DeleteKeyExistErrInfo deletes the key exist error info for the lazy check.
	DeleteKeyExistErrInfo(k Key)
	// WalkBuffer iterates all buffered kv pairs.
	WalkBuffer(f func(k Key, v []byte) error) error
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option.
	SetOption(opt Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt Option)
	// GetOption gets an option.
	GetOption(opt Option) interface{}
	// GetMemBuffer return the MemBuffer binding to this UnionStore.
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

type existErrInfo struct {
	idxName string
	value   string
}

// NewExistErrInfo is used to new an existErrInfo
func NewExistErrInfo(idxName string, value string) *existErrInfo {
	return &existErrInfo{idxName: idxName, value: value}
}

// GetIdxName gets the index name of the existed error.
func (e *existErrInfo) GetIdxName() string {
	return e.idxName
}

// GetValue gets the existed value of the existed error.
func (e *existErrInfo) GetValue() string {
	return e.value
}

// Err generates the error for existErrInfo
func (e *existErrInfo) Err() error {
	return ErrKeyExists.FastGenByArgs(e.value, e.idxName)
}

// unionStore is an in-memory Store which contains a buffer for write and a
// snapshot for read.
type unionStore struct {
	*BufferStore
	keyExistErrs map[string]*existErrInfo // for the lazy check
	opts         options
}

// NewUnionStore builds a new UnionStore.
func NewUnionStore(snapshot Snapshot) UnionStore {
	return &unionStore{
		BufferStore:  NewBufferStore(snapshot),
		keyExistErrs: make(map[string]*existErrInfo),
		opts:         make(map[Option]interface{}),
	}
}

// Get implements the Retriever interface.
func (us *unionStore) Get(ctx context.Context, k Key) ([]byte, error) {
	v, err := us.MemBuffer.Get(ctx, k)
	if IsErrNotFound(err) {
		if _, ok := us.opts.Get(PresumeKeyNotExists); ok {
			e, ok := us.opts.Get(PresumeKeyNotExistsError)
			if ok {
				us.keyExistErrs[string(k)] = e.(*existErrInfo)
				if val, ok := us.opts.Get(CheckExists); ok {
					checkExistMap := val.(map[string]struct{})
					checkExistMap[string(k)] = struct{}{}
				}
			}
			return nil, ErrNotExist
		}
		v, err = us.BufferStore.r.Get(ctx, k)
	}
	if err != nil {
		return v, err
	}
	if len(v) == 0 {
		return nil, ErrNotExist
	}
	return v, nil
}

func (us *unionStore) GetKeyExistErrInfo(k Key) *existErrInfo {
	if c, ok := us.keyExistErrs[string(k)]; ok {
		return c
	}
	return nil
}

func (us *unionStore) DeleteKeyExistErrInfo(k Key) {
	delete(us.keyExistErrs, string(k))
}

// SetOption implements the UnionStore SetOption interface.
func (us *unionStore) SetOption(opt Option, val interface{}) {
	us.opts[opt] = val
}

// DelOption implements the UnionStore DelOption interface.
func (us *unionStore) DelOption(opt Option) {
	delete(us.opts, opt)
}

// GetOption implements the UnionStore GetOption interface.
func (us *unionStore) GetOption(opt Option) interface{} {
	return us.opts[opt]
}

// GetMemBuffer return the MemBuffer binding to this UnionStore.
func (us *unionStore) GetMemBuffer() MemBuffer {
	return us.BufferStore.MemBuffer
}

func (us *unionStore) NewStagingBuffer() MemBuffer {
	return us.BufferStore.NewStagingBuffer()
}

func (us *unionStore) Flush() (int, error) {
	return us.BufferStore.Flush()
}

func (us *unionStore) Discard() {
	us.BufferStore.Discard()
}

type options map[Option]interface{}

func (opts options) Get(opt Option) (interface{}, bool) {
	v, ok := opts[opt]
	return v, ok
}
