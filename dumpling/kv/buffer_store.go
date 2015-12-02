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
	"github.com/juju/errors"
)

type bufferStore struct {
	r       Retriever
	wbuffer MemBuffer
}

// NewBufferStore creates a BufferStore using r for read.
func NewBufferStore(r Retriever) BufferStore {
	return newBufferStore(r)
}

func newBufferStore(r Retriever) *bufferStore {
	return &bufferStore{
		r:       r,
		wbuffer: &lazyMemBuffer{},
	}
}

func (s *bufferStore) Get(k Key) ([]byte, error) {
	val, err := s.wbuffer.Get(k)
	if IsErrNotFound(err) {
		val, err = s.r.Get(k)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(val) == 0 {
		return nil, errors.Trace(ErrNotExist)
	}
	return val, nil
}

func (s *bufferStore) Seek(k Key) (Iterator, error) {
	bufferIt, err := s.wbuffer.Seek(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	readerIt, err := s.r.Seek(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newUnionIter(bufferIt, readerIt), nil
}

func (s *bufferStore) Set(k Key, v []byte) error {
	if len(v) == 0 {
		return errors.Trace(ErrCannotSetNilValue)
	}
	err := s.wbuffer.Set(k, v)
	return errors.Trace(err)
}

func (s *bufferStore) Delete(k Key) error {
	val, err := s.wbuffer.Get(k)
	if IsErrNotFound(err) {
		val, err = s.r.Get(k)
	}
	if err != nil {
		return errors.Trace(err)
	}

	if len(val) == 0 {
		return errors.Trace(ErrNotExist)
	}

	err = s.wbuffer.Set(k, nil)
	return errors.Trace(err)
}

func (s *bufferStore) WalkBuffer(f func(k Key, v []byte) error) error {
	iter, err := s.wbuffer.Seek(nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		if err := f([]byte(iter.Key()), iter.Value()); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *bufferStore) Save(m Mutator) error {
	err := s.WalkBuffer(func(k Key, v []byte) error {
		if len(v) == 0 {
			return errors.Trace(m.Delete(k))
		}
		return errors.Trace(m.Set(k, v))
	})
	return errors.Trace(err)
}

func (s *bufferStore) Release() {
	s.wbuffer.Release()
}
