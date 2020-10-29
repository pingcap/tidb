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

// BufferStore wraps a Retriever for read and a MemBuffer for buffered write.
// Common usage pattern:
//	bs := NewBufferStore(r) // use BufferStore to wrap a Retriever
//	// ...
//	// read/write on bs
//	// ...
//	bs.SaveTo(m)	        // save above operations to a Mutator
type BufferStore struct {
	MemBuffer
	r Retriever
}

// NewBufferStore creates a BufferStore using r for read.
func NewBufferStore(r Retriever) *BufferStore {
	return &BufferStore{
		r:         r,
		MemBuffer: NewMemDbBuffer(),
	}
}

// NewBufferStoreFrom creates a BufferStore from retriever and mem-buffer.
func NewBufferStoreFrom(r Retriever, buf MemBuffer) *BufferStore {
	return &BufferStore{
		r:         r,
		MemBuffer: buf,
	}
}

// NewStagingBufferStore returns a BufferStore with buffer derived from the buffer.
func NewStagingBufferStore(buf MemBuffer) *BufferStore {
	return &BufferStore{
		r:         buf,
		MemBuffer: buf.NewStagingBuffer(),
	}
}

// Get implements the Retriever interface.
func (s *BufferStore) Get(ctx context.Context, k Key) ([]byte, error) {
	val, err := s.MemBuffer.Get(ctx, k)
	if IsErrNotFound(err) {
		val, err = s.r.Get(ctx, k)
	}
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, ErrNotExist
	}
	return val, nil
}

// Iter implements the Retriever interface.
func (s *BufferStore) Iter(k Key, upperBound Key) (Iterator, error) {
	bufferIt, err := s.MemBuffer.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := s.r.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	return NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse implements the Retriever interface.
func (s *BufferStore) IterReverse(k Key) (Iterator, error) {
	bufferIt, err := s.MemBuffer.IterReverse(k)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := s.r.IterReverse(k)
	if err != nil {
		return nil, err
	}
	return NewUnionIter(bufferIt, retrieverIt, true)
}

// WalkBuffer iterates all buffered kv pairs.
func (s *BufferStore) WalkBuffer(f func(k Key, v []byte) error) error {
	return WalkMemBuffer(s.MemBuffer, f)
}
