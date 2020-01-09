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
	"bytes"
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv/memdb"
)

// memDbBuffer implements the MemBuffer interface.
type memDbBuffer struct {
	db              *memdb.DB
	entrySizeLimit  int
	bufferLenLimit  uint64
	bufferSizeLimit uint64
}

type memDbIter struct {
	iter    memdb.Iterator
	start   []byte
	end     []byte
	reverse bool
}

// NewMemDbBuffer creates a new memDbBuffer.
func NewMemDbBuffer(initBlockSize int) MemBuffer {
	return &memDbBuffer{
		db:              memdb.New(initBlockSize),
		entrySizeLimit:  TxnEntrySizeLimit,
		bufferSizeLimit: atomic.LoadUint64(&TxnTotalSizeLimit),
	}
}

// Iter creates an Iterator.
func (m *memDbBuffer) Iter(k Key, upperBound Key) (Iterator, error) {
	i := &memDbIter{
		iter:    m.db.NewIterator(),
		start:   k,
		end:     upperBound,
		reverse: false,
	}

	if k == nil {
		i.iter.SeekToFirst()
	} else {
		i.iter.Seek(k)
	}
	return i, nil
}

func (m *memDbBuffer) SetCap(cap int) {

}

func (m *memDbBuffer) IterReverse(k Key) (Iterator, error) {
	i := &memDbIter{
		iter:    m.db.NewIterator(),
		end:     k,
		reverse: true,
	}
	if k == nil {
		i.iter.SeekToLast()
	} else {
		i.iter.SeekForExclusivePrev(k)
	}
	return i, nil
}

// Get returns the value associated with key.
func (m *memDbBuffer) Get(ctx context.Context, k Key) ([]byte, error) {
	v := m.db.Get(k)
	if v == nil {
		return nil, ErrNotExist
	}
	return v, nil
}

// Set associates key with value.
func (m *memDbBuffer) Set(k Key, v []byte) error {
	if len(v) == 0 {
		return errors.Trace(ErrCannotSetNilValue)
	}
	if len(k)+len(v) > m.entrySizeLimit {
		return ErrEntryTooLarge.GenWithStackByArgs(m.entrySizeLimit, len(k)+len(v))
	}

	m.db.Put(k, v)
	if m.Size() > int(m.bufferSizeLimit) {
		return ErrTxnTooLarge.GenWithStackByArgs(m.Size())
	}
	return nil
}

// Delete removes the entry from buffer with provided key.
func (m *memDbBuffer) Delete(k Key) error {
	m.db.Put(k, nil)
	return nil
}

// Size returns sum of keys and values length.
func (m *memDbBuffer) Size() int {
	return m.db.Size()
}

// Len returns the number of entries in the DB.
func (m *memDbBuffer) Len() int {
	return m.db.Len()
}

// Reset cleanup the MemBuffer.
func (m *memDbBuffer) Reset() {
	m.db.Reset()
}

// Next implements the Iterator Next.
func (i *memDbIter) Next() error {
	if i.reverse {
		i.iter.Prev()
	} else {
		i.iter.Next()
	}
	return nil
}

// Valid implements the Iterator Valid.
func (i *memDbIter) Valid() bool {
	if !i.reverse {
		return i.iter.Valid() && (i.end == nil || bytes.Compare(i.Key(), i.end) < 0)
	}
	return i.iter.Valid()
}

// Key implements the Iterator Key.
func (i *memDbIter) Key() Key {
	return i.iter.Key()
}

// Value implements the Iterator Value.
func (i *memDbIter) Value() []byte {
	return i.iter.Value()
}

// Close Implements the Iterator Close.
func (i *memDbIter) Close() {

}

// WalkMemBuffer iterates all buffered kv pairs in memBuf
func WalkMemBuffer(memBuf MemBuffer, f func(k Key, v []byte) error) error {
	iter, err := memBuf.Iter(nil, nil)
	if err != nil {
		return errors.Trace(err)
	}

	defer iter.Close()
	for iter.Valid() {
		if err = f(iter.Key(), iter.Value()); err != nil {
			return errors.Trace(err)
		}
		err = iter.Next()
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}
