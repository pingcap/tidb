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
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/goleveldb/leveldb/comparer"
	"github.com/pingcap/goleveldb/leveldb/iterator"
	"github.com/pingcap/goleveldb/leveldb/memdb"
	"github.com/pingcap/goleveldb/leveldb/util"
	"github.com/pingcap/parser/terror"
)

type Contract byte

const (
	None Contract = iota
	MustExist
	MustNotExist
)

var ErrContract = errors.New("contract violate")

// contractBuffer implements the MemBuffer interface.
type contractBuffer struct {
	db              *memdb.DB
	entrySizeLimit  int
	bufferLenLimit  uint64
	bufferSizeLimit int
}

type contractBufferIter struct {
	iter    iterator.Iterator
	reverse bool
}

// NewContractBuffer creates a new MemBuffer.
func NewContractBuffer(cap int) MemBuffer {
	return &contractBuffer{
		db:              memdb.New(comparer.DefaultComparer, cap),
		entrySizeLimit:  TxnEntrySizeLimit,
		bufferLenLimit:  atomic.LoadUint64(&TxnEntryCountLimit),
		bufferSizeLimit: TxnTotalSizeLimit,
	}
}

// Iter creates an Iterator.
func (m *contractBuffer) Iter(k Key, upperBound Key) (Iterator, error) {
	i := &contractBufferIter{iter: m.db.NewIterator(&util.Range{Start: []byte(k), Limit: []byte(upperBound)}), reverse: false}

	err := i.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return i, nil
}

func (m *contractBuffer) SetCap(cap int) {

}

func (m *contractBuffer) IterReverse(k Key) (Iterator, error) {
	var i *contractBufferIter
	if k == nil {
		i = &contractBufferIter{iter: m.db.NewIterator(&util.Range{}), reverse: true}
	} else {
		i = &contractBufferIter{iter: m.db.NewIterator(&util.Range{Limit: []byte(k)}), reverse: true}
	}
	i.iter.Last()
	return i, nil
}

// Get returns the value associated with key.
func (m *contractBuffer) Get(k Key) ([]byte, error) {
	v, err := m.db.Get(k)
	if terror.ErrorEqual(err, leveldb.ErrNotFound) {
		return nil, ErrNotExist
	}
	return v[:len(v)-1], nil
}

// Set associates key with value.
func (m *contractBuffer) Set(k Key, v []byte) error {
	if len(v) == 0 {
		return errors.Trace(ErrCannotSetNilValue)
	}
	if len(k)+len(v) > m.entrySizeLimit {
		return ErrEntryTooLarge.GenWithStack("entry too large, size: %d", len(k)+len(v))
	}

	err := m.db.Put(k, append(v, byte(None)))
	if m.Size() > m.bufferSizeLimit {
		return ErrTxnTooLarge.GenWithStack("transaction too large, size:%d", m.Size())
	}
	if m.Len() > int(m.bufferLenLimit) {
		return ErrTxnTooLarge.GenWithStack("transaction too large, len:%d", m.Len())
	}
	return errors.Trace(err)
}

// Delete removes the entry from buffer with provided key.
func (m *contractBuffer) Delete(k Key) error {
	err := m.db.Put(k, []byte{byte(None)})
	return errors.Trace(err)
}

// Size returns sum of keys and values length.
func (m *contractBuffer) Size() int {
	return m.db.Size()
}

// Len returns the number of entries in the DB.
func (m *contractBuffer) Len() int {
	return m.db.Len()
}

// Reset cleanup the MemBuffer.
func (m *contractBuffer) Reset() {
	m.db.Reset()
}

// Next implements the Iterator Next.
func (i *contractBufferIter) Next() error {
	if i.reverse {
		i.iter.Prev()
	} else {
		i.iter.Next()
	}
	return nil
}

// Valid implements the Iterator Valid.
func (i *contractBufferIter) Valid() bool {
	return i.iter.Valid()
}

// Key implements the Iterator Key.
func (i *contractBufferIter) Key() Key {
	return i.iter.Key()
}

// Value implements the Iterator Value.
func (i *contractBufferIter) Value() []byte {
	ret := i.iter.Value()
	return ret[:len(ret)-1]
}

// Close Implements the Iterator Close.
func (i *contractBufferIter) Close() {
	i.iter.Release()
}

// Get returns the value associated with key.
func (m *contractBuffer) GetWithContract(k Key) ([]byte, error) {
	v, err := m.db.Get(k)
	if terror.ErrorEqual(err, leveldb.ErrNotFound) {
		return nil, ErrNotExist
	}
	return v, nil
}

// SetWithContract associates key with value.
func (m *contractBuffer) SetWithContract(k Key, v []byte, contract Contract) error {
	if len(v) == 0 {
		return errors.Trace(ErrCannotSetNilValue)
	}
	if len(k)+len(v) > m.entrySizeLimit {
		return ErrEntryTooLarge.GenWithStack("entry too large, size: %d", len(k)+len(v))
	}

	if contract != None {
		find, err := m.GetWithContract(k)
		if err == ErrNotExist {
			// First operation, write the contract
			v = append(v, byte(contract))
		} else {
			// Otherwise, check contract, keep the original contract
			if contract == MustNotExist {
				return errors.Trace(ErrContract)
			}
			v = append(v, find[len(find)-1])
		}
	} else {
		v = append(v, byte(None))
	}

	err := m.db.Put(k, v)
	if m.Size() > m.bufferSizeLimit {
		return ErrTxnTooLarge.GenWithStack("transaction too large, size:%d", m.Size())
	}
	if m.Len() > int(m.bufferLenLimit) {
		return ErrTxnTooLarge.GenWithStack("transaction too large, len:%d", m.Len())
	}
	return errors.Trace(err)
}

// Delete removes the entry from buffer with provided key.
func (m *contractBuffer) DeleteWithContract(k Key, contract Contract) error {
	v := byte(None)
	if contract != None {
		find, err := m.GetWithContract(k)
		if err == ErrNotExist {
			// First operation, write the contract
			v = byte(contract)
		} else {
			// Otherwise, check contract, keep the original contract
			if contract == MustNotExist {
				return errors.Trace(ErrContract)
			}
			v = find[len(find)-1]
		}
	}

	err := m.db.Put(k, []byte{v})
	return errors.Trace(err)
}
