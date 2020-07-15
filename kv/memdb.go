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
)

var (
	_ MemBuffer     = new(memDB)
	_ StagingBuffer = sandboxWrapper{}
)

// KeyFlags are metadata associated with key
type KeyFlags uint8

const (
	flagPresumeKeyNotExists KeyFlags = 1 << iota

	// flagTouched is a internal flag to help `Mark` operation.
	// The default value of `KeyFlags` is 0 and the flags returned by any operation will set `flagTouched`.
	// When merge two flags, if the newer one haven't set the `flagTouched`, the newer value will be ignored.
	flagTouched KeyFlags = 0x80
)

// MarkPresumeKeyNotExists marks the existence of the associated key is checked lazily.
func (m KeyFlags) MarkPresumeKeyNotExists() KeyFlags {
	return (m | flagPresumeKeyNotExists) | flagTouched
}

// UnmarkPresumeKeyNotExists reverts MarkPresumeKeyNotExists.
func (m KeyFlags) UnmarkPresumeKeyNotExists() KeyFlags {
	return (m & ^flagPresumeKeyNotExists) | flagTouched
}

// HasPresumeKeyNotExists retruns whether the associated key use lazy check.
func (m KeyFlags) HasPresumeKeyNotExists() bool {
	return m&flagPresumeKeyNotExists != 0
}

func (m KeyFlags) isTouched() bool {
	return m&flagTouched != 0
}

// Merge used to merge two KeyFlags.
func (m KeyFlags) Merge(old KeyFlags) KeyFlags {
	// Only consider flagPresumeKeyNotExists in merge operation for now.
	// We should always respect to the older setting,
	// the delete operation will overwrite flags in root tree instead of invoke merge.
	if old.isTouched() {
		return old
	}
	return m
}

type memDB struct {
	entrySizeLimit  uint64
	bufferSizeLimit uint64
	buffers         []*sandbox
	dirty           bool
}

func newMemDB() *memDB {
	return &memDB{
		buffers:         append(make([]*sandbox, 0, 3), NewSandbox()),
		entrySizeLimit:  atomic.LoadUint64(&TxnEntrySizeLimit),
		bufferSizeLimit: atomic.LoadUint64(&TxnTotalSizeLimit),
	}
}

func (m *memDB) Reset() {
	for i := len(m.buffers) - 1; i > 0; i-- {
		m.buffers[i].Discard()
		m.buffers[i] = nil
	}
	m.buffers[0].Discard()
	m.buffers = m.buffers[:1]
}

func (m *memDB) GetStagingBuffer(h StagingHandle) StagingBuffer {
	if h == InvalidStagingHandle {
		return nil
	}
	idx := int(h) - 1
	if h == LastActiveStagingHandle {
		idx = len(m.buffers) - 1
	}
	return sandboxWrapper{m.buffers[idx]}
}

func (m *memDB) GetFlags(k Key) (KeyFlags, error) {
	var exists bool
	for i := len(m.buffers) - 1; i >= 0; i-- {
		flags, ok := m.buffers[i].GetFlags(k)
		if !ok {
			continue
		}
		exists = true
		if flags.isTouched() {
			return flags, nil
		}
	}
	if !exists {
		return 0, ErrNotExist
	}
	return 0, nil
}

func (m *memDB) Get(ctx context.Context, k Key) ([]byte, error) {
	for i := len(m.buffers) - 1; i >= 0; i-- {
		v, ok := m.buffers[i].Get(k)
		if !ok {
			continue
		}
		return v, nil
	}

	return nil, ErrNotExist
}

func (m *memDB) SetWithFlags(k Key, flags KeyFlags, v []byte) error {
	if len(v) == 0 {
		return errors.Trace(ErrCannotSetNilValue)
	}
	if uint64(len(k)+len(v)) > m.entrySizeLimit {
		return ErrEntryTooLarge.GenWithStackByArgs(m.entrySizeLimit, len(k)+len(v))
	}

	if len(m.buffers) == 1 {
		m.dirty = true
	}
	m.getStagingBuffer().PutWithFlags(k, flags, v)
	if m.Size() > int(m.bufferSizeLimit) {
		return ErrTxnTooLarge.GenWithStackByArgs(atomic.LoadUint64(&TxnTotalSizeLimit))
	}
	return nil
}

func (m *memDB) Set(k Key, v []byte) error {
	return m.SetWithFlags(k, 0, v)
}

func (m *memDB) SetFlags(k Key, flags KeyFlags) {
	m.getStagingBuffer().UpdateFlags(k, flags)
}

func (m *memDB) Delete(k Key) error {
	if len(m.buffers) == 1 {
		m.dirty = true
	}
	m.getStagingBuffer().Put(k, nil)
	if m.Size() > int(m.bufferSizeLimit) {
		return ErrTxnTooLarge.GenWithStackByArgs(atomic.LoadUint64(&TxnTotalSizeLimit))
	}
	return nil
}

func (m *memDB) Size() int {
	var sum int
	for _, buf := range m.buffers {
		sum += buf.Size()
	}
	return sum
}

func (m *memDB) Len() int {
	var sum int
	for _, buf := range m.buffers {
		sum += buf.Len()
	}
	return sum
}

func (m *memDB) Dirty() bool {
	return m.dirty
}

// Iter creates an Iterator.
func (m *memDB) Iter(k Key, upperBound Key) (Iterator, error) {
	if len(m.buffers) == 1 {
		return m.iter(0, k, upperBound, false), nil
	}

	idx := len(m.buffers) - 1
	i1 := m.iter(idx, k, upperBound, false)
	i2 := m.iter(idx-1, k, upperBound, false)
	it, err := NewUnionIter(i1, i2, false)
	if err != nil {
		return nil, err
	}
	idx -= 2

	// This is unlikely in current codebase.
	for ; idx >= 0; idx-- {
		it, err = NewUnionIter(m.iter(idx, k, upperBound, false), it, false)
		if err != nil {
			return nil, err
		}
	}

	return it, nil
}

func (m *memDB) IterReverse(k Key) (Iterator, error) {
	if len(m.buffers) == 1 {
		return m.iter(0, nil, k, true), nil
	}

	idx := len(m.buffers) - 1
	i1 := m.iter(idx, nil, k, true)
	i2 := m.iter(idx-1, nil, k, true)
	it, err := NewUnionIter(i1, i2, true)
	if err != nil {
		return nil, err
	}
	idx -= 2

	// This is unlikely in current codebase.
	for ; idx >= 0; idx-- {
		it, err = NewUnionIter(m.iter(idx, nil, k, true), it, true)
		if err != nil {
			return nil, err
		}
	}

	return it, nil
}

func (m *memDB) Staging() StagingHandle {
	m.buffers = append(m.buffers, m.getStagingBuffer().Derive())
	return StagingHandle(len(m.buffers))
}

func (m *memDB) Release(h StagingHandle) (int, error) {
	if int(h) != len(m.buffers) {
		// This should never happens in production environmen.
		// Use panic to make debug easier.
		panic("cannot release staging buffer")
	}
	idx := int(h) - 1
	count := m.buffers[idx].Flush()
	if len(m.buffers) == 2 && count > 0 {
		m.dirty = true
	}
	m.buffers[idx] = nil
	m.buffers = m.buffers[:idx]
	return count, nil
}

func (m *memDB) Cleanup(h StagingHandle) {
	if int(h) > len(m.buffers) {
		return
	}
	if int(h) < len(m.buffers) {
		// This should never happens in production environmen.
		// Use panic to make debug easier.
		panic("cannot cleanup staging buffer")
	}
	idx := int(h) - 1
	m.buffers[idx].Discard()
	m.buffers[idx] = nil
	m.buffers = m.buffers[:idx]
}

func (m *memDB) getStagingBuffer() *sandbox {
	return m.buffers[len(m.buffers)-1]
}

func (m *memDB) iter(idx int, start Key, end Key, reverse bool) *simpleIter {
	i := &simpleIter{
		iter:    m.buffers[idx].NewIterator(),
		start:   start,
		end:     end,
		reverse: reverse,
	}
	i.init()
	return i
}

type simpleIter struct {
	iter    *sandboxIterator
	start   []byte
	end     []byte
	reverse bool
}

// Next implements the Iterator Next.
func (i *simpleIter) Next() error {
	if i.reverse {
		i.iter.Prev()
	} else {
		i.iter.Next()
	}
	return nil
}

// Valid implements the Iterator Valid.
func (i *simpleIter) Valid() bool {
	if !i.reverse {
		return i.iter.Valid() && (i.end == nil || bytes.Compare(i.Key(), i.end) < 0)
	}
	return i.iter.Valid()
}

// Key implements the Iterator Key.
func (i *simpleIter) Key() Key {
	return i.iter.Key()
}

// Value implements the Iterator Value.
func (i *simpleIter) Value() []byte {
	return i.iter.Value()
}

// Close Implements the Iterator Close.
func (i *simpleIter) Close() {}

func (i *simpleIter) init() {
	if i.reverse {
		if i.end == nil {
			i.iter.SeekToLast()
			return
		}
		i.iter.SeekForExclusivePrev(i.end)
		return
	}
	if i.start == nil {
		i.iter.SeekToFirst()
		return
	}
	i.iter.Seek(i.start)
}

// WalkMemBuffer iterates all buffered kv pairs in memBuf
func WalkMemBuffer(memBuf Retriever, f func(k Key, v []byte) error) error {
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

// BufferBatchGetter is the type for BatchGet with MemBuffer.
type BufferBatchGetter struct {
	buffer   MemBuffer
	middle   Getter
	snapshot Snapshot
}

// NewBufferBatchGetter creates a new BufferBatchGetter.
func NewBufferBatchGetter(buffer MemBuffer, middleCache Getter, snapshot Snapshot) *BufferBatchGetter {
	return &BufferBatchGetter{buffer: buffer, middle: middleCache, snapshot: snapshot}
}

// BatchGet implements the BatchGetter interface.
func (b *BufferBatchGetter) BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error) {
	if b.buffer.Len() == 0 {
		return b.snapshot.BatchGet(ctx, keys)
	}
	bufferValues := make([][]byte, len(keys))
	shrinkKeys := make([]Key, 0, len(keys))
	for i, key := range keys {
		val, err := b.buffer.Get(ctx, key)
		if err == nil {
			bufferValues[i] = val
			continue
		}
		if !IsErrNotFound(err) {
			return nil, errors.Trace(err)
		}
		if b.middle != nil {
			val, err = b.middle.Get(ctx, key)
			if err == nil {
				bufferValues[i] = val
				continue
			}
		}
		shrinkKeys = append(shrinkKeys, key)
	}
	storageValues, err := b.snapshot.BatchGet(ctx, shrinkKeys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, key := range keys {
		if len(bufferValues[i]) == 0 {
			continue
		}
		storageValues[string(key)] = bufferValues[i]
	}
	return storageValues, nil
}

type sandboxWrapper struct {
	box *sandbox
}

func (w sandboxWrapper) Get(ctx context.Context, k Key) ([]byte, error) {
	v, ok := w.box.Get(k)
	if !ok {
		return nil, ErrNotExist
	}
	return v, nil
}

func (w sandboxWrapper) Iter(k Key, upperBound Key) (Iterator, error) {
	i := &simpleIter{
		iter:    w.box.NewIterator(),
		start:   k,
		end:     upperBound,
		reverse: false,
	}
	i.init()
	return i, nil
}

func (w sandboxWrapper) IterReverse(k Key) (Iterator, error) {
	i := &simpleIter{
		iter:    w.box.NewIterator(),
		end:     k,
		reverse: true,
	}
	i.init()
	return i, nil
}

func (w sandboxWrapper) Size() int {
	return w.box.Size()
}

func (w sandboxWrapper) Len() int {
	return w.box.Len()
}
