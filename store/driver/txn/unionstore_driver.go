// Copyright 2021 PingCAP, Inc.
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

package txn

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	tikvstore "github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/unionstore"
)

// memBuffer wraps unionstore.MemDB as kv.MemBuffer.
type memBuffer struct {
	*unionstore.MemDB
}

func newMemBuffer(m *unionstore.MemDB) kv.MemBuffer {
	if m == nil {
		return nil
	}
	return &memBuffer{MemDB: m}
}

func (m *memBuffer) Delete(k kv.Key) error {
	return m.MemDB.Delete(k)
}

func (m *memBuffer) DeleteWithFlags(k kv.Key, ops ...tikvstore.FlagsOp) error {
	return m.MemDB.DeleteWithFlags(k, ops...)
}

func (m *memBuffer) Get(_ context.Context, key kv.Key) ([]byte, error) {
	return m.MemDB.Get(key)
}

func (m *memBuffer) GetFlags(key kv.Key) (tikvstore.KeyFlags, error) {
	return m.MemDB.GetFlags(key)
}

func (m *memBuffer) Staging() kv.StagingHandle {
	return kv.StagingHandle(m.MemDB.Staging())
}

func (m *memBuffer) Cleanup(h kv.StagingHandle) {
	m.MemDB.Cleanup(int(h))
}

func (m *memBuffer) Release(h kv.StagingHandle) {
	m.MemDB.Release(int(h))
}

func (m *memBuffer) InspectStage(handle kv.StagingHandle, f func(kv.Key, tikvstore.KeyFlags, []byte)) {
	tf := func(key []byte, flag tikvstore.KeyFlags, value []byte) {
		f(kv.Key(key), flag, value)
	}
	m.MemDB.InspectStage(int(handle), tf)
}

func (m *memBuffer) Set(key kv.Key, value []byte) error {
	return m.MemDB.Set(key, value)
}

func (m *memBuffer) SetWithFlags(key kv.Key, value []byte, ops ...kv.FlagsOp) error {
	return m.MemDB.SetWithFlags(key, value, ops...)
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (m *memBuffer) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	it, err := m.MemDB.Iter(k, upperBound)
	return &tikvIterator{Iterator: it}, errors.Trace(err)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
// TODO: Add lower bound limit
func (m *memBuffer) IterReverse(k kv.Key) (kv.Iterator, error) {
	it, err := m.MemDB.IterReverse(k)
	return &tikvIterator{Iterator: it}, errors.Trace(err)
}

// SnapshotIter returns a Iterator for a snapshot of MemBuffer.
func (m *memBuffer) SnapshotIter(k, upperbound kv.Key) kv.Iterator {
	it := m.MemDB.SnapshotIter(k, upperbound)
	return &tikvIterator{Iterator: it}
}

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (m *memBuffer) SnapshotGetter() kv.Getter {
	return newKVGetter(m.MemDB.SnapshotGetter())
}

//tikvUnionStore implements kv.UnionStore
type tikvUnionStore struct {
	*unionstore.KVUnionStore
}

func (u *tikvUnionStore) GetMemBuffer() kv.MemBuffer {
	return newMemBuffer(u.KVUnionStore.GetMemBuffer())
}

func (u *tikvUnionStore) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	return u.KVUnionStore.Get(ctx, k)
}

func (u *tikvUnionStore) HasPresumeKeyNotExists(k kv.Key) bool {
	return u.KVUnionStore.HasPresumeKeyNotExists(k)
}

func (u *tikvUnionStore) UnmarkPresumeKeyNotExists(k kv.Key) {
	u.KVUnionStore.UnmarkPresumeKeyNotExists(k)
}

func (u *tikvUnionStore) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	it, err := u.KVUnionStore.Iter(k, upperBound)
	return newKVIterator(it), errors.Trace(err)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
// TODO: Add lower bound limit
func (u *tikvUnionStore) IterReverse(k kv.Key) (kv.Iterator, error) {
	it, err := u.KVUnionStore.IterReverse(k)
	return newKVIterator(it), errors.Trace(err)
}

type tikvGetter struct {
	unionstore.Getter
}

func newKVGetter(getter unionstore.Getter) kv.Getter {
	return &tikvGetter{Getter: getter}
}

func (g *tikvGetter) Get(_ context.Context, k kv.Key) ([]byte, error) {
	return g.Getter.Get(k)
}

// tikvIterator wraps unionstore.Iterator as kv.Iterator
type tikvIterator struct {
	unionstore.Iterator
}

func newKVIterator(it unionstore.Iterator) kv.Iterator {
	if it == nil {
		return nil
	}
	return &tikvIterator{Iterator: it}
}

func (it *tikvIterator) Key() kv.Key {
	return kv.Key(it.Iterator.Key())
}
