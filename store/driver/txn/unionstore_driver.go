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

	"github.com/pingcap/tidb/kv"
	derr "github.com/pingcap/tidb/store/driver/error"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
)

// memBuffer wraps tikv.MemDB as kv.MemBuffer.
type memBuffer struct {
	*tikv.MemDB
}

func newMemBuffer(m *tikv.MemDB) kv.MemBuffer {
	if m == nil {
		return nil
	}
	return &memBuffer{MemDB: m}
}

func (m *memBuffer) Delete(k kv.Key) error {
	return m.MemDB.Delete(k)
}

func (m *memBuffer) DeleteWithFlags(k kv.Key, ops ...kv.FlagsOp) error {
	err := m.MemDB.DeleteWithFlags(k, getTiKVFlagsOps(ops)...)
	return derr.ToTiDBErr(err)
}

func (m *memBuffer) Get(_ context.Context, key kv.Key) ([]byte, error) {
	data, err := m.MemDB.Get(key)
	return data, derr.ToTiDBErr(err)
}

func (m *memBuffer) GetFlags(key kv.Key) (kv.KeyFlags, error) {
	data, err := m.MemDB.GetFlags(key)
	return getTiDBKeyFlags(data), derr.ToTiDBErr(err)
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

func (m *memBuffer) InspectStage(handle kv.StagingHandle, f func(kv.Key, kv.KeyFlags, []byte)) {
	tf := func(key []byte, flag tikvstore.KeyFlags, value []byte) {
		f(kv.Key(key), getTiDBKeyFlags(flag), value)
	}
	m.MemDB.InspectStage(int(handle), tf)
}

func (m *memBuffer) Set(key kv.Key, value []byte) error {
	err := m.MemDB.Set(key, value)
	return derr.ToTiDBErr(err)
}

func (m *memBuffer) SetWithFlags(key kv.Key, value []byte, ops ...kv.FlagsOp) error {
	err := m.MemDB.SetWithFlags(key, value, getTiKVFlagsOps(ops)...)
	return derr.ToTiDBErr(err)
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (m *memBuffer) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	it, err := m.MemDB.Iter(k, upperBound)
	return &tikvIterator{Iterator: it}, derr.ToTiDBErr(err)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
// TODO: Add lower bound limit
func (m *memBuffer) IterReverse(k kv.Key) (kv.Iterator, error) {
	it, err := m.MemDB.IterReverse(k)
	return &tikvIterator{Iterator: it}, derr.ToTiDBErr(err)
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

type tikvGetter struct {
	tikv.Getter
}

func newKVGetter(getter tikv.Getter) kv.Getter {
	return &tikvGetter{Getter: getter}
}

func (g *tikvGetter) Get(_ context.Context, k kv.Key) ([]byte, error) {
	data, err := g.Getter.Get(k)
	return data, derr.ToTiDBErr(err)
}

// tikvIterator wraps tikv.Iterator as kv.Iterator
type tikvIterator struct {
	tikv.Iterator
}

func newKVIterator(it tikv.Iterator) kv.Iterator {
	if it == nil {
		return nil
	}
	return &tikvIterator{Iterator: it}
}

func (it *tikvIterator) Key() kv.Key {
	return kv.Key(it.Iterator.Key())
}

func getTiDBKeyFlags(flag tikvstore.KeyFlags) kv.KeyFlags {
	var v kv.KeyFlags
	if flag.HasPresumeKeyNotExists() {
		v = kv.ApplyFlagsOps(v, kv.SetPresumeKeyNotExists)
	}
	if flag.HasNeedLocked() {
		v = kv.ApplyFlagsOps(v, kv.SetNeedLocked)
	}
	return v
}

func getTiKVFlagsOp(op kv.FlagsOp) tikvstore.FlagsOp {
	switch op {
	case kv.SetPresumeKeyNotExists:
		return tikvstore.SetPresumeKeyNotExists
	case kv.SetNeedLocked:
		return tikvstore.SetNeedLocked
	}
	return 0
}

func getTiKVFlagsOps(ops []kv.FlagsOp) []tikvstore.FlagsOp {
	v := make([]tikvstore.FlagsOp, len(ops))
	for i := range ops {
		v[i] = getTiKVFlagsOp(ops[i])
	}
	return v
}
