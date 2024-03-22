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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"context"

	"github.com/pingcap/tidb/pkg/kv"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
)

// memBuffer wraps tikv.MemDB as kv.MemBuffer.
type memBuffer struct {
	tikv.MemBuffer
	isPipelinedDML bool
}

func newMemBuffer(m tikv.MemBuffer, isPipelinedDML bool) kv.MemBuffer {
	if m == nil {
		return nil
	}
	return &memBuffer{MemBuffer: m, isPipelinedDML: isPipelinedDML}
}

func (m *memBuffer) Size() int {
	return m.MemBuffer.Size()
}

func (m *memBuffer) Delete(k kv.Key) error {
	return m.MemBuffer.Delete(k)
}

func (m *memBuffer) RemoveFromBuffer(k kv.Key) {
	m.MemBuffer.RemoveFromBuffer(k)
}

func (m *memBuffer) DeleteWithFlags(k kv.Key, ops ...kv.FlagsOp) error {
	err := m.MemBuffer.DeleteWithFlags(k, getTiKVFlagsOps(ops)...)
	return derr.ToTiDBErr(err)
}

func (m *memBuffer) UpdateFlags(k kv.Key, ops ...kv.FlagsOp) {
	m.MemBuffer.UpdateFlags(k, getTiKVFlagsOps(ops)...)
}

func (m *memBuffer) Get(ctx context.Context, key kv.Key) ([]byte, error) {
	data, err := m.MemBuffer.Get(ctx, key)
	return data, derr.ToTiDBErr(err)
}

func (m *memBuffer) GetFlags(key kv.Key) (kv.KeyFlags, error) {
	data, err := m.MemBuffer.GetFlags(key)
	return getTiDBKeyFlags(data), derr.ToTiDBErr(err)
}

func (m *memBuffer) Staging() kv.StagingHandle {
	return kv.StagingHandle(m.MemBuffer.Staging())
}

func (m *memBuffer) Cleanup(h kv.StagingHandle) {
	m.MemBuffer.Cleanup(int(h))
}

func (m *memBuffer) Release(h kv.StagingHandle) {
	m.MemBuffer.Release(int(h))
}

func (m *memBuffer) InspectStage(handle kv.StagingHandle, f func(kv.Key, kv.KeyFlags, []byte)) {
	tf := func(key []byte, flag tikvstore.KeyFlags, value []byte) {
		f(key, getTiDBKeyFlags(flag), value)
	}
	m.MemBuffer.InspectStage(int(handle), tf)
}

func (m *memBuffer) Set(key kv.Key, value []byte) error {
	err := m.MemBuffer.Set(key, value)
	return derr.ToTiDBErr(err)
}

func (m *memBuffer) SetWithFlags(key kv.Key, value []byte, ops ...kv.FlagsOp) error {
	err := m.MemBuffer.SetWithFlags(key, value, getTiKVFlagsOps(ops)...)
	return derr.ToTiDBErr(err)
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (m *memBuffer) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	it, err := m.MemBuffer.Iter(k, upperBound)
	return &tikvIterator{Iterator: it}, derr.ToTiDBErr(err)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
func (m *memBuffer) IterReverse(k, lowerBound kv.Key) (kv.Iterator, error) {
	it, err := m.MemBuffer.IterReverse(k, lowerBound)
	return &tikvIterator{Iterator: it}, derr.ToTiDBErr(err)
}

// SnapshotIter returns an Iterator for a snapshot of MemBuffer.
func (m *memBuffer) SnapshotIter(k, upperbound kv.Key) kv.Iterator {
	if m.isPipelinedDML {
		return &kv.EmptyIterator{}
	}
	it := m.MemBuffer.SnapshotIter(k, upperbound)
	return &tikvIterator{Iterator: it}
}

func (m *memBuffer) SnapshotIterReverse(k, lowerBound kv.Key) kv.Iterator {
	if m.isPipelinedDML {
		return &kv.EmptyIterator{}
	}
	it := m.MemBuffer.SnapshotIterReverse(k, lowerBound)
	return &tikvIterator{Iterator: it}
}

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (m *memBuffer) SnapshotGetter() kv.Getter {
	if m.isPipelinedDML {
		return &kv.EmptyRetriever{}
	}
	return newKVGetter(m.MemBuffer.SnapshotGetter())
}

// GetLocal implements kv.MemBuffer interface
func (m *memBuffer) GetLocal(ctx context.Context, key []byte) ([]byte, error) {
	data, err := m.MemBuffer.GetLocal(ctx, key)
	return data, derr.ToTiDBErr(err)
}

func (m *memBuffer) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	data, err := m.MemBuffer.BatchGet(ctx, keys)
	return data, derr.ToTiDBErr(err)
}

type tikvGetter struct {
	tikv.Getter
}

func newKVGetter(getter tikv.Getter) kv.Getter {
	return &tikvGetter{Getter: getter}
}

func (g *tikvGetter) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	data, err := g.Getter.Get(ctx, k)
	return data, derr.ToTiDBErr(err)
}

// tikvIterator wraps tikv.Iterator as kv.Iterator
type tikvIterator struct {
	tikv.Iterator
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

	if flag.HasAssertExist() {
		v = kv.ApplyFlagsOps(v, kv.SetAssertExist)
	} else if flag.HasAssertNotExist() {
		v = kv.ApplyFlagsOps(v, kv.SetAssertNotExist)
	} else if flag.HasAssertUnknown() {
		v = kv.ApplyFlagsOps(v, kv.SetAssertUnknown)
	}

	if flag.HasNeedConstraintCheckInPrewrite() {
		v = kv.ApplyFlagsOps(v, kv.SetNeedConstraintCheckInPrewrite)
	}

	return v
}

func getTiKVFlagsOp(op kv.FlagsOp) tikvstore.FlagsOp {
	switch op {
	case kv.SetPresumeKeyNotExists:
		return tikvstore.SetPresumeKeyNotExists
	case kv.SetNeedLocked:
		return tikvstore.SetNeedLocked
	case kv.SetAssertExist:
		return tikvstore.SetAssertExist
	case kv.SetAssertNotExist:
		return tikvstore.SetAssertNotExist
	case kv.SetAssertUnknown:
		return tikvstore.SetAssertUnknown
	case kv.SetAssertNone:
		return tikvstore.SetAssertNone
	case kv.SetNeedConstraintCheckInPrewrite:
		return tikvstore.SetNeedConstraintCheckInPrewrite
	case kv.SetPreviousPresumeKeyNotExists:
		return tikvstore.SetPreviousPresumeKNE
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
