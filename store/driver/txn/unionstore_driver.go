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

	"github.com/pingcap/tidb/kv"
	derr "github.com/pingcap/tidb/store/driver/error"
	tikverr "github.com/tikv/client-go/v2/error"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/util"
)

type visibilityChecker interface {
	invisible(kv.Key) bool
}

// memBuffer wraps tikv.MemDB as kv.MemBuffer.
type memBuffer struct {
	*tikv.MemDB
	invisibleKeys map[string]struct{}
}

func newMemBuffer(m *tikv.MemDB, invisibleKeys map[string]struct{}) kv.MemBuffer {
	if m == nil {
		return nil
	}
	return &memBuffer{MemDB: m, invisibleKeys: invisibleKeys}
}

func (m *memBuffer) addInvisibleKey(k kv.Key) {
	m.Lock()
	m.invisibleKeys[util.String(k)] = struct{}{}
	m.Unlock()
}

func (m *memBuffer) delInvisibleKey(k kv.Key) {
	m.Lock()
	delete(m.invisibleKeys, util.String(k))
	m.Unlock()
}

func (m *memBuffer) invisible(k kv.Key) bool {
	// shall be protected by MemBuffer.RLock
	_, ok := m.invisibleKeys[util.String(k)]
	return ok
}

func (m *memBuffer) Size() int {
	return m.MemDB.Size()
}

func (m *memBuffer) Delete(k kv.Key) error {
	err := m.MemDB.Delete(k)
	m.delInvisibleKey(k)
	return derr.ToTiDBErr(err)
}

func (m *memBuffer) DeleteWithFlags(k kv.Key, ops ...kv.FlagsOp) error {
	err := m.MemDB.DeleteWithFlags(k, getTiKVFlagsOps(ops)...)
	m.delInvisibleKey(k)
	return derr.ToTiDBErr(err)
}

func (m *memBuffer) Get(_ context.Context, key kv.Key) ([]byte, error) {
	if m.invisible(key) {
		return nil, kv.ErrNotExist
	}
	data, err := m.MemDB.Get(key)
	return data, derr.ToTiDBErr(err)
}

func (m *memBuffer) GetFlags(key kv.Key) (kv.KeyFlags, error) {
	// do not check `invisibleKeys` here since LockKeys may set flags on keys and those flags are always visible.
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
	m.delInvisibleKey(key)
	return derr.ToTiDBErr(err)
}

func (m *memBuffer) SetWithFlags(key kv.Key, value []byte, ops ...kv.FlagsOp) error {
	err := m.MemDB.SetWithFlags(key, value, getTiKVFlagsOps(ops)...)
	m.delInvisibleKey(key)
	return derr.ToTiDBErr(err)
}

func (m *memBuffer) ChangeLockIntoPut(key kv.Key, value []byte) error {
	// only change LOCK into PUT when the key does not existed, otherwise, we may mark a visible key as invisible.
	if _, err := m.MemDB.Get(key); tikverr.IsErrNotFound(err) {
		m.addInvisibleKey(key)
		return derr.ToTiDBErr(m.MemDB.Set(key, value))
	}
	return nil
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (m *memBuffer) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	it, err := m.MemDB.Iter(k, upperBound)
	return newKVIterator(it, m), derr.ToTiDBErr(err)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
// TODO: Add lower bound limit
func (m *memBuffer) IterReverse(k kv.Key) (kv.Iterator, error) {
	it, err := m.MemDB.IterReverse(k)
	return newKVIterator(it, m), derr.ToTiDBErr(err)
}

// SnapshotIter returns a Iterator for a snapshot of MemBuffer.
func (m *memBuffer) SnapshotIter(k, upperbound kv.Key) kv.Iterator {
	it := m.MemDB.SnapshotIter(k, upperbound)
	return newKVIterator(it, m)
}

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (m *memBuffer) SnapshotGetter() kv.Getter {
	return newKVGetter(m.MemDB.SnapshotGetter(), m)
}

type tikvGetter struct {
	tikv.Getter
	checker visibilityChecker
}

func newKVGetter(getter tikv.Getter, checker visibilityChecker) kv.Getter {
	return &tikvGetter{getter, checker}
}

func (g *tikvGetter) Get(_ context.Context, k kv.Key) ([]byte, error) {
	if g.checker.invisible(k) {
		return nil, kv.ErrNotExist
	}
	data, err := g.Getter.Get(k)
	return data, derr.ToTiDBErr(err)
}

// tikvIterator wraps tikv.Iterator as kv.Iterator
type tikvIterator struct {
	tikv.Iterator
	checker visibilityChecker
	initErr error
}

func newKVIterator(iterator tikv.Iterator, checker visibilityChecker) kv.Iterator {
	it := &tikvIterator{iterator, checker, nil}
	if it.Valid() && it.checker.invisible(it.Key()) {
		// skip first invisible key
		it.initErr = it.Next()
	}
	return it
}

func (it *tikvIterator) Key() kv.Key {
	return kv.Key(it.Iterator.Key())
}

func (it *tikvIterator) Next() error {
	if it.initErr != nil {
		err := it.initErr
		it.initErr = nil
		return err
	}
	for {
		err := it.Iterator.Next()
		if err != nil {
			return err
		}
		if !it.Valid() {
			return nil
		}
		if !it.checker.invisible(it.Key()) {
			return nil
		}
	}
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
