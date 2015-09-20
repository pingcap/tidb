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

package localstore

import (
	"bytes"
	"math"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/util/codec"
)

var (
	_ kv.Snapshot = (*dbSnapshot)(nil)
	_ kv.Iterator = (*dbIter)(nil)
)

type dbSnapshot struct {
	engine.Snapshot
}

func (s *dbSnapshot) Get(k kv.Key) ([]byte, error) {
	// engine Snapshot return nil, nil for value not found,
	// so here we will check nil and return kv.ErrNotExist.

	// get newest version, (0, MaxUint64)
	// Key arrangement:
	// Key -> META
	// ...
	// Key_ver
	// Key_ver-1
	// Key_ver-2
	// ...
	// Key_ver-n
	// Key_0
	// NextKey -> META
	// NextKey_xxx
	startKey := MvccEncodeVersionKey(k, kv.Version{math.MaxUint64})
	endKey := MvccEncodeVersionKey(k, kv.Version{0})

	// get raw iterator
	it := s.Snapshot.NewIterator(startKey)
	defer it.Release()

	var rawKey []byte
	var v []byte
	if it.Next() {
		// If scan exceed this key's all versions
		// it.Key() > endKey.
		if kv.EncodedKey(it.Key()).Cmp(endKey) < 0 {
			// Check newest version of this key.
			// If it's tombstone, just skip it.
			if !IsTombstone(it.Value()) {
				rawKey = it.Key()
				v = it.Value()
			}
		}
	}

	// No such key or v is nil.
	if rawKey == nil || v == nil {
		return nil, kv.ErrNotExist
	}

	return v, nil
}

func (s *dbSnapshot) NewIterator(param interface{}) kv.Iterator {
	k, ok := param.([]byte)
	if !ok {
		log.Errorf("leveldb iterator parameter error, %+v", param)
		return nil
	}
	return newDBIter(s, k)
}

func (s *dbSnapshot) Release() {
	if s.Snapshot != nil {
		s.Snapshot.Release()
		s.Snapshot = nil
	}
}

type dbIter struct {
	s        *dbSnapshot
	startKey kv.Key
	valid    bool
	k        kv.Key
	v        []byte
}

func newDBIter(s *dbSnapshot, startKey kv.Key) *dbIter {
	it := &dbIter{
		s:        s,
		startKey: startKey,
		valid:    true,
	}
	it.Next(nil)
	return it
}

func (it *dbIter) Next(fn kv.FnKeyCmp) (kv.Iterator, error) {
	encKey := codec.EncodeBytes(nil, it.startKey)
	// max key
	encEndKey := codec.EncodeBytes(nil, []byte{0xff, 0xff})
	for {
		engineIter := it.s.Snapshot.NewIterator(encKey)
		defer engineIter.Release()

		// Check if overflow
		if !engineIter.Next() {
			it.valid = false
			return it, nil
		}

		// Check if overflow
		metaKey := engineIter.Key()
		if bytes.Compare(metaKey, encEndKey) >= 0 {
			it.valid = false
			return it, nil
		}

		// Get real key from metaKey
		key, _, err := MvccDecode(metaKey)
		if err != nil {
			// It's not a valid metaKey, maybe overflow.
			it.valid = false
			return it, err
		}
		// Get kv pair.
		val, err := it.s.Get(key)
		if err != nil && err != kv.ErrNotExist {
			// Get this version error
			it.valid = false
			return it, err
		}
		if val != nil {
			it.k = key
			it.v = val
			it.startKey = key.Next()
			return it, nil
		}
		// Current key's all versions are deleted, just go next key.
		encKey = codec.EncodeBytes(nil, key.Next())
	}
	return it, nil
}

func (it *dbIter) Valid() bool {
	return it.valid
}

func (it *dbIter) Key() string {
	return string(it.k)
}

func (it *dbIter) Value() []byte {
	return it.v
}

func (it *dbIter) Close() {}
