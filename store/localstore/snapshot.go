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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/errors2"
)

var (
	_ kv.Snapshot     = (*dbSnapshot)(nil)
	_ kv.MvccSnapshot = (*dbSnapshot)(nil)
	_ kv.Iterator     = (*dbIter)(nil)
)

type dbSnapshot struct {
	db      engine.DB
	version kv.Version // transaction begin version
}

func (s *dbSnapshot) MvccGet(k kv.Key, ver kv.Version) ([]byte, error) {
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
	startKey := MvccEncodeVersionKey(k, ver)
	endKey := MvccEncodeVersionKey(k, kv.MinVersion)

	// get raw iterator
	it, err := s.db.Seek(startKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer it.Release()

	var rawKey []byte
	var v []byte
	if it.Next() {
		// If scan exceed this key's all versions
		// it.Key() > endKey.
		if kv.EncodedKey(it.Key()).Cmp(endKey) < 0 {
			// Check newest version of this key.
			// If it's tombstone, just skip it.
			if !isTombstone(it.Value()) {
				rawKey = it.Key()
				v = it.Value()
			}
		}
	}
	// No such key (or it's tombstone).
	if rawKey == nil {
		return nil, kv.ErrNotExist
	}
	return v, nil
}

func (s *dbSnapshot) NewMvccIterator(k kv.Key, ver kv.Version) kv.Iterator {
	return newDBIter(s, k, ver)
}

func (s *dbSnapshot) Get(k kv.Key) ([]byte, error) {
	// Get latest version.
	return s.MvccGet(k, s.version)
}

func (s *dbSnapshot) NewIterator(param interface{}) kv.Iterator {
	k, ok := param.([]byte)
	if !ok {
		log.Errorf("leveldb iterator parameter error, %+v", param)
		return nil
	}
	return newDBIter(s, k, s.version)
}

func (s *dbSnapshot) MvccRelease() {
	s.Release()
}

func (s *dbSnapshot) Release() {}

type dbIter struct {
	s               *dbSnapshot
	startKey        kv.Key
	valid           bool
	exceptedVersion kv.Version
	k               kv.Key
	v               []byte
}

func newDBIter(s *dbSnapshot, startKey kv.Key, exceptedVer kv.Version) *dbIter {
	it := &dbIter{
		s:               s,
		startKey:        startKey,
		valid:           true,
		exceptedVersion: exceptedVer,
	}
	it.Next(nil)
	return it
}

func (it *dbIter) Next(fn kv.FnKeyCmp) (kv.Iterator, error) {
	encKey := codec.EncodeBytes(nil, it.startKey)
	// max key
	encEndKey := codec.EncodeBytes(nil, []byte{0xff, 0xff})
	var retErr error
	var engineIter engine.Iterator
	for {
		engineIter, retErr = it.s.db.Seek(encKey)
		if retErr != nil {
			return nil, errors.Trace(retErr)
		}
		// Check if overflow
		if !engineIter.Next() {
			it.valid = false
			break
		}

		metaKey := engineIter.Key()
		// Check if meet the end of table.
		if bytes.Compare(metaKey, encEndKey) >= 0 {
			it.valid = false
			break
		}
		// Get real key from metaKey
		key, _, err := MvccDecode(metaKey)
		if err != nil {
			// It's not a valid metaKey, maybe overflow (other data).
			it.valid = false
			break
		}
		// Get kv pair.
		val, err := it.s.MvccGet(key, it.exceptedVersion)
		if err != nil && !errors2.ErrorEqual(err, kv.ErrNotExist) {
			// Get this version error
			it.valid = false
			retErr = err
			break
		}
		if val != nil {
			it.k = key
			it.v = val
			it.startKey = key.Next()
			break
		}
		// Release the iterator, and update key
		engineIter.Release()
		// Current key's all versions are deleted, just go next key.
		encKey = codec.EncodeBytes(nil, key.Next())
	}
	engineIter.Release()
	return it, errors.Trace(retErr)
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
