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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/util/bytes"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/errors2"
)

var (
	_ kv.Snapshot     = (*dbSnapshot)(nil)
	_ kv.MvccSnapshot = (*dbSnapshot)(nil)
	_ kv.Iterator     = (*dbIter)(nil)
)

// dbSnapshot implements MvccSnapshot interface.
type dbSnapshot struct {
	db      engine.DB
	rawIt   engine.Iterator
	version kv.Version // transaction begin version
}

func (s *dbSnapshot) internalSeek(startKey []byte) (engine.Iterator, error) {
	if s.rawIt == nil {
		var err error
		s.rawIt, err = s.db.Seek([]byte{0})
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	ok := s.rawIt.Seek(startKey)
	if !ok {
		s.rawIt.Release()
		s.rawIt = nil
		return nil, kv.ErrNotExist
	}
	return s.rawIt, nil
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

	it, err := s.internalSeek(startKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var rawKey []byte
	var v []byte
	// Check if the scan is not exceed this key's all versions and the value is not
	// tombstone.
	if kv.EncodedKey(it.Key()).Cmp(endKey) < 0 && !isTombstone(it.Value()) {
		rawKey = it.Key()
		v = it.Value()
	}
	// No such key (or it's tombstone).
	if rawKey == nil {
		return nil, kv.ErrNotExist
	}
	return bytes.CloneBytes(v), nil
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

func (s *dbSnapshot) Release() {
	if s.rawIt != nil {
		s.rawIt.Release()
		s.rawIt = nil
	}
}

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
	it.Next()
	return it
}

func (it *dbIter) Next() (kv.Iterator, error) {
	encKey := codec.EncodeBytes(nil, it.startKey)
	var retErr error
	var engineIter engine.Iterator
	for {
		var err error
		engineIter, err = it.s.internalSeek(encKey)
		if err != nil {
			it.valid = false
			retErr = err
			break
		}

		metaKey := engineIter.Key()
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
			it.k = bytes.CloneBytes(key)
			it.v = bytes.CloneBytes(val)
			it.startKey = key.Next()
			break
		}
		// Current key's all versions are deleted, just go next key.
		encKey = codec.EncodeBytes(nil, key.Next())
	}
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
