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
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/bytes"
	"github.com/pingcap/tidb/util/codec"
)

var (
	_ kv.Snapshot     = (*dbSnapshot)(nil)
	_ kv.MvccSnapshot = (*dbSnapshot)(nil)
	_ kv.Iterator     = (*dbIter)(nil)
)

// dbSnapshot implements MvccSnapshot interface.
type dbSnapshot struct {
	store    *dbStore
	db       engine.DB
	rawIt    engine.Iterator
	version  kv.Version // transaction begin version
	released bool
}

var minKey = []byte{0}

func newSnapshot(store *dbStore, db engine.DB, ver kv.Version) *dbSnapshot {
	ss := &dbSnapshot{
		store:   store,
		db:      db,
		version: ver,
	}

	return ss
}

func (s *dbSnapshot) internalSeek(startKey []byte) (engine.Iterator, error) {
	s.store.mu.RLock()
	defer s.store.mu.RUnlock()

	if s.store.closed {
		return nil, errors.Trace(ErrDBClosed)
	}

	if s.rawIt == nil {
		var err error
		s.rawIt, err = s.db.Seek(minKey)
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

func (s *dbSnapshot) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	m := make(map[string][]byte)
	for _, k := range keys {
		v, err := s.Get(k)
		if err != nil && !kv.IsErrNotFound(err) {
			return nil, errors.Trace(err)
		}
		if len(v) > 0 {
			m[string(k)] = v
		}
	}
	return m, nil
}

func (s *dbSnapshot) RangeGet(start, end kv.Key, limit int) (map[string][]byte, error) {
	m := make(map[string][]byte)
	it := s.NewIterator([]byte(start))
	defer it.Close()
	endKey := string(end)
	for i := 0; i < limit; i++ {
		if !it.Valid() {
			break
		}
		if it.Key() > endKey {
			break
		}
		m[it.Key()] = it.Value()
		err := it.Next()
		if err != nil {
			return nil, err
		}
	}
	return m, nil
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
	if s.released {
		return
	}

	s.released = true
	if s.rawIt != nil {
		// TODO: check whether Release will panic if store is closed.
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

func (it *dbIter) Next() error {
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
		if err != nil && !terror.ErrorEqual(err, kv.ErrNotExist) {
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
	return errors.Trace(retErr)
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
