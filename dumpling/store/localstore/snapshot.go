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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/bytes"
)

var (
	_ kv.Snapshot = (*dbSnapshot)(nil)
	_ kv.Iterator = (*dbIter)(nil)
)

// dbSnapshot implements MvccSnapshot interface.
type dbSnapshot struct {
	store   *dbStore
	db      engine.DB
	version kv.Version // transaction begin version
}

func newSnapshot(store *dbStore, db engine.DB, ver kv.Version) *dbSnapshot {
	ss := &dbSnapshot{
		store:   store,
		db:      db,
		version: ver,
	}

	return ss
}

// mvccSeek seeks for the first key in db which has a k >= key and a version <=
// snapshot's version, returns kv.ErrNotExist if such key is not found. If exact
// is true, only k == key can be returned.
func (s *dbSnapshot) mvccSeek(key kv.Key, exact bool) (kv.Key, []byte, error) {
	s.store.mu.RLock()
	defer s.store.mu.RUnlock()

	if s.store.closed {
		return nil, nil, errors.Trace(ErrDBClosed)
	}

	// Key layout:
	// ...
	// Key (Meta)      -- (1)
	// Key_verMax      -- (2)
	// ...
	// Key_ver+1       -- (3)
	// Key_ver         -- (4)
	// Key_ver-1       -- (5)
	// ...
	// Key_0           -- (6)
	// NextKey (Meta)  -- (7)
	// ...
	// EOF
	for {
		mvccKey := MvccEncodeVersionKey(key, s.version)
		mvccK, v, err := s.db.Seek(mvccKey) // search for [4...EOF)
		if err != nil {
			if terror.ErrorEqual(err, engine.ErrNotFound) { // EOF
				err = errors.Wrap(err, kv.ErrNotExist)
			}
			return nil, nil, errors.Trace(err)
		}
		k, _, err := MvccDecode(mvccK)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if key.Cmp(k) != 0 { // currently on [7]
			if exact {
				return nil, nil, errors.Trace(kv.ErrNotExist)
			}
			// search for NextKey
			key = k
			continue
		}
		if isTombstone(v) { // current key is deleted
			if exact {
				return nil, nil, errors.Trace(kv.ErrNotExist)
			}
			// search for NextKey's meta
			key = key.Next()
			continue
		}
		return k, v, nil
	}
}

func (s *dbSnapshot) Get(key kv.Key) ([]byte, error) {
	_, v, err := s.mvccSeek(key, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return v, nil
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
	it, err := s.Seek(start)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (s *dbSnapshot) Seek(k kv.Key) (kv.Iterator, error) {
	it, err := newDBIter(s, k)
	return it, errors.Trace(err)
}

func (s *dbSnapshot) Release() {
}

type dbIter struct {
	s     *dbSnapshot
	valid bool
	k     kv.Key
	v     []byte
}

func newDBIter(s *dbSnapshot, startKey kv.Key) (*dbIter, error) {
	k, v, err := s.mvccSeek(startKey, false)
	if err != nil {
		if terror.ErrorEqual(err, kv.ErrNotExist) {
			err = nil
		}
		return &dbIter{valid: false}, errors.Trace(err)
	}

	return &dbIter{
		s:     s,
		valid: true,
		k:     k,
		v:     v,
	}, nil
}

func (it *dbIter) Next() error {
	k, v, err := it.s.mvccSeek(it.k.Next(), false)
	if err != nil {
		it.valid = false
		if !terror.ErrorEqual(err, kv.ErrNotExist) {
			return errors.Trace(err)
		}
	}
	it.k, it.v = bytes.CloneBytes(k), bytes.CloneBytes(v)
	return nil
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
