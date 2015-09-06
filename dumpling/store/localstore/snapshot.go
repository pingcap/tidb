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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
)

var (
	_ kv.Snapshot = (*dbSnapshot)(nil)
	_ kv.Iterator = (*dbIter)(nil)
)

type dbSnapshot struct {
	engine.Snapshot
}

func (s *dbSnapshot) Get(k []byte) ([]byte, error) {
	// engine Snapshot return nil, nil for value not found,
	// so here we will check nil and return kv.ErrNotExist.
	v, err := s.Snapshot.Get(k)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, kv.ErrNotExist
	}

	return v, nil
}

func (s *dbSnapshot) NewIterator(param interface{}) kv.Iterator {
	startKey, ok := param.([]byte)
	if !ok {
		log.Errorf("leveldb iterator parameter error, %+v", param)
		return nil
	}
	it := s.Snapshot.NewIterator(startKey)
	return newDBIter(it)
}

func (s *dbSnapshot) Release() {
	if s.Snapshot != nil {
		s.Snapshot.Release()
		s.Snapshot = nil
	}
}

type dbIter struct {
	engine.Iterator
	valid bool
}

func newDBIter(it engine.Iterator) *dbIter {
	return &dbIter{
		Iterator: it,
		valid:    it.Next(),
	}
}

func (it *dbIter) Next(fn kv.FnKeyCmp) (kv.Iterator, error) {
	it.valid = it.Iterator.Next()
	return it, nil
}

func (it *dbIter) Valid() bool {
	return it.valid
}

func (it *dbIter) Key() string {
	return string(it.Iterator.Key())
}

func (it *dbIter) Value() []byte {
	return it.Iterator.Value()
}

func (it *dbIter) Close() {
	if it.Iterator != nil {
		it.Iterator.Release()
		it.Iterator = nil
	}
}
