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

package tikv

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
)

var (
	_ kv.Snapshot = (*tikvSnapshot)(nil)
	_ kv.Iterator = (*tikvIter)(nil)
)

const tikvBatchSize = 1000

// tikvSnapshot implements MvccSnapshot interface.
type tikvSnapshot struct {
	storeName string
}

// newTiKVSnapshot creates a snapshot of an TiKV store.
func newTiKVSnapshot(storeName string) *tikvSnapshot {
	return &tikvSnapshot{
		storeName: storeName,
	}
}

// Get gets the value for key k from snapshot.
func (s *tikvSnapshot) Get(k kv.Key) ([]byte, error) {
	// TODO: impl this
	return nil, nil
	//g := tikv.NewGet([]byte(k))
	//g.AddColumn(tikvColFamilyBytes, tikvQualifierBytes)
	//v, err := internalGet(s, g)
	//if err != nil {
	//return nil, errors.Trace(err)
	//}
	//return v, nil
}

// BatchGet implements kv.Snapshot.BatchGet interface.
func (s *tikvSnapshot) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	m := make(map[string][]byte, len(keys))
	var err error
	for _, key := range keys {
		k := string(key)
		m[k], err = s.Get(key)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return m, nil
	//gets := make([]*tikv.Get, len(keys))
	//for i, key := range keys {
	//g := tikv.NewGet(key)
	//g.AddColumn(tikvColFamilyBytes, tikvQualifierBytes)
	//gets[i] = g
	//}
	//rows, err := s.txn.Gets(s.storeName, gets)
	//if err != nil {
	//return nil, errors.Trace(err)
	//}

	//m := make(map[string][]byte, len(rows))
	//for _, r := range rows {
	//k := string(r.Row)
	//v := r.Columns[tikvFmlAndQual].Value
	//m[k] = v
	//}
	//return m, nil
}

//func internalGet(s *tikvSnapshot, g *tikv.Get) ([]byte, error) {
//r, err := s.txn.Get(s.storeName, g)
//if err != nil {
//return nil, errors.Trace(err)
//}
//if r == nil || len(r.Columns) == 0 {
//return nil, errors.Trace(kv.ErrNotExist)
//}
//return r.Columns[tikvFmlAndQual].Value, nil
//}

func (s *tikvSnapshot) Seek(k kv.Key) (kv.Iterator, error) {
	// TODO: impl this
	return newTiKVIter(), nil
	//scanner := s.txn.GetScanner([]byte(s.storeName), []byte(k), nil, tikvBatchSize)
	//return newInnerScanner(scanner), nil
}

func (s *tikvSnapshot) Release() {
	// TODO: impl this
	//if s.txn != nil {
	//s.txn.Release()
	//s.txn = nil
	//}
}

type tikvIter struct {
	valid bool
	k     kv.Key
	v     []byte
}

func newTiKVIter() *tikvIter {
	return &tikvIter{}
}

func (it *tikvIter) Next() error {
	// TODO: impl this
	return nil
}

func (it *tikvIter) Valid() bool {
	return it.valid
}

func (it *tikvIter) Key() kv.Key {
	return it.k
}

func (it *tikvIter) Value() []byte {
	return it.v
}

func (it *tikvIter) Close() {
	it.valid = false
}
