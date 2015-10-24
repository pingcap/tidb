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

package hbasekv

import (
	"github.com/c4pt0r/go-hbase"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis"
	"github.com/pingcap/tidb/kv"
)

var (
	_ kv.Snapshot     = (*hbaseSnapshot)(nil)
	_ kv.MvccSnapshot = (*hbaseSnapshot)(nil)
	_ kv.Iterator     = (*hbaseIter)(nil)
)

type hbaseSnapshot struct {
	txn       *themis.Txn
	storeName string
}

func (s *hbaseSnapshot) Get(k kv.Key) ([]byte, error) {
	g := hbase.NewGet([]byte(k))
	g.AddColumn([]byte(ColFamily), []byte(Qualifier))
	v, err := innerGet(s, g)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return v, nil
}

// MvccGet returns the specific version of given key, if the version doesn't
// exist, returns the nearest(lower) version's data.
func (s *hbaseSnapshot) MvccGet(k kv.Key, ver kv.Version) ([]byte, error) {
	g := hbase.NewGet([]byte(k))
	g.AddColumn([]byte(ColFamily), []byte(Qualifier))
	g.TsRangeFrom = 0
	g.TsRangeTo = ver.Ver + 1
	v, err := innerGet(s, g)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return v, nil
}

func innerGet(s *hbaseSnapshot, g *hbase.Get) ([]byte, error) {
	r, err := s.txn.Get(s.storeName, g)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if r == nil || len(r.Columns) == 0 {
		return nil, kv.ErrNotExist
	}
	return r.Columns[FmlAndQual].Value, nil
}

func (s *hbaseSnapshot) NewIterator(param interface{}) kv.Iterator {
	k, ok := param.([]byte)
	if !ok {
		log.Errorf("hbase iterator parameter error, %+v", param)
		return nil
	}

	scanner := s.txn.GetScanner([]byte(s.storeName), k, nil)
	return innerNewIterator(scanner)
}

// MvccIterator seeks to the key in the specific version's snapshot, if the
// version doesn't exist, returns the nearest(lower) version's snaphot.
func (s *hbaseSnapshot) NewMvccIterator(k kv.Key, ver kv.Version) kv.Iterator {
	scanner := s.txn.GetScanner([]byte(s.storeName), k, nil)
	scanner.SetTimeRange(0, ver.Ver+1)
	return innerNewIterator(scanner)
}

func innerNewIterator(scanner *themis.ThemisScanner) kv.Iterator {
	it := &hbaseIter{
		ThemisScanner: scanner,
	}
	it.Next()
	return it
}

func (s *hbaseSnapshot) Release() {
	if s.txn != nil {
		s.txn.Release()
		s.txn = nil
	}
}

// Release releases this snapshot.
func (s *hbaseSnapshot) MvccRelease() {
	s.Release()
}

type hbaseIter struct {
	*themis.ThemisScanner
	rs    *hbase.ResultRow
	valid bool
}

func (it *hbaseIter) Next() (kv.Iterator, error) {
	it.rs = it.ThemisScanner.Next()
	it.valid = it.rs != nil && len(it.rs.Columns) > 0 && !it.ThemisScanner.Closed()
	return it, nil
}

func (it *hbaseIter) Valid() bool {
	return it.valid
}

func (it *hbaseIter) Key() string {
	return string(it.rs.Row)
}

func (it *hbaseIter) Value() []byte {
	return it.rs.Columns[FmlAndQual].Value
}

func (it *hbaseIter) Close() {
	if it.ThemisScanner != nil {
		it.ThemisScanner.Close()
		it.ThemisScanner = nil
	}
	it.rs = nil
}
