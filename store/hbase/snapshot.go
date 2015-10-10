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
	"github.com/pingcap/go-themis"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/codec"
)

var (
	_ kv.Snapshot = (*hbaseSnapshot)(nil)
	_ kv.Iterator = (*hbaseIter)(nil)
)

type hbaseSnapshot struct {
	txn       *themis.Txn
	storeName string
}

func (s *hbaseSnapshot) Get(k kv.Key) ([]byte, error) {
	g := hbase.NewGet(codec.EncodeBytes(nil, k))
	g.AddColumn([]byte(ColFamily), []byte(Qualifier))
	s.txn.Get(s.storeName, g)
	r, err := s.txn.Get(s.storeName, g)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, kv.ErrNotExist
	}
	return r.Columns[ColFamily+":"+Qualifier].Value, nil
}

func (s *hbaseSnapshot) NewIterator(startKey interface{}) kv.Iterator {
	scanner := s.txn.GetScanner([]byte(s.storeName), startKey.([]byte), nil)
	it := &hbaseIter{
		ThemisScanner: scanner,
	}
	r, _ := it.Next(nil)
	return r
}

func (s *hbaseSnapshot) Release() {
	if s.txn != nil {
		s.txn.Release()
		s.txn = nil
	}
}

type hbaseIter struct {
	*themis.ThemisScanner
	rs    *hbase.ResultRow
	valid bool
}

func (it *hbaseIter) Next(fn kv.FnKeyCmp) (kv.Iterator, error) {
	it.rs = it.ThemisScanner.Next()
	it.valid = it.rs != nil && !it.ThemisScanner.Closed()
	return it, nil
}

func (it *hbaseIter) Valid() bool {
	return it.valid
}

func (it *hbaseIter) Key() string {
	return string(it.rs.Row)
}

func (it *hbaseIter) Value() []byte {
	return it.rs.Columns[ColFamily+":"+Qualifier].Value
}

func (it *hbaseIter) Close() {
	if it.ThemisScanner != nil {
		it.ThemisScanner.Close()
		it.ThemisScanner = nil
	}
}
