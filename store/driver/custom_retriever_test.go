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
package driver

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	txn2 "github.com/pingcap/tidb/store/driver/txn"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

type testCustomRetrieverSuite struct {
	store kv.Storage
}

var _ = SerialSuites(&testCustomRetrieverSuite{})

func (s *testCustomRetrieverSuite) SetUpTest(c *C) {
	var err error
	s.store, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
}

func (s *testCustomRetrieverSuite) TearDownTest(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testCustomRetrieverSuite) TestSnapshotGetWithCustomRetrievers(c *C) {
	snap := s.prepareSnapshot(c, [][]interface{}{
		{"a0", "s0"},
		{"a01", "s01"},
		{"a1", "s1"},
		{"a12", "s12"},
		{"a2", "s2"},
	})

	snap.SetOption(kv.SortedCustomRetrievers, []*txn2.RangedKVRetriever{
		s.newMemBufferRetriever(c, "a1", "a2", [][]interface{}{
			{"a0", "v0"},
			{"a02", "v02"},
			{"a1", "v1"},
			{"a11", "v11"},
			{"a1x", ""},
		}),
		s.newMemBufferRetriever(c, "a3", "a4", [][]interface{}{
			{"a1", "vx"},
		}),
	})

	cases := [][]interface{}{
		{"a0", "s0"},
		{"a01", "s01"},
		{"a02", kv.ErrNotExist},
		{"a03", kv.ErrNotExist},
		{"a1", "v1"},
		{"a11", "v11"},
		{"a12", kv.ErrNotExist},
		{"a13", kv.ErrNotExist},
		{"a1x", kv.ErrNotExist},
	}

	ctx := context.Background()
	for _, ca := range cases {
		val, err := snap.Get(ctx, makeBytes(ca[0]))
		if expectedErr, ok := ca[1].(error); ok {
			c.Assert(errors.ErrorEqual(expectedErr, err), IsTrue)
			c.Assert(val, IsNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(makeBytes(ca[1]), BytesEquals, val)
		}
	}
}

func (s *testCustomRetrieverSuite) TestSnapshotBatchGetWithCustomRetrievers(c *C) {
	snap := s.prepareSnapshot(c, [][]interface{}{
		{"a0", "s0"},
		{"a01", "s01"},
		{"a1", "s1"},
		{"a12", "s12"},
		{"a2", "s2"},
	})

	snap.SetOption(kv.SortedCustomRetrievers, []*txn2.RangedKVRetriever{
		s.newMemBufferRetriever(c, "a1", "a2", [][]interface{}{
			{"a0", "v0"},
			{"a02", "v02"},
			{"a1", "v1"},
			{"a11", "v11"},
			{"a1x", ""},
		}),
		s.newMemBufferRetriever(c, "a3", "a4", [][]interface{}{
			{"a1", "vx"},
		}),
	})

	cases := []struct {
		keys   []interface{}
		result map[string]string
	}{
		{
			keys:   []interface{}{},
			result: map[string]string{},
		},
		{
			keys:   []interface{}{"a0"},
			result: map[string]string{"a0": "s0"},
		},
		{
			keys:   []interface{}{"a02"},
			result: map[string]string{},
		},
		{
			keys:   []interface{}{"a1"},
			result: map[string]string{"a1": "v1"},
		},
		{
			keys:   []interface{}{"a0", "a01", "a02", "a03", "a1", "a11", "a12", "a13", "a1x", "a3", "a5"},
			result: map[string]string{"a0": "s0", "a01": "s01", "a1": "v1", "a11": "v11"},
		},
	}

	ctx := context.Background()
	for _, ca := range cases {
		keys := make([]kv.Key, 0)
		for _, k := range ca.keys {
			keys = append(keys, makeBytes(k))
		}

		m, err := snap.BatchGet(ctx, keys)
		c.Assert(err, IsNil)
		c.Assert(m, NotNil)
		c.Assert(len(m), Equals, len(ca.result))
		for k, expectedVal := range ca.result {
			val, ok := m[k]
			c.Assert(ok, IsTrue)
			c.Assert(val, BytesEquals, makeBytes(expectedVal))
		}
	}
}

func (s *testCustomRetrieverSuite) TestSnapshotIterWithCustomRetrievers(c *C) {
	snap := s.prepareSnapshot(c, [][]interface{}{
		{"a0", "s0"},
		{"a01", "s01"},
		{"a1", "s1"},
		{"a12", "s12"},
		{"a2", "s2"},
		{"a4", "s4"},
		{"a7", "s7"},
		{"a9", "s9"},
	})

	snap.SetOption(kv.SortedCustomRetrievers, []*txn2.RangedKVRetriever{
		s.newMemBufferRetriever(c, "a1", "a2", [][]interface{}{
			{"a0", "v0"},
			{"a02", "v02"},
			{"a1", "v1"},
			{"a11", "v11"},
			{"a1x", ""},
		}),
		s.newMemBufferRetriever(c, "a3", "a4", [][]interface{}{
			{"a1", "vx"},
			{"a31", "v31"},
		}),
		txn2.NewRangeRetriever(&kv.EmptyRetriever{}, makeBytes("a5"), makeBytes("a6")),
	})

	cases := []struct {
		query   []interface{}
		reverse bool
		result  [][]interface{}
	}{
		{
			query: []interface{}{nil, "a1"},
			result: [][]interface{}{
				{"a0", "s0"},
				{"a01", "s01"},
			},
		},
		{
			query: []interface{}{"a1", "a2"},
			result: [][]interface{}{
				{"a1", "v1"},
				{"a11", "v11"},
			},
		},
		{
			query: []interface{}{"a01", "a9"},
			result: [][]interface{}{
				{"a01", "s01"},
				{"a1", "v1"},
				{"a11", "v11"},
				{"a2", "s2"},
				{"a31", "v31"},
				{"a4", "s4"},
				{"a7", "s7"},
			},
		},
		{
			query:   []interface{}{"a9"},
			reverse: true,
			result: [][]interface{}{
				{"a7", "s7"},
				{"a4", "s4"},
				{"a31", "v31"},
				{"a2", "s2"},
				{"a11", "v11"},
				{"a1", "v1"},
				{"a01", "s01"},
				{"a0", "s0"},
			},
		},
	}

	for _, ca := range cases {
		var iter kv.Iterator
		var err error
		if ca.reverse {
			iter, err = snap.IterReverse(makeBytes(ca.query[0]))
			c.Assert(err, IsNil)
		} else {
			iter, err = snap.Iter(makeBytes(ca.query[0]), makeBytes(ca.query[1]))
			c.Assert(err, IsNil)
		}

		for i := range ca.result {
			c.Assert(iter.Valid(), IsTrue)
			gotKey := iter.Key()
			gotValue := iter.Value()
			expectedKey := makeBytes(ca.result[i][0])
			expectedValue := makeBytes(ca.result[i][1])
			c.Assert([]byte(gotKey), BytesEquals, expectedKey)
			c.Assert(gotValue, BytesEquals, expectedValue)
			err = iter.Next()
			c.Assert(err, IsNil)
		}

		c.Assert(iter.Valid(), IsFalse)
	}
}

func (s *testCustomRetrieverSuite) prepareSnapshot(c *C, data [][]interface{}) kv.Snapshot {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	defer func() {
		if txn.Valid() {
			txn.Rollback()
		}
	}()

	for _, d := range data {
		err = txn.Set(makeBytes(d[0]), makeBytes(d[1]))
		c.Assert(err, IsNil)
	}

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	return s.store.GetSnapshot(kv.MaxVersion)
}

func (s *testCustomRetrieverSuite) newMemBufferRetriever(c *C, start interface{}, end interface{}, data [][]interface{}) *txn2.RangedKVRetriever {
	tmpTxn, err := transaction.NewTiKVTxn(nil, nil, 0, "")
	c.Assert(err, IsNil)
	memBuffer := txn2.NewTiKVTxn(tmpTxn).GetMemBuffer()
	for _, d := range data {
		k := makeBytes(d[0])
		val := makeBytes(d[1])
		if len(val) == 0 {
			// to test delete case
			err := memBuffer.Set(k, []byte("12345"))
			c.Assert(err, IsNil)
			err = memBuffer.Delete(k)
			c.Assert(err, IsNil)
		} else {
			err := memBuffer.Set(k, makeBytes(d[1]))
			c.Assert(err, IsNil)
		}
	}

	return txn2.NewRangeRetriever(memBuffer, makeBytes(start), makeBytes(end))
}

func makeBytes(s interface{}) []byte {
	if s == nil {
		return nil
	}

	switch key := s.(type) {
	case string:
		return []byte(key)
	default:
		return key.([]byte)
	}
}
