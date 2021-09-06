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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
)

type testTxnSuite struct {
	store kv.Storage
}

var _ = SerialSuites(&testTxnSuite{})

func (s *testTxnSuite) SetUpTest(c *C) {
	var err error
	s.store, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
}

func (s *testTxnSuite) TearDownTest(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testTxnSuite) prepareSnapshot(c *C, data [][]interface{}) kv.Snapshot {
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

func (s *testTxnSuite) TestTxnGet(c *C) {
	s.prepareSnapshot(c, [][]interface{}{{"k1", "v1"}})
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)

	// should return snapshot value if no dirty data
	v, err := txn.Get(context.Background(), kv.Key("k1"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("v1"))

	// insert but not commit
	err = txn.Set(kv.Key("k1"), kv.Key("v1+"))
	c.Assert(err, IsNil)

	// should return dirty data if dirty data exists
	v, err = txn.Get(context.Background(), kv.Key("k1"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("v1+"))

	err = txn.Set(kv.Key("k2"), []byte("v2+"))
	c.Assert(err, IsNil)

	// should return dirty data if dirty data exists
	v, err = txn.Get(context.Background(), kv.Key("k2"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("v2+"))

	// delete but not commit
	err = txn.Delete(kv.Key("k1"))
	c.Assert(err, IsNil)

	// should return kv.ErrNotExist if deleted
	v, err = txn.Get(context.Background(), kv.Key("k1"))
	c.Assert(v, IsNil)
	c.Assert(kv.ErrNotExist.Equal(err), IsTrue)

	// should return kv.ErrNotExist if not exist
	v, err = txn.Get(context.Background(), kv.Key("kn"))
	c.Assert(v, IsNil)
	c.Assert(kv.ErrNotExist.Equal(err), IsTrue)
}

func (s *testTxnSuite) TestTxnBatchGet(c *C) {
	s.prepareSnapshot(c, [][]interface{}{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k4", "v4"}})
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)

	result, err := txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k2"), kv.Key("k3"), kv.Key("kn")})
	c.Assert(err, IsNil)
	c.Assert(len(result), Equals, 3)
	c.Assert(result["k1"], BytesEquals, []byte("v1"))
	c.Assert(result["k2"], BytesEquals, []byte("v2"))
	c.Assert(result["k3"], BytesEquals, []byte("v3"))

	// make some dirty data
	err = txn.Set(kv.Key("k1"), []byte("v1+"))
	c.Assert(err, IsNil)
	err = txn.Set(kv.Key("k4"), []byte("v4+"))
	c.Assert(err, IsNil)
	err = txn.Delete(kv.Key("k2"))
	c.Assert(err, IsNil)

	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k2"), kv.Key("k3"), kv.Key("k4"), kv.Key("kn")})
	c.Assert(err, IsNil)
	c.Assert(len(result), Equals, 3)
	c.Assert(result["k1"], BytesEquals, []byte("v1+"))
	c.Assert(result["k3"], BytesEquals, []byte("v3"))
	c.Assert(result["k4"], BytesEquals, []byte("v4+"))
}

func (s *testTxnSuite) TestTxnScan(c *C) {
	s.prepareSnapshot(c, [][]interface{}{{"k1", "v1"}, {"k3", "v3"}, {"k5", "v5"}, {"k7", "v7"}, {"k9", "v9"}})
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)

	iter, err := txn.Iter(kv.Key("k3"), kv.Key("k9"))
	c.Assert(err, IsNil)
	checkIter(c, iter, [][]interface{}{{"k3", "v3"}, {"k5", "v5"}, {"k7", "v7"}})

	iter, err = txn.IterReverse(kv.Key("k9"))
	c.Assert(err, IsNil)
	checkIter(c, iter, [][]interface{}{{"k7", "v7"}, {"k5", "v5"}, {"k3", "v3"}, {"k1", "v1"}})

	// make some dirty data
	err = txn.Set(kv.Key("k1"), []byte("v1+"))
	c.Assert(err, IsNil)
	err = txn.Set(kv.Key("k3"), []byte("v3+"))
	c.Assert(err, IsNil)
	err = txn.Set(kv.Key("k31"), []byte("v31+"))
	c.Assert(err, IsNil)
	err = txn.Delete(kv.Key("k5"))
	c.Assert(err, IsNil)

	iter, err = txn.Iter(kv.Key("k3"), kv.Key("k9"))
	c.Assert(err, IsNil)
	checkIter(c, iter, [][]interface{}{{"k3", "v3+"}, {"k31", "v31+"}, {"k7", "v7"}})

	iter, err = txn.IterReverse(kv.Key("k9"))
	c.Assert(err, IsNil)
	checkIter(c, iter, [][]interface{}{{"k7", "v7"}, {"k31", "v31+"}, {"k3", "v3+"}, {"k1", "v1+"}})
}

func checkIter(c *C, iter kv.Iterator, expected [][]interface{}) {
	for _, pair := range expected {
		expectedKey := makeBytes(pair[0])
		expectedValue := makeBytes(pair[1])

		c.Assert(iter.Valid(), IsTrue)
		c.Assert([]byte(iter.Key()), BytesEquals, expectedKey)
		c.Assert(iter.Value(), BytesEquals, expectedValue)

		err := iter.Next()
		c.Assert(err, IsNil)
	}

	c.Assert(iter.Valid(), IsFalse)
}
