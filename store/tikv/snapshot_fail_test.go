// Copyright 2016 PingCAP, Inc.
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
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/unistore"
)

type testSnapshotFailSuite struct {
	OneByOneSuite
	store *tikvStore
}

var _ = SerialSuites(&testSnapshotFailSuite{})

func (s *testSnapshotFailSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)
	s.store = store.(*tikvStore)
}

func (s *testSnapshotFailSuite) TearDownSuite(c *C) {
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testSnapshotFailSuite) TestBatchGetResponseKeyError(c *C) {
	// Meaningless to test with tikv because it has a mock key error
	if *WithTiKV {
		return
	}
	// Put two KV pairs
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set([]byte("k1"), []byte("v1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("k2"), []byte("v2"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcBatchGetResult", `1*return("keyError")`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcBatchGetResult"), IsNil)
	}()

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	res, err := txn.BatchGet(context.Background(), []kv.Key{[]byte("k1"), []byte("k2")})
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2")})
}

func (s *testSnapshotFailSuite) TestScanResponseKeyError(c *C) {
	// Meaningless to test with tikv because it has a mock key error
	if *WithTiKV {
		return
	}
	// Put two KV pairs
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set([]byte("k1"), []byte("v1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("k2"), []byte("v2"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("k3"), []byte("v3"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcScanResult", `1*return("keyError")`), IsNil)
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	iter, err := txn.Iter([]byte("a"), []byte("z"))
	c.Assert(err, IsNil)
	c.Assert(iter.Key(), DeepEquals, kv.Key("k1"))
	c.Assert(iter.Value(), DeepEquals, []byte("v1"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Key(), DeepEquals, kv.Key("k2"))
	c.Assert(iter.Value(), DeepEquals, []byte("v2"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Key(), DeepEquals, kv.Key("k3"))
	c.Assert(iter.Value(), DeepEquals, []byte("v3"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Valid(), IsFalse)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcScanResult"), IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcScanResult", `1*return("keyError")`), IsNil)
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	iter, err = txn.Iter([]byte("k2"), []byte("k4"))
	c.Assert(err, IsNil)
	c.Assert(iter.Key(), DeepEquals, kv.Key("k2"))
	c.Assert(iter.Value(), DeepEquals, []byte("v2"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Key(), DeepEquals, kv.Key("k3"))
	c.Assert(iter.Value(), DeepEquals, []byte("v3"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Valid(), IsFalse)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcScanResult"), IsNil)
}
