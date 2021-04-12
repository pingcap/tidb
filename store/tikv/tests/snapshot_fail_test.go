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

package tikv_test

import (
	"context"
	"math"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/kv"
)

type testSnapshotFailSuite struct {
	OneByOneSuite
	store tikv.StoreProbe
}

var _ = SerialSuites(&testSnapshotFailSuite{})

func (s *testSnapshotFailSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)
	s.store = tikv.StoreProbe{KVStore: store}
}

func (s *testSnapshotFailSuite) TearDownSuite(c *C) {
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testSnapshotFailSuite) cleanup(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	iter, err := txn.Iter([]byte(""), []byte(""))
	c.Assert(err, IsNil)
	for iter.Valid() {
		err = txn.Delete(iter.Key())
		c.Assert(err, IsNil)
		err = iter.Next()
		c.Assert(err, IsNil)
	}
	c.Assert(txn.Commit(context.TODO()), IsNil)
}

func (s *testSnapshotFailSuite) TestBatchGetResponseKeyError(c *C) {
	// Meaningless to test with tikv because it has a mock key error
	if *WithTiKV {
		return
	}
	defer s.cleanup(c)

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
	res, err := txn.BatchGet(context.Background(), [][]byte{[]byte("k1"), []byte("k2")})
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2")})
}

func (s *testSnapshotFailSuite) TestScanResponseKeyError(c *C) {
	// Meaningless to test with tikv because it has a mock key error
	if *WithTiKV {
		return
	}
	defer s.cleanup(c)

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
	c.Assert(iter.Key(), DeepEquals, []byte("k1"))
	c.Assert(iter.Value(), DeepEquals, []byte("v1"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Key(), DeepEquals, []byte("k2"))
	c.Assert(iter.Value(), DeepEquals, []byte("v2"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Key(), DeepEquals, []byte("k3"))
	c.Assert(iter.Value(), DeepEquals, []byte("v3"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Valid(), IsFalse)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcScanResult"), IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcScanResult", `1*return("keyError")`), IsNil)
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	iter, err = txn.Iter([]byte("k2"), []byte("k4"))
	c.Assert(err, IsNil)
	c.Assert(iter.Key(), DeepEquals, []byte("k2"))
	c.Assert(iter.Value(), DeepEquals, []byte("v2"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Key(), DeepEquals, []byte("k3"))
	c.Assert(iter.Value(), DeepEquals, []byte("v3"))
	c.Assert(iter.Next(), IsNil)
	c.Assert(iter.Valid(), IsFalse)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcScanResult"), IsNil)
}

func (s *testSnapshotFailSuite) TestRetryPointGetWithTS(c *C) {
	defer s.cleanup(c)

	snapshot := s.store.GetSnapshot(math.MaxUint64)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/snapshotGetTSAsync", `pause`), IsNil)
	ch := make(chan error)
	go func() {
		_, err := snapshot.Get(context.Background(), []byte("k4"))
		ch <- err
	}()

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set([]byte("k4"), []byte("v4"))
	c.Assert(err, IsNil)
	txn.SetOption(kv.EnableAsyncCommit, true)
	txn.SetOption(kv.Enable1PC, false)
	txn.SetOption(kv.GuaranteeLinearizability, false)
	// Prewrite an async-commit lock and do not commit it.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/asyncCommitDoNothing", `return`), IsNil)
	committer, err := txn.NewCommitter(1)
	c.Assert(err, IsNil)
	// Sets its minCommitTS to one second later, so the lock will be ignored by point get.
	committer.SetMinCommitTS(committer.GetStartTS() + (1000 << 18))
	err = committer.Execute(context.Background())
	c.Assert(err, IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/snapshotGetTSAsync"), IsNil)

	err = <-ch
	c.Assert(err, ErrorMatches, ".*key not exist")

	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/asyncCommitDoNothing"), IsNil)
}

func (s *testSnapshotFailSuite) TestRetryPointGetResolveTS(c *C) {
	defer s.cleanup(c)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set([]byte("k1"), []byte("v1"))
	err = txn.Set([]byte("k2"), []byte("v2"))
	c.Assert(err, IsNil)
	txn.SetOption(kv.EnableAsyncCommit, false)
	txn.SetOption(kv.Enable1PC, false)
	txn.SetOption(kv.GuaranteeLinearizability, false)

	// Prewrite the lock without committing it
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/beforeCommit", `pause`), IsNil)
	ch := make(chan struct{})
	committer, err := txn.NewCommitter(1)
	c.Assert(committer.GetPrimaryKey(), DeepEquals, []byte("k1"))
	go func() {
		c.Assert(err, IsNil)
		err = committer.Execute(context.Background())
		c.Assert(err, IsNil)
		ch <- struct{}{}
	}()

	// Wait until prewrite finishes
	time.Sleep(200 * time.Millisecond)
	// Should get nothing with max version, and **not pushing forward minCommitTS** of the primary lock
	snapshot := s.store.GetSnapshot(math.MaxUint64)
	_, err = snapshot.Get(context.Background(), []byte("k2"))
	c.Assert(err, ErrorMatches, ".*key not exist")

	initialCommitTS := committer.GetCommitTS()
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/beforeCommit"), IsNil)

	<-ch
	// check the minCommitTS is not pushed forward
	snapshot = s.store.GetSnapshot(initialCommitTS)
	v, err := snapshot.Get(context.Background(), []byte("k2"))
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("v2"))
}
