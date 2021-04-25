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

func (s *testSnapshotFailSuite) TestRetryMaxTsPointGetSkipLock(c *C) {
	defer s.cleanup(c)

	// Prewrite k1 and k2 with async commit but don't commit them
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set([]byte("k1"), []byte("v1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("k2"), []byte("v2"))
	c.Assert(err, IsNil)
	txn.SetOption(kv.EnableAsyncCommit, true)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/asyncCommitDoNothing", "return"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/twoPCShortLockTTL", "return"), IsNil)
	committer, err := txn.NewCommitter(1)
	c.Assert(err, IsNil)
	err = committer.Execute(context.Background())
	c.Assert(err, IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/twoPCShortLockTTL"), IsNil)

	snapshot := s.store.GetSnapshot(math.MaxUint64)
	getCh := make(chan []byte)
	go func() {
		// Sleep a while to make the TTL of the first txn expire, then we make sure we resolve lock by this get
		time.Sleep(200 * time.Millisecond)
		c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/beforeSendPointGet", "1*off->pause"), IsNil)
		res, err := snapshot.Get(context.Background(), []byte("k2"))
		c.Assert(err, IsNil)
		getCh <- res
	}()
	// The get should be blocked by the failpoint. But the lock should have been resolved.
	select {
	case res := <-getCh:
		c.Errorf("too early %s", string(res))
	case <-time.After(1 * time.Second):
	}

	// Prewrite k1 and k2 again without committing them
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	txn.SetOption(kv.EnableAsyncCommit, true)
	err = txn.Set([]byte("k1"), []byte("v3"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("k2"), []byte("v4"))
	c.Assert(err, IsNil)
	committer, err = txn.NewCommitter(1)
	c.Assert(err, IsNil)
	err = committer.Execute(context.Background())
	c.Assert(err, IsNil)

	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/beforeSendPointGet"), IsNil)

	// After disabling the failpoint, the get request should bypass the new locks and read the old result
	select {
	case res := <-getCh:
		c.Assert(res, DeepEquals, []byte("v2"))
	case <-time.After(1 * time.Second):
		c.Errorf("get timeout")
	}
}

func (s *testSnapshotFailSuite) TestRetryPointGetResolveTS(c *C) {
	defer s.cleanup(c)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	c.Assert(txn.Set([]byte("k1"), []byte("v1")), IsNil)
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
