// Copyright 2020 PingCAP, Inc.
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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx"
)

type testOnePCSuite struct {
	OneByOneSuite
	testAsyncCommitCommon
	bo *Backoffer
}

var _ = SerialSuites(&testOnePCSuite{})

func (s *testOnePCSuite) SetUpTest(c *C) {
	s.testAsyncCommitCommon.setUpTest(c, true)
	s.bo = NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testOnePCSuite) Test1PC(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableOnePC = true
	})

	ctx := context.WithValue(context.Background(), sessionctx.ConnID, uint64(1))

	k1 := []byte("k1")
	v1 := []byte("v1")

	txn := s.begin(c)
	err := txn.Set(k1, v1)
	c.Assert(err, IsNil)
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isOnePC(), IsTrue)
	c.Assert(txn.committer.onePCCommitTS, Equals, txn.committer.commitTS)
	c.Assert(txn.committer.onePCCommitTS, Greater, txn.startTS)

	// 1PC doesn't work if connID == 0
	k2 := []byte("k2")
	v2 := []byte("v2")

	txn = s.begin(c)
	err = txn.Set(k2, v2)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isOnePC(), IsFalse)
	c.Assert(txn.committer.onePCCommitTS, Equals, uint64(0))
	c.Assert(txn.committer.commitTS, Greater, txn.startTS)

	// 1PC doesn't work if config not set
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableOnePC = false
	})
	k3 := []byte("k3")
	v3 := []byte("v3")

	txn = s.begin(c)
	err = txn.Set(k3, v3)
	c.Assert(err, IsNil)
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isOnePC(), IsFalse)
	c.Assert(txn.committer.onePCCommitTS, Equals, uint64(0))
	c.Assert(txn.committer.commitTS, Greater, txn.startTS)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableOnePC = true
	})

	// Test multiple keys
	k4 := []byte("k4")
	v4 := []byte("v4")
	k5 := []byte("k5")
	v5 := []byte("v5")
	k6 := []byte("k6")
	v6 := []byte("v6")

	txn = s.begin(c)
	err = txn.Set(k4, v4)
	c.Assert(err, IsNil)
	err = txn.Set(k5, v5)
	c.Assert(err, IsNil)
	err = txn.Set(k6, v6)
	c.Assert(err, IsNil)
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isOnePC(), IsTrue)
	c.Assert(txn.committer.onePCCommitTS, Equals, txn.committer.commitTS)
	c.Assert(txn.committer.onePCCommitTS, Greater, txn.startTS)
	// Check keys are committed with the same version
	s.mustGetFromSnapshot(c, txn.commitTS, k4, v4)
	s.mustGetFromSnapshot(c, txn.commitTS, k5, v5)
	s.mustGetFromSnapshot(c, txn.commitTS, k6, v6)
	s.mustGetNoneFromSnapshot(c, txn.commitTS-1, k4)
	s.mustGetNoneFromSnapshot(c, txn.commitTS-1, k5)
	s.mustGetNoneFromSnapshot(c, txn.commitTS-1, k6)

	// Overwriting in MVCC
	v6New := []byte("v6new")
	txn = s.begin(c)
	err = txn.Set(k6, v6New)
	c.Assert(err, IsNil)
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isOnePC(), IsTrue)
	c.Assert(txn.committer.onePCCommitTS, Equals, txn.committer.commitTS)
	c.Assert(txn.committer.onePCCommitTS, Greater, txn.startTS)
	s.mustGetFromSnapshot(c, txn.commitTS, k6, v6New)
	s.mustGetFromSnapshot(c, txn.commitTS-1, k6, v6)

	// Check all keys
	keys := [][]byte{k1, k2, k3, k4, k5, k6}
	values := [][]byte{v1, v2, v3, v4, v5, v6New}
	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	snap := s.store.GetSnapshot(ver)
	for i, k := range keys {
		v, err := snap.Get(ctx, k)
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, values[i])
	}
}

func (s *testOnePCSuite) Test1PCIsolation(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableOnePC = true
	})

	ctx := context.WithValue(context.Background(), sessionctx.ConnID, uint64(1))

	k := []byte("k")
	v1 := []byte("v1")

	txn := s.begin(c)
	txn.Set(k, v1)
	err := txn.Commit(ctx)
	c.Assert(err, IsNil)

	v2 := []byte("v2")
	txn = s.begin(c)
	txn.Set(k, v2)

	time.Sleep(time.Millisecond * 300)

	txn2 := s.begin(c)
	s.mustGetFromTxn(c, txn2, k, v1)

	err = txn.Commit(ctx)
	c.Assert(txn.committer.isOnePC(), IsTrue)
	c.Assert(err, IsNil)

	s.mustGetFromTxn(c, txn2, k, v1)
	c.Assert(txn2.Rollback(), IsNil)

	s.mustGetFromSnapshot(c, txn.commitTS, k, v2)
	s.mustGetFromSnapshot(c, txn.commitTS-1, k, v1)
}

func (s *testOnePCSuite) Test1PCDisallowMultiRegion(c *C) {
	// This test doesn't support tikv mode.
	if *WithTiKV {
		return
	}

	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableOnePC = true
	})

	ctx := context.WithValue(context.Background(), sessionctx.ConnID, uint64(1))

	txn := s.begin(c)

	keys := []string{"k0", "k1", "k2", "k3"}
	values := []string{"v0", "v1", "v2", "v3"}

	err := txn.Set([]byte(keys[0]), []byte(values[0]))
	c.Assert(err, IsNil)
	err = txn.Set([]byte(keys[3]), []byte(values[3]))
	c.Assert(err, IsNil)
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)

	// 1PC doesn't work if it affects multiple regions.
	loc, err := s.store.regionCache.LocateKey(s.bo, []byte(keys[2]))
	c.Assert(err, IsNil)
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(loc.Region.id, newRegionID, []byte(keys[2]), []uint64{newPeerID}, newPeerID)

	txn = s.begin(c)
	err = txn.Set([]byte(keys[1]), []byte(values[1]))
	c.Assert(err, IsNil)
	err = txn.Set([]byte(keys[2]), []byte(values[2]))
	c.Assert(err, IsNil)
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isOnePC(), IsFalse)
	c.Assert(txn.committer.onePCCommitTS, Equals, uint64(0))
	c.Assert(txn.committer.commitTS, Greater, txn.startTS)

	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	snap := s.store.GetSnapshot(ver)
	for i, k := range keys {
		v, err := snap.Get(ctx, []byte(k))
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, []byte(values[i]))
	}
}
