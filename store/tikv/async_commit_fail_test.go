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
	"bytes"
	"context"
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/store/mockstore/unistore"
)

type testAsyncCommitFailSuite struct {
	OneByOneSuite
	testAsyncCommitCommon
	cluster cluster.Cluster
	store   *tikvStore
}

var _ = SerialSuites(&testAsyncCommitFailSuite{})

func (s *testAsyncCommitFailSuite) SetUpTest(c *C) {
	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)

	s.store = store.(*tikvStore)
}

// TestFailCommitPrimaryRpcErrors tests rpc errors are handled properly when
// committing primary region task.
func (s *testAsyncCommitFailSuite) TestFailAsyncCommitPrewriteRpcErrors(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.Enable = true
	})
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/noRetryOnRpcError", "return(true)"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcPrewriteTimeout", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcPrewriteTimeout"), IsNil)
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/noRetryOnRpcError"), IsNil)
	}()
	// The rpc error will be wrapped to ErrResultUndetermined.
	t1, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = t1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	ctx := context.WithValue(context.Background(), sessionctx.ConnID, uint64(1))
	err = t1.Commit(ctx)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, terror.ErrResultUndetermined), IsTrue, Commentf("%s", errors.ErrorStack(err)))

	// We don't need to call "Rollback" after "Commit" fails.
	err = t1.Rollback()
	c.Assert(err, Equals, kv.ErrInvalidTxn)

	// Create a new transaction to check. The previous transaction should actually commit.
	t2, err := s.store.Begin()
	c.Assert(err, IsNil)
	res, err := t2.Get(context.Background(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(res, []byte("a1")), IsTrue)
}

func (s *testAsyncCommitFailSuite) TestSecondaryListInPrimaryLock(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.Enable = true
	})

	s.putAlphabets(c, s.store)

	// Split into several regions.
	for _, splitKey := range []string{"h", "o", "u"} {
		bo := NewBackofferWithVars(context.Background(), 5000, nil)
		loc, err := s.store.GetRegionCache().LocateKey(bo, []byte(splitKey))
		c.Assert(err, IsNil)
		newRegionID := s.cluster.AllocID()
		newPeerID := s.cluster.AllocID()
		s.cluster.Split(loc.Region.GetID(), newRegionID, []byte(splitKey), []uint64{newPeerID}, newPeerID)
		s.store.GetRegionCache().InvalidateCachedRegion(loc.Region)
	}

	// Ensure the region has been split
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := s.store.GetRegionCache().LocateKey(bo, []byte("i"))
	c.Assert(err, IsNil)
	c.Assert([]byte(loc.StartKey), BytesEquals, []byte("h"))
	c.Assert([]byte(loc.EndKey), BytesEquals, []byte("o"))

	loc, err = s.store.GetRegionCache().LocateKey(bo, []byte("p"))
	c.Assert(err, IsNil)
	c.Assert([]byte(loc.StartKey), BytesEquals, []byte("o"))
	c.Assert([]byte(loc.EndKey), BytesEquals, []byte("u"))

	var connID uint64 = 0
	test := func(keys []string, values []string) {
		connID++
		ctx := context.WithValue(context.Background(), sessionctx.ConnID, connID)

		txn, err := s.store.Begin()
		c.Assert(err, IsNil)
		for i := range keys {
			txn.Set([]byte(keys[i]), []byte(values[i]))
		}

		c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/asyncCommitDoNothing", "return"), IsNil)

		err = txn.Commit(ctx)
		c.Assert(err, IsNil)

		tikvTxn := txn.(*tikvTxn)
		primary := tikvTxn.committer.primary()
		bo := NewBackofferWithVars(context.Background(), 5000, nil)
		txnStatus, err := s.store.lockResolver.getTxnStatus(bo, txn.StartTS(), primary, 0, 0, false)
		c.Assert(err, IsNil)
		c.Assert(txnStatus.IsCommitted(), IsFalse)
		c.Assert(txnStatus.action, Equals, kvrpcpb.Action_NoAction)
		// Currently when the transaction has no secondary, the `secondaries` field of the txnStatus
		// will be set nil. So here initialize the `expectedSecondaries` to nil too.
		var expectedSecondaries [][]byte
		for _, k := range keys {
			if !bytes.Equal([]byte(k), primary) {
				expectedSecondaries = append(expectedSecondaries, []byte(k))
			}
		}
		sort.Slice(expectedSecondaries, func(i, j int) bool {
			return bytes.Compare(expectedSecondaries[i], expectedSecondaries[j]) < 0
		})

		gotSecondaries := txnStatus.primaryLock.GetSecondaries()
		sort.Slice(gotSecondaries, func(i, j int) bool {
			return bytes.Compare(gotSecondaries[i], gotSecondaries[j]) < 0
		})

		c.Assert(gotSecondaries, DeepEquals, expectedSecondaries)

		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/asyncCommitDoNothing"), IsNil)
	}

	test([]string{"a"}, []string{"a1"})
	test([]string{"a", "b"}, []string{"a2", "b2"})
	test([]string{"a", "b", "d"}, []string{"a3", "b3", "d3"})
	test([]string{"a", "b", "h", "i", "u"}, []string{"a4", "b4", "h4", "i4", "u4"})
	test([]string{"i", "a", "z", "u", "b"}, []string{"i5", "a5", "z5", "u5", "b5"})
}
