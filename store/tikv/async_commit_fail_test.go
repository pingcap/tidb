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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
)

type testAsyncCommitFailSuite struct {
	OneByOneSuite
	testAsyncCommitCommon
}

var _ = SerialSuites(&testAsyncCommitFailSuite{})

func (s *testAsyncCommitFailSuite) SetUpTest(c *C) {
	s.testAsyncCommitCommon.setUpTest(c)
}

// TestFailCommitPrimaryRpcErrors tests rpc errors are handled properly when
// committing primary region task.
func (s *testAsyncCommitFailSuite) TestFailAsyncCommitPrewriteRpcErrors(c *C) {
	// This test doesn't support tikv mode because it needs setting failpoint in unistore.
	if *WithTiKV {
		return
	}

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/noRetryOnRpcError", "return(true)"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcPrewriteTimeout", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcPrewriteTimeout"), IsNil)
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/noRetryOnRpcError"), IsNil)
	}()
	// The rpc error will be wrapped to ErrResultUndetermined.
	t1 := s.beginAsyncCommit(c)
	err := t1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	ctx := context.WithValue(context.Background(), sessionctx.ConnID, uint64(1))
	err = t1.Commit(ctx)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, terror.ErrResultUndetermined), IsTrue, Commentf("%s", errors.ErrorStack(err)))

	// We don't need to call "Rollback" after "Commit" fails.
	err = t1.Rollback()
	c.Assert(err, Equals, kv.ErrInvalidTxn)

	// Create a new transaction to check. The previous transaction should actually commit.
	t2 := s.beginAsyncCommit(c)
	res, err := t2.Get(context.Background(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(res, []byte("a1")), IsTrue)
}

func (s *testAsyncCommitFailSuite) TestAsyncCommitPrewriteCancelled(c *C) {
	// This test doesn't support tikv mode because it needs setting failpoint in unistore.
	if *WithTiKV {
		return
	}

	// Split into two regions.
	splitKey := "s"
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := s.store.GetRegionCache().LocateKey(bo, []byte(splitKey))
	c.Assert(err, IsNil)
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(loc.Region.GetID(), newRegionID, []byte(splitKey), []uint64{newPeerID}, newPeerID)
	s.store.GetRegionCache().InvalidateCachedRegion(loc.Region)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcPrewriteResult", `1*return("writeConflict")->sleep(50)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcPrewriteResult"), IsNil)
	}()

	t1 := s.beginAsyncCommit(c)
	err = t1.Set([]byte("a"), []byte("a"))
	c.Assert(err, IsNil)
	err = t1.Set([]byte("z"), []byte("z"))
	c.Assert(err, IsNil)
	ctx := context.WithValue(context.Background(), sessionctx.ConnID, uint64(1))
	err = t1.Commit(ctx)
	c.Assert(err, NotNil)
	c.Assert(kv.ErrWriteConflict.Equal(err), IsTrue, Commentf("%s", errors.ErrorStack(err)))
}

func (s *testAsyncCommitFailSuite) TestPointGetWithAsyncCommit(c *C) {
	s.putAlphabets(c, true)

	txn := s.beginAsyncCommit(c)
	txn.Set([]byte("a"), []byte("v1"))
	txn.Set([]byte("b"), []byte("v2"))
	s.mustPointGet(c, []byte("a"), []byte("a"))
	s.mustPointGet(c, []byte("b"), []byte("b"))

	// PointGet cannot ignore async commit transactions' locks.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/asyncCommitDoNothing", "return"), IsNil)
	ctx := context.WithValue(context.Background(), sessionctx.ConnID, uint64(1))
	err := txn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isAsyncCommit(), IsTrue)
	s.mustPointGet(c, []byte("a"), []byte("v1"))
	s.mustPointGet(c, []byte("b"), []byte("v2"))
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/asyncCommitDoNothing"), IsNil)

	// PointGet will not push the `max_ts` to its ts which is MaxUint64.
	txn2 := s.beginAsyncCommit(c)
	s.mustGetFromTxn(c, txn2, []byte("a"), []byte("v1"))
	s.mustGetFromTxn(c, txn2, []byte("b"), []byte("v2"))
	err = txn2.Rollback()
	c.Assert(err, IsNil)
}

func (s *testAsyncCommitFailSuite) TestSecondaryListInPrimaryLock(c *C) {
	// This test doesn't support tikv mode.
	if *WithTiKV {
		return
	}

	s.putAlphabets(c, true)

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

		txn := s.beginAsyncCommit(c)
		for i := range keys {
			txn.Set([]byte(keys[i]), []byte(values[i]))
		}

		c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/asyncCommitDoNothing", "return"), IsNil)

		err = txn.Commit(ctx)
		c.Assert(err, IsNil)

		primary := txn.committer.primary()
		bo := NewBackofferWithVars(context.Background(), 5000, nil)
		txnStatus, err := s.store.lockResolver.getTxnStatus(bo, txn.StartTS(), primary, 0, 0, false, false)
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
		txn.committer.cleanup(context.Background())
		txn.committer.cleanWg.Wait()
	}

	test([]string{"a"}, []string{"a1"})
	test([]string{"a", "b"}, []string{"a2", "b2"})
	test([]string{"a", "b", "d"}, []string{"a3", "b3", "d3"})
	test([]string{"a", "b", "h", "i", "u"}, []string{"a4", "b4", "h4", "i4", "u4"})
	test([]string{"i", "a", "z", "u", "b"}, []string{"i5", "a5", "z5", "u5", "b5"})
}

func (s *testAsyncCommitFailSuite) TestAsyncCommitContextCancelCausingUndetermined(c *C) {
	// For an async commit transaction, if RPC returns context.Canceled error when prewriting, the
	// transaction should go to undetermined state.
	txn := s.beginAsyncCommit(c)
	err := txn.Set([]byte("a"), []byte("va"))
	c.Assert(err, IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/rpcContextCancelErr", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/rpcContextCancelErr"), IsNil)
	}()

	ctx := context.WithValue(context.Background(), sessionctx.ConnID, uint64(1))
	err = txn.Commit(ctx)
	c.Assert(err, NotNil)
	c.Assert(txn.committer.mu.undeterminedErr, NotNil)
}
