// Copyright 2018 PingCAP, Inc.
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
	"time"

	gofail "github.com/coreos/gofail/runtime"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/terror"
	"golang.org/x/net/context"
)

// TestFailCommitPrimaryRpcErrors tests rpc errors are handled properly when
// committing primary region task.
func (s *testCommitterSuite) TestFailCommitPrimaryRpcErrors(c *C) {
	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult", `return("timeout")`)
	defer gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult")
	// The rpc error will be wrapped to ErrResultUndetermined.
	t1 := s.begin(c)
	err := t1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = t1.Commit(context.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, terror.ErrResultUndetermined), IsTrue, Commentf("%s", errors.ErrorStack(err)))
}

// TestFailCommitPrimaryRegionError tests RegionError is handled properly when
// committing primary region task.
func (s *testCommitterSuite) TestFailCommitPrimaryRegionError(c *C) {
	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult", `return("notLeader")`)
	defer gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult")
	// Ensure it returns the original error without wrapped to ErrResultUndetermined
	// if it exceeds max retry timeout on RegionError.
	t2 := s.begin(c)
	err := t2.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	err = t2.Commit(context.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorNotEqual(err, terror.ErrResultUndetermined), IsTrue)
}

// TestFailCommitPrimaryRPCErrorThenRegionError tests the case when commit first
// receive a rpc timeout, then region errors afterwrards.
func (s *testCommitterSuite) TestFailCommitPrimaryRPCErrorThenRegionError(c *C) {
	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult", `1*return("timeout")->return("notLeader")`)
	defer gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult")
	// The region error will be wrapped to ErrResultUndetermined.
	t1 := s.begin(c)
	err := t1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = t1.Commit(context.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, terror.ErrResultUndetermined), IsTrue, Commentf("%s", errors.ErrorStack(err)))
}

// TestFailCommitPrimaryKeyError tests KeyError is handled properly when
// committing primary region task.
func (s *testCommitterSuite) TestFailCommitPrimaryKeyError(c *C) {
	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult", `return("keyError")`)
	defer gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult")
	// Ensure it returns the original error without wrapped to ErrResultUndetermined
	// if it meets KeyError.
	t3 := s.begin(c)
	err := t3.Set([]byte("c"), []byte("c1"))
	c.Assert(err, IsNil)
	err = t3.Commit(context.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorNotEqual(err, terror.ErrResultUndetermined), IsTrue)
}

func (s *testCommitterSuite) TestFailCommitPrimaryKeyRegionError(c *C) {
	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult", `return("notLeader")`)
	defer gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult")
	// Ensure it returns the original error without wrapped to ErrResultUndetermined
	// if it meets KeyError.
	t3 := s.begin(c)
	err := t3.Set([]byte("c"), []byte("c1"))
	c.Assert(err, IsNil)
	err = t3.Commit(context.Background())
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorNotEqual(err, terror.ErrResultUndetermined), IsTrue)
}

func (s *testCommitterSuite) TestFailCommitTimeout(c *C) {
	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitTimeout", `return(true)`)
	defer gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitTimeout")
	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("c"), []byte("c1"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, NotNil)

	txn2 := s.begin(c)
	value, err := txn2.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(len(value), Greater, 0)
	_, err = txn2.Get([]byte("b"))
	c.Assert(err, IsNil)
	c.Assert(len(value), Greater, 0)
}

func (s *testCommitterSuite) TestFailPrewritePartialTimeOut(c *C) {
	s.mustCommit(c, map[string]string{
		"a": "a0",
		"b": "b0",
	})

	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)

	gofail.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `1*return("timeout")`)
	err = txn.Commit(context.Background())
	c.Assert(err, NotNil)
	gofail.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult")

	txnCheck := s.begin(c)
	var value []byte
	value, err = txnCheck.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, []byte("a0"))

	value, err = txnCheck.Get([]byte("b"))
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, []byte("b0"))
}

func (s *testCommitterSuite) TestFailCommitPrimaryKeyTimeOut(c *C) {
	s.mustCommit(c, map[string]string{
		"a": "a0",
		"b": "b0",
	})

	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)

	gofail.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `2*off->1*return("timeout")`)
	err = txn.Commit(context.Background())
	c.Assert(err, NotNil)
	gofail.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult")

	txnCheck := s.begin(c)
	var value []byte
	value, err = txnCheck.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, []byte("a0"))

	value, err = txnCheck.Get([]byte("b"))
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, []byte("b0"))
}

func (s *testCommitterSuite) TestFailGetTimeOut(c *C) {
	s.mustCommit(c, map[string]string{
		"a": "a0",
	})

	txn := s.begin(c)
	gofail.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `1*return("timeout")`)
	value, err := txn.Get([]byte("a"))
	c.Assert(err, NotNil)
	c.Assert(value, IsNil)
	gofail.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult")
}

func (s *testCommitterSuite) TestFailCommitSecondaryKeyTimeOut(c *C) {
	s.mustCommit(c, map[string]string{
		"a": "a0",
		"b": "b0",
	})

	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)

	gofail.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `3*off->1*return("timeout")`)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	gofail.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult")

	txnCheck := s.begin(c)
	var value []byte
	value, err = txnCheck.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, []byte("a1"))

	value, err = txnCheck.Get([]byte("b"))
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, []byte("b1"))
}

func (s *testCommitterSuite) TestFailLockKeysTimeOut(c *C) {
	s.mustCommit(c, map[string]string{"a": "a0"})

	txn1 := s.begin(c)
	err := txn1.LockKeys([]byte("a"))
	c.Assert(err, IsNil)

	txn2 := s.begin(c)
	err = txn2.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)

	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult", `return("timeout")`)
	err = txn1.Commit(context.Background())
	c.Assert(err, NotNil)
	gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult")

	err = txn2.Commit(context.Background())
	c.Assert(err, IsNil)

	txnCheck := s.begin(c)
	var value []byte
	value, err = txnCheck.Get([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, []byte("a1"))
}

func (s *testCommitterSuite) TestTwoPhaseCommitActionString(c *C) {
	action := twoPhaseCommitAction(0)
	c.Assert(action.String(), Equals, "unknown")
}

// @TODO: add it back in the future.
func (s *testCommitterSuite) TestFailNewTwoPhaseCommitter(c *C) {
	c.Skip("change global variables will make other cases fail because of parallel")
	txn := s.begin(c)
	c.Assert(txn.Set([]byte("a"), []byte("wqelkyuoqasfweweqwehladfewrj")), IsNil)
	c.Assert(txn.Set([]byte("b"), []byte("b1")), IsNil)

	temp := kv.TxnEntrySizeLimit
	kv.TxnEntrySizeLimit = 10
	tpc, err := newTwoPhaseCommitter(txn)
	c.Assert(tpc, IsNil)
	c.Assert(err, Equals, kv.ErrEntryTooLarge)
	kv.TxnEntrySizeLimit = temp

	temp = kv.TxnTotalSizeLimit
	kv.TxnTotalSizeLimit = 1
	tpc, err = newTwoPhaseCommitter(txn)
	c.Assert(tpc, IsNil)
	c.Assert(err, Equals, kv.ErrTxnTooLarge)
	kv.TxnTotalSizeLimit = temp
}

func (s *testCommitterSuite) TestTxnLockTTL(c *C) {
	expireTime := txnLockTTL(time.Now(), 16*1024*1024*1024)
	c.Assert(expireTime >= maxLockTTL, IsTrue)
}

func (s *testCommitterSuite) TestFailDoActionOnKeys(c *C) {
	txn := s.begin(c)
	txn.Set([]byte("a"), []byte("a0"))
	tpc, err := newTwoPhaseCommitter(txn)
	c.Assert(err, IsNil)
	gofail.Enable("github.com/pingcap/tidb/store/tikv/GroupKeysByRegionFail", `return(true)`)
	err = tpc.doActionOnKeys(NewBackoffer(context.Background(), 100), actionPrewrite, [][]byte{[]byte("a")})
	gofail.Disable("github.com/pingcap/tidb/store/tikv/GroupKeysByRegionFail")
	c.Assert(err, NotNil)
}

func (s *testCommitterSuite) TestFailPrewriteSingleBatchRegionError(c *C) {
	txn := s.begin(c)
	txn.Set([]byte("a"), []byte("a0"))
	tpc, err := newTwoPhaseCommitter(txn)
	c.Assert(err, IsNil)

	bo := NewBackoffer(context.Background(), 100)
	groups, firstRegion, err := s.store.regionCache.GroupKeysByRegion(bo, [][]byte{[]byte("a")})
	c.Assert(err, IsNil)
	var batches []batchKeys
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], tpc.keyValueSize, txnCommitBatchSize)
	delete(groups, firstRegion)

	gofail.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `return("PrewriteNotLeader")`)
	err = tpc.prewriteSingleBatch(bo, batches[0])
	c.Assert(err, NotNil)
	gofail.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult")
}

func (s *testCommitterSuite) TestFailPrewriteSingleBatchBodyMissing(c *C) {
	txn := s.begin(c)
	txn.Set([]byte("a"), []byte("a0"))
	tpc, err := newTwoPhaseCommitter(txn)
	c.Assert(err, IsNil)

	bo := NewBackoffer(context.Background(), 100)
	groups, firstRegion, err := s.store.regionCache.GroupKeysByRegion(bo, [][]byte{[]byte("a")})
	c.Assert(err, IsNil)
	var batches []batchKeys
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], tpc.keyValueSize, txnCommitBatchSize)
	delete(groups, firstRegion)

	gofail.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `return("PrewriteBodyMissing")`)
	err = tpc.prewriteSingleBatch(bo, batches[0])
	c.Assert(err, NotNil)
	gofail.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult")
}

func (s *testCommitterSuite) TestFailCommitSingleBatchRegionError(c *C) {
	txn := s.begin(c)
	txn.Set([]byte("a"), []byte("a0"))
	tpc, err := newTwoPhaseCommitter(txn)
	c.Assert(err, IsNil)

	bo := NewBackoffer(context.Background(), 100)
	groups, firstRegion, err := s.store.regionCache.GroupKeysByRegion(bo, [][]byte{[]byte("a")})
	c.Assert(err, IsNil)
	var batches []batchKeys
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], tpc.keySize, txnCommitBatchSize)
	delete(groups, firstRegion)

	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult", `return("notLeader")`)
	err = tpc.commitSingleBatch(bo, batches[0])
	c.Assert(err, NotNil)
	gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcCommitResult")
}

func (s *testCommitterSuite) TestFailCommitSingleBatchBodyMissing(c *C) {
	txn := s.begin(c)
	txn.Set([]byte("a"), []byte("a0"))
	tpc, err := newTwoPhaseCommitter(txn)
	c.Assert(err, IsNil)

	bo := NewBackoffer(context.Background(), 100)
	groups, firstRegion, err := s.store.regionCache.GroupKeysByRegion(bo, [][]byte{[]byte("a")})
	c.Assert(err, IsNil)
	var batches []batchKeys
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], tpc.keySize, txnCommitBatchSize)
	delete(groups, firstRegion)

	gofail.Enable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult", `return("CommitBodyMissing")`)
	err = tpc.commitSingleBatch(bo, batches[0])
	c.Assert(err, NotNil)
	gofail.Disable("github.com/pingcap/tidb/store/tikv/tikvStoreSendReqResult")
}

func (s *testCommitterSuite) TestFailCleanupSingleBatchTimeout(c *C) {
	txn := s.begin(c)
	txn.Set([]byte("a"), []byte("a0"))
	tpc, err := newTwoPhaseCommitter(txn)
	c.Assert(err, IsNil)

	bo := NewBackoffer(context.Background(), 100)
	groups, firstRegion, err := s.store.regionCache.GroupKeysByRegion(bo, [][]byte{[]byte("a")})
	c.Assert(err, IsNil)
	var batches []batchKeys
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], tpc.keySize, txnCommitBatchSize)
	delete(groups, firstRegion)

	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcRollbackResult", `return("timeout")`)
	err = tpc.cleanupSingleBatch(bo, batches[0])
	c.Assert(err, NotNil)
	gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcRollbackResult")
}

func (s *testCommitterSuite) TestFailCleanupSingleBatchRegionError(c *C) {
	txn := s.begin(c)
	txn.Set([]byte("a"), []byte("a0"))
	tpc, err := newTwoPhaseCommitter(txn)
	c.Assert(err, IsNil)

	bo := NewBackoffer(context.Background(), 300)
	groups, firstRegion, err := s.store.regionCache.GroupKeysByRegion(bo, [][]byte{[]byte("a")})
	c.Assert(err, IsNil)
	var batches []batchKeys
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], tpc.keySize, txnCommitBatchSize)
	delete(groups, firstRegion)

	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcRollbackResult", `return("notLeader")`)
	err = tpc.cleanupSingleBatch(bo, batches[0])
	c.Assert(err, NotNil)
	gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcRollbackResult")
}

func (s *testCommitterSuite) TestFailCleanupSingleBatchKeyError(c *C) {
	txn := s.begin(c)
	txn.Set([]byte("a"), []byte("a0"))
	tpc, err := newTwoPhaseCommitter(txn)
	c.Assert(err, IsNil)

	bo := NewBackoffer(context.Background(), 1000)
	groups, firstRegion, err := s.store.regionCache.GroupKeysByRegion(bo, [][]byte{[]byte("a")})
	c.Assert(err, IsNil)
	var batches []batchKeys
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], tpc.keySize, txnCommitBatchSize)
	delete(groups, firstRegion)

	gofail.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcRollbackResult", `return("keyError")`)
	err = tpc.cleanupSingleBatch(bo, batches[0])
	c.Assert(err, NotNil)
	gofail.Disable("github.com/pingcap/tidb/store/mockstore/mocktikv/rpcRollbackResult")
}
