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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
)

// TxnProbe wraps a txn and exports internal states for testing purpose.
type TxnProbe struct {
	*KVTxn
}

// GetCommitTS returns the commit ts.
func (txn TxnProbe) GetCommitTS() uint64 {
	return txn.commitTS
}

// IsAsyncCommit returns if the txn is committed using async commit.
func (txn TxnProbe) IsAsyncCommit() bool {
	return txn.committer.isAsyncCommit()
}

// NewCommitter creates an committer.
func (txn TxnProbe) NewCommitter(sessionID uint64) (CommitterProbe, error) {
	committer, err := newTwoPhaseCommitterWithInit(txn.KVTxn, sessionID)
	return CommitterProbe{twoPhaseCommitter: committer}, err
}

// GetCommitter returns the transaction committer.
func (txn TxnProbe) GetCommitter() CommitterProbe {
	return CommitterProbe{txn.committer}
}

func newTwoPhaseCommitterWithInit(txn *KVTxn, sessionID uint64) (*twoPhaseCommitter, error) {
	c, err := newTwoPhaseCommitter(txn, sessionID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = c.initKeysAndMutations(); err != nil {
		return nil, errors.Trace(err)
	}
	return c, nil
}

// CommitterProbe wraps a 2PC committer and exports internal states for testing purpose.
type CommitterProbe struct {
	*twoPhaseCommitter
}

// SetPrimaryKey resets the committer's commit ts.
func (c CommitterProbe) SetPrimaryKey(key []byte) {
	c.primaryKey = key
}

// GetPrimaryKey returns primary key of the committer.
func (c CommitterProbe) GetPrimaryKey() []byte {
	return c.primaryKey
}

// SetCommitTS resets the committer's commit ts.
func (c CommitterProbe) SetCommitTS(ts uint64) {
	c.commitTS = ts
}

// GetCommitTS returns the commit ts of the committer.
func (c CommitterProbe) GetCommitTS() uint64 {
	return c.commitTS
}

// PrewriteMutations performs the first phase of commit.
func (c CommitterProbe) PrewriteMutations(ctx context.Context) error {
	return c.prewriteMutations(NewBackofferWithVars(ctx, PrewriteMaxBackoff, nil), c.mutations)
}

// CommitMutations performs the second phase of commit.
func (c CommitterProbe) CommitMutations(ctx context.Context) error {
	return c.commitMutations(NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), nil), c.mutationsOfKeys([][]byte{c.primaryKey}))
}

// Cleanup cleans dirty data of a committer.
func (c CommitterProbe) Cleanup(ctx context.Context) {
	c.cleanup(ctx)
	c.cleanWg.Wait()
}

// IsOnePC returns if the committer is using one PC.
func (c CommitterProbe) IsOnePC() bool {
	return c.isOnePC()
}

// IsAsyncCommit returns if the committer uses async commit.
func (c CommitterProbe) IsAsyncCommit() bool {
	return c.isAsyncCommit()
}

// GetOnePCCommitTS returns the commit ts of one pc.
func (c CommitterProbe) GetOnePCCommitTS() uint64 {
	return c.onePCCommitTS
}

// IsTTLUninitialized  returns if the TTL manager is uninitialized.
func (c CommitterProbe) IsTTLUninitialized() bool {
	return c.ttlManager.state == stateUninitialized
}

// GetUndeterminedErr returns the encountered undetermined error (if any).
func (c CommitterProbe) GetUndeterminedErr() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.undeterminedErr
}

// LockProbe exposes some lock utilities for testing purpose.
type LockProbe struct {
}

// ExtractLockFromKeyErr makes a Lock based on a key error.
func (l LockProbe) ExtractLockFromKeyErr(err *pb.KeyError) (*Lock, error) {
	return extractLockFromKeyErr(err)
}

// NewLockStatus returns a txn state that has been locked.
func (l LockProbe) NewLockStatus(keys [][]byte, useAsyncCommit bool, minCommitTS uint64) TxnStatus {
	return TxnStatus{
		primaryLock: &kvrpcpb.LockInfo{
			Secondaries:    keys,
			UseAsyncCommit: useAsyncCommit,
			MinCommitTs:    minCommitTS,
		},
	}
}

// LockResolverProbe wraps a LockResolver and exposes internal stats for testing purpose.
type LockResolverProbe struct {
	*LockResolver
}

// ResolveLockAsync tries to resolve a lock using the txn states.
func (l LockResolverProbe) ResolveLockAsync(bo *Backoffer, lock *Lock, status TxnStatus) error {
	return l.resolveLockAsync(bo, lock, status)
}

// GetTxnStatus sends the CheckTxnStatus request to the TiKV server.
func (l LockResolverProbe) GetTxnStatus(bo *Backoffer, txnID uint64, primary []byte,
	callerStartTS, currentTS uint64, rollbackIfNotExist bool, forceSyncCommit bool, lockInfo *Lock) (TxnStatus, error) {
	return l.getTxnStatus(bo, txnID, primary, callerStartTS, currentTS, rollbackIfNotExist, forceSyncCommit, lockInfo)
}

// GetSecondariesFromTxnStatus returns the secondary locks from txn status.
func (l LockResolverProbe) GetSecondariesFromTxnStatus(status TxnStatus) [][]byte {
	return status.primaryLock.GetSecondaries()
}
