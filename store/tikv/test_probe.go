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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

// StoreProbe wraps KVSTore and exposes internal states for testing purpose.
type StoreProbe struct {
	*KVStore
}

// NewLockResolver creates a new LockResolver instance.
func (s StoreProbe) NewLockResolver() LockResolverProbe {
	return LockResolverProbe{LockResolver: newLockResolver(s.KVStore)}
}

// TxnProbe wraps a txn and exports internal states for testing purpose.
type TxnProbe struct {
	*KVTxn
}

// SetStartTS resets the txn's start ts.
func (txn TxnProbe) SetStartTS(ts uint64) {
	txn.startTS = ts
}

// GetCommitTS returns the commit ts.
func (txn TxnProbe) GetCommitTS() uint64 {
	return txn.commitTS
}

// GetUnionStore returns transaction's embedded unionstore.
func (txn TxnProbe) GetUnionStore() kv.UnionStore {
	return txn.us
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

// SetCommitter sets the bind committer of a transaction.
func (txn TxnProbe) SetCommitter(committer CommitterProbe) {
	txn.committer = committer.twoPhaseCommitter
}

// ClearStoreTxnLatches clears store's txn latch scheduler.
func (txn TxnProbe) ClearStoreTxnLatches() {
	txn.store.txnLatches = nil
}

// CollectLockedKeys returns all locked keys of a transaction.
func (txn TxnProbe) CollectLockedKeys() [][]byte {
	return txn.collectLockedKeys()
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

// InitKeysAndMutations prepares the committer for commit.
func (c CommitterProbe) InitKeysAndMutations() error {
	return c.initKeysAndMutations()
}

// SetPrimaryKey resets the committer's commit ts.
func (c CommitterProbe) SetPrimaryKey(key []byte) {
	c.primaryKey = key
}

// GetPrimaryKey returns primary key of the committer.
func (c CommitterProbe) GetPrimaryKey() []byte {
	return c.primaryKey
}

// GetMutations returns the mutation buffer to commit.
func (c CommitterProbe) GetMutations() CommitterMutations {
	return c.mutations
}

// SetMutations replace the mutation buffer.
func (c CommitterProbe) SetMutations(muts CommitterMutations) {
	c.mutations = muts.(*memBufferMutations)
}

// SetCommitTS resets the committer's commit ts.
func (c CommitterProbe) SetCommitTS(ts uint64) {
	c.commitTS = ts
}

// GetCommitTS returns the commit ts of the committer.
func (c CommitterProbe) GetCommitTS() uint64 {
	return c.commitTS
}

// GetMinCommittTS returns the minimal commit ts can be used.
func (c CommitterProbe) GetMinCommitTS() uint64 {
	return c.minCommitTS
}

// SetMinCommitTS sets the minimal commit ts can be used.
func (c CommitterProbe) SetMinCommitTS(ts uint64) {
	c.minCommitTS = ts
}

// SetSessionID sets the session id of the committer.
func (c CommitterProbe) SetSessionID(id uint64) {
	c.sessionID = id
}

// GetForUpdateTS returns the pessimistic ForUpdate ts.
func (c CommitterProbe) GetForUpdateTS() uint64 {
	return c.forUpdateTS
}

// SetForUpdateTS sets pessimistic ForUpdate ts.
func (c CommitterProbe) SetForUpdateTS(ts uint64) {
	c.forUpdateTS = ts
}

// GetStartTS returns the start ts of the transaction.
func (c CommitterProbe) GetStartTS() uint64 {
	return c.startTS
}

// GetLockTTL returns the lock ttl duration of the transaction.
func (c CommitterProbe) GetLockTTL() uint64 {
	return c.lockTTL
}

// SetTxnSize resets the txn size of the committer and updates lock TTL.
func (c CommitterProbe) SetTxnSize(sz int) {
	c.txnSize = sz
	c.lockTTL = txnLockTTL(c.txn.startTime, sz)
}

// SetUseAsyncCommit enables async commit feature.
func (c CommitterProbe) SetUseAsyncCommit() {
	c.useAsyncCommit = 1
}

// Execute runs the commit process.
func (c CommitterProbe) Execute(ctx context.Context) error {
	return c.execute(ctx)
}

// PrewriteMutations performs the first phase of commit.
func (c CommitterProbe) PrewriteMutations(ctx context.Context) error {
	return c.prewriteMutations(NewBackofferWithVars(ctx, PrewriteMaxBackoff, nil), c.mutations)
}

// CommitMutations performs the second phase of commit.
func (c CommitterProbe) CommitMutations(ctx context.Context) error {
	return c.commitMutations(NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), nil), c.mutationsOfKeys([][]byte{c.primaryKey}))
}

// MutationsOfKeys returns mutations match the keys.
func (c CommitterProbe) MutationsOfKeys(keys [][]byte) CommitterMutations {
	return c.mutationsOfKeys(keys)
}

// PessimisticRollbackMutations rolls mutations back.
func (c CommitterProbe) PessimisticRollbackMutations(ctx context.Context, muts CommitterMutations) error {
	return c.pessimisticRollbackMutations(NewBackofferWithVars(ctx, pessimisticRollbackMaxBackoff, nil), muts)
}

// Cleanup cleans dirty data of a committer.
func (c CommitterProbe) Cleanup(ctx context.Context) {
	c.cleanup(ctx)
	c.cleanWg.Wait()
}

// WaitCleanup waits for the committer to complete.
func (c CommitterProbe) WaitCleanup() {
	c.cleanWg.Wait()
}

// IsOnePC returns if the committer is using one PC.
func (c CommitterProbe) IsOnePC() bool {
	return c.isOnePC()
}

// BuildPrewriteRequest builds rpc request for mutation.
func (c CommitterProbe) BuildPrewriteRequest(regionID, regionConf, regionVersion uint64, mutations CommitterMutations, txnSize uint64) *tikvrpc.Request {
	var batch batchMutations
	batch.mutations = mutations
	batch.region = RegionVerID{regionID, regionConf, regionVersion}
	return c.buildPrewriteRequest(batch, txnSize)
}

// IsAsyncCommit returns if the committer uses async commit.
func (c CommitterProbe) IsAsyncCommit() bool {
	return c.isAsyncCommit()
}

// CheckAsyncCommit returns if async commit is available.
func (c CommitterProbe) CheckAsyncCommit() bool {
	return c.checkAsyncCommit()
}

// GetOnePCCommitTS returns the commit ts of one pc.
func (c CommitterProbe) GetOnePCCommitTS() uint64 {
	return c.onePCCommitTS
}

// IsTTLUninitialized returns if the TTL manager is uninitialized.
func (c CommitterProbe) IsTTLUninitialized() bool {
	state := atomic.LoadUint32((*uint32)(&c.ttlManager.state))
	return state == uint32(stateUninitialized)
}

// IsTTLRunning returns if the TTL manager is running state.
func (c CommitterProbe) IsTTLRunning() bool {
	state := atomic.LoadUint32((*uint32)(&c.ttlManager.state))
	return state == uint32(stateRunning)
}

// CloseTTLManager closes the TTL manager.
func (c CommitterProbe) CloseTTLManager() {
	c.ttlManager.close()
}

// GetUndeterminedErr returns the encountered undetermined error (if any).
func (c CommitterProbe) GetUndeterminedErr() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.undeterminedErr
}

// SetNoFallBack disallows async commit to fall back to normal mode.
func (c CommitterProbe) SetNoFallBack() {
	c.testingKnobs.noFallBack = true
}

// SetPrimaryKeyBlocker is used to block committer after primary is sent.
func (c CommitterProbe) SetPrimaryKeyBlocker(ac, bk chan struct{}) {
	c.testingKnobs.acAfterCommitPrimary = ac
	c.testingKnobs.bkAfterCommitPrimary = bk
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

// ResolveLock resolves single lock.
func (l LockResolverProbe) ResolveLock(ctx context.Context, lock *Lock) error {
	bo := NewBackofferWithVars(ctx, pessimisticLockMaxBackoff, nil)
	return l.resolveLock(bo, lock, TxnStatus{}, false, make(map[RegionVerID]struct{}))
}

// ResolvePessimisticLock resolves single pessimistic lock.
func (l LockResolverProbe) ResolvePessimisticLock(ctx context.Context, lock *Lock) error {
	bo := NewBackofferWithVars(ctx, pessimisticLockMaxBackoff, nil)
	return l.resolvePessimisticLock(bo, lock, make(map[RegionVerID]struct{}))
}

// GetTxnStatus sends the CheckTxnStatus request to the TiKV server.
func (l LockResolverProbe) GetTxnStatus(bo *Backoffer, txnID uint64, primary []byte,
	callerStartTS, currentTS uint64, rollbackIfNotExist bool, forceSyncCommit bool, lockInfo *Lock) (TxnStatus, error) {
	return l.getTxnStatus(bo, txnID, primary, callerStartTS, currentTS, rollbackIfNotExist, forceSyncCommit, lockInfo)
}

// GetTxnStatusFromLock queries tikv for a txn's status.
func (l LockResolverProbe) GetTxnStatusFromLock(bo *Backoffer, lock *Lock, callerStartTS uint64, forceSyncCommit bool) (TxnStatus, error) {
	return l.getTxnStatusFromLock(bo, lock, callerStartTS, forceSyncCommit)
}

// GetSecondariesFromTxnStatus returns the secondary locks from txn status.
func (l LockResolverProbe) GetSecondariesFromTxnStatus(status TxnStatus) [][]byte {
	return status.primaryLock.GetSecondaries()
}

// SetMeetLockCallback is called whenever it meets locks.
func (l LockResolverProbe) SetMeetLockCallback(f func([]*Lock)) {
	l.testingKnobs.meetLock = f
}

// ConfigProbe exposes configurations and global variables for testing purpose.
type ConfigProbe struct{}

// GetTxnCommitBatchSize returns the batch size to commit txn.
func (c ConfigProbe) GetTxnCommitBatchSize() uint64 {
	return txnCommitBatchSize
}

// GetBigTxnThreshold returns the txn size to be considered as big txn.
func (c ConfigProbe) GetBigTxnThreshold() int {
	return bigTxnThreshold
}
