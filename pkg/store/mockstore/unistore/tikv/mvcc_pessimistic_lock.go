// Copyright 2019-present PingCAP, Inc.
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

package tikv

import (
	"bytes"
	"math"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/badger"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/kverrors"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/util/lockwaiter"
	"github.com/tikv/client-go/v2/oracle"
)

// PessimisticLock will add pessimistic lock on key
func (store *MVCCStore) PessimisticLock(reqCtx *requestCtx, req *kvrpcpb.PessimisticLockRequest, resp *kvrpcpb.PessimisticLockResponse) (*lockwaiter.Waiter, error) {
	waiter, err := store.pessimisticLockInner(reqCtx, req, resp)
	if err != nil && req.GetWakeUpMode() == kvrpcpb.PessimisticLockWakeUpMode_WakeUpModeForceLock {
		// The execution of `pessimisticLockInner` is broken by error. If resp.Results is not completely set yet, fill it with LockResultFailed.
		for len(resp.Results) < len(req.Mutations) {
			resp.Results = append(resp.Results, &kvrpcpb.PessimisticLockKeyResult{
				Type: kvrpcpb.PessimisticLockKeyResultType_LockResultFailed,
			})
		}
	}

	return waiter, err
}

func (store *MVCCStore) pessimisticLockInner(reqCtx *requestCtx, req *kvrpcpb.PessimisticLockRequest, resp *kvrpcpb.PessimisticLockResponse) (*lockwaiter.Waiter, error) {
	mutations := req.Mutations
	if !req.ReturnValues {
		mutations = sortMutations(req.Mutations)
	}
	startTS := req.StartVersion
	regCtx := reqCtx.regCtx
	hashVals := mutationsToHashVals(mutations)
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	if req.LockOnlyIfExists && !req.ReturnValues {
		return nil, errors.New("LockOnlyIfExists is set for LockKeys but ReturnValues is not set")
	}
	if req.GetWakeUpMode() == kvrpcpb.PessimisticLockWakeUpMode_WakeUpModeForceLock && len(req.Mutations) > 1 {
		return nil, errors.New("Trying to lock more than one key in WakeUpModeForceLock, which is not supported yet")
	}
	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)
	var dup bool
	for _, m := range mutations {
		lock, err := store.checkConflictInLockStore(reqCtx, m, startTS)
		if err != nil {
			var resourceGroupTag []byte
			if req.Context != nil {
				resourceGroupTag = req.Context.ResourceGroupTag
			}
			return store.handleCheckPessimisticErr(startTS, err, req.IsFirstLock, req.WaitTimeout, m.Key, resourceGroupTag)
		}
		if lock != nil {
			if lock.Op != uint8(kvrpcpb.Op_PessimisticLock) {
				return nil, errors.New("lock type not match")
			}
			if lock.ForUpdateTS >= req.ForUpdateTs {
				// It's a duplicate command, we can simply return values.
				dup = true
				break
			}
			// Single statement rollback key, we can overwrite it.
		}
		if bytes.Equal(m.Key, req.PrimaryLock) {
			txnStatus := store.checkExtraTxnStatus(reqCtx, m.Key, startTS)
			if txnStatus.isRollback {
				return nil, kverrors.ErrAlreadyRollback
			} else if txnStatus.isOpLockCommitted() {
				dup = true
				break
			}
		}
	}
	items, err := store.getDBItems(reqCtx, mutations)
	lockedWithConflictTSList := make([]uint64, 0, len(mutations))
	if err != nil {
		return nil, err
	}
	if !dup {
		for i, m := range mutations {
			latestExtraMeta := store.getLatestExtraMetaForKey(reqCtx, m)
			lock, lockedWithConflictTS, err1 := store.buildPessimisticLock(m, items[i], latestExtraMeta, req)
			lockedWithConflictTSList = append(lockedWithConflictTSList, lockedWithConflictTS)
			if err1 != nil {
				return nil, err1
			}
			if lock == nil {
				continue
			}
			batch.PessimisticLock(m.Key, lock)
		}
		err = store.dbWriter.Write(batch)
		if err != nil {
			return nil, err
		}
	}
	if req.Force {
		dbMeta := mvcc.DBUserMeta(items[0].UserMeta())
		val, err1 := items[0].ValueCopy(nil)
		if err1 != nil {
			return nil, err1
		}
		resp.Value = val
		resp.CommitTs = dbMeta.CommitTS()
	}

	if req.GetWakeUpMode() == kvrpcpb.PessimisticLockWakeUpMode_WakeUpModeNormal {
		if req.ReturnValues || req.CheckExistence {
			for _, item := range items {
				if item == nil {
					if req.ReturnValues {
						resp.Values = append(resp.Values, nil)
					}
					resp.NotFounds = append(resp.NotFounds, true)
					continue
				}
				val, err1 := item.ValueCopy(nil)
				if err1 != nil {
					return nil, err1
				}
				if req.ReturnValues {
					resp.Values = append(resp.Values, val)
				}
				resp.NotFounds = append(resp.NotFounds, len(val) == 0)
			}
		}
	} else if req.GetWakeUpMode() == kvrpcpb.PessimisticLockWakeUpMode_WakeUpModeForceLock {
		for i, item := range items {
			res := &kvrpcpb.PessimisticLockKeyResult{
				Type:                 kvrpcpb.PessimisticLockKeyResultType_LockResultNormal,
				Value:                nil,
				Existence:            false,
				LockedWithConflictTs: 0,
			}

			if lockedWithConflictTSList[i] != 0 {
				res.Type = kvrpcpb.PessimisticLockKeyResultType_LockResultLockedWithConflict
				res.LockedWithConflictTs = lockedWithConflictTSList[i]
				if item == nil {
					res.Value = nil
					res.Existence = false
				} else {
					val, err1 := item.ValueCopy(nil)
					if err1 != nil {
						return nil, err1
					}
					res.Value = val
					res.Existence = len(val) != 0
				}
			} else if req.ReturnValues {
				if item != nil {
					val, err1 := item.ValueCopy(nil)
					if err1 != nil {
						return nil, err1
					}
					res.Value = val
					res.Existence = len(val) != 0
				}
			} else if req.CheckExistence {
				if item != nil {
					val, err1 := item.ValueCopy(nil)
					if err1 != nil {
						return nil, err1
					}
					res.Existence = len(val) != 0
				}
			}

			resp.Results = append(resp.Results, res)
		}
	} else {
		panic("unreachable")
	}
	return nil, err
}

// extraTxnStatus can be rollback or Op_Lock that only contains transaction status info, no values.
type extraTxnStatus struct {
	commitTS   uint64
	isRollback bool
}

func (s extraTxnStatus) isOpLockCommitted() bool {
	return s.commitTS > 0
}

func (store *MVCCStore) checkExtraTxnStatus(reqCtx *requestCtx, key []byte, startTS uint64) extraTxnStatus {
	txn := reqCtx.getDBReader().GetTxn()
	txnStatusKey := mvcc.EncodeExtraTxnStatusKey(key, startTS)
	item, err := txn.Get(txnStatusKey)
	if err != nil {
		return extraTxnStatus{}
	}
	userMeta := mvcc.DBUserMeta(item.UserMeta())
	if userMeta.CommitTS() == 0 {
		return extraTxnStatus{isRollback: true}
	}
	return extraTxnStatus{commitTS: userMeta.CommitTS()}
}

// PessimisticRollbackWithScanFirst is used to scan the region first to collect related pessimistic locks and
// then pessimistic rollback them.
func (store *MVCCStore) PessimisticRollbackWithScanFirst(reqCtx *requestCtx, req *kvrpcpb.PessimisticRollbackRequest) error {
	if len(req.Keys) > 0 {
		return errors.Errorf("pessimistic rollback request invalid: there should be no input lock keys but got %v", len(req.Keys))
	}
	locks, err := store.scanPessimisticLocks(reqCtx, req.StartVersion, req.ForUpdateTs)
	if err != nil {
		return err
	}
	keys := make([][]byte, 0, len(locks))
	for _, lock := range locks {
		keys = append(keys, lock.Key)
	}
	req.Keys = keys
	return store.PessimisticRollback(reqCtx, req)
}

// PessimisticRollback implements the MVCCStore interface.
func (store *MVCCStore) PessimisticRollback(reqCtx *requestCtx, req *kvrpcpb.PessimisticRollbackRequest) error {
	keys := sortKeys(req.Keys)
	hashVals := keysToHashVals(keys...)
	regCtx := reqCtx.regCtx
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)
	startTS := req.StartVersion
	var batch mvcc.WriteBatch
	for _, k := range keys {
		lock := store.getLock(reqCtx, k)
		if lock != nil &&
			lock.Op == uint8(kvrpcpb.Op_PessimisticLock) &&
			lock.StartTS == startTS &&
			lock.ForUpdateTS <= req.ForUpdateTs {
			if batch == nil {
				batch = store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)
			}
			batch.PessimisticRollback(k)
		}
	}
	var err error
	if batch != nil {
		err = store.dbWriter.Write(batch)
	}
	store.lockWaiterManager.WakeUp(startTS, 0, hashVals)
	store.DeadlockDetectCli.CleanUp(startTS)
	return err
}

// TxnHeartBeat implements the MVCCStore interface.
func (store *MVCCStore) TxnHeartBeat(reqCtx *requestCtx, req *kvrpcpb.TxnHeartBeatRequest) (lockTTL uint64, err error) {
	hashVals := keysToHashVals(req.PrimaryLock)
	regCtx := reqCtx.regCtx
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)
	lock := store.getLock(reqCtx, req.PrimaryLock)
	if lock != nil && lock.StartTS == req.StartVersion {
		if !bytes.Equal(lock.Primary, req.PrimaryLock) {
			return 0, errors.New("heartbeat on non-primary key")
		}
		if lock.TTL < uint32(req.AdviseLockTtl) {
			lock.TTL = uint32(req.AdviseLockTtl)
			batch := store.dbWriter.NewWriteBatch(req.StartVersion, 0, reqCtx.rpcCtx)
			batch.PessimisticLock(req.PrimaryLock, lock)
			err = store.dbWriter.Write(batch)
			if err != nil {
				return 0, err
			}
		}
		return uint64(lock.TTL), nil
	}
	return 0, errors.New("lock doesn't exists")
}

// TxnStatus is the result of `CheckTxnStatus` API.
type TxnStatus struct {
	commitTS uint64
	action   kvrpcpb.Action
	lockInfo *kvrpcpb.LockInfo
}

// CheckTxnStatus implements the MVCCStore interface.
func (store *MVCCStore) CheckTxnStatus(reqCtx *requestCtx,
	req *kvrpcpb.CheckTxnStatusRequest) (txnStatusRes TxnStatus, err error) {
	hashVals := keysToHashVals(req.PrimaryKey)
	regCtx := reqCtx.regCtx
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)
	lock := store.getLock(reqCtx, req.PrimaryKey)
	batch := store.dbWriter.NewWriteBatch(req.LockTs, 0, reqCtx.rpcCtx)
	if lock != nil && lock.StartTS == req.LockTs {
		if !bytes.Equal(req.PrimaryKey, lock.Primary) {
			return TxnStatus{}, &kverrors.ErrPrimaryMismatch{
				Key:  req.PrimaryKey,
				Lock: lock,
			}
		}

		// For an async-commit lock, never roll it back or push forward it MinCommitTS.
		if lock.UseAsyncCommit && !req.ForceSyncCommit {
			log.S().Debugf("async commit startTS=%v secondaries=%v minCommitTS=%v", lock.StartTS, lock.Secondaries, lock.MinCommitTS)
			return TxnStatus{0, kvrpcpb.Action_NoAction, lock.ToLockInfo(req.PrimaryKey)}, nil
		}

		// If the lock has already outdated, clean up it.
		if uint64(oracle.ExtractPhysical(lock.StartTS))+uint64(lock.TTL) < uint64(oracle.ExtractPhysical(req.CurrentTs)) {
			// If the resolving lock and primary lock are both pessimistic type, just pessimistic rollback locks.
			if req.ResolvingPessimisticLock && lock.Op == uint8(kvrpcpb.Op_PessimisticLock) {
				batch.PessimisticRollback(req.PrimaryKey)
				return TxnStatus{0, kvrpcpb.Action_TTLExpirePessimisticRollback, nil}, store.dbWriter.Write(batch)
			}
			batch.Rollback(req.PrimaryKey, true)
			return TxnStatus{0, kvrpcpb.Action_TTLExpireRollback, nil}, store.dbWriter.Write(batch)
		}
		// If this is a large transaction and the lock is active, push forward the minCommitTS.
		// lock.minCommitTS == 0 may be a secondary lock, or not a large transaction.
		// For async commit protocol, the minCommitTS is always greater than zero, but async commit will not be a large transaction.
		action := kvrpcpb.Action_NoAction
		if req.CallerStartTs == maxSystemTS {
			action = kvrpcpb.Action_MinCommitTSPushed
		} else if lock.MinCommitTS > 0 && !lock.UseAsyncCommit {
			action = kvrpcpb.Action_MinCommitTSPushed
			// We *must* guarantee the invariance lock.minCommitTS >= callerStartTS + 1
			if lock.MinCommitTS < req.CallerStartTs+1 {
				lock.MinCommitTS = max(req.CallerStartTs+1, req.CurrentTs)
				batch.PessimisticLock(req.PrimaryKey, lock)
				if err = store.dbWriter.Write(batch); err != nil {
					return TxnStatus{0, action, nil}, err
				}
			}
		}
		return TxnStatus{0, action, lock.ToLockInfo(req.PrimaryKey)}, nil
	}

	// The current transaction lock not exists, check the transaction commit info
	commitTS, err := store.checkCommitted(reqCtx.getDBReader(), req.PrimaryKey, req.LockTs)
	if commitTS > 0 {
		return TxnStatus{commitTS, kvrpcpb.Action_NoAction, nil}, nil
	}
	// Check if the transaction already rollbacked
	status := store.checkExtraTxnStatus(reqCtx, req.PrimaryKey, req.LockTs)
	if status.isRollback {
		return TxnStatus{0, kvrpcpb.Action_NoAction, nil}, nil
	}
	if status.isOpLockCommitted() {
		commitTS = status.commitTS
		return TxnStatus{commitTS, kvrpcpb.Action_NoAction, nil}, nil
	}
	// If current transaction is not prewritted before, it may be pessimistic lock.
	// When pessimistic txn rollback statement, it may not leave a 'rollbacked' tombstone.
	// Or maybe caused by concurrent prewrite operation.
	// Especially in the non-block reading case, the secondary lock is likely to be
	// written before the primary lock.
	// Currently client will always set this flag to true when resolving locks
	if req.RollbackIfNotExist {
		if req.ResolvingPessimisticLock {
			return TxnStatus{0, kvrpcpb.Action_LockNotExistDoNothing, nil}, nil
		}
		batch.Rollback(req.PrimaryKey, false)
		err = store.dbWriter.Write(batch)
		return TxnStatus{0, kvrpcpb.Action_LockNotExistRollback, nil}, nil
	}
	return TxnStatus{0, kvrpcpb.Action_NoAction, nil}, &kverrors.ErrTxnNotFound{
		PrimaryKey: req.PrimaryKey,
		StartTS:    req.LockTs,
	}
}

// SecondaryLocksStatus is the result of `CheckSecondaryLocksStatus` API.
type SecondaryLocksStatus struct {
	locks    []*kvrpcpb.LockInfo
	commitTS uint64
}

// CheckSecondaryLocks implements the MVCCStore interface.
func (store *MVCCStore) CheckSecondaryLocks(reqCtx *requestCtx, keys [][]byte, startTS uint64) (SecondaryLocksStatus, error) {
	sortKeys(keys)
	hashVals := keysToHashVals(keys...)
	log.S().Debugf("%d check secondary %v", startTS, hashVals)
	regCtx := reqCtx.regCtx
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)
	locks := make([]*kvrpcpb.LockInfo, 0, len(keys))
	for i, key := range keys {
		lock := store.getLock(reqCtx, key)
		if !(lock != nil && lock.StartTS == startTS) {
			commitTS, err := store.checkCommitted(reqCtx.getDBReader(), key, startTS)
			if err != nil {
				return SecondaryLocksStatus{}, err
			}
			if commitTS > 0 {
				return SecondaryLocksStatus{commitTS: commitTS}, nil
			}
			status := store.checkExtraTxnStatus(reqCtx, key, startTS)
			if status.isOpLockCommitted() {
				return SecondaryLocksStatus{commitTS: status.commitTS}, nil
			}
			if !status.isRollback {
				batch.Rollback(key, false)
				err = store.dbWriter.Write(batch)
			}
			return SecondaryLocksStatus{commitTS: 0}, err
		}
		if lock.Op == uint8(kvrpcpb.Op_PessimisticLock) {
			batch.Rollback(key, true)
			err := store.dbWriter.Write(batch)
			if err != nil {
				return SecondaryLocksStatus{}, err
			}
			store.lockWaiterManager.WakeUp(startTS, 0, []uint64{hashVals[i]})
			store.DeadlockDetectCli.CleanUp(startTS)
			return SecondaryLocksStatus{commitTS: 0}, nil
		}
		locks = append(locks, lock.ToLockInfo(key))
	}
	return SecondaryLocksStatus{locks: locks}, nil
}

func (store *MVCCStore) normalizeWaitTime(lockWaitTime int64) time.Duration {
	if lockWaitTime > store.conf.PessimisticTxn.WaitForLockTimeout {
		lockWaitTime = store.conf.PessimisticTxn.WaitForLockTimeout
	} else if lockWaitTime == 0 {
		lockWaitTime = store.conf.PessimisticTxn.WaitForLockTimeout
	}
	return time.Duration(lockWaitTime) * time.Millisecond
}

func (store *MVCCStore) handleCheckPessimisticErr(startTS uint64, err error, isFirstLock bool, lockWaitTime int64, key []byte, resourceGroupTag []byte) (*lockwaiter.Waiter, error) {
	if locked, ok := err.(*kverrors.ErrLocked); ok {
		if lockWaitTime != lockwaiter.LockNoWait {
			keyHash := farm.Fingerprint64(locked.Key)
			waitTimeDuration := store.normalizeWaitTime(lockWaitTime)
			lock := locked.Lock
			log.S().Debugf("%d blocked by %d on key %d", startTS, lock.StartTS, keyHash)
			waiter := store.lockWaiterManager.NewWaiter(startTS, lock.StartTS, keyHash, waitTimeDuration)
			if !isFirstLock {
				store.DeadlockDetectCli.Detect(startTS, lock.StartTS, keyHash, key, resourceGroupTag)
			}
			return waiter, err
		}
	}
	return nil, err
}

// buildPessimisticLock builds the lock according to the request and the current state of the key.
// Returns the built lock, and the LockedWithConflictTS (if any, otherwise 0).
func (store *MVCCStore) buildPessimisticLock(m *kvrpcpb.Mutation, item *badger.Item, latestExtraMeta mvcc.DBUserMeta,
	req *kvrpcpb.PessimisticLockRequest) (*mvcc.Lock, uint64, error) {
	var lockedWithConflictTS uint64 = 0

	var writeConflictError error

	if item != nil {
		if !req.Force {
			userMeta := mvcc.DBUserMeta(item.UserMeta())
			if latestExtraMeta != nil && latestExtraMeta.CommitTS() > userMeta.CommitTS() {
				userMeta = latestExtraMeta
			}

			if userMeta.CommitTS() > req.ForUpdateTs {
				writeConflictError = &kverrors.ErrConflict{
					StartTS:          req.StartVersion,
					ConflictTS:       userMeta.StartTS(),
					ConflictCommitTS: userMeta.CommitTS(),
					Key:              item.KeyCopy(nil),
					Reason:           kvrpcpb.WriteConflict_PessimisticRetry,
				}

				if req.GetWakeUpMode() == kvrpcpb.PessimisticLockWakeUpMode_WakeUpModeNormal {
					return nil, 0, writeConflictError
				} else if req.GetWakeUpMode() != kvrpcpb.PessimisticLockWakeUpMode_WakeUpModeForceLock {
					panic("unreachable")
				}
				lockedWithConflictTS = userMeta.CommitTS()
			}
		}
		if m.Assertion == kvrpcpb.Assertion_NotExist && !item.IsEmpty() {
			if lockedWithConflictTS != 0 {
				// If constraint is violated, disable locking with conflict behavior.
				if writeConflictError == nil {
					panic("unreachable")
				}
				return nil, 0, writeConflictError
			}
			return nil, 0, &kverrors.ErrKeyAlreadyExists{Key: m.Key}
		}
	}

	if ok, err := doesNeedLock(item, req); !ok {
		if err != nil {
			return nil, 0, err
		}
		if lockedWithConflictTS > 0 {
			// If lockIfOnlyExist is used on a not-existing key, disable locking with conflict behavior.
			if writeConflictError == nil {
				panic("unreachable")
			}
			return nil, 0, writeConflictError
		}
		return nil, 0, nil
	}
	actualWrittenForUpdateTS := req.ForUpdateTs
	if lockedWithConflictTS > 0 {
		actualWrittenForUpdateTS = lockedWithConflictTS
	}
	lock := &mvcc.Lock{
		LockHdr: mvcc.LockHdr{
			StartTS:     req.StartVersion,
			ForUpdateTS: actualWrittenForUpdateTS,
			Op:          uint8(kvrpcpb.Op_PessimisticLock),
			TTL:         uint32(req.LockTtl),
			PrimaryLen:  uint16(len(req.PrimaryLock)),
		},
		Primary: req.PrimaryLock,
	}
	return lock, lockedWithConflictTS, nil
}

// getLatestExtraMetaForKey returns the userMeta of the extra txn status key with the biggest commit ts.
// It's used to assert whether a key has been written / locked by another transaction in the fair locking mechanism.
// Theoretically, the rollback record should be ignored. But we have no way to check the rollback record in the
// unistore. Returning record with a bigger commit ts may cause extra retry, but it's safe.
func (store *MVCCStore) getLatestExtraMetaForKey(reqCtx *requestCtx, m *kvrpcpb.Mutation) mvcc.DBUserMeta {
	it := reqCtx.getDBReader().GetExtraIter()
	rbStartKey := mvcc.EncodeExtraTxnStatusKey(m.Key, math.MaxUint64)
	rbEndKey := mvcc.EncodeExtraTxnStatusKey(m.Key, 0)

	for it.Seek(rbStartKey); it.Valid(); it.Next() {
		item := it.Item()
		if len(rbEndKey) != 0 && bytes.Compare(item.Key(), rbEndKey) > 0 {
			break
		}
		key := item.Key()
		if len(key) == 0 || (key[0] != tableExtraPrefix && key[0] != metaExtraPrefix) {
			continue
		}

		meta := mvcc.DBUserMeta(item.UserMeta())
		return meta
	}
	return nil
}
