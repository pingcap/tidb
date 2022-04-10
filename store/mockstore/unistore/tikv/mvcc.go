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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/badger"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/mockstore/unistore/config"
	"github.com/pingcap/tidb/store/mockstore/unistore/lockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore/pd"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/kverrors"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/store/mockstore/unistore/util/lockwaiter"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// MVCCStore is a wrapper of badger.DB to provide MVCC functions.
type MVCCStore struct {
	dir       string
	db        *badger.DB
	lockStore *lockstore.MemStore
	dbWriter  mvcc.DBWriter
	safePoint *SafePoint
	pdClient  pd.Client
	closeCh   chan bool

	conf *config.Config

	latestTS          uint64
	lockWaiterManager *lockwaiter.Manager
	DeadlockDetectCli *DetectorClient
	DeadlockDetectSvr *DetectorServer
}

// NewMVCCStore creates a new MVCCStore
func NewMVCCStore(conf *config.Config, bundle *mvcc.DBBundle, dataDir string, safePoint *SafePoint,
	writer mvcc.DBWriter, pdClient pd.Client) *MVCCStore {
	store := &MVCCStore{
		db:                bundle.DB,
		dir:               dataDir,
		lockStore:         bundle.LockStore,
		safePoint:         safePoint,
		pdClient:          pdClient,
		closeCh:           make(chan bool),
		dbWriter:          writer,
		conf:              conf,
		lockWaiterManager: lockwaiter.NewManager(conf),
	}
	store.DeadlockDetectSvr = NewDetectorServer()
	store.DeadlockDetectCli = NewDetectorClient(store.lockWaiterManager, pdClient)
	writer.Open()
	if pdClient != nil {
		// pdClient is nil in unit test.
		go store.runUpdateSafePointLoop()
	}
	return store
}

func (store *MVCCStore) updateLatestTS(ts uint64) {
	for {
		old := atomic.LoadUint64(&store.latestTS)
		if old < ts {
			if !atomic.CompareAndSwapUint64(&store.latestTS, old, ts) {
				continue
			}
		}
		return
	}
}

func (store *MVCCStore) getLatestTS() uint64 {
	return atomic.LoadUint64(&store.latestTS)
}

// Close closes the MVCCStore.
func (store *MVCCStore) Close() error {
	store.dbWriter.Close()
	close(store.closeCh)

	err := store.dumpMemLocks()
	if err != nil {
		log.Fatal("dump mem locks failed", zap.Error(err))
	}
	return nil
}

type lockEntryHdr struct {
	keyLen uint32
	valLen uint32
}

func (store *MVCCStore) dumpMemLocks() error {
	tmpFileName := store.dir + "/lock_store.tmp"
	f, err := os.OpenFile(tmpFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return errors.Trace(err)
	}
	writer := bufio.NewWriter(f)
	cnt := 0
	it := store.lockStore.NewIterator()
	hdrBuf := make([]byte, 8)
	hdr := (*lockEntryHdr)(unsafe.Pointer(&hdrBuf[0]))
	for it.SeekToFirst(); it.Valid(); it.Next() {
		hdr.keyLen = uint32(len(it.Key()))
		hdr.valLen = uint32(len(it.Value()))
		_, err = writer.Write(hdrBuf)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = writer.Write(it.Key())
		if err != nil {
			return errors.Trace(err)
		}
		_, err = writer.Write(it.Value())
		if err != nil {
			return errors.Trace(err)
		}
		cnt++
	}
	err = writer.Flush()
	if err != nil {
		return errors.Trace(err)
	}
	err = f.Sync()
	if err != nil {
		return errors.Trace(err)
	}
	err = f.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return os.Rename(tmpFileName, store.dir+"/lock_store")
}

func (store *MVCCStore) getDBItems(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation) (items []*badger.Item, err error) {
	txn := reqCtx.getDBReader().GetTxn()
	keys := make([][]byte, len(mutations))
	for i, m := range mutations {
		keys[i] = m.Key
	}
	return txn.MultiGet(keys)
}

func sortMutations(mutations []*kvrpcpb.Mutation) []*kvrpcpb.Mutation {
	fn := func(i, j int) bool {
		return bytes.Compare(mutations[i].Key, mutations[j].Key) < 0
	}
	if sort.SliceIsSorted(mutations, fn) {
		return mutations
	}
	sort.Slice(mutations, fn)
	return mutations
}

func sortPrewrite(req *kvrpcpb.PrewriteRequest) []*kvrpcpb.Mutation {
	if len(req.IsPessimisticLock) == 0 {
		return sortMutations(req.Mutations)
	}
	sorter := pessimisticPrewriteSorter{PrewriteRequest: req}
	if sort.IsSorted(sorter) {
		return req.Mutations
	}
	sort.Sort(sorter)
	return req.Mutations
}

type pessimisticPrewriteSorter struct {
	*kvrpcpb.PrewriteRequest
}

func (sorter pessimisticPrewriteSorter) Less(i, j int) bool {
	return bytes.Compare(sorter.Mutations[i].Key, sorter.Mutations[j].Key) < 0
}

func (sorter pessimisticPrewriteSorter) Len() int {
	return len(sorter.Mutations)
}

func (sorter pessimisticPrewriteSorter) Swap(i, j int) {
	sorter.Mutations[i], sorter.Mutations[j] = sorter.Mutations[j], sorter.Mutations[i]
	sorter.IsPessimisticLock[i], sorter.IsPessimisticLock[j] = sorter.IsPessimisticLock[j], sorter.IsPessimisticLock[i]
}

func sortKeys(keys [][]byte) [][]byte {
	less := func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	}
	if sort.SliceIsSorted(keys, less) {
		return keys
	}
	sort.Slice(keys, less)
	return keys
}

// PessimisticLock will add pessimistic lock on key
func (store *MVCCStore) PessimisticLock(reqCtx *requestCtx, req *kvrpcpb.PessimisticLockRequest, resp *kvrpcpb.PessimisticLockResponse) (*lockwaiter.Waiter, error) {
	mutations := req.Mutations
	if !req.ReturnValues {
		mutations = sortMutations(req.Mutations)
	}
	startTS := req.StartVersion
	regCtx := reqCtx.regCtx
	hashVals := mutationsToHashVals(mutations)
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

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
	if err != nil {
		return nil, err
	}
	if !dup {
		for i, m := range mutations {
			lock, err1 := store.buildPessimisticLock(m, items[i], req)
			if err1 != nil {
				return nil, err1
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
				lock.MinCommitTS = req.CallerStartTs + 1

				// Remove this condition should not affect correctness.
				// We do it because pushing forward minCommitTS as far as possible could avoid
				// the lock been pushed again several times, and thus reduce write operations.
				if lock.MinCommitTS < req.CurrentTs {
					lock.MinCommitTS = req.CurrentTs
				}
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
		if lock != nil && lock.StartTS == startTS {
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
		} else {
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

func (store *MVCCStore) buildPessimisticLock(m *kvrpcpb.Mutation, item *badger.Item,
	req *kvrpcpb.PessimisticLockRequest) (*mvcc.Lock, error) {
	if item != nil {
		userMeta := mvcc.DBUserMeta(item.UserMeta())
		if !req.Force {
			if userMeta.CommitTS() > req.ForUpdateTs {
				return nil, &kverrors.ErrConflict{
					StartTS:          req.StartVersion,
					ConflictTS:       userMeta.StartTS(),
					ConflictCommitTS: userMeta.CommitTS(),
					Key:              item.KeyCopy(nil),
				}
			}
		}
		if m.Assertion == kvrpcpb.Assertion_NotExist && !item.IsEmpty() {
			return nil, &kverrors.ErrKeyAlreadyExists{Key: m.Key}
		}
	}
	lock := &mvcc.Lock{
		LockHdr: mvcc.LockHdr{
			StartTS:     req.StartVersion,
			ForUpdateTS: req.ForUpdateTs,
			Op:          uint8(kvrpcpb.Op_PessimisticLock),
			TTL:         uint32(req.LockTtl),
			PrimaryLen:  uint16(len(req.PrimaryLock)),
		},
		Primary: req.PrimaryLock,
	}
	return lock, nil
}

// Prewrite implements the MVCCStore interface.
func (store *MVCCStore) Prewrite(reqCtx *requestCtx, req *kvrpcpb.PrewriteRequest) error {
	mutations := sortPrewrite(req)
	regCtx := reqCtx.regCtx
	hashVals := mutationsToHashVals(mutations)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	isPessimistic := req.ForUpdateTs > 0
	var err error
	if isPessimistic {
		err = store.prewritePessimistic(reqCtx, mutations, req)
	} else {
		err = store.prewriteOptimistic(reqCtx, mutations, req)
	}
	if err != nil {
		return err
	}

	if reqCtx.onePCCommitTS != 0 {
		// TODO: Is it correct to pass the hashVals directly here, considering that some of the keys may
		// have no pessimistic lock?
		if isPessimistic {
			store.lockWaiterManager.WakeUp(req.StartVersion, reqCtx.onePCCommitTS, hashVals)
			store.DeadlockDetectCli.CleanUp(req.StartVersion)
		}
	}
	return nil
}

func (store *MVCCStore) prewriteOptimistic(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation, req *kvrpcpb.PrewriteRequest) error {
	startTS := req.StartVersion
	// Must check the LockStore first.
	for _, m := range mutations {
		lock, err := store.checkConflictInLockStore(reqCtx, m, startTS)
		if err != nil {
			return err
		}
		if lock != nil {
			// duplicated command
			return nil
		}
		if bytes.Equal(m.Key, req.PrimaryLock) {
			status := store.checkExtraTxnStatus(reqCtx, m.Key, req.StartVersion)
			if status.isRollback {
				return kverrors.ErrAlreadyRollback
			}
			if status.isOpLockCommitted() {
				// duplicated command
				return nil
			}
		}
	}
	items, err := store.getDBItems(reqCtx, mutations)
	if err != nil {
		return err
	}
	for i, m := range mutations {
		item := items[i]
		if item != nil {
			userMeta := mvcc.DBUserMeta(item.UserMeta())
			if userMeta.CommitTS() > startTS {
				return &kverrors.ErrConflict{
					StartTS:          startTS,
					ConflictTS:       userMeta.StartTS(),
					ConflictCommitTS: userMeta.CommitTS(),
					Key:              item.KeyCopy(nil),
				}
			}
		}
		// Op_CheckNotExists type requests should not add lock
		if m.Op == kvrpcpb.Op_CheckNotExists {
			if item != nil {
				val, err := item.Value()
				if err != nil {
					return err
				}
				if len(val) > 0 {
					return &kverrors.ErrKeyAlreadyExists{Key: m.Key}
				}
			}
			continue
		}
		// TODO add memory lock for async commit protocol.
	}
	return store.prewriteMutations(reqCtx, mutations, req, items)
}

func (store *MVCCStore) prewritePessimistic(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation, req *kvrpcpb.PrewriteRequest) error {
	startTS := req.StartVersion
	for i, m := range mutations {
		if m.Op == kvrpcpb.Op_CheckNotExists {
			return kverrors.ErrInvalidOp{Op: m.Op}
		}
		lock := store.getLock(reqCtx, m.Key)
		isPessimisticLock := len(req.IsPessimisticLock) > 0 && req.IsPessimisticLock[i]
		lockExists := lock != nil
		lockMatch := lockExists && lock.StartTS == startTS
		if isPessimisticLock {
			valid := lockExists && lockMatch
			if !valid {
				return errors.New("pessimistic lock not found")
			}
			if lock.Op != uint8(kvrpcpb.Op_PessimisticLock) {
				// Duplicated command.
				return nil
			}
			// Do not overwrite lock ttl if prewrite ttl smaller than pessimisitc lock ttl
			if uint64(lock.TTL) > req.LockTtl {
				req.LockTtl = uint64(lock.TTL)
			}
		} else {
			// non pessimistic lock in pessimistic transaction, e.g. non-unique index.
			valid := !lockExists || lockMatch
			if !valid {
				// Safe to set TTL to zero because the transaction of the lock is committed
				// or rollbacked or must be rollbacked.
				lock.TTL = 0
				return kverrors.BuildLockErr(m.Key, lock)
			}
			if lockMatch {
				// Duplicate command.
				return nil
			}
		}
		// TODO add memory lock for async commit protocol.
	}
	items, err := store.getDBItems(reqCtx, mutations)
	if err != nil {
		return err
	}
	return store.prewriteMutations(reqCtx, mutations, req, items)
}

func (store *MVCCStore) prewriteMutations(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation,
	req *kvrpcpb.PrewriteRequest, items []*badger.Item) error {
	var minCommitTS uint64
	if req.UseAsyncCommit || req.TryOnePc {
		// Get minCommitTS for async commit protocol. After all keys are locked in memory lock.
		physical, logical, tsErr := store.pdClient.GetTS(context.Background())
		if tsErr != nil {
			return tsErr
		}
		minCommitTS = uint64(physical)<<18 + uint64(logical)
		if req.MaxCommitTs > 0 && minCommitTS > req.MaxCommitTs {
			req.UseAsyncCommit = false
			req.TryOnePc = false
		}
		if req.UseAsyncCommit {
			reqCtx.asyncMinCommitTS = minCommitTS
		}
	}

	if req.UseAsyncCommit && minCommitTS > req.MinCommitTs {
		req.MinCommitTs = minCommitTS
	}

	if req.TryOnePc {
		committed, err := store.tryOnePC(reqCtx, mutations, req, items, minCommitTS, req.MaxCommitTs)
		if err != nil {
			return err
		}
		// If 1PC succeeded, exit immediately.
		if committed {
			return nil
		}
	}

	batch := store.dbWriter.NewWriteBatch(req.StartVersion, 0, reqCtx.rpcCtx)

	for i, m := range mutations {
		if m.Op == kvrpcpb.Op_CheckNotExists {
			continue
		}
		lock, err1 := store.buildPrewriteLock(reqCtx, m, items[i], req)
		if err1 != nil {
			return err1
		}
		batch.Prewrite(m.Key, lock)
	}

	return store.dbWriter.Write(batch)
}

func (store *MVCCStore) tryOnePC(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation,
	req *kvrpcpb.PrewriteRequest, items []*badger.Item, minCommitTS uint64, maxCommitTS uint64) (bool, error) {
	if maxCommitTS != 0 && minCommitTS > maxCommitTS {
		log.Debug("1pc transaction fallbacks due to minCommitTS exceeds maxCommitTS",
			zap.Uint64("startTS", req.StartVersion),
			zap.Uint64("minCommitTS", minCommitTS),
			zap.Uint64("maxCommitTS", maxCommitTS))
		return false, nil
	}
	if minCommitTS < req.StartVersion {
		log.Fatal("1pc commitTS less than startTS", zap.Uint64("startTS", req.StartVersion), zap.Uint64("minCommitTS", minCommitTS))
	}

	reqCtx.onePCCommitTS = minCommitTS
	store.updateLatestTS(minCommitTS)
	batch := store.dbWriter.NewWriteBatch(req.StartVersion, minCommitTS, reqCtx.rpcCtx)

	for i, m := range mutations {
		if m.Op == kvrpcpb.Op_CheckNotExists {
			continue
		}
		lock, err1 := store.buildPrewriteLock(reqCtx, m, items[i], req)
		if err1 != nil {
			return false, err1
		}
		// batch.Commit will panic if the key is not locked. So there need to be a special function
		// for it to commit without deleting lock.
		batch.Commit(m.Key, lock)
	}

	if err := store.dbWriter.Write(batch); err != nil {
		return false, err
	}

	return true, nil
}

func encodeFromOldRow(oldRow, buf []byte) ([]byte, error) {
	var (
		colIDs []int64
		datums []types.Datum
	)
	for len(oldRow) > 1 {
		var d types.Datum
		var err error
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return nil, err
		}
		colID := d.GetInt64()
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return nil, err
		}
		colIDs = append(colIDs, colID)
		datums = append(datums, d)
	}
	var encoder rowcodec.Encoder
	return encoder.Encode(&stmtctx.StatementContext{}, colIDs, datums, buf)
}

func (store *MVCCStore) buildPrewriteLock(reqCtx *requestCtx, m *kvrpcpb.Mutation, item *badger.Item,
	req *kvrpcpb.PrewriteRequest) (*mvcc.Lock, error) {
	lock := &mvcc.Lock{
		LockHdr: mvcc.LockHdr{
			StartTS:        req.StartVersion,
			TTL:            uint32(req.LockTtl),
			PrimaryLen:     uint16(len(req.PrimaryLock)),
			MinCommitTS:    req.MinCommitTs,
			UseAsyncCommit: req.UseAsyncCommit,
			SecondaryNum:   uint32(len(req.Secondaries)),
		},
		Primary:     req.PrimaryLock,
		Value:       m.Value,
		Secondaries: req.Secondaries,
	}
	// Note that this is not fully consistent with TiKV. TiKV doesn't always get the value from Write CF. In
	// AssertionLevel_Fast, TiKV skips checking assertion if Write CF is not read, in order not to harm the performance.
	// However, unistore can always check it. It's better not to assume the store's behavior about assertion when the
	// mode is set to AssertionLevel_Fast.
	if req.AssertionLevel != kvrpcpb.AssertionLevel_Off {
		if item == nil || item.IsEmpty() {
			if m.Assertion == kvrpcpb.Assertion_Exist {
				log.Error("ASSERTION FAIL!!! non-exist for must exist key", zap.Stringer("mutation", m))
				return nil, &kverrors.ErrAssertionFailed{
					StartTS:          req.StartVersion,
					Key:              m.Key,
					Assertion:        m.Assertion,
					ExistingStartTS:  0,
					ExistingCommitTS: 0,
				}
			}
		} else {
			if m.Assertion == kvrpcpb.Assertion_NotExist {
				log.Error("ASSERTION FAIL!!! exist for must non-exist key", zap.Stringer("mutation", m))
				userMeta := mvcc.DBUserMeta(item.UserMeta())
				return nil, &kverrors.ErrAssertionFailed{
					StartTS:          req.StartVersion,
					Key:              m.Key,
					Assertion:        m.Assertion,
					ExistingStartTS:  userMeta.StartTS(),
					ExistingCommitTS: userMeta.CommitTS(),
				}
			}
		}
	}
	var err error
	lock.Op = uint8(m.Op)
	if lock.Op == uint8(kvrpcpb.Op_Insert) {
		if item != nil && item.ValueSize() > 0 {
			return nil, &kverrors.ErrKeyAlreadyExists{Key: m.Key}
		}
		lock.Op = uint8(kvrpcpb.Op_Put)
	}
	if rowcodec.IsRowKey(m.Key) && lock.Op == uint8(kvrpcpb.Op_Put) {
		if rowcodec.IsNewFormat(m.Value) {
			reqCtx.buf = m.Value
		} else {
			reqCtx.buf, err = encodeFromOldRow(m.Value, reqCtx.buf)
			if err != nil {
				log.Error("encode data failed", zap.Binary("value", m.Value), zap.Binary("key", m.Key), zap.Stringer("op", m.Op), zap.Error(err))
				return nil, err
			}
		}
		lock.Value = reqCtx.buf
	}

	lock.ForUpdateTS = req.ForUpdateTs
	return lock, nil
}

func (store *MVCCStore) getLock(req *requestCtx, key []byte) *mvcc.Lock {
	req.buf = store.lockStore.Get(key, req.buf)
	if len(req.buf) == 0 {
		return nil
	}
	lock := mvcc.DecodeLock(req.buf)
	return &lock
}

func (store *MVCCStore) checkConflictInLockStore(
	req *requestCtx, mutation *kvrpcpb.Mutation, startTS uint64) (*mvcc.Lock, error) {
	req.buf = store.lockStore.Get(mutation.Key, req.buf)
	if len(req.buf) == 0 {
		return nil, nil
	}
	lock := mvcc.DecodeLock(req.buf)
	if lock.StartTS == startTS {
		// Same ts, no need to overwrite.
		return &lock, nil
	}
	return nil, kverrors.BuildLockErr(mutation.Key, &lock)
}

const maxSystemTS uint64 = math.MaxUint64

// Commit implements the MVCCStore interface.
func (store *MVCCStore) Commit(req *requestCtx, keys [][]byte, startTS, commitTS uint64) error {
	sortKeys(keys)
	store.updateLatestTS(commitTS)
	regCtx := req.regCtx
	hashVals := keysToHashVals(keys...)
	batch := store.dbWriter.NewWriteBatch(startTS, commitTS, req.rpcCtx)
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	var buf []byte
	var tmpDiff int
	var isPessimisticTxn bool
	for _, key := range keys {
		var lockErr error
		var checkErr error
		var lock mvcc.Lock
		buf = store.lockStore.Get(key, buf)
		if len(buf) == 0 {
			// We never commit partial keys in Commit request, so if one lock is not found,
			// the others keys must not be found too.
			lockErr = kverrors.ErrLockNotFound
		} else {
			lock = mvcc.DecodeLock(buf)
			if lock.StartTS != startTS {
				lockErr = kverrors.ErrReplaced
			}
		}
		if lockErr != nil {
			// Maybe the secondary keys committed by other concurrent transactions using lock resolver,
			// check commit info from store
			checkErr = store.handleLockNotFound(req, key, startTS, commitTS)
			if checkErr == nil {
				continue
			}
			log.Error("commit failed, no correspond lock found",
				zap.Binary("key", key), zap.Uint64("start ts", startTS), zap.String("lock", fmt.Sprintf("%v", lock)), zap.Error(lockErr))
			return lockErr
		}
		if commitTS < lock.MinCommitTS {
			log.Info("trying to commit with smaller commitTs than minCommitTs",
				zap.Uint64("commit ts", commitTS), zap.Uint64("min commit ts", lock.MinCommitTS), zap.Binary("key", key))
			return &kverrors.ErrCommitExpire{
				StartTs:     startTS,
				CommitTs:    commitTS,
				MinCommitTs: lock.MinCommitTS,
				Key:         key,
			}
		}
		if lock.Op == uint8(kvrpcpb.Op_PessimisticLock) {
			log.Warn("commit a pessimistic lock with Lock type", zap.Binary("key", key), zap.Uint64("start ts", startTS), zap.Uint64("commit ts", commitTS))
			lock.Op = uint8(kvrpcpb.Op_Lock)
		}
		isPessimisticTxn = lock.ForUpdateTS > 0
		tmpDiff += len(key) + len(lock.Value)
		batch.Commit(key, &lock)
	}
	atomic.AddInt64(regCtx.Diff(), int64(tmpDiff))
	err := store.dbWriter.Write(batch)
	store.lockWaiterManager.WakeUp(startTS, commitTS, hashVals)
	if isPessimisticTxn {
		store.DeadlockDetectCli.CleanUp(startTS)
	}
	return err
}

func (store *MVCCStore) handleLockNotFound(reqCtx *requestCtx, key []byte, startTS, commitTS uint64) error {
	txn := reqCtx.getDBReader().GetTxn()
	txn.SetReadTS(commitTS)
	item, err := txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return errors.Trace(err)
	}
	if item == nil {
		return kverrors.ErrLockNotFound
	}
	userMeta := mvcc.DBUserMeta(item.UserMeta())
	if userMeta.StartTS() == startTS {
		// Already committed.
		return nil
	}
	return kverrors.ErrLockNotFound
}

const (
	rollbackStatusDone    = 0
	rollbackStatusNoLock  = 1
	rollbackStatusNewLock = 2
	rollbackPessimistic   = 3
	rollbackStatusLocked  = 4
)

// Rollback implements the MVCCStore interface.
func (store *MVCCStore) Rollback(reqCtx *requestCtx, keys [][]byte, startTS uint64) error {
	sortKeys(keys)
	hashVals := keysToHashVals(keys...)
	log.S().Debugf("%d rollback %v", startTS, hashVals)
	regCtx := reqCtx.regCtx
	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	statuses := make([]int, len(keys))
	for i, key := range keys {
		var rollbackErr error
		statuses[i], rollbackErr = store.rollbackKeyReadLock(reqCtx, batch, key, startTS, 0)
		if rollbackErr != nil {
			return errors.Trace(rollbackErr)
		}
	}
	for i, key := range keys {
		status := statuses[i]
		if status == rollbackStatusDone || status == rollbackPessimistic {
			// rollback pessimistic lock doesn't need to read db.
			continue
		}
		err := store.rollbackKeyReadDB(reqCtx, batch, key, startTS, statuses[i] == rollbackStatusNewLock)
		if err != nil {
			return err
		}
	}
	store.DeadlockDetectCli.CleanUp(startTS)
	err := store.dbWriter.Write(batch)
	return errors.Trace(err)
}

func (store *MVCCStore) rollbackKeyReadLock(reqCtx *requestCtx, batch mvcc.WriteBatch, key []byte,
	startTS, currentTs uint64) (int, error) {
	reqCtx.buf = store.lockStore.Get(key, reqCtx.buf)
	hasLock := len(reqCtx.buf) > 0
	if hasLock {
		lock := mvcc.DecodeLock(reqCtx.buf)
		if lock.StartTS < startTS {
			// The lock is old, means this is written by an old transaction, and the current transaction may not arrive.
			// We should write a rollback lock.
			batch.Rollback(key, false)
			return rollbackStatusDone, nil
		}
		if lock.StartTS == startTS {
			if currentTs > 0 && uint64(oracle.ExtractPhysical(lock.StartTS))+uint64(lock.TTL) >= uint64(oracle.ExtractPhysical(currentTs)) {
				return rollbackStatusLocked, kverrors.BuildLockErr(key, &lock)
			}
			// We can not simply delete the lock because the prewrite may be sent multiple times.
			// To prevent that we update it a rollback lock.
			batch.Rollback(key, true)
			return rollbackStatusDone, nil
		}
		// lock.startTS > startTS, go to DB to check if the key is committed.
		return rollbackStatusNewLock, nil
	}
	return rollbackStatusNoLock, nil
}

func (store *MVCCStore) rollbackKeyReadDB(req *requestCtx, batch mvcc.WriteBatch, key []byte, startTS uint64, hasLock bool) error {
	commitTS, err := store.checkCommitted(req.getDBReader(), key, startTS)
	if err != nil {
		return err
	}
	if commitTS != 0 {
		return kverrors.ErrAlreadyCommitted(commitTS)
	}
	// commit not found, rollback this key
	batch.Rollback(key, false)
	return nil
}

func (store *MVCCStore) checkCommitted(reader *dbreader.DBReader, key []byte, startTS uint64) (uint64, error) {
	txn := reader.GetTxn()
	item, err := txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return 0, errors.Trace(err)
	}
	if item == nil {
		return 0, nil
	}
	userMeta := mvcc.DBUserMeta(item.UserMeta())
	if userMeta.StartTS() == startTS {
		return userMeta.CommitTS(), nil
	}
	it := reader.GetIter()
	it.SetAllVersions(true)
	for it.Seek(key); it.Valid(); it.Next() {
		item = it.Item()
		if !bytes.Equal(item.Key(), key) {
			break
		}
		userMeta = mvcc.DBUserMeta(item.UserMeta())
		if userMeta.StartTS() == startTS {
			return userMeta.CommitTS(), nil
		}
	}
	return 0, nil
}

// LockPair contains a pair of key and lock. It's used for reading through locks.
type LockPair struct {
	key  []byte
	lock *mvcc.Lock
}

func getValueFromLock(lock *mvcc.Lock) []byte {
	if lock.Op == byte(kvrpcpb.Op_Put) {
		// lock owns the value so needn't to safeCopy it.
		return lock.Value
	}
	return nil
}

// *LockPair is not nil if the lock in the committed timestamp set. Read operations can get value from it without deep copy.
func checkLock(lock mvcc.Lock, key []byte, startTS uint64, resolved []uint64, committed []uint64) (*LockPair, error) {
	if inTSSet(lock.StartTS, resolved) {
		return nil, nil
	}
	lockVisible := lock.StartTS <= startTS
	isWriteLock := lock.Op == uint8(kvrpcpb.Op_Put) || lock.Op == uint8(kvrpcpb.Op_Del)
	isPrimaryGet := startTS == maxSystemTS && bytes.Equal(lock.Primary, key) && !lock.UseAsyncCommit
	if lockVisible && isWriteLock && !isPrimaryGet {
		if inTSSet(lock.StartTS, committed) {
			return &LockPair{safeCopy(key), &lock}, nil
		}
		return nil, kverrors.BuildLockErr(safeCopy(key), &lock)
	}
	return nil, nil
}

// checkLockForRcCheckTS checks the lock for `RcCheckTS` isolation level in transaction read.
func checkLockForRcCheckTS(lock mvcc.Lock, key []byte, startTS uint64, resolved []uint64) error {
	if inTSSet(lock.StartTS, resolved) {
		return nil
	}
	isWriteLock := lock.Op == uint8(kvrpcpb.Op_Put) || lock.Op == uint8(kvrpcpb.Op_Del)
	if !isWriteLock {
		return nil
	}
	return &kverrors.ErrConflict{
		StartTS:    startTS,
		ConflictTS: lock.StartTS,
		Key:        safeCopy(key),
	}
}

// CheckKeysLockForRcCheckTS is used to check version timestamp if `RcCheckTS` isolation level is used.
func (store *MVCCStore) CheckKeysLockForRcCheckTS(startTS uint64, resolved []uint64, keys ...[]byte) error {
	var buf []byte
	for _, key := range keys {
		buf = store.lockStore.Get(key, buf)
		if len(buf) == 0 {
			continue
		}
		lock := mvcc.DecodeLock(buf)
		err := checkLockForRcCheckTS(lock, key, startTS, resolved)
		if err != nil {
			return err
		}
	}
	return nil
}

// CheckKeysLock implements the MVCCStore interface.
func (store *MVCCStore) CheckKeysLock(startTS uint64, resolved, committed []uint64, keys ...[]byte) ([]*LockPair, error) {
	var buf []byte
	var lockPairs []*LockPair
	for _, key := range keys {
		buf = store.lockStore.Get(key, buf)
		if len(buf) == 0 {
			continue
		}
		lock := mvcc.DecodeLock(buf)
		lockPair, err := checkLock(lock, key, startTS, resolved, committed)
		if lockPair != nil {
			lockPairs = append(lockPairs, lockPair)
		}
		if err != nil {
			return nil, err
		}
	}
	return lockPairs, nil
}

// CheckRangeLock implements the MVCCStore interface.
func (store *MVCCStore) CheckRangeLock(startTS uint64, startKey, endKey []byte, resolved []uint64) error {
	it := store.lockStore.NewIterator()
	for it.Seek(startKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), endKey) {
			break
		}
		lock := mvcc.DecodeLock(it.Value())
		_, err := checkLock(lock, it.Key(), startTS, resolved, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// Cleanup implements the MVCCStore interface.
func (store *MVCCStore) Cleanup(reqCtx *requestCtx, key []byte, startTS, currentTs uint64) error {
	hashVals := keysToHashVals(key)
	regCtx := reqCtx.regCtx
	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	status, err := store.rollbackKeyReadLock(reqCtx, batch, key, startTS, currentTs)
	if err != nil {
		return err
	}
	if status != rollbackStatusDone {
		err := store.rollbackKeyReadDB(reqCtx, batch, key, startTS, status == rollbackStatusNewLock)
		if err != nil {
			return err
		}
		rbStatus := store.checkExtraTxnStatus(reqCtx, key, startTS)
		if rbStatus.isOpLockCommitted() {
			return kverrors.ErrAlreadyCommitted(rbStatus.commitTS)
		}
	}
	err = store.dbWriter.Write(batch)
	store.lockWaiterManager.WakeUp(startTS, 0, hashVals)
	return err
}

func (store *MVCCStore) appendScannedLock(locks []*kvrpcpb.LockInfo, it *lockstore.Iterator, maxTS uint64) []*kvrpcpb.LockInfo {
	lock := mvcc.DecodeLock(it.Value())
	if lock.StartTS < maxTS {
		locks = append(locks, &kvrpcpb.LockInfo{
			PrimaryLock: lock.Primary,
			LockVersion: lock.StartTS,
			Key:         safeCopy(it.Key()),
			LockTtl:     uint64(lock.TTL),
		})
	}
	return locks
}

// ScanLock implements the MVCCStore interface.
func (store *MVCCStore) ScanLock(reqCtx *requestCtx, maxTS uint64, limit int) ([]*kvrpcpb.LockInfo, error) {
	var locks []*kvrpcpb.LockInfo
	it := store.lockStore.NewIterator()
	for it.Seek(reqCtx.regCtx.RawStart()); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), reqCtx.regCtx.RawEnd()) {
			return locks, nil
		}
		if len(locks) == limit {
			return locks, nil
		}
		locks = store.appendScannedLock(locks, it, maxTS)
	}
	return locks, nil
}

// PhysicalScanLock implements the MVCCStore interface.
func (store *MVCCStore) PhysicalScanLock(startKey []byte, maxTS uint64, limit int) []*kvrpcpb.LockInfo {
	var locks []*kvrpcpb.LockInfo
	it := store.lockStore.NewIterator()
	for it.Seek(startKey); it.Valid(); it.Next() {
		if len(locks) == limit {
			break
		}
		locks = store.appendScannedLock(locks, it, maxTS)
	}
	return locks
}

// ResolveLock implements the MVCCStore interface.
func (store *MVCCStore) ResolveLock(reqCtx *requestCtx, lockKeys [][]byte, startTS, commitTS uint64) error {
	regCtx := reqCtx.regCtx
	if len(lockKeys) == 0 {
		it := store.lockStore.NewIterator()
		for it.Seek(regCtx.RawStart()); it.Valid(); it.Next() {
			if exceedEndKey(it.Key(), regCtx.RawEnd()) {
				break
			}
			lock := mvcc.DecodeLock(it.Value())
			if lock.StartTS != startTS {
				continue
			}
			lockKeys = append(lockKeys, safeCopy(it.Key()))
		}
		if len(lockKeys) == 0 {
			return nil
		}
	}
	hashVals := keysToHashVals(lockKeys...)
	batch := store.dbWriter.NewWriteBatch(startTS, commitTS, reqCtx.rpcCtx)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	var buf []byte
	var tmpDiff int
	for _, lockKey := range lockKeys {
		buf = store.lockStore.Get(lockKey, buf)
		if len(buf) == 0 {
			continue
		}
		lock := mvcc.DecodeLock(buf)
		if lock.StartTS != startTS {
			continue
		}
		if commitTS > 0 {
			tmpDiff += len(lockKey) + len(lock.Value)
			batch.Commit(lockKey, &lock)
		} else {
			batch.Rollback(lockKey, true)
		}
	}
	atomic.AddInt64(regCtx.Diff(), int64(tmpDiff))
	err := store.dbWriter.Write(batch)
	return err
}

// UpdateSafePoint implements the MVCCStore interface.
func (store *MVCCStore) UpdateSafePoint(safePoint uint64) {
	// We use the gcLock to make sure safePoint can only increase.
	store.db.UpdateSafeTs(safePoint)
	store.safePoint.UpdateTS(safePoint)
	log.Info("safePoint is updated to", zap.Uint64("ts", safePoint), zap.Time("time", tsToTime(safePoint)))
}

func tsToTime(ts uint64) time.Time {
	return time.Unix(0, int64(ts>>18)*1000000)
}

// StartDeadlockDetection implements the MVCCStore interface.
func (store *MVCCStore) StartDeadlockDetection(isRaft bool) {
	if isRaft {
		go store.DeadlockDetectCli.sendReqLoop()
		return
	}

	go func() {
		for {
			select {
			case req := <-store.DeadlockDetectCli.sendCh:
				resp := store.DeadlockDetectSvr.Detect(req)
				if resp != nil {
					store.DeadlockDetectCli.waitMgr.WakeUpForDeadlock(resp)
				}
			case <-store.closeCh:
				return
			}
		}
	}()
}

// MvccGetByKey gets mvcc information using input key as rawKey
func (store *MVCCStore) MvccGetByKey(reqCtx *requestCtx, key []byte) (*kvrpcpb.MvccInfo, error) {
	mvccInfo := &kvrpcpb.MvccInfo{}
	lock := store.getLock(reqCtx, key)
	if lock != nil {
		mvccInfo.Lock = &kvrpcpb.MvccLock{
			Type:       kvrpcpb.Op(lock.Op),
			StartTs:    lock.StartTS,
			Primary:    lock.Primary,
			ShortValue: lock.Value,
		}
	}
	reader := reqCtx.getDBReader()
	isRowKey := rowcodec.IsRowKey(key)
	// Get commit writes from db
	err := reader.GetMvccInfoByKey(key, isRowKey, mvccInfo)
	if err != nil {
		return nil, err
	}
	// Get rollback writes from rollback store
	err = store.getExtraMvccInfo(key, reqCtx, mvccInfo)
	if err != nil {
		return nil, err
	}
	sort.Slice(mvccInfo.Writes, func(i, j int) bool {
		return mvccInfo.Writes[i].CommitTs > mvccInfo.Writes[j].CommitTs
	})
	mvccInfo.Values = make([]*kvrpcpb.MvccValue, len(mvccInfo.Writes))
	for i := 0; i < len(mvccInfo.Writes); i++ {
		write := mvccInfo.Writes[i]
		mvccInfo.Values[i] = &kvrpcpb.MvccValue{
			StartTs: write.StartTs,
			Value:   write.ShortValue,
		}
	}
	return mvccInfo, nil
}

func (store *MVCCStore) getExtraMvccInfo(rawkey []byte,
	reqCtx *requestCtx, mvccInfo *kvrpcpb.MvccInfo) error {
	it := reqCtx.getDBReader().GetExtraIter()
	rbStartKey := mvcc.EncodeExtraTxnStatusKey(rawkey, math.MaxUint64)
	rbEndKey := mvcc.EncodeExtraTxnStatusKey(rawkey, 0)
	for it.Seek(rbStartKey); it.Valid(); it.Next() {
		item := it.Item()
		if len(rbEndKey) != 0 && bytes.Compare(item.Key(), rbEndKey) > 0 {
			break
		}
		key := item.Key()
		if len(key) == 0 || (key[0] != tableExtraPrefix && key[0] != metaExtraPrefix) {
			continue
		}
		rollbackTs := mvcc.DecodeKeyTS(key)
		curRecord := &kvrpcpb.MvccWrite{
			Type:     kvrpcpb.Op_Rollback,
			StartTs:  rollbackTs,
			CommitTs: rollbackTs,
		}
		mvccInfo.Writes = append(mvccInfo.Writes, curRecord)
	}
	return nil
}

// MvccGetByStartTs implements the MVCCStore interface.
func (store *MVCCStore) MvccGetByStartTs(reqCtx *requestCtx, startTs uint64) (*kvrpcpb.MvccInfo, []byte, error) {
	reader := reqCtx.getDBReader()
	startKey := reqCtx.regCtx.RawStart()
	endKey := reqCtx.regCtx.RawEnd()
	rawKey, err := reader.GetKeyByStartTs(startKey, endKey, startTs)
	if err != nil {
		return nil, nil, err
	}
	if rawKey == nil {
		return nil, nil, nil
	}
	res, err := store.MvccGetByKey(reqCtx, rawKey)
	if err != nil {
		return nil, nil, err
	}
	return res, rawKey, nil
}

// DeleteFileInRange implements the MVCCStore interface.
func (store *MVCCStore) DeleteFileInRange(start, end []byte) {
	store.db.DeleteFilesInRange(start, end)
	start[0]++
	end[0]++
	store.db.DeleteFilesInRange(start, end)
}

// Get implements the MVCCStore interface.
func (store *MVCCStore) Get(reqCtx *requestCtx, key []byte, version uint64) ([]byte, error) {
	if reqCtx.isSnapshotIsolation() {
		lockPairs, err := store.CheckKeysLock(version, reqCtx.rpcCtx.ResolvedLocks, reqCtx.rpcCtx.CommittedLocks, key)
		if err != nil {
			return nil, err
		}
		if len(lockPairs) != 0 {
			return getValueFromLock(lockPairs[0].lock), nil
		}
	} else if reqCtx.isRcCheckTSIsolationLevel() {
		err := store.CheckKeysLockForRcCheckTS(version, reqCtx.rpcCtx.ResolvedLocks, key)
		if err != nil {
			return nil, err
		}
	}
	val, err := reqCtx.getDBReader().Get(key, version)
	if val == nil {
		return nil, err
	}
	return safeCopy(val), err
}

// BatchGet implements the MVCCStore interface.
func (store *MVCCStore) BatchGet(reqCtx *requestCtx, keys [][]byte, version uint64) []*kvrpcpb.KvPair {
	pairs := make([]*kvrpcpb.KvPair, 0, len(keys))
	var remain [][]byte
	if reqCtx.isSnapshotIsolation() {
		remain = make([][]byte, 0, len(keys))
		for _, key := range keys {
			lockPairs, err := store.CheckKeysLock(version, reqCtx.rpcCtx.ResolvedLocks, reqCtx.rpcCtx.CommittedLocks, key)
			if err != nil {
				pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Error: convertToKeyError(err)})
			} else if len(lockPairs) != 0 {
				value := getValueFromLock(lockPairs[0].lock)
				if value != nil {
					pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
				}
			} else {
				remain = append(remain, key)
			}
		}
	} else if reqCtx.isRcCheckTSIsolationLevel() {
		remain = make([][]byte, 0, len(keys))
		for _, key := range keys {
			err := store.CheckKeysLockForRcCheckTS(version, reqCtx.rpcCtx.ResolvedLocks, key)
			if err != nil {
				pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Error: convertToKeyError(err)})
			} else {
				remain = append(remain, key)
			}
		}
	} else {
		remain = keys
	}
	batchGetFunc := func(key, value []byte, err error) {
		if len(value) != 0 {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Key:   safeCopy(key),
				Value: safeCopy(value),
				Error: convertToKeyError(err),
			})
		}
	}
	reqCtx.getDBReader().BatchGet(remain, version, batchGetFunc)
	return pairs
}

func (store *MVCCStore) collectRangeLock(startTS uint64, startKey, endKey []byte, resolved, committed []uint64,
	isolationLEvel kvrpcpb.IsolationLevel) []*kvrpcpb.KvPair {
	var pairs []*kvrpcpb.KvPair
	it := store.lockStore.NewIterator()
	for it.Seek(startKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), endKey) {
			break
		}
		lock := mvcc.DecodeLock(it.Value())
		if isolationLEvel == kvrpcpb.IsolationLevel_SI {
			lockPair, err := checkLock(lock, it.Key(), startTS, resolved, committed)
			if lockPair != nil {
				pairs = append(pairs, &kvrpcpb.KvPair{
					Key: lockPair.key,
					// deleted key's value is nil
					Value: getValueFromLock(lockPair.lock),
				})
			} else if err != nil {
				pairs = append(pairs, &kvrpcpb.KvPair{
					Error: convertToKeyError(err),
					Key:   safeCopy(it.Key()),
				})
			}
		} else if isolationLEvel == kvrpcpb.IsolationLevel_RCCheckTS {
			err := checkLockForRcCheckTS(lock, it.Key(), startTS, resolved)
			if err != nil {
				pairs = append(pairs, &kvrpcpb.KvPair{
					Error: convertToKeyError(err),
					Key:   safeCopy(it.Key()),
				})
			}
		}
	}
	return pairs
}

func inTSSet(startTS uint64, tsSet []uint64) bool {
	for _, v := range tsSet {
		if startTS == v {
			return true
		}
	}
	return false
}

type kvScanProcessor struct {
	pairs      []*kvrpcpb.KvPair
	sampleStep uint32
	scanCnt    uint32
}

func (p *kvScanProcessor) Process(key, value []byte) (err error) {
	if p.sampleStep > 0 {
		p.scanCnt++
		if (p.scanCnt-1)%p.sampleStep != 0 {
			return nil
		}
	}
	p.pairs = append(p.pairs, &kvrpcpb.KvPair{
		Key:   safeCopy(key),
		Value: safeCopy(value),
	})
	return nil
}

func (p *kvScanProcessor) SkipValue() bool {
	return false
}

// Scan implements the MVCCStore interface.
func (store *MVCCStore) Scan(reqCtx *requestCtx, req *kvrpcpb.ScanRequest) []*kvrpcpb.KvPair {
	var startKey, endKey []byte
	if req.Reverse {
		startKey = req.EndKey
		if len(startKey) == 0 {
			startKey = reqCtx.regCtx.RawStart()
		}
		endKey = req.StartKey
	} else {
		startKey = req.StartKey
		endKey = req.EndKey
		if len(endKey) == 0 {
			endKey = reqCtx.regCtx.RawEnd()
		}
		if len(endKey) == 0 {
			// Don't scan internal keys.
			endKey = InternalKeyPrefix
		}
	}
	var lockPairs []*kvrpcpb.KvPair
	limit := req.GetLimit()
	if req.SampleStep == 0 {
		if reqCtx.isSnapshotIsolation() || reqCtx.isRcCheckTSIsolationLevel() {
			if bytes.Compare(startKey, endKey) <= 0 {
				lockPairs = store.collectRangeLock(req.GetVersion(), startKey, endKey, reqCtx.rpcCtx.ResolvedLocks,
					reqCtx.rpcCtx.CommittedLocks, reqCtx.rpcCtx.IsolationLevel)
			} else {
				lockPairs = store.collectRangeLock(req.GetVersion(), endKey, startKey, reqCtx.rpcCtx.ResolvedLocks,
					reqCtx.rpcCtx.CommittedLocks, reqCtx.rpcCtx.IsolationLevel)
			}
		}
	} else {
		limit = req.SampleStep * limit
	}
	var scanProc = &kvScanProcessor{
		sampleStep: req.SampleStep,
	}
	reader := reqCtx.getDBReader()
	var err error
	if req.Reverse {
		err = reader.ReverseScan(startKey, endKey, int(limit), req.GetVersion(), scanProc)
	} else {
		err = reader.Scan(startKey, endKey, int(limit), req.GetVersion(), scanProc)
	}
	if err != nil {
		scanProc.pairs = append(scanProc.pairs[:0], &kvrpcpb.KvPair{
			Error: convertToKeyError(err),
		})
		return scanProc.pairs
	}
	pairs := append(lockPairs, scanProc.pairs...)
	sort.SliceStable(pairs, func(i, j int) bool {
		cmp := bytes.Compare(pairs[i].Key, pairs[j].Key)
		if req.Reverse {
			cmp = -cmp
		}
		return cmp < 0
	})
	validPairs := pairs[:0]
	var prev *kvrpcpb.KvPair
	for _, pair := range pairs {
		if prev != nil && bytes.Equal(prev.Key, pair.Key) {
			continue
		}
		prev = pair
		if pair.Error != nil || len(pair.Value) != 0 {
			validPairs = append(validPairs, pair)
			if len(validPairs) >= int(limit) {
				break
			}
		}
	}
	return validPairs
}

func (store *MVCCStore) runUpdateSafePointLoop() {
	var lastSafePoint uint64
	ticker := time.NewTicker(time.Minute)
	for {
		safePoint, err := store.pdClient.GetGCSafePoint(context.Background())
		if err != nil {
			log.Error("get GC safePoint error", zap.Error(err))
		} else if lastSafePoint < safePoint {
			store.UpdateSafePoint(safePoint)
			lastSafePoint = safePoint
		}
		select {
		case <-store.closeCh:
			return
		case <-ticker.C:
		}
	}
}

// SafePoint represents a safe point.
type SafePoint struct {
	timestamp uint64
}

// UpdateTS is used to record the timestamp of updating the table's schema information.
// These changing schema operations don't include 'truncate table' and 'rename table'.
func (sp *SafePoint) UpdateTS(ts uint64) {
	for {
		old := atomic.LoadUint64(&sp.timestamp)
		if old < ts {
			if !atomic.CompareAndSwapUint64(&sp.timestamp, old, ts) {
				continue
			}
		}
		break
	}
}

// CreateCompactionFilter implements badger.CompactionFilterFactory function.
func (sp *SafePoint) CreateCompactionFilter(targetLevel int, startKey, endKey []byte) badger.CompactionFilter {
	return &GCCompactionFilter{
		targetLevel: targetLevel,
		safePoint:   atomic.LoadUint64(&sp.timestamp),
	}
}

// GCCompactionFilter implements the badger.CompactionFilter interface.
type GCCompactionFilter struct {
	targetLevel int
	safePoint   uint64
}

const (
	metaPrefix byte = 'm'
	// 'm' + 1 = 'n'
	metaExtraPrefix byte = 'n'
	tablePrefix     byte = 't'
	// 't' + 1 = 'u
	tableExtraPrefix byte = 'u'
)

// Filter implements the badger.CompactionFilter interface.
// Since we use txn ts as badger version, we only need to filter Delete, Rollback and Op_Lock.
// It is called for the first valid version before safe point, older versions are discarded automatically.
func (f *GCCompactionFilter) Filter(key, value, userMeta []byte) badger.Decision {
	switch key[0] {
	case metaPrefix, tablePrefix:
		// For latest version, we need to remove `delete` key, which has value len 0.
		if mvcc.DBUserMeta(userMeta).CommitTS() < f.safePoint && len(value) == 0 {
			return badger.DecisionMarkTombstone
		}
	case metaExtraPrefix, tableExtraPrefix:
		// For latest version, we can only remove `delete` key, which has value len 0.
		if mvcc.DBUserMeta(userMeta).StartTS() < f.safePoint {
			return badger.DecisionDrop
		}
	}
	// Older version are discarded automatically, we need to keep the first valid version.
	return badger.DecisionKeep
}

var (
	baseGuard       = badger.Guard{MatchLen: 64, MinSize: 64 * 1024}
	raftGuard       = badger.Guard{Prefix: []byte{0}, MatchLen: 1, MinSize: 64 * 1024}
	metaGuard       = badger.Guard{Prefix: []byte{'m'}, MatchLen: 1, MinSize: 64 * 1024}
	metaExtraGuard  = badger.Guard{Prefix: []byte{'n'}, MatchLen: 1, MinSize: 1}
	tableGuard      = badger.Guard{Prefix: []byte{'t'}, MatchLen: 9, MinSize: 1 * 1024 * 1024}
	tableIndexGuard = badger.Guard{Prefix: []byte{'t'}, MatchLen: 11, MinSize: 1 * 1024 * 1024}
	tableExtraGuard = badger.Guard{Prefix: []byte{'u'}, MatchLen: 1, MinSize: 1}
)

// Guards implements the badger.CompactionFilter interface.
// Guards returns specifications that may splits the SST files
// A key is associated to a guard that has the longest matched Prefix.
func (f *GCCompactionFilter) Guards() []badger.Guard {
	if f.targetLevel < 4 {
		// do not split index and row for top levels.
		return []badger.Guard{
			baseGuard, raftGuard, metaGuard, metaExtraGuard, tableGuard, tableExtraGuard,
		}
	}
	// split index and row for bottom levels.
	return []badger.Guard{
		baseGuard, raftGuard, metaGuard, metaExtraGuard, tableIndexGuard, tableExtraGuard,
	}
}
