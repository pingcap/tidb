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
	"fmt"
	"os"
	"slices"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/badger"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/config"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/lockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/pd"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/kverrors"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/util/lockwaiter"
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
	fn := func(i, j *kvrpcpb.Mutation) int {
		return bytes.Compare(i.Key, j.Key)
	}
	if slices.IsSortedFunc(mutations, fn) {
		return mutations
	}
	slices.SortFunc(mutations, fn)
	return mutations
}

func sortPrewrite(req *kvrpcpb.PrewriteRequest) []*kvrpcpb.Mutation {
	if len(req.PessimisticActions) == 0 {
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
	sorter.PessimisticActions[i], sorter.PessimisticActions[j] = sorter.PessimisticActions[j], sorter.PessimisticActions[i]
}

func sortKeys(keys [][]byte) [][]byte {
	if slices.IsSortedFunc(keys, bytes.Compare) {
		return keys
	}
	slices.SortFunc(keys, bytes.Compare)
	return keys
}

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
		Reason:     kvrpcpb.WriteConflict_RcCheckTs,
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

// ReadBufferFromLock implements the MVCCStore interface.
func (store *MVCCStore) ReadBufferFromLock(startTS uint64, keys ...[]byte) []*kvrpcpb.KvPair {
	var buf []byte
	pairs := make([]*kvrpcpb.KvPair, 0, len(keys))
	for _, key := range keys {
		buf = store.lockStore.Get(key, buf)
		if len(buf) == 0 {
			continue
		}
		lock := mvcc.DecodeLock(buf)
		if lock.StartTS != startTS {
			continue
		}
		switch lock.Op {
		case uint8(kvrpcpb.Op_Put):
			val := getValueFromLock(&lock)
			if val == nil {
				panic("Op_Put has a nil value")
			}
			pairs = append(
				pairs, &kvrpcpb.KvPair{
					Key:   key,
					Value: safeCopy(val),
				},
			)
		case uint8(kvrpcpb.Op_Del):
			pairs = append(
				pairs, &kvrpcpb.KvPair{
					Key:   key,
					Value: []byte{},
				},
			)
		default:
			panic("unexpected op. Optimistic txn should only contain put and delete locks")
		}
	}
	return pairs
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
