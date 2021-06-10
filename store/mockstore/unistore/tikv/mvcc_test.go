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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/store/mockstore/unistore/config"
	"github.com/pingcap/tidb/store/mockstore/unistore/lockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/store/mockstore/unistore/util/lockwaiter"
)

var _ = Suite(&testMvccSuite{})
var maxTs = uint64(math.MaxUint64)
var lockTTL = uint64(50)

type testMvccSuite struct{}

type TestStore struct {
	MvccStore *MVCCStore
	Svr       *Server
	DBPath    string
	LogPath   string
	c         *C
}

func (ts *TestStore) newReqCtx() *requestCtx {
	return ts.newReqCtxWithKeys([]byte{'t'}, []byte{'u'})
}

func (ts *TestStore) newReqCtxWithKeys(rawStartKey, rawEndKey []byte) *requestCtx {
	return &requestCtx{
		regCtx: &regionCtx{
			latches:     newLatches(),
			rawStartKey: rawStartKey,
			rawEndKey:   rawEndKey,
		},
		rpcCtx: &kvrpcpb.Context{
			RegionId:    1,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
			Peer: &metapb.Peer{
				Id:      1,
				StoreId: 1,
				Role:    metapb.PeerRole_Voter,
			},
		},
		svr: ts.Svr,
	}
}

func newMutation(op kvrpcpb.Op, key, value []byte) *kvrpcpb.Mutation {
	return &kvrpcpb.Mutation{
		Op:    op,
		Key:   key,
		Value: value,
	}
}

func CreateTestDB(dbPath, LogPath string) (*badger.DB, error) {
	subPath := fmt.Sprintf("/%d", 0)
	opts := badger.DefaultOptions
	opts.Dir = dbPath + subPath
	opts.ValueDir = LogPath + subPath
	opts.ManagedTxns = true
	return badger.Open(opts)
}

func NewTestStore(dbPrefix string, logPrefix string, c *C) (*TestStore, error) {
	dbPath, err := os.MkdirTemp("", dbPrefix)
	if err != nil {
		return nil, err
	}
	LogPath, err := os.MkdirTemp("", logPrefix)
	if err != nil {
		return nil, err
	}
	safePoint := &SafePoint{}
	db, err := CreateTestDB(dbPath, LogPath)
	if err != nil {
		return nil, err
	}
	dbBundle := &mvcc.DBBundle{
		DB:        db,
		LockStore: lockstore.NewMemStore(4096),
	}
	// Some raft store path problems could not be found using simple store in tests
	// writer := NewDBWriter(dbBundle, safePoint)
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	snapPath := filepath.Join(dbPath, "snap")
	err = os.MkdirAll(kvPath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(raftPath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	err = os.Mkdir(snapPath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	writer := NewDBWriter(dbBundle)

	rm, err := NewMockRegionManager(dbBundle, 1, RegionOptions{
		StoreAddr:  "127.0.0.1:10086",
		PDAddr:     "127.0.0.1:2379",
		RegionSize: 96 * 1024 * 1024,
	})
	if err != nil {
		return nil, err
	}
	pdClient := NewMockPD(rm)
	store := NewMVCCStore(&config.DefaultConf, dbBundle, dbPath, safePoint, writer, pdClient)
	svr := NewServer(nil, store, nil)
	return &TestStore{
		MvccStore: store,
		Svr:       svr,
		DBPath:    dbPath,
		LogPath:   LogPath,
		c:         c,
	}, nil
}

func CleanTestStore(store *TestStore) {
	_ = os.RemoveAll(store.DBPath)
	_ = os.RemoveAll(store.LogPath)
}

// PessimisticLock will add pessimistic lock on key
func PessimisticLock(pk []byte, key []byte, startTs uint64, lockTTL uint64, forUpdateTs uint64,
	isFirstLock bool, forceLock bool, store *TestStore) (*lockwaiter.Waiter, error) {
	req := &kvrpcpb.PessimisticLockRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_PessimisticLock, key, nil)},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
		ForUpdateTs:  forUpdateTs,
		IsFirstLock:  isFirstLock,
		Force:        forceLock,
	}
	waiter, err := store.MvccStore.PessimisticLock(store.newReqCtx(), req, &kvrpcpb.PessimisticLockResponse{})
	return waiter, err
}

// PrewriteOptimistic raises optimistic prewrite requests on store
func PrewriteOptimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	minCommitTs uint64, useAsyncCommit bool, secondaries [][]byte, store *TestStore) error {
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:      []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Put, key, value)},
		PrimaryLock:    pk,
		StartVersion:   startTs,
		LockTtl:        lockTTL,
		MinCommitTs:    minCommitTs,
		UseAsyncCommit: useAsyncCommit,
		Secondaries:    secondaries,
	}
	return store.MvccStore.prewriteOptimistic(store.newReqCtx(), prewriteReq.Mutations, prewriteReq)
}

// PrewritePessimistic raises pessmistic prewrite requests
func PrewritePessimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	isPessimisticLock []bool, forUpdateTs uint64, store *TestStore) error {
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:         []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Put, key, value)},
		PrimaryLock:       pk,
		StartVersion:      startTs,
		LockTtl:           lockTTL,
		IsPessimisticLock: isPessimisticLock,
		ForUpdateTs:       forUpdateTs,
	}
	return store.MvccStore.prewritePessimistic(store.newReqCtx(), prewriteReq.Mutations, prewriteReq)
}

func MustCheckTxnStatus(pk []byte, lockTs uint64, callerStartTs uint64,
	currentTs uint64, rollbackIfNotExists bool, ttl, commitTs uint64, action kvrpcpb.Action, s *TestStore) {
	resTTL, resCommitTs, resAction, err := CheckTxnStatus(pk, lockTs, callerStartTs, currentTs, rollbackIfNotExists, s)
	s.c.Assert(err, IsNil)
	s.c.Assert(resTTL, Equals, ttl)
	s.c.Assert(resCommitTs, Equals, commitTs)
	s.c.Assert(resAction, Equals, action)
}

func CheckTxnStatus(pk []byte, lockTs uint64, callerStartTs uint64,
	currentTs uint64, rollbackIfNotExists bool, store *TestStore) (uint64, uint64, kvrpcpb.Action, error) {
	req := &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         pk,
		LockTs:             lockTs,
		CallerStartTs:      callerStartTs,
		CurrentTs:          currentTs,
		RollbackIfNotExist: rollbackIfNotExists,
	}
	ttl := uint64(0)
	txnStatus, err := store.MvccStore.CheckTxnStatus(store.newReqCtx(), req)
	if txnStatus.lockInfo != nil {
		ttl = txnStatus.lockInfo.LockTtl
	}
	return ttl, txnStatus.commitTS, txnStatus.action, err
}

func CheckSecondaryLocksStatus(keys [][]byte, startTS uint64, store *TestStore) ([]*kvrpcpb.LockInfo, uint64, error) {
	status, err := store.MvccStore.CheckSecondaryLocks(store.newReqCtx(), keys, startTS)
	return status.locks, status.commitTS, err
}

func MustLocked(key []byte, pessimistic bool, store *TestStore) {
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	store.c.Assert(lock, NotNil)
	if pessimistic {
		store.c.Assert(lock.ForUpdateTS, Greater, uint64(0))
	} else {
		store.c.Assert(lock.ForUpdateTS, Equals, uint64(0))
	}
}

func MustPessimisticLocked(key []byte, startTs, forUpdateTs uint64, store *TestStore) {
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	store.c.Assert(lock, NotNil)
	store.c.Assert(lock.StartTS, Equals, startTs)
	store.c.Assert(lock.ForUpdateTS, Equals, forUpdateTs)
}

func MustUnLocked(key []byte, store *TestStore) {
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	store.c.Assert(lock, IsNil)
}

func MustPrewritePut(pk, key []byte, val []byte, startTs uint64, store *TestStore) {
	MustPrewriteOptimistic(pk, key, val, startTs, 50, startTs, store)
}

func MustPrewritePutLockErr(pk, key []byte, val []byte, startTs uint64, store *TestStore) {
	err := PrewriteOptimistic(pk, key, val, startTs, lockTTL, startTs, false, [][]byte{}, store)
	store.c.Assert(err, NotNil)
	lockedErr := err.(*ErrLocked)
	store.c.Assert(lockedErr, NotNil)
}

func MustPrewritePutErr(pk, key []byte, val []byte, startTs uint64, store *TestStore) {
	err := PrewriteOptimistic(pk, key, val, startTs, lockTTL, startTs, false, [][]byte{}, store)
	store.c.Assert(err, NotNil)
}

func MustPrewriteInsert(pk, key []byte, val []byte, startTs uint64, store *TestStore) {
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Insert, key, val)},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
		MinCommitTs:  startTs,
	}
	err := store.MvccStore.prewriteOptimistic(store.newReqCtx(), prewriteReq.Mutations, prewriteReq)
	store.c.Assert(err, IsNil)
}

func MustPrewriteInsertAlreadyExists(pk, key []byte, val []byte, startTs uint64, store *TestStore) {
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Insert, key, val)},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
		MinCommitTs:  startTs,
	}
	err := store.MvccStore.prewriteOptimistic(store.newReqCtx(), prewriteReq.Mutations, prewriteReq)
	store.c.Assert(err, NotNil)
	existErr := err.(*ErrKeyAlreadyExists)
	store.c.Assert(existErr, NotNil)
}

func MustPrewriteOpCheckExistAlreadyExist(pk, key []byte, startTs uint64, store *TestStore) {
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_CheckNotExists, key, nil)},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
		MinCommitTs:  startTs,
	}
	err := store.MvccStore.prewriteOptimistic(store.newReqCtx(), prewriteReq.Mutations, prewriteReq)
	store.c.Assert(err, NotNil)
	existErr := err.(*ErrKeyAlreadyExists)
	store.c.Assert(existErr, NotNil)
}

func MustPrewriteOpCheckExistOk(pk, key []byte, startTs uint64, store *TestStore) {
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_CheckNotExists, key, nil)},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
		MinCommitTs:  startTs,
	}
	err := store.MvccStore.prewriteOptimistic(store.newReqCtx(), prewriteReq.Mutations, prewriteReq)
	store.c.Assert(err, IsNil)
	var buf []byte
	buf = store.MvccStore.lockStore.Get(key, buf)
	store.c.Assert(len(buf), Equals, 0)
}

func MustPrewriteDelete(pk, key []byte, startTs uint64, store *TestStore) {
	MustPrewriteOptimistic(pk, key, nil, startTs, 50, startTs, store)
}

func MustAcquirePessimisticLock(pk, key []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	_, err := PessimisticLock(pk, key, startTs, lockTTL, forUpdateTs, false, false, store)
	store.c.Assert(err, IsNil)
}

func MustAcquirePessimisticLockForce(pk, key []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	_, err := PessimisticLock(pk, key, startTs, lockTTL, forUpdateTs, false, true, store)
	store.c.Assert(err, IsNil)
}

func MustAcquirePessimisticLockErr(pk, key []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	_, err := PessimisticLock(pk, key, startTs, lockTTL, forUpdateTs, false, false, store)
	store.c.Assert(err, NotNil)
}

func MustPessimisitcPrewriteDelete(pk, key []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	MustPrewritePessimistic(pk, key, nil, startTs, 5000, []bool{true}, forUpdateTs, store)
}

func MustPessimisticRollback(key []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	err := store.MvccStore.PessimisticRollback(store.newReqCtx(), &kvrpcpb.PessimisticRollbackRequest{
		StartVersion: startTs,
		ForUpdateTs:  forUpdateTs,
		Keys:         [][]byte{key},
	})
	store.c.Assert(err, IsNil)
}

func MustPrewriteOptimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	minCommitTs uint64, store *TestStore) {
	store.c.Assert(PrewriteOptimistic(pk, key, value, startTs, lockTTL, minCommitTs, false, [][]byte{}, store), IsNil)
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	store.c.Assert(uint64(lock.TTL), Equals, lockTTL)
	store.c.Assert(bytes.Compare(lock.Value, value), Equals, 0)
}

func MustPrewriteOptimisticAsyncCommit(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	minCommitTs uint64, secondaries [][]byte, store *TestStore) {
	store.c.Assert(PrewriteOptimistic(pk, key, value, startTs, lockTTL, minCommitTs, true, secondaries, store), IsNil)
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	store.c.Assert(uint64(lock.TTL), Equals, lockTTL)
	store.c.Assert(bytes.Compare(lock.Value, value), Equals, 0)
}

func MustPrewritePessimisticPut(pk []byte, key []byte, value []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	store.c.Assert(PrewritePessimistic(pk, key, value, startTs, lockTTL, []bool{true}, forUpdateTs, store), IsNil)
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	store.c.Assert(lock.ForUpdateTS, Equals, forUpdateTs)
	store.c.Assert(bytes.Compare(lock.Value, value), Equals, 0)
}

func MustPrewritePessimisticDelete(pk []byte, key []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	store.c.Assert(PrewritePessimistic(pk, key, nil, startTs, lockTTL, []bool{true}, forUpdateTs, store), IsNil)
}
func MustPrewritePessimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	isPessimisticLock []bool, forUpdateTs uint64, store *TestStore) {
	store.c.Assert(PrewritePessimistic(pk, key, value, startTs, lockTTL, isPessimisticLock, forUpdateTs, store), IsNil)
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	store.c.Assert(lock.ForUpdateTS, Equals, forUpdateTs)
	store.c.Assert(bytes.Compare(lock.Value, value), Equals, 0)
}

func MustPrewritePessimisticPutErr(pk []byte, key []byte, value []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	err := PrewritePessimistic(pk, key, value, startTs, lockTTL, []bool{true}, forUpdateTs, store)
	store.c.Assert(err, NotNil)
}

func MustCommitKeyPut(key, val []byte, startTs, commitTs uint64, store *TestStore) {
	err := store.MvccStore.Commit(store.newReqCtx(), [][]byte{key}, startTs, commitTs)
	store.c.Assert(err, IsNil)
	getVal, err := store.newReqCtx().getDBReader().Get(key, commitTs)
	store.c.Assert(err, IsNil)
	store.c.Assert(bytes.Compare(getVal, val), Equals, 0)
}

func MustCommit(key []byte, startTs, commitTs uint64, store *TestStore) {
	err := store.MvccStore.Commit(store.newReqCtx(), [][]byte{key}, startTs, commitTs)
	store.c.Assert(err, IsNil)
}

func MustCommitErr(key []byte, startTs, commitTs uint64, store *TestStore) {
	err := store.MvccStore.Commit(store.newReqCtx(), [][]byte{key}, startTs, commitTs)
	store.c.Assert(err, NotNil)
}

func MustRollbackKey(key []byte, startTs uint64, store *TestStore) {
	err := store.MvccStore.Rollback(store.newReqCtx(), [][]byte{key}, startTs)
	store.c.Assert(err, IsNil)
	store.c.Assert(store.MvccStore.lockStore.Get(key, nil), IsNil)
	status := store.MvccStore.checkExtraTxnStatus(store.newReqCtx(), key, startTs)
	store.c.Assert(status.isRollback, IsTrue)
}

func MustRollbackErr(key []byte, startTs uint64, store *TestStore) {
	err := store.MvccStore.Rollback(store.newReqCtx(), [][]byte{key}, startTs)
	store.c.Assert(err, NotNil)
}

func MustGetNone(key []byte, startTs uint64, store *TestStore) {
	val := MustGet(key, startTs, store)
	store.c.Assert(len(val), Equals, 0)
}

func MustGetVal(key, val []byte, startTs uint64, store *TestStore) {
	getVal := MustGet(key, startTs, store)
	store.c.Assert(val, DeepEquals, getVal)
}

func MustGetErr(key []byte, startTs uint64, store *TestStore) {
	_, err := kvGet(key, startTs, store)
	store.c.Assert(err, NotNil)
}

func kvGet(key []byte, readTs uint64, store *TestStore) ([]byte, error) {
	err := store.MvccStore.CheckKeysLock(readTs, nil, key)
	if err != nil {
		return nil, err
	}
	getVal, err := store.newReqCtx().getDBReader().Get(key, readTs)
	return getVal, err
}

func MustGet(key []byte, readTs uint64, store *TestStore) (val []byte) {
	val, err := kvGet(key, readTs, store)
	store.c.Assert(err, IsNil)
	return val
}

func MustPrewriteLock(pk []byte, key []byte, startTs uint64, store *TestStore) {
	err := store.MvccStore.Prewrite(store.newReqCtx(), &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Lock, key, nil)},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
	})
	store.c.Assert(err, IsNil)
}

func MustPrewriteLockErr(pk []byte, key []byte, startTs uint64, store *TestStore) {
	err := store.MvccStore.Prewrite(store.newReqCtx(), &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Lock, key, nil)},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
	})
	store.c.Assert(err, NotNil)
}

func MustGC(key []byte, safePoint uint64, s *TestStore) {
	s.MvccStore.UpdateSafePoint(safePoint)
}

func MustCleanup(key []byte, startTs, currentTs uint64, store *TestStore) {
	err := store.MvccStore.Cleanup(store.newReqCtx(), key, startTs, currentTs)
	store.c.Assert(err, IsNil)
}

func MustCleanupErr(key []byte, startTs, currentTs uint64, store *TestStore) {
	err := store.MvccStore.Cleanup(store.newReqCtx(), key, startTs, currentTs)
	store.c.Assert(err, NotNil)
}

func MustTxnHeartBeat(pk []byte, startTs, adviceTTL, expectedTTL uint64, store *TestStore) {
	lockTTL, err := store.MvccStore.TxnHeartBeat(store.newReqCtx(), &kvrpcpb.TxnHeartBeatRequest{
		PrimaryLock:   pk,
		StartVersion:  startTs,
		AdviseLockTtl: adviceTTL,
	})
	store.c.Assert(err, IsNil)
	store.c.Assert(lockTTL, Equals, expectedTTL)
}

func MustGetRollback(key []byte, ts uint64, store *TestStore) {
	res := store.MvccStore.checkExtraTxnStatus(store.newReqCtx(), key, ts)
	store.c.Assert(res.isRollback, IsTrue)
}

func (s *testMvccSuite) TestBasicOptimistic(c *C) {
	var err error
	store, err := NewTestStore("basic_optimistic_db", "basic_optimistic_log", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	key1 := []byte("key1")
	val1 := []byte("val1")
	ttl := uint64(200)
	MustPrewriteOptimistic(key1, key1, val1, 1, ttl, 0, store)
	MustCommitKeyPut(key1, val1, 1, 2, store)
	// Read using smaller ts results in nothing
	getVal, _ := store.newReqCtx().getDBReader().Get(key1, 1)
	c.Assert(getVal, IsNil)
}

func (s *testMvccSuite) TestPessimiticTxnTTL(c *C) {
	var err error
	store, err := NewTestStore("pessimisitc_txn_ttl_db", "pessimisitc_txn_ttl_log", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	// Pessimisitc lock key1
	key1 := []byte("key1")
	val1 := []byte("val1")
	startTs := uint64(1)
	lockTTL := uint64(1000)
	_, err = PessimisticLock(key1, key1, startTs, lockTTL, startTs, true, false, store)
	c.Assert(err, IsNil)

	// Prewrite key1 with smaller lock ttl, lock ttl will not be changed
	MustPrewritePessimistic(key1, key1, val1, startTs, lockTTL-500, []bool{true}, startTs, store)
	lock := store.MvccStore.getLock(store.newReqCtx(), key1)
	c.Assert(uint64(lock.TTL), Equals, uint64(1000))

	key2 := []byte("key2")
	val2 := []byte("val2")
	_, err = PessimisticLock(key2, key2, 3, 300, 3, true, false, store)
	c.Assert(err, IsNil)

	// Prewrite key1 with larger lock ttl, lock ttl will be updated
	MustPrewritePessimistic(key2, key2, val2, 3, 2000, []bool{true}, 3, store)
	lock2 := store.MvccStore.getLock(store.newReqCtx(), key2)
	c.Assert(uint64(lock2.TTL), Equals, uint64(2000))
}

func (s *testMvccSuite) TestRollback(c *C) {
	var err error
	store, err := NewTestStore("RollbackData", "RollbackLog", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	key := []byte("tkey")
	val := []byte("value")
	startTs := uint64(1)
	lockTTL := uint64(100)
	// Add a Rollback whose start ts is 1.
	MustPrewriteOptimistic(key, key, val, startTs, lockTTL, 0, store)
	MustRollbackKey(key, startTs, store)

	MustPrewriteOptimistic(key, key, val, startTs+1, lockTTL, 0, store)
	MustRollbackKey(key, startTs+1, store)
	res := store.MvccStore.checkExtraTxnStatus(store.newReqCtx(), key, startTs)
	c.Assert(res.isRollback, IsTrue)

	// Test collapse rollback
	k := []byte("tk")
	v := []byte("v")
	// Add a Rollback whose start ts is 1
	MustPrewritePut(k, k, v, 1, store)
	MustRollbackKey(k, 1, store)
	MustGetRollback(k, 1, store)

	// Add a Rollback whose start ts is 3, the previous Rollback whose
	// start ts is 1 will be collapsed in tikv, but unistore will not
	MustPrewritePut(k, k, v, 2, store)
	MustRollbackKey(k, 2, store)
	MustGetNone(k, 2, store)
	MustGetRollback(k, 2, store)
	MustGetRollback(k, 1, store)
}

func (s *testMvccSuite) TestOverwritePessimisitcLock(c *C) {
	var err error
	store, err := NewTestStore("OverWritePessimisticData", "OverWritePessimisticLog", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	key := []byte("key")
	startTs := uint64(1)
	lockTTL := uint64(100)
	forUpdateTs := uint64(100)
	// pessimistic lock one key
	_, err = PessimisticLock(key, key, startTs, lockTTL, forUpdateTs, true, false, store)
	c.Assert(err, IsNil)
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	c.Assert(lock.ForUpdateTS, Equals, forUpdateTs)

	// pessimistic lock this key again using larger forUpdateTs
	_, err = PessimisticLock(key, key, startTs, lockTTL, forUpdateTs+7, true, false, store)
	c.Assert(err, IsNil)
	lock2 := store.MvccStore.getLock(store.newReqCtx(), key)
	c.Assert(lock2.ForUpdateTS, Equals, forUpdateTs+7)

	// pessimistic lock one key using smaller forUpdateTsTs
	_, err = PessimisticLock(key, key, startTs, lockTTL, forUpdateTs-7, true, false, store)
	c.Assert(err, IsNil)
	lock3 := store.MvccStore.getLock(store.newReqCtx(), key)
	c.Assert(lock3.ForUpdateTS, Equals, forUpdateTs+7)
}

func (s *testMvccSuite) TestCheckTxnStatus(c *C) {
	var err error
	store, err := NewTestStore("CheckTxnStatusDB", "CheckTxnStatusLog", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	var resTTL, resCommitTs uint64
	var action kvrpcpb.Action
	pk := []byte("tpk")
	startTs := uint64(1)
	callerStartTs := uint64(3)
	currentTs := uint64(5)

	// Try to check a not exist thing.
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, callerStartTs, currentTs, true, store)
	c.Assert(resTTL, Equals, uint64(0))
	c.Assert(resCommitTs, Equals, uint64(0))
	c.Assert(action, Equals, kvrpcpb.Action_LockNotExistRollback)
	c.Assert(err, IsNil)

	// Using same startTs, prewrite will fail, since checkTxnStatus has rollbacked the key
	val := []byte("val")
	lockTTL := uint64(100)
	minCommitTs := uint64(20)
	err = PrewriteOptimistic(pk, pk, val, startTs, lockTTL, minCommitTs, false, [][]byte{}, store)
	c.Assert(err, Equals, ErrAlreadyRollback)

	// Prewrite a large txn
	startTs = 2
	MustPrewriteOptimistic(pk, pk, val, startTs, lockTTL, minCommitTs, store)
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, callerStartTs, currentTs, true, store)
	c.Assert(resTTL, Equals, lockTTL)
	c.Assert(resCommitTs, Equals, uint64(0))
	c.Assert(err, IsNil)
	c.Assert(action, Equals, kvrpcpb.Action_MinCommitTSPushed)

	// Update min_commit_ts to current_ts. minCommitTs 20 -> 25
	newCallerTs := uint64(25)
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, newCallerTs, true, store)
	c.Assert(resTTL, Equals, lockTTL)
	c.Assert(resCommitTs, Equals, uint64(0))
	c.Assert(err, IsNil)
	c.Assert(action, Equals, kvrpcpb.Action_MinCommitTSPushed)
	lock := store.MvccStore.getLock(store.newReqCtx(), pk)
	c.Assert(lock.StartTS, Equals, startTs)
	c.Assert(uint64(lock.TTL), Equals, lockTTL)
	c.Assert(lock.MinCommitTS, Equals, newCallerTs+1)

	// When caller_start_ts < lock.min_commit_ts, here 25 < 26, no need to update it.
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, newCallerTs, true, store)
	c.Assert(resTTL, Equals, lockTTL)
	c.Assert(resCommitTs, Equals, uint64(0))
	c.Assert(err, IsNil)
	c.Assert(action, Equals, kvrpcpb.Action_MinCommitTSPushed)
	lock = store.MvccStore.getLock(store.newReqCtx(), pk)
	c.Assert(lock.StartTS, Equals, startTs)
	c.Assert(uint64(lock.TTL), Equals, lockTTL)
	c.Assert(lock.MinCommitTS, Equals, newCallerTs+1)

	// current_ts(25) < lock.min_commit_ts(26) < caller_start_ts(35)
	currentTs = uint64(25)
	newCallerTs = 35
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, currentTs, true, store)
	c.Assert(resTTL, Equals, lockTTL)
	c.Assert(resCommitTs, Equals, uint64(0))
	c.Assert(err, IsNil)
	c.Assert(action, Equals, kvrpcpb.Action_MinCommitTSPushed)
	lock = store.MvccStore.getLock(store.newReqCtx(), pk)
	c.Assert(lock.StartTS, Equals, startTs)
	c.Assert(uint64(lock.TTL), Equals, lockTTL)
	c.Assert(lock.MinCommitTS, Equals, newCallerTs+1) // minCommitTS updated to 36

	// current_ts is max value 40, but no effect since caller_start_ts is smaller than minCommitTs
	currentTs = uint64(40)
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, currentTs, true, store)
	c.Assert(resTTL, Equals, lockTTL)
	c.Assert(resCommitTs, Equals, uint64(0))
	c.Assert(err, IsNil)
	c.Assert(action, Equals, kvrpcpb.Action_MinCommitTSPushed)
	lock = store.MvccStore.getLock(store.newReqCtx(), pk)
	c.Assert(lock.StartTS, Equals, startTs)
	c.Assert(uint64(lock.TTL), Equals, lockTTL)
	c.Assert(lock.MinCommitTS, Equals, newCallerTs+1) // minCommitTS updated to 36

	// commit this key, commitTs(35) smaller than minCommitTs(36)
	commitTs := uint64(35)
	err = store.MvccStore.Commit(store.newReqCtx(), [][]byte{pk}, startTs, commitTs)
	c.Assert(err, NotNil)

	// commit this key, using correct commitTs
	commitTs = uint64(41)
	MustCommitKeyPut(pk, val, startTs, commitTs, store)

	// check committed txn status
	currentTs = uint64(42)
	newCallerTs = uint64(42)
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, currentTs, true, store)
	c.Assert(resTTL, Equals, uint64(0))
	c.Assert(resCommitTs, Equals, uint64(41))
	c.Assert(err, IsNil)
	c.Assert(action, Equals, kvrpcpb.Action_NoAction)
}

func (s *testMvccSuite) TestCheckSecondaryLocksStatus(c *C) {
	var err error
	store, err := NewTestStore("CheckSecondaryLocksStatusDB", "CheckSecondaryLocksStatusLog", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	pk := []byte("pk")
	secondary := []byte("secondary")
	val := []byte("val")

	MustPrewritePut(pk, secondary, val, 1, store)
	MustCommit(secondary, 1, 3, store)
	MustRollbackKey(secondary, 5, store)
	MustPrewritePut(pk, secondary, val, 7, store)
	MustCommit(secondary, 7, 9, store)

	// No lock
	// 9: start_ts = 7
	// 5: rollback
	// 3: start_ts = 1

	// Lock is committed
	locks, commitTS, err := CheckSecondaryLocksStatus([][]byte{secondary}, 1, store)
	c.Assert(err, IsNil)
	c.Assert(len(locks), Equals, 0)
	c.Assert(commitTS, Equals, uint64(3))
	MustGet(secondary, 1, store)

	// Op_Lock lock is committed
	locks, commitTS, err = CheckSecondaryLocksStatus([][]byte{secondary}, 7, store)
	c.Assert(err, IsNil)
	c.Assert(len(locks), Equals, 0)
	c.Assert(commitTS, Equals, uint64(9))
	MustGet(secondary, 7, store)

	// Lock is already rolled back
	locks, commitTS, err = CheckSecondaryLocksStatus([][]byte{secondary}, 5, store)
	c.Assert(err, IsNil)
	c.Assert(len(locks), Equals, 0)
	c.Assert(commitTS, Equals, uint64(0))
	MustGetRollback(secondary, 5, store)

	// No commit info
	locks, commitTS, err = CheckSecondaryLocksStatus([][]byte{secondary}, 6, store)
	c.Assert(err, IsNil)
	c.Assert(len(locks), Equals, 0)
	c.Assert(commitTS, Equals, uint64(0))
	MustGetRollback(secondary, 6, store)

	// If there is a pessimistic lock on the secondary key:
	MustAcquirePessimisticLock(pk, secondary, 11, 11, store)
	// After CheckSecondaryLockStatus, the lock should be rolled back
	locks, commitTS, err = CheckSecondaryLocksStatus([][]byte{secondary}, 11, store)
	c.Assert(err, IsNil)
	c.Assert(len(locks), Equals, 0)
	c.Assert(commitTS, Equals, uint64(0))
	MustGetRollback(secondary, 11, store)

	// If there is an optimistic lock on the secondary key:
	MustPrewritePut(pk, secondary, val, 13, store)
	// After CheckSecondaryLockStatus, the lock should remain and be returned back
	locks, commitTS, err = CheckSecondaryLocksStatus([][]byte{secondary}, 13, store)
	c.Assert(err, IsNil)
	c.Assert(len(locks), Equals, 1)
	c.Assert(commitTS, Equals, uint64(0))
	MustLocked(secondary, false, store)
}

func (s *testMvccSuite) TestMvccGet(c *C) {
	var err error
	store, err := NewTestStore("TestMvccGetBy", "TestMvccGetBy", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	lockTTL := uint64(100)
	pk := []byte("t1_r1")
	pkVal := []byte("pkVal")
	startTs1 := uint64(1)
	commitTs1 := uint64(2)
	// write one record
	MustPrewriteOptimistic(pk, pk, pkVal, startTs1, lockTTL, 0, store)
	MustCommitKeyPut(pk, pkVal, startTs1, commitTs1, store)

	// update this record
	startTs2 := uint64(3)
	commitTs2 := uint64(4)
	newVal := []byte("aba")
	MustPrewriteOptimistic(pk, pk, newVal, startTs2, lockTTL, 0, store)
	MustCommitKeyPut(pk, newVal, startTs2, commitTs2, store)

	// read using mvcc
	var res *kvrpcpb.MvccInfo
	res, err = store.MvccStore.MvccGetByKey(store.newReqCtx(), pk)
	c.Assert(err, IsNil)
	c.Assert(len(res.Writes), Equals, 2)

	// prewrite and then rollback
	// Add a Rollback whose start ts is 5.
	startTs3 := uint64(5)
	rollbackVal := []byte("rollbackVal")
	MustPrewriteOptimistic(pk, pk, rollbackVal, startTs3, lockTTL, 0, store)
	MustRollbackKey(pk, startTs3, store)

	// put empty value
	startTs4 := uint64(7)
	commitTs4 := uint64(8)
	emptyVal := []byte("")
	MustPrewriteOptimistic(pk, pk, emptyVal, startTs4, lockTTL, 0, store)
	MustCommitKeyPut(pk, emptyVal, startTs4, commitTs4, store)

	// read using mvcc
	res, err = store.MvccStore.MvccGetByKey(store.newReqCtx(), pk)
	c.Assert(err, IsNil)
	c.Assert(len(res.Writes), Equals, 4)

	c.Assert(res.Writes[3].StartTs, Equals, startTs1)
	c.Assert(res.Writes[3].CommitTs, Equals, commitTs1)
	c.Assert(bytes.Compare(res.Writes[3].ShortValue, pkVal), Equals, 0)

	c.Assert(res.Writes[2].StartTs, Equals, startTs2)
	c.Assert(res.Writes[2].CommitTs, Equals, commitTs2)
	c.Assert(bytes.Compare(res.Writes[2].ShortValue, newVal), Equals, 0)

	c.Assert(res.Writes[1].StartTs, Equals, startTs3)
	c.Assert(res.Writes[1].CommitTs, Equals, startTs3)
	c.Assert(bytes.Compare(res.Writes[1].ShortValue, emptyVal), Equals, 0)

	c.Assert(res.Writes[0].StartTs, Equals, startTs4)
	c.Assert(res.Writes[0].CommitTs, Equals, commitTs4)
	c.Assert(bytes.Compare(res.Writes[0].ShortValue, emptyVal), Equals, 0)

	// read using MvccGetByStartTs using key current ts
	res2, resKey, err := store.MvccStore.MvccGetByStartTs(store.newReqCtx(), startTs4)
	c.Assert(err, IsNil)
	c.Assert(res2, NotNil)
	c.Assert(bytes.Compare(resKey, pk), Equals, 0)
	c.Assert(len(res2.Writes), Equals, 4)

	c.Assert(res2.Writes[3].StartTs, Equals, startTs1)
	c.Assert(res2.Writes[3].CommitTs, Equals, commitTs1)
	c.Assert(bytes.Compare(res2.Writes[3].ShortValue, pkVal), Equals, 0)

	c.Assert(res2.Writes[2].StartTs, Equals, startTs2)
	c.Assert(res2.Writes[2].CommitTs, Equals, commitTs2)
	c.Assert(bytes.Compare(res2.Writes[2].ShortValue, newVal), Equals, 0)

	c.Assert(res2.Writes[1].StartTs, Equals, startTs3)
	c.Assert(res2.Writes[1].CommitTs, Equals, startTs3)
	c.Assert(res2.Writes[1].Type, Equals, kvrpcpb.Op_Rollback)
	c.Assert(bytes.Compare(res2.Writes[1].ShortValue, emptyVal), Equals, 0)

	c.Assert(res2.Writes[0].StartTs, Equals, startTs4)
	c.Assert(res2.Writes[0].CommitTs, Equals, commitTs4)
	c.Assert(res2.Writes[0].Type, Equals, kvrpcpb.Op_Del)
	c.Assert(bytes.Compare(res2.Writes[0].ShortValue, emptyVal), Equals, 0)

	// read using MvccGetByStartTs using non exists startTs
	startTsNonExists := uint64(1000)
	res3, resKey, err := store.MvccStore.MvccGetByStartTs(store.newReqCtx(), startTsNonExists)
	c.Assert(err, IsNil)
	c.Assert(resKey, IsNil)
	c.Assert(res3, IsNil)

	// read using old startTs
	res4, resKey, err := store.MvccStore.MvccGetByStartTs(store.newReqCtx(), startTs2)
	c.Assert(err, IsNil)
	c.Assert(res4, NotNil)
	c.Assert(bytes.Compare(resKey, pk), Equals, 0)
	c.Assert(len(res4.Writes), Equals, 4)
	c.Assert(res4.Writes[1].StartTs, Equals, startTs3)
	c.Assert(res4.Writes[1].CommitTs, Equals, startTs3)
	c.Assert(bytes.Compare(res4.Writes[1].ShortValue, emptyVal), Equals, 0)

	res4, resKey, err = store.MvccStore.MvccGetByStartTs(store.newReqCtxWithKeys([]byte("t1_r1"), []byte("t1_r2")), startTs2)
	c.Assert(err, IsNil)
	c.Assert(res4, NotNil)
	c.Assert(bytes.Compare(resKey, pk), Equals, 0)
	c.Assert(len(res4.Writes), Equals, 4)
	c.Assert(res4.Writes[1].StartTs, Equals, startTs3)
	c.Assert(res4.Writes[1].CommitTs, Equals, startTs3)
	c.Assert(bytes.Compare(res4.Writes[1].ShortValue, emptyVal), Equals, 0)
}

func (s *testMvccSuite) TestPrimaryKeyOpLock(c *C) {
	store, err := NewTestStore("PrimaryKeyOpLock", "PrimaryKeyOpLock", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	pk := func() []byte { return []byte("tpk") }
	val2 := []byte("val2")
	// prewrite 100 Op_Lock
	MustPrewriteLock(pk(), pk(), 100, store)
	err = store.MvccStore.Commit(store.newReqCtx(), [][]byte{pk()}, 100, 101)
	c.Assert(err, IsNil)
	_, commitTS, _, _ := CheckTxnStatus(pk(), 100, 110, 110, false, store)
	c.Assert(commitTS, Equals, uint64(101))

	// prewrite 110 Op_Put
	err = store.MvccStore.Prewrite(store.newReqCtx(), &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Put, pk(), val2)},
		PrimaryLock:  pk(),
		StartVersion: 110,
		LockTtl:      100,
	})
	c.Assert(err, IsNil)
	err = store.MvccStore.Commit(store.newReqCtx(), [][]byte{pk()}, 110, 111)
	c.Assert(err, IsNil)

	// prewrite 120 Op_Lock
	MustPrewriteLock(pk(), pk(), 120, store)
	err = store.MvccStore.Commit(store.newReqCtx(), [][]byte{pk()}, 120, 121)
	c.Assert(err, IsNil)

	// the older commit record should exist
	_, commitTS, _, _ = CheckTxnStatus(pk(), 120, 130, 130, false, store)
	c.Assert(commitTS, Equals, uint64(121))
	_, commitTS, _, _ = CheckTxnStatus(pk(), 110, 130, 130, false, store)
	c.Assert(commitTS, Equals, uint64(111))
	_, commitTS, _, _ = CheckTxnStatus(pk(), 100, 130, 130, false, store)
	c.Assert(commitTS, Equals, uint64(101))

	getVal, err := store.newReqCtx().getDBReader().Get(pk(), 90)
	c.Assert(err, IsNil)
	c.Assert(getVal, IsNil)
	getVal, err = store.newReqCtx().getDBReader().Get(pk(), 110)
	c.Assert(err, IsNil)
	c.Assert(getVal, IsNil)
	getVal, err = store.newReqCtx().getDBReader().Get(pk(), 111)
	c.Assert(err, IsNil)
	c.Assert(getVal, DeepEquals, val2)
	getVal, err = store.newReqCtx().getDBReader().Get(pk(), 130)
	c.Assert(err, IsNil)
	c.Assert(getVal, DeepEquals, val2) // Op_Lock value should not be recorded and returned
}

func (s *testMvccSuite) TestMvccTxnRead(c *C) {
	store, err := NewTestStore("TestMvccTxnRead", "TestMvccTxnRead", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	// nothing at start
	k1 := []byte("tk1")
	v1 := []byte("v1")
	MustGetNone(k1, 1, store)

	// prewrite and rollback
	MustPrewriteOptimistic(k1, k1, v1, 2, 10, 2, store)
	MustRollbackKey(k1, 2, store)

	// read results in nothing
	MustGetNone(k1, 1, store)

	MustPrewriteLock(k1, k1, 3, store)
	MustCommit(k1, 3, 4, store)
	// lock should left nothing
	MustGetNone(k1, 5, store)

	v := []byte("v")
	k2 := []byte("tk2")
	v2 := []byte("v2")
	MustPrewriteOptimistic(k1, k1, v, 5, 10, 5, store)
	MustPrewriteOptimistic(k1, k2, v2, 5, 10, 5, store)
	// should not be affected by later locks
	MustGetNone(k1, 4, store)
	// should read pending locks
	MustGetErr(k1, 7, store)
	// should ignore the primary lock and get none when reading the latest record
	MustGetNone(k1, maxTs, store)
	// should read secondary locks even when reading the latest record
	MustGetErr(k2, maxTs, store)
	MustCommit(k1, 5, 10, store)
	MustCommit(k2, 5, 10, store)
	MustGetNone(k1, 3, store)
	// should not read with ts < commit_ts
	MustGetNone(k1, 7, store)
	// should read with ts > commit_ts
	MustGetVal(k1, v, 13, store)
	// should read the latest record if `ts == u64::max_value()`
	MustGetVal(k2, v2, uint64(math.MaxUint64), store)

	MustPrewriteDelete(k1, k1, 15, store)
	// should ignore the lock and get previous record when reading the latest record
	MustGetVal(k1, v, maxTs, store)
	MustCommit(k1, 15, 20, store)
	MustGetNone(k1, 3, store)
	MustGetNone(k1, 7, store)
	MustGetVal(k1, v, 13, store)
	MustGetVal(k1, v, 17, store)
	MustGetNone(k1, 23, store)

	// intersecting timestamps with pessimistic txn
	// T1: start_ts = 25, commit_ts = 27
	// T2: start_ts = 23, commit_ts = 31
	MustPrewritePut(k1, k1, v, 25, store)
	MustCommit(k1, 25, 27, store)
	MustAcquirePessimisticLock(k1, k1, 23, 29, store)
	MustGetVal(k1, v, 30, store)
	MustPessimisitcPrewriteDelete(k1, k1, 23, 29, store)
	MustGetErr(k1, 30, store)
	// should read the latest record when `ts == u64::max_value()`
	// even if lock.start_ts(23) < latest write.commit_ts(27)
	MustGetVal(k1, v, maxTs, store)
	MustCommit(k1, 23, 31, store)
	MustGetVal(k1, v, 30, store)
	MustGetNone(k1, 32, store)
}

func (s *testMvccSuite) TestTxnPrewrite(c *C) {
	store, err := NewTestStore("TestTxnPrewrite", "TestTxnPrewrite", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	// nothing at start
	k := []byte("tk")
	v := []byte("v")
	MustPrewritePut(k, k, v, 5, store)
	// Key is locked.
	MustLocked(k, false, store)
	// Retry prewrite
	MustPrewritePut(k, k, v, 5, store)
	// Conflict
	MustPrewritePutLockErr(k, k, v, 6, store)

	MustCommit(k, 5, 10, store)
	MustGetVal(k, v, 10, store)
	// Delayed prewrite request after committing should do nothing
	MustPrewritePutErr(k, k, v, 5, store)
	MustUnLocked(k, store)
	// Write conflict
	MustPrewritePutErr(k, k, v, 6, store)
	MustUnLocked(k, store)
	// Not conflict
	MustPrewriteLock(k, k, 12, store)
	MustLocked(k, false, store)
	MustRollbackKey(k, 12, store)
	// Cannot retry Prewrite after rollback
	MustPrewritePutErr(k, k, nil, 12, store)
	// Can prewrite after rollback
	MustPrewriteDelete(k, k, 13, store)
	MustRollbackKey(k, 13, store)
	MustUnLocked(k, store)
}

func (s *testMvccSuite) TestPrewriteInsert(c *C) {
	store, err := NewTestStore("TestPrewriteInsert", "TestPrewriteInsert", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	// nothing at start
	k1 := []byte("tk1")
	v1 := []byte("v1")
	v2 := []byte("v2")
	v3 := []byte("v3")

	MustPrewritePut(k1, k1, v1, 1, store)
	MustCommit(k1, 1, 2, store)
	// "k1" already exist, returns AlreadyExist error
	MustPrewriteInsertAlreadyExists(k1, k1, v2, 3, store)
	// Delete "k1"
	MustPrewriteDelete(k1, k1, 4, store)
	MustCommit(k1, 4, 5, store)
	// After delete "k1", insert returns ok
	MustPrewriteInsert(k1, k1, v2, 6, store)
	MustCommit(k1, 6, 7, store)
	// Rollback
	MustPrewritePut(k1, k1, v3, 8, store)
	MustRollbackKey(k1, 8, store)
	MustPrewriteInsertAlreadyExists(k1, k1, v2, 9, store)
	// Delete "k1" again
	MustPrewriteDelete(k1, k1, 10, store)
	MustCommit(k1, 10, 11, store)
	// Rollback again
	MustPrewritePut(k1, k1, v3, 12, store)
	MustRollbackKey(k1, 12, store)
	// After delete "k1", insert returns ok
	MustPrewriteInsert(k1, k1, v2, 13, store)
	MustCommit(k1, 13, 14, store)
	MustGetVal(k1, v2, 15, store)
}

func (s *testMvccSuite) TestRollbackKey(c *C) {
	store, err := NewTestStore("TestRollbackKey", "TestRollbackKey", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	k := []byte("tk")
	v := []byte("v")
	MustPrewritePut(k, k, v, 5, store)
	MustCommit(k, 5, 10, store)

	// Lock
	MustPrewriteLock(k, k, 15, store)
	MustLocked(k, false, store)

	// Rollback lock
	MustRollbackKey(k, 15, store)
	MustUnLocked(k, store)
	MustGetVal(k, v, 16, store)

	// Rollback delete
	MustPrewriteDelete(k, k, 17, store)
	MustLocked(k, false, store)
	MustRollbackKey(k, 17, store)
	MustGetVal(k, v, 18, store)
}

func (s *testMvccSuite) TestCleanup(c *C) {
	store, err := NewTestStore("TestCleanup", "TestCleanup", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	k := []byte("tk")
	v := []byte("v")
	// Cleanup's logic is mostly similar to rollback, except the TTL check. Tests that
	// not related to TTL check should be covered by other test cases
	MustPrewritePut(k, k, v, 10, store)
	MustLocked(k, false, store)
	MustTxnHeartBeat(k, 10, 100, 100, store)
	// Check the last txn_heart_beat has set the lock's TTL to 100
	MustTxnHeartBeat(k, 10, 90, 100, store)
	// TTL not expired. Do nothing but returns an error
	MustCleanupErr(k, 10, 20, store)
	MustLocked(k, false, store)
	MustCleanup(k, 11, 20, store)
	// TTL expired. The lock should be removed
	MustCleanup(k, 10, 120<<18, store)
	MustUnLocked(k, store)
}

func (s *testMvccSuite) TestCommit(c *C) {
	store, err := NewTestStore("TestCommit", "TestCommit", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	k := []byte("tk")
	v := []byte("v")
	// Not prewrite yet
	MustCommitErr(k, 1, 2, store)
	MustPrewritePut(k, k, v, 5, store)
	// Start_ts not match
	MustCommitErr(k, 4, 5, store)
	MustRollbackKey(k, 5, store)
	// Commit after rollback
	MustCommitErr(k, 5, 6, store)

	k1 := []byte("tk1")
	v1 := []byte("v")
	k2 := []byte("tk2")
	k3 := []byte("tk3")

	MustPrewritePut(k1, k1, v1, 10, store)
	MustPrewriteLock(k1, k2, 10, store)
	MustPrewriteDelete(k1, k3, 10, store)
	MustLocked(k1, false, store)
	MustLocked(k2, false, store)
	MustLocked(k3, false, store)
	MustCommit(k1, 10, 15, store)
	MustCommit(k2, 10, 15, store)
	MustCommit(k3, 10, 15, store)
	MustGetVal(k1, v1, 16, store)
	MustGetNone(k2, 16, store)
	MustGetNone(k3, 16, store)
	// Commit again has no effect
	MustCommit(k1, 10, 15, store)
	// Secondary Op_Lock keys could not be committed more than once on unistore
	// MustCommit(k2, 10, 15, store)
	MustCommit(k3, 10, 15, store)
	MustGetVal(k1, v1, 16, store)
	MustGetNone(k2, 16, store)
	MustGetNone(k3, 16, store)

	// The rollback should be failed since the transaction was committed before
	MustRollbackErr(k1, 10, store)
	MustGetVal(k1, v1, 17, store)

	// Rollback before prewrite
	kr := []byte("tkr")
	MustRollbackKey(kr, 5, store)
	MustPrewriteLockErr(kr, kr, 5, store)
}

func (s *testMvccSuite) TestMinCommitTs(c *C) {
	store, err := NewTestStore("TestMinCommitTs", "TestMinCommitTs", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	k := []byte("tk")
	v := []byte("v")

	MustPrewriteOptimistic(k, k, v, 10, 100, 11, store)
	MustCheckTxnStatus(k, 10, 20, 20, false, 100, 0,
		kvrpcpb.Action_MinCommitTSPushed, store)
	// The the min_commit_ts should be ts(20, 1)
	MustCommitErr(k, 10, 15, store)
	MustCommitErr(k, 10, 20, store)
	MustCommit(k, 10, 21, store)

	MustPrewriteOptimistic(k, k, v, 30, 100, 30, store)
	MustCheckTxnStatus(k, 30, 40, 40, false, 100, 0,
		kvrpcpb.Action_MinCommitTSPushed, store)
	MustCommit(k, 30, 50, store)
}

func (s *testMvccSuite) TestGC(c *C) {
	c.Skip("GC work is hand over to badger.")
	store, err := NewTestStore("TestGC", "TestGC", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	k := []byte("tk")
	v1 := []byte("v1")
	v2 := []byte("v2")
	v3 := []byte("v3")
	v4 := []byte("v4")

	MustPrewritePut(k, k, v1, 5, store)
	MustCommit(k, 5, 10, store)
	MustPrewritePut(k, k, v2, 15, store)
	MustCommit(k, 15, 20, store)
	MustPrewriteDelete(k, k, 25, store)
	MustCommit(k, 25, 30, store)
	MustPrewritePut(k, k, v3, 35, store)
	MustCommit(k, 35, 40, store)
	MustPrewriteLock(k, k, 45, store)
	MustCommit(k, 45, 50, store)
	MustPrewritePut(k, k, v4, 55, store)
	MustRollbackKey(k, 55, store)

	// Transactions:
	// startTS commitTS Command
	// --
	// 55      -        PUT "x55" (Rollback)
	// 45      50       LOCK
	// 35      40       PUT "x35"
	// 25      30       DELETE
	// 15      20       PUT "x15"
	//  5      10       PUT "x5"

	// CF data layout:
	// ts CFDefault   CFWrite
	// --
	// 55             Rollback(PUT,50)
	// 50             Commit(LOCK,45)
	// 45
	// 40             Commit(PUT,35)
	// 35   x35
	// 30             Commit(Delete,25)
	// 25
	// 20             Commit(PUT,15)
	// 15   x15
	// 10             Commit(PUT,5)
	// 5    x5
	MustGC(k, 12, store)
	MustGetVal(k, v1, 12, store)
	MustGC(k, 22, store)
	MustGetVal(k, v2, 22, store)
	MustGetNone(k, 12, store)
	MustGC(k, 32, store)
	MustGetNone(k, 22, store)
	MustGetNone(k, 35, store)
	MustGC(k, 60, store)
	MustGetVal(k, v3, 62, store)
}

func (s *testMvccSuite) TestPessimisticLock(c *C) {
	store, err := NewTestStore("TestPessimisticLock", "TestPessimisticLock", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	k := []byte("tk")
	v := []byte("v")
	// Normal
	MustAcquirePessimisticLock(k, k, 1, 1, store)
	MustPessimisticLocked(k, 1, 1, store)
	MustPrewritePessimistic(k, k, v, 1, 100, []bool{true}, 1, store)
	MustLocked(k, true, store)
	MustCommit(k, 1, 2, store)
	MustUnLocked(k, store)

	// Lock conflict
	MustPrewritePut(k, k, v, 3, store)
	MustAcquirePessimisticLockErr(k, k, 4, 4, store)
	MustCleanup(k, 3, 0, store)
	MustUnLocked(k, store)
	MustAcquirePessimisticLock(k, k, 5, 5, store)
	MustPrewriteLockErr(k, k, 6, store)
	MustCleanup(k, 5, 0, store)
	MustUnLocked(k, store)

	// Data conflict
	MustPrewritePut(k, k, v, 7, store)
	MustCommit(k, 7, 9, store)
	MustUnLocked(k, store)
	MustPrewriteLockErr(k, k, 8, store)
	MustAcquirePessimisticLockErr(k, k, 8, 8, store)
	MustAcquirePessimisticLock(k, k, 8, 9, store)
	MustPrewritePessimisticPut(k, k, v, 8, 8, store)
	MustCommit(k, 8, 10, store)
	MustUnLocked(k, store)

	// Rollback
	MustAcquirePessimisticLock(k, k, 11, 11, store)
	MustPessimisticLocked(k, 11, 11, store)
	MustCleanup(k, 11, 0, store)
	MustAcquirePessimisticLockErr(k, k, 11, 11, store)
	MustPrewritePessimisticPutErr(k, k, v, 11, 11, store)
	MustUnLocked(k, store)

	MustAcquirePessimisticLock(k, k, 12, 12, store)
	MustPrewritePessimisticPut(k, k, v, 12, 12, store)
	MustLocked(k, true, store)
	MustCleanup(k, 12, 0, store)
	MustAcquirePessimisticLockErr(k, k, 12, 12, store)
	MustPrewritePessimisticPutErr(k, k, v, 12, 12, store)
	MustPrewriteLockErr(k, k, 12, store)
	MustUnLocked(k, store)

	// Duplicated
	v3 := []byte("v3")
	MustAcquirePessimisticLock(k, k, 13, 13, store)
	MustPessimisticLocked(k, 13, 13, store)
	MustAcquirePessimisticLock(k, k, 13, 13, store)
	MustPessimisticLocked(k, 13, 13, store)
	MustPrewritePessimisticPut(k, k, v3, 13, 13, store)
	MustLocked(k, true, store)
	MustCommit(k, 13, 14, store)
	MustUnLocked(k, store)
	MustCommit(k, 13, 14, store)
	MustUnLocked(k, store)
	MustGetVal(k, v3, 15, store)

	// Pessimistic lock doesn't block reads
	MustAcquirePessimisticLock(k, k, 15, 15, store)
	MustPessimisticLocked(k, 15, 15, store)
	MustGetVal(k, v3, 16, store)
	MustPrewritePessimisticDelete(k, k, 15, 15, store)
	MustGetErr(k, 16, store)
	MustCommit(k, 15, 17, store)

	// Rollback
	MustAcquirePessimisticLock(k, k, 18, 18, store)
	MustRollbackKey(k, 18, store)
	MustPessimisticRollback(k, 18, 18, store)
	MustUnLocked(k, store)
	MustPrewritePut(k, k, v, 19, store)
	MustCommit(k, 19, 20, store)
	// Here unistore is different from tikv, unistore will not store the pessimistic rollback into db
	// so the concurrent pessimistic lock acquire will succeed
	// MustAcquirePessimisticLockErr(k, k, 18, 21, store)
	MustUnLocked(k, store)

	// Prewrite non-exist pessimistic lock
	MustPrewritePessimisticPutErr(k, k, v, 22, 22, store)

	// LockTypeNotMatch
	MustPrewritePut(k, k, v, 23, store)
	MustLocked(k, false, store)
	MustAcquirePessimisticLockErr(k, k, 23, 23, store)
	MustCleanup(k, 23, 0, store)
	MustAcquirePessimisticLock(k, k, 24, 24, store)
	MustPessimisticLocked(k, 24, 24, store)
	MustCommit(k, 24, 25, store)

	// Acquire lock on a prewritten key should fail
	MustAcquirePessimisticLock(k, k, 26, 26, store)
	MustPessimisticLocked(k, 26, 26, store)
	MustPrewritePessimisticDelete(k, k, 26, 26, store)
	MustLocked(k, true, store)
	MustAcquirePessimisticLockErr(k, k, 26, 26, store)
	MustLocked(k, true, store)

	// Acquire lock on a committed key should fail
	MustCommit(k, 26, 27, store)
	MustUnLocked(k, store)
	MustGetNone(k, 28, store)
	MustAcquirePessimisticLockErr(k, k, 26, 26, store)
	MustUnLocked(k, store)
	MustGetNone(k, 28, store)
	// Pessimistic prewrite on a committed key should fail
	MustPrewritePessimisticPutErr(k, k, v, 26, 26, store)
	MustUnLocked(k, store)
	MustGetNone(k, 28, store)
	// Currently we cannot avoid this TODO
	MustAcquirePessimisticLock(k, k, 26, 29, store)
	MustPessimisticRollback(k, 26, 29, store)
	MustUnLocked(k, store)

	// Rollback collapsed
	MustRollbackKey(k, 32, store)
	MustRollbackKey(k, 33, store)
	MustAcquirePessimisticLockErr(k, k, 32, 32, store)
	MustAcquirePessimisticLockErr(k, k, 32, 34, store)
	MustUnLocked(k, store)

	// Acquire lock when there is lock with different for_update_ts
	MustAcquirePessimisticLock(k, k, 35, 36, store)
	MustPessimisticLocked(k, 35, 36, store)
	MustAcquirePessimisticLock(k, k, 35, 35, store)
	MustPessimisticLocked(k, 35, 36, store)
	MustAcquirePessimisticLock(k, k, 35, 37, store)
	MustPessimisticLocked(k, 35, 37, store)

	// Cannot prewrite when there is another transaction's pessimistic lock
	v = []byte("vvv")
	MustPrewritePessimisticPutErr(k, k, v, 36, 36, store)
	MustPrewritePessimisticPutErr(k, k, v, 36, 38, store)
	MustPessimisticLocked(k, 35, 37, store)
	// Cannot prewrite when there is another transaction's non-pessimistic lock
	MustPrewritePessimisticPut(k, k, v, 35, 37, store)
	MustLocked(k, true, store)
	v1 := []byte("v1")
	MustPrewritePessimisticPutErr(k, k, v1, 36, 38, store)
	MustLocked(k, true, store)

	// Commit pessimistic transaction's key but with smaller commit_ts than for_update_ts
	// Currently not checked, so in this case it will actually be successfully committed
	MustCommit(k, 35, 36, store)
	MustUnLocked(k, store)
	MustGetVal(k, v, 37, store)

	// Prewrite meets pessimistic lock on a non-pessimistic key
	// Currently not checked, so prewrite will success, and commit pessimistic lock will success
	MustAcquirePessimisticLock(k, k, 40, 40, store)
	MustLocked(k, true, store)
	store.c.Assert(PrewriteOptimistic(k, k, v, 40, lockTTL, 40, false, [][]byte{}, store), IsNil)
	MustLocked(k, true, store)
	MustCommit(k, 40, 41, store)
	MustUnLocked(k, store)
}

func (s *testMvccSuite) TestResolveCommit(c *C) {
	store, err := NewTestStore("TestRedundantCommit", "TestRedundantCommit", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	pk := []byte("tpk")
	v := []byte("v")
	sk := []byte("tsk")

	// Lock and prewrite keys
	MustAcquirePessimisticLock(pk, pk, 1, 1, store)
	MustAcquirePessimisticLock(pk, sk, 1, 1, store)
	MustPrewritePessimistic(pk, pk, v, 1, 100, []bool{true}, 1, store)
	MustPrewritePessimistic(pk, sk, v, 1, 100, []bool{true}, 1, store)

	// Resolve secondary key
	MustCommit(pk, 1, 2, store)
	err = store.MvccStore.ResolveLock(store.newReqCtx(), [][]byte{sk}, 2, 3)
	c.Assert(err, IsNil)
	skLock := store.MvccStore.getLock(store.newReqCtx(), sk)
	c.Assert(skLock, NotNil)
	err = store.MvccStore.ResolveLock(store.newReqCtx(), [][]byte{sk}, 1, 2)
	c.Assert(err, IsNil)

	// Commit secondary key, not reporting lock not found
	MustCommit(sk, 1, 2, store)

	// Commit secondary key, not reporting error replaced
	k2 := []byte("tk2")
	v2 := []byte("v2")
	MustAcquirePessimisticLock(k2, k2, 3, 3, store)
	MustCommit(sk, 1, 2, store)
	MustPrewritePessimistic(k2, k2, v2, 3, 100, []bool{true}, 3, store)
	MustCommit(k2, 3, 4, store)

	// The error path
	kvTxn := store.MvccStore.db.NewTransaction(true)
	e := &badger.Entry{
		Key: y.KeyWithTs(sk, 3),
	}
	e.SetDelete()
	err = kvTxn.SetEntry(e)
	c.Assert(err, IsNil)
	err = kvTxn.Commit()
	c.Assert(err, IsNil)
	MustCommitErr(sk, 1, 3, store)
	MustAcquirePessimisticLock(sk, sk, 5, 5, store)
	MustCommitErr(sk, 1, 3, store)
}

func MustLoad(startTS, commitTS uint64, store *TestStore, pairs ...string) {
	var keys = make([][]byte, 0, len(pairs))
	var vals = make([][]byte, 0, len(pairs))
	for _, pair := range pairs {
		strs := strings.Split(pair, ":")
		keys = append(keys, []byte(strs[0]))
		vals = append(vals, []byte(strs[1]))
	}
	for i := 0; i < len(keys); i++ {
		MustPrewritePut(keys[0], keys[i], vals[i], startTS, store)
	}
	for i := 0; i < len(keys); i++ {
		MustCommit(keys[i], startTS, commitTS, store)
	}
}

func (s *testMvccSuite) TestBatchGet(c *C) {
	store, err := NewTestStore("TestBatchGet", "TestBatchGet", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)
	MustLoad(100, 101, store, "ta:1", "tb:2", "tc:3")
	MustPrewritePut([]byte("ta"), []byte("ta"), []byte("0"), 103, store)
	keys := [][]byte{[]byte("ta"), []byte("tb"), []byte("tc")}
	pairs := store.MvccStore.BatchGet(store.newReqCtx(), keys, 104)
	c.Assert(len(pairs), Equals, 3)
	c.Assert(pairs[0].Error, NotNil)
	c.Assert(string(pairs[1].Value), Equals, "2")
	c.Assert(string(pairs[2].Value), Equals, "3")
}

func (s *testMvccSuite) TestCommitPessimisticLock(c *C) {
	store, err := NewTestStore("TestCommitPessimistic", "TestCommitPessimistic", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)
	k := []byte("ta")
	MustAcquirePessimisticLock(k, k, 10, 10, store)
	MustCommitErr(k, 20, 30, store)
	MustCommit(k, 10, 20, store)
	MustGet(k, 30, store)
}

func (s *testMvccSuite) TestOpCheckNotExist(c *C) {
	store, err := NewTestStore("TestOpCheckNotExist", "TestOpCheckNotExist", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	k := []byte("ta")
	v := []byte("v")
	MustPrewritePut(k, k, v, 1, store)
	MustCommit(k, 1, 2, store)
	MustPrewriteOpCheckExistAlreadyExist(k, k, 3, store)
	MustPrewriteDelete(k, k, 4, store)
	MustCommit(k, 4, 5, store)
	MustPrewriteOpCheckExistOk(k, k, 6, store)
	MustPrewritePut(k, k, v, 7, store)
	MustRollbackKey(k, 7, store)
	MustPrewriteOpCheckExistOk(k, k, 8, store)
}

func (s *testMvccSuite) TestPessimisticLockForce(c *C) {
	store, err := NewTestStore("TestPessimisticLockForce", "TestPessimisticLockForce", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	k := []byte("ta")
	v := []byte("v")
	v2 := []byte("v2")
	MustPrewritePut(k, k, v, 5, store)
	MustCommit(k, 5, 10, store)
	MustAcquirePessimisticLockForce(k, k, 1, 1, store)
	MustLocked(k, true, store)
	MustPrewritePessimisticPut(k, k, v2, 1, 10, store)
	MustCommit(k, 1, 11, store)
	MustUnLocked(k, store)
	MustGetVal(k, v2, 13, store)
}

func (s *testMvccSuite) TestScanSampleStep(c *C) {
	store, err := NewTestStore("TestScanSampleStep", "TestScanSampleStep", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)
	for i := 0; i < 1000; i++ {
		k := genScanSampleStepKey(i)
		MustPrewritePut(k, k, k, 1, store)
		MustCommit(k, 1, 2, store)
	}
	sampleStep := 10
	scanReq := &kvrpcpb.ScanRequest{
		StartKey:   genScanSampleStepKey(100),
		EndKey:     genScanSampleStepKey(900),
		Limit:      100,
		Version:    2,
		SampleStep: uint32(sampleStep),
	}
	pairs := store.MvccStore.Scan(store.newReqCtx(), scanReq)
	c.Assert(len(pairs), Equals, 80)
	for i, pair := range pairs {
		c.Assert(genScanSampleStepKey(100+i*sampleStep), BytesEquals, pair.Key)
	}
	scanReq.Limit = 20
	pairs = store.MvccStore.Scan(store.newReqCtx(), scanReq)
	c.Assert(len(pairs), Equals, 20)
	for i, pair := range pairs {
		c.Assert(genScanSampleStepKey(100+i*sampleStep), BytesEquals, pair.Key)
	}
}

func genScanSampleStepKey(i int) []byte {
	return []byte(fmt.Sprintf("t%0.4d", i))
}

func (s *testMvccSuite) TestAsyncCommitPrewrite(c *C) {
	store, err := NewTestStore("TestAsyncCommitPrewrite", "TestAsyncCommitPrewrite", c)
	c.Assert(err, IsNil)
	defer CleanTestStore(store)

	pk := []byte("tpk")
	pkVal := []byte("tpkVal")
	secKey1 := []byte("tSecKey1")
	secVal1 := []byte("secVal1")
	secKey2 := []byte("tSecKey2")
	secVal2 := []byte("secVal2")

	MustPrewriteOptimisticAsyncCommit(pk, pk, pkVal, 1, 100, 0, [][]byte{secKey1, secKey2}, store)
	MustPrewriteOptimisticAsyncCommit(pk, secKey1, secVal1, 1, 100, 0, [][]byte{}, store)
	MustPrewriteOptimisticAsyncCommit(pk, secKey2, secVal2, 1, 100, 0, [][]byte{}, store)
	pkLock := store.MvccStore.getLock(store.newReqCtx(), pk)
	store.c.Assert(pkLock.LockHdr.SecondaryNum, Equals, uint32(2))
	store.c.Assert(bytes.Compare(pkLock.Secondaries[0], secKey1), Equals, 0)
	store.c.Assert(bytes.Compare(pkLock.Secondaries[1], secKey2), Equals, 0)
	store.c.Assert(pkLock.UseAsyncCommit, Equals, true)
	store.c.Assert(pkLock.MinCommitTS, Greater, uint64(0))

	secLock := store.MvccStore.getLock(store.newReqCtx(), secKey2)
	store.c.Assert(secLock.LockHdr.SecondaryNum, Equals, uint32(0))
	store.c.Assert(len(secLock.Secondaries), Equals, 0)
	store.c.Assert(secLock.UseAsyncCommit, Equals, true)
	store.c.Assert(secLock.MinCommitTS, Greater, uint64(0))
	store.c.Assert(bytes.Compare(secLock.Value, secVal2), Equals, 0)
}
