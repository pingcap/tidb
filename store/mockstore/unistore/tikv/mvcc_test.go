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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/store/mockstore/unistore/config"
	"github.com/pingcap/tidb/store/mockstore/unistore/lockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/kverrors"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/store/mockstore/unistore/util/lockwaiter"
	"github.com/stretchr/testify/require"
)

var maxTs = uint64(math.MaxUint64)
var lockTTL = uint64(50)

type TestStore struct {
	MvccStore *MVCCStore
	Svr       *Server
	DBPath    string
	LogPath   string
	t         *testing.T
}

func (ts *TestStore) newReqCtx() *requestCtx {
	return ts.newReqCtxWithKeys([]byte{'t'}, []byte{'u'})
}

func (ts *TestStore) newReqCtxWithKeys(rawStartKey, rawEndKey []byte) *requestCtx {
	epoch := &metapb.RegionEpoch{ConfVer: 1, Version: 1}
	peer := &metapb.Peer{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter}
	return &requestCtx{
		regCtx: &regionCtx{
			meta: &metapb.Region{
				Id:          1,
				RegionEpoch: epoch,
				Peers:       []*metapb.Peer{peer},
			},
			latches:     newLatches(),
			rawStartKey: rawStartKey,
			rawEndKey:   rawEndKey,
		},
		rpcCtx: &kvrpcpb.Context{
			RegionId:    1,
			RegionEpoch: epoch,
			Peer:        peer,
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

func NewTestStore(dbPrefix string, logPrefix string, t *testing.T) (*TestStore, func()) {
	dbPath := t.TempDir()
	LogPath := t.TempDir()
	safePoint := &SafePoint{}
	db, err := CreateTestDB(dbPath, LogPath)
	require.NoError(t, err)
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
	require.NoError(t, err)
	err = os.MkdirAll(raftPath, os.ModePerm)
	require.NoError(t, err)
	err = os.Mkdir(snapPath, os.ModePerm)
	require.NoError(t, err)
	writer := NewDBWriter(dbBundle)

	rm, err := NewMockRegionManager(dbBundle, 1, RegionOptions{
		StoreAddr:  "127.0.0.1:10086",
		PDAddr:     "127.0.0.1:2379",
		RegionSize: 96 * 1024 * 1024,
	})
	require.NoError(t, err)
	pdClient := NewMockPD(rm)
	store := NewMVCCStore(&config.DefaultConf, dbBundle, dbPath, safePoint, writer, pdClient)
	svr := NewServer(nil, store, nil)

	clean := func() {
		require.NoError(t, store.Close())
		require.NoError(t, db.Close())
	}
	return &TestStore{
		MvccStore: store,
		Svr:       svr,
		DBPath:    dbPath,
		LogPath:   LogPath,
		t:         t,
	}, clean
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
	return PrewriteOptimisticWithAssertion(pk, key, value, startTs, lockTTL, minCommitTs, useAsyncCommit, secondaries,
		kvrpcpb.Assertion_None, kvrpcpb.AssertionLevel_Off, store)
}

// PrewriteOptimisticWithAssertion raises optimistic prewrite requests on store, with specified assertion config
func PrewriteOptimisticWithAssertion(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	minCommitTs uint64, useAsyncCommit bool, secondaries [][]byte, assertion kvrpcpb.Assertion,
	assertionLevel kvrpcpb.AssertionLevel, store *TestStore) error {
	op := kvrpcpb.Op_Put
	if value == nil {
		op = kvrpcpb.Op_Del
	}
	mutation := newMutation(op, key, value)
	mutation.Assertion = assertion
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:      []*kvrpcpb.Mutation{mutation},
		PrimaryLock:    pk,
		StartVersion:   startTs,
		LockTtl:        lockTTL,
		MinCommitTs:    minCommitTs,
		UseAsyncCommit: useAsyncCommit,
		Secondaries:    secondaries,
		AssertionLevel: assertionLevel,
	}
	return store.MvccStore.prewriteOptimistic(store.newReqCtx(), prewriteReq.Mutations, prewriteReq)
}

// PrewritePessimistic raises pessimistic prewrite requests
func PrewritePessimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	isPessimisticLock []bool, forUpdateTs uint64, store *TestStore) error {
	return PrewritePessimisticWithAssertion(pk, key, value, startTs, lockTTL, isPessimisticLock, forUpdateTs,
		kvrpcpb.Assertion_None, kvrpcpb.AssertionLevel_Off, store)
}

// PrewritePessimisticWithAssertion raises pessimistic prewrite requests, with specified assertion config
func PrewritePessimisticWithAssertion(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	isPessimisticLock []bool, forUpdateTs uint64, assertion kvrpcpb.Assertion, assertionLevel kvrpcpb.AssertionLevel,
	store *TestStore) error {
	mutation := newMutation(kvrpcpb.Op_Put, key, value)
	mutation.Assertion = assertion
	prewriteReq := &kvrpcpb.PrewriteRequest{
		Mutations:         []*kvrpcpb.Mutation{mutation},
		PrimaryLock:       pk,
		StartVersion:      startTs,
		LockTtl:           lockTTL,
		IsPessimisticLock: isPessimisticLock,
		ForUpdateTs:       forUpdateTs,
		AssertionLevel:    assertionLevel,
	}
	return store.MvccStore.prewritePessimistic(store.newReqCtx(), prewriteReq.Mutations, prewriteReq)
}

func MustCheckTxnStatus(pk []byte, lockTs uint64, callerStartTs uint64,
	currentTs uint64, rollbackIfNotExists bool, ttl, commitTs uint64, action kvrpcpb.Action, s *TestStore) {
	resTTL, resCommitTs, resAction, err := CheckTxnStatus(pk, lockTs, callerStartTs, currentTs, rollbackIfNotExists, s)
	require.NoError(s.t, err)
	require.Equal(s.t, ttl, resTTL)
	require.Equal(s.t, commitTs, resCommitTs)
	require.Equal(s.t, action, resAction)
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
	require.NotNil(store.t, lock)
	if pessimistic {
		require.Greater(store.t, lock.ForUpdateTS, uint64(0))
	} else {
		require.Equal(store.t, uint64(0), lock.ForUpdateTS)
	}
}

func MustPessimisticLocked(key []byte, startTs, forUpdateTs uint64, store *TestStore) {
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	require.NotNil(store.t, lock)
	require.Equal(store.t, startTs, lock.StartTS)
	require.Equal(store.t, forUpdateTs, lock.ForUpdateTS)
}

func MustUnLocked(key []byte, store *TestStore) {
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	require.Nil(store.t, lock)
}

func MustPrewritePut(pk, key []byte, val []byte, startTs uint64, store *TestStore) {
	MustPrewriteOptimistic(pk, key, val, startTs, 50, startTs, store)
}

func MustPrewritePutLockErr(pk, key []byte, val []byte, startTs uint64, store *TestStore) {
	err := PrewriteOptimistic(pk, key, val, startTs, lockTTL, startTs, false, [][]byte{}, store)
	require.Error(store.t, err)
	lockedErr := err.(*kverrors.ErrLocked)
	require.NotNil(store.t, lockedErr)
}

func MustPrewritePutErr(pk, key []byte, val []byte, startTs uint64, store *TestStore) {
	err := PrewriteOptimistic(pk, key, val, startTs, lockTTL, startTs, false, [][]byte{}, store)
	require.Error(store.t, err)
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
	require.NoError(store.t, err)
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
	require.Error(store.t, err)
	existErr := err.(*kverrors.ErrKeyAlreadyExists)
	require.NotNil(store.t, existErr)
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
	require.Error(store.t, err)
	existErr := err.(*kverrors.ErrKeyAlreadyExists)
	require.NotNil(store.t, existErr)
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
	require.NoError(store.t, err)
	var buf []byte
	buf = store.MvccStore.lockStore.Get(key, buf)
	require.Equal(store.t, 0, len(buf))
}

func MustPrewriteDelete(pk, key []byte, startTs uint64, store *TestStore) {
	MustPrewriteOptimistic(pk, key, nil, startTs, 50, startTs, store)
}

func MustAcquirePessimisticLock(pk, key []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	_, err := PessimisticLock(pk, key, startTs, lockTTL, forUpdateTs, false, false, store)
	require.NoError(store.t, err)
}

func MustAcquirePessimisticLockForce(pk, key []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	_, err := PessimisticLock(pk, key, startTs, lockTTL, forUpdateTs, false, true, store)
	require.NoError(store.t, err)
}

func MustAcquirePessimisticLockErr(pk, key []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	_, err := PessimisticLock(pk, key, startTs, lockTTL, forUpdateTs, false, false, store)
	require.Error(store.t, err)
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
	require.NoError(store.t, err)
}

func MustPrewriteOptimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	minCommitTs uint64, store *TestStore) {
	require.NoError(store.t, PrewriteOptimistic(pk, key, value, startTs, lockTTL, minCommitTs, false, [][]byte{}, store))
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	require.Equal(store.t, lockTTL, uint64(lock.TTL))
	require.Equal(store.t, 0, bytes.Compare(lock.Value, value))
}

func MustPrewriteOptimisticAsyncCommit(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	minCommitTs uint64, secondaries [][]byte, store *TestStore) {
	require.NoError(store.t, PrewriteOptimistic(pk, key, value, startTs, lockTTL, minCommitTs, true, secondaries, store))
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	require.Equal(store.t, lockTTL, uint64(lock.TTL))
	require.Equal(store.t, 0, bytes.Compare(lock.Value, value))
}

func MustPrewritePessimisticPut(pk []byte, key []byte, value []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	require.NoError(store.t, PrewritePessimistic(pk, key, value, startTs, lockTTL, []bool{true}, forUpdateTs, store))
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	require.Equal(store.t, forUpdateTs, lock.ForUpdateTS)
	require.Equal(store.t, 0, bytes.Compare(lock.Value, value))
}

func MustPrewritePessimisticDelete(pk []byte, key []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	require.NoError(store.t, PrewritePessimistic(pk, key, nil, startTs, lockTTL, []bool{true}, forUpdateTs, store))
}
func MustPrewritePessimistic(pk []byte, key []byte, value []byte, startTs uint64, lockTTL uint64,
	isPessimisticLock []bool, forUpdateTs uint64, store *TestStore) {
	require.NoError(store.t, PrewritePessimistic(pk, key, value, startTs, lockTTL, isPessimisticLock, forUpdateTs, store))
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	require.Equal(store.t, forUpdateTs, lock.ForUpdateTS)
	require.Equal(store.t, 0, bytes.Compare(lock.Value, value))
}

func MustPrewritePessimisticPutErr(pk []byte, key []byte, value []byte, startTs uint64, forUpdateTs uint64, store *TestStore) {
	err := PrewritePessimistic(pk, key, value, startTs, lockTTL, []bool{true}, forUpdateTs, store)
	require.Error(store.t, err)
}

func MustCommitKeyPut(key, val []byte, startTs, commitTs uint64, store *TestStore) {
	err := store.MvccStore.Commit(store.newReqCtx(), [][]byte{key}, startTs, commitTs)
	require.NoError(store.t, err)
	getVal, err := store.newReqCtx().getDBReader().Get(key, commitTs)
	require.NoError(store.t, err)
	require.Equal(store.t, 0, bytes.Compare(getVal, val))
}

func MustCommit(key []byte, startTs, commitTs uint64, store *TestStore) {
	err := store.MvccStore.Commit(store.newReqCtx(), [][]byte{key}, startTs, commitTs)
	require.NoError(store.t, err)
}

func MustCommitErr(key []byte, startTs, commitTs uint64, store *TestStore) {
	err := store.MvccStore.Commit(store.newReqCtx(), [][]byte{key}, startTs, commitTs)
	require.Error(store.t, err)
}

func MustRollbackKey(key []byte, startTs uint64, store *TestStore) {
	err := store.MvccStore.Rollback(store.newReqCtx(), [][]byte{key}, startTs)
	require.NoError(store.t, err)
	require.Nil(store.t, store.MvccStore.lockStore.Get(key, nil))
	status := store.MvccStore.checkExtraTxnStatus(store.newReqCtx(), key, startTs)
	require.True(store.t, status.isRollback)
}

func MustRollbackErr(key []byte, startTs uint64, store *TestStore) {
	err := store.MvccStore.Rollback(store.newReqCtx(), [][]byte{key}, startTs)
	require.Error(store.t, err)
}

func MustGetNone(key []byte, startTs uint64, store *TestStore) {
	val := MustGet(key, startTs, store)
	require.Len(store.t, val, 0)
}

func MustGetVal(key, val []byte, startTs uint64, store *TestStore) {
	getVal := MustGet(key, startTs, store)
	require.Equal(store.t, getVal, val)
}

func MustGetErr(key []byte, startTs uint64, store *TestStore) {
	_, err := kvGet(key, startTs, nil, nil, store)
	require.Error(store.t, err)
}

func kvGet(key []byte, readTs uint64, resolved, committed []uint64, store *TestStore) ([]byte, error) {
	reqCtx := store.newReqCtx()
	reqCtx.rpcCtx.ResolvedLocks = resolved
	reqCtx.rpcCtx.CommittedLocks = committed
	return store.MvccStore.Get(reqCtx, key, readTs)
}

func MustGet(key []byte, readTs uint64, store *TestStore) (val []byte) {
	val, err := kvGet(key, readTs, nil, nil, store)
	require.NoError(store.t, err)
	return val
}

func MustPrewriteLock(pk []byte, key []byte, startTs uint64, store *TestStore) {
	err := store.MvccStore.Prewrite(store.newReqCtx(), &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Lock, key, nil)},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
	})
	require.NoError(store.t, err)
}

func MustPrewriteLockErr(pk []byte, key []byte, startTs uint64, store *TestStore) {
	err := store.MvccStore.Prewrite(store.newReqCtx(), &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Lock, key, nil)},
		PrimaryLock:  pk,
		StartVersion: startTs,
		LockTtl:      lockTTL,
	})
	require.Error(store.t, err)
}

func MustCleanup(key []byte, startTs, currentTs uint64, store *TestStore) {
	err := store.MvccStore.Cleanup(store.newReqCtx(), key, startTs, currentTs)
	require.NoError(store.t, err)
}

func MustCleanupErr(key []byte, startTs, currentTs uint64, store *TestStore) {
	err := store.MvccStore.Cleanup(store.newReqCtx(), key, startTs, currentTs)
	require.Error(store.t, err)
}

func MustTxnHeartBeat(pk []byte, startTs, adviceTTL, expectedTTL uint64, store *TestStore) {
	lockTTL, err := store.MvccStore.TxnHeartBeat(store.newReqCtx(), &kvrpcpb.TxnHeartBeatRequest{
		PrimaryLock:   pk,
		StartVersion:  startTs,
		AdviseLockTtl: adviceTTL,
	})
	require.NoError(store.t, err)
	require.Equal(store.t, expectedTTL, lockTTL)
}

func MustGetRollback(key []byte, ts uint64, store *TestStore) {
	res := store.MvccStore.checkExtraTxnStatus(store.newReqCtx(), key, ts)
	require.True(store.t, res.isRollback)
}

func TestBasicOptimistic(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

	key1 := []byte("key1")
	val1 := []byte("val1")
	ttl := uint64(200)
	MustPrewriteOptimistic(key1, key1, val1, 1, ttl, 0, store)
	MustCommitKeyPut(key1, val1, 1, 2, store)
	// Read using smaller ts results in nothing
	getVal, _ := store.newReqCtx().getDBReader().Get(key1, 1)
	require.Nil(t, getVal)
}

func TestPessimiticTxnTTL(t *testing.T) {
	var err error
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

	// Pessimisitc lock key1
	key1 := []byte("key1")
	val1 := []byte("val1")
	startTs := uint64(1)
	lockTTL := uint64(1000)
	_, err = PessimisticLock(key1, key1, startTs, lockTTL, startTs, true, false, store)
	require.NoError(t, err)

	// Prewrite key1 with smaller lock ttl, lock ttl will not be changed
	MustPrewritePessimistic(key1, key1, val1, startTs, lockTTL-500, []bool{true}, startTs, store)
	lock := store.MvccStore.getLock(store.newReqCtx(), key1)
	require.Equal(t, uint64(1000), uint64(lock.TTL))

	key2 := []byte("key2")
	val2 := []byte("val2")
	_, err = PessimisticLock(key2, key2, 3, 300, 3, true, false, store)
	require.NoError(t, err)

	// Prewrite key1 with larger lock ttl, lock ttl will be updated
	MustPrewritePessimistic(key2, key2, val2, 3, 2000, []bool{true}, 3, store)
	lock2 := store.MvccStore.getLock(store.newReqCtx(), key2)
	require.Equal(t, uint64(2000), uint64(lock2.TTL))
}

func TestRollback(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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
	require.True(t, res.isRollback)

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

func TestOverwritePessimisitcLock(t *testing.T) {
	var err error
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

	key := []byte("key")
	startTs := uint64(1)
	lockTTL := uint64(100)
	forUpdateTs := uint64(100)
	// pessimistic lock one key
	_, err = PessimisticLock(key, key, startTs, lockTTL, forUpdateTs, true, false, store)
	require.NoError(t, err)
	lock := store.MvccStore.getLock(store.newReqCtx(), key)
	require.Equal(t, forUpdateTs, lock.ForUpdateTS)

	// pessimistic lock this key again using larger forUpdateTs
	_, err = PessimisticLock(key, key, startTs, lockTTL, forUpdateTs+7, true, false, store)
	require.NoError(t, err)
	lock2 := store.MvccStore.getLock(store.newReqCtx(), key)
	require.Equal(t, forUpdateTs+7, lock2.ForUpdateTS)

	// pessimistic lock one key using smaller forUpdateTsTs
	_, err = PessimisticLock(key, key, startTs, lockTTL, forUpdateTs-7, true, false, store)
	require.NoError(t, err)
	lock3 := store.MvccStore.getLock(store.newReqCtx(), key)
	require.Equal(t, forUpdateTs+7, lock3.ForUpdateTS)
}

func TestCheckTxnStatus(t *testing.T) {
	var err error
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

	var resTTL, resCommitTs uint64
	var action kvrpcpb.Action
	pk := []byte("tpk")
	startTs := uint64(1)
	callerStartTs := uint64(3)
	currentTs := uint64(5)

	// Try to check a not exist thing.
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, callerStartTs, currentTs, true, store)
	require.Equal(t, uint64(0), resTTL)
	require.Equal(t, uint64(0), resCommitTs)
	require.Equal(t, kvrpcpb.Action_LockNotExistRollback, action)
	require.NoError(t, err)

	// Using same startTs, prewrite will fail, since checkTxnStatus has rollbacked the key
	val := []byte("val")
	lockTTL := uint64(100)
	minCommitTs := uint64(20)
	err = PrewriteOptimistic(pk, pk, val, startTs, lockTTL, minCommitTs, false, [][]byte{}, store)
	require.ErrorIs(t, err, kverrors.ErrAlreadyRollback)

	// Prewrite a large txn
	startTs = 2
	MustPrewriteOptimistic(pk, pk, val, startTs, lockTTL, minCommitTs, store)
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, callerStartTs, currentTs, true, store)
	require.Equal(t, lockTTL, resTTL)
	require.Equal(t, uint64(0), resCommitTs)
	require.NoError(t, err)
	require.Equal(t, kvrpcpb.Action_MinCommitTSPushed, action)

	// Update min_commit_ts to current_ts. minCommitTs 20 -> 25
	newCallerTs := uint64(25)
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, newCallerTs, true, store)
	require.Equal(t, lockTTL, resTTL)
	require.Equal(t, uint64(0), resCommitTs)
	require.NoError(t, err)
	require.Equal(t, kvrpcpb.Action_MinCommitTSPushed, action)
	lock := store.MvccStore.getLock(store.newReqCtx(), pk)
	require.Equal(t, startTs, lock.StartTS)
	require.Equal(t, lockTTL, uint64(lock.TTL))
	require.Equal(t, newCallerTs+1, lock.MinCommitTS)

	// When caller_start_ts < lock.min_commit_ts, here 25 < 26, no need to update it.
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, newCallerTs, true, store)
	require.Equal(t, lockTTL, resTTL)
	require.Equal(t, uint64(0), resCommitTs)
	require.NoError(t, err)
	require.Equal(t, kvrpcpb.Action_MinCommitTSPushed, action)
	lock = store.MvccStore.getLock(store.newReqCtx(), pk)
	require.Equal(t, startTs, lock.StartTS)
	require.Equal(t, lockTTL, uint64(lock.TTL))
	require.Equal(t, newCallerTs+1, lock.MinCommitTS)

	// current_ts(25) < lock.min_commit_ts(26) < caller_start_ts(35)
	currentTs = uint64(25)
	newCallerTs = 35
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, currentTs, true, store)
	require.Equal(t, lockTTL, resTTL)
	require.Equal(t, uint64(0), resCommitTs)
	require.NoError(t, err)
	require.Equal(t, kvrpcpb.Action_MinCommitTSPushed, action)
	lock = store.MvccStore.getLock(store.newReqCtx(), pk)
	require.Equal(t, startTs, lock.StartTS)
	require.Equal(t, lockTTL, uint64(lock.TTL))
	require.Equal(t, newCallerTs+1, lock.MinCommitTS)

	// current_ts is max value 40, but no effect since caller_start_ts is smaller than minCommitTs
	currentTs = uint64(40)
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, currentTs, true, store)
	require.Equal(t, lockTTL, resTTL)
	require.Equal(t, uint64(0), resCommitTs)
	require.NoError(t, err)
	require.Equal(t, kvrpcpb.Action_MinCommitTSPushed, action)
	lock = store.MvccStore.getLock(store.newReqCtx(), pk)
	require.Equal(t, startTs, lock.StartTS)
	require.Equal(t, lockTTL, uint64(lock.TTL))
	require.Equal(t, newCallerTs+1, lock.MinCommitTS)

	// commit this key, commitTs(35) smaller than minCommitTs(36)
	commitTs := uint64(35)
	err = store.MvccStore.Commit(store.newReqCtx(), [][]byte{pk}, startTs, commitTs)
	require.Error(t, err)

	// commit this key, using correct commitTs
	commitTs = uint64(41)
	MustCommitKeyPut(pk, val, startTs, commitTs, store)

	// check committed txn status
	currentTs = uint64(42)
	newCallerTs = uint64(42)
	resTTL, resCommitTs, action, err = CheckTxnStatus(pk, startTs, newCallerTs, currentTs, true, store)
	require.Equal(t, uint64(0), resTTL)
	require.Equal(t, uint64(41), resCommitTs)
	require.NoError(t, err)
	require.Equal(t, kvrpcpb.Action_NoAction, action)
}

func TestCheckSecondaryLocksStatus(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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
	require.NoError(t, err)
	require.Len(t, locks, 0)
	require.Equal(t, uint64(3), commitTS)
	MustGet(secondary, 1, store)

	// Op_Lock lock is committed
	locks, commitTS, err = CheckSecondaryLocksStatus([][]byte{secondary}, 7, store)
	require.NoError(t, err)
	require.Len(t, locks, 0)
	require.Equal(t, uint64(9), commitTS)
	MustGet(secondary, 7, store)

	// Lock is already rolled back
	locks, commitTS, err = CheckSecondaryLocksStatus([][]byte{secondary}, 5, store)
	require.NoError(t, err)
	require.Len(t, locks, 0)
	require.Equal(t, uint64(0), commitTS)
	MustGetRollback(secondary, 5, store)

	// No commit info
	locks, commitTS, err = CheckSecondaryLocksStatus([][]byte{secondary}, 6, store)
	require.NoError(t, err)
	require.Len(t, locks, 0)
	require.Equal(t, uint64(0), commitTS)
	MustGetRollback(secondary, 6, store)

	// If there is a pessimistic lock on the secondary key:
	MustAcquirePessimisticLock(pk, secondary, 11, 11, store)
	// After CheckSecondaryLockStatus, the lock should be rolled back
	locks, commitTS, err = CheckSecondaryLocksStatus([][]byte{secondary}, 11, store)
	require.NoError(t, err)
	require.Len(t, locks, 0)
	require.Equal(t, uint64(0), commitTS)
	MustGetRollback(secondary, 11, store)

	// If there is an optimistic lock on the secondary key:
	MustPrewritePut(pk, secondary, val, 13, store)
	// After CheckSecondaryLockStatus, the lock should remain and be returned back
	locks, commitTS, err = CheckSecondaryLocksStatus([][]byte{secondary}, 13, store)
	require.NoError(t, err)
	require.Len(t, locks, 1)
	require.Equal(t, uint64(0), commitTS)
	MustLocked(secondary, false, store)
}

func TestMvccGet(t *testing.T) {
	var err error
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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
	require.NoError(t, err)
	require.Equal(t, 2, len(res.Writes))

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
	require.NoError(t, err)
	require.Equal(t, 4, len(res.Writes))

	require.Equal(t, startTs1, res.Writes[3].StartTs)
	require.Equal(t, commitTs1, res.Writes[3].CommitTs)
	require.Equal(t, 0, bytes.Compare(res.Writes[3].ShortValue, pkVal))

	require.Equal(t, startTs2, res.Writes[2].StartTs)
	require.Equal(t, commitTs2, res.Writes[2].CommitTs)
	require.Equal(t, 0, bytes.Compare(res.Writes[2].ShortValue, newVal))

	require.Equal(t, startTs3, res.Writes[1].StartTs)
	require.Equal(t, startTs3, res.Writes[1].CommitTs)
	require.Equal(t, 0, bytes.Compare(res.Writes[1].ShortValue, emptyVal))

	require.Equal(t, startTs4, res.Writes[0].StartTs)
	require.Equal(t, commitTs4, res.Writes[0].CommitTs)
	require.Equal(t, 0, bytes.Compare(res.Writes[0].ShortValue, emptyVal))

	// read using MvccGetByStartTs using key current ts
	res2, resKey, err := store.MvccStore.MvccGetByStartTs(store.newReqCtx(), startTs4)
	require.NoError(t, err)
	require.NotNil(t, res2)
	require.Equal(t, 0, bytes.Compare(resKey, pk))
	require.Equal(t, 4, len(res2.Writes))

	require.Equal(t, startTs1, res2.Writes[3].StartTs)
	require.Equal(t, commitTs1, res2.Writes[3].CommitTs)
	require.Equal(t, 0, bytes.Compare(res2.Writes[3].ShortValue, pkVal))

	require.Equal(t, startTs2, res2.Writes[2].StartTs)
	require.Equal(t, commitTs2, res2.Writes[2].CommitTs)
	require.Equal(t, 0, bytes.Compare(res2.Writes[2].ShortValue, newVal))

	require.Equal(t, startTs3, res2.Writes[1].StartTs)
	require.Equal(t, startTs3, res2.Writes[1].CommitTs)
	require.Equal(t, kvrpcpb.Op_Rollback, res2.Writes[1].Type)
	require.Equal(t, 0, bytes.Compare(res2.Writes[1].ShortValue, emptyVal))

	require.Equal(t, startTs4, res2.Writes[0].StartTs)
	require.Equal(t, commitTs4, res2.Writes[0].CommitTs)
	require.Equal(t, kvrpcpb.Op_Del, res2.Writes[0].Type)
	require.Equal(t, 0, bytes.Compare(res2.Writes[0].ShortValue, emptyVal))

	// read using MvccGetByStartTs using non exists startTs
	startTsNonExists := uint64(1000)
	res3, resKey, err := store.MvccStore.MvccGetByStartTs(store.newReqCtx(), startTsNonExists)
	require.NoError(t, err)
	require.Nil(t, resKey)
	require.Nil(t, res3)

	// read using old startTs
	res4, resKey, err := store.MvccStore.MvccGetByStartTs(store.newReqCtx(), startTs2)
	require.NoError(t, err)
	require.NotNil(t, res4)
	require.Equal(t, 0, bytes.Compare(resKey, pk))
	require.Len(t, res4.Writes, 4)
	require.Equal(t, startTs3, res4.Writes[1].StartTs)
	require.Equal(t, startTs3, res4.Writes[1].CommitTs)
	require.Equal(t, 0, bytes.Compare(res4.Writes[1].ShortValue, emptyVal))

	res4, resKey, err = store.MvccStore.MvccGetByStartTs(store.newReqCtxWithKeys([]byte("t1_r1"), []byte("t1_r2")), startTs2)
	require.NoError(t, err)
	require.NotNil(t, res4)
	require.Equal(t, 0, bytes.Compare(resKey, pk))
	require.Len(t, res4.Writes, 4)
	require.Equal(t, startTs3, res4.Writes[1].StartTs)
	require.Equal(t, startTs3, res4.Writes[1].CommitTs)
	require.Equal(t, 0, bytes.Compare(res4.Writes[1].ShortValue, emptyVal))
}

func TestPrimaryKeyOpLock(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

	pk := func() []byte { return []byte("tpk") }
	val2 := []byte("val2")
	// prewrite 100 Op_Lock
	MustPrewriteLock(pk(), pk(), 100, store)
	err := store.MvccStore.Commit(store.newReqCtx(), [][]byte{pk()}, 100, 101)
	require.NoError(t, err)
	_, commitTS, _, _ := CheckTxnStatus(pk(), 100, 110, 110, false, store)
	require.Equal(t, uint64(101), commitTS)

	// prewrite 110 Op_Put
	err = store.MvccStore.Prewrite(store.newReqCtx(), &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{newMutation(kvrpcpb.Op_Put, pk(), val2)},
		PrimaryLock:  pk(),
		StartVersion: 110,
		LockTtl:      100,
	})
	require.NoError(t, err)
	err = store.MvccStore.Commit(store.newReqCtx(), [][]byte{pk()}, 110, 111)
	require.NoError(t, err)

	// prewrite 120 Op_Lock
	MustPrewriteLock(pk(), pk(), 120, store)
	err = store.MvccStore.Commit(store.newReqCtx(), [][]byte{pk()}, 120, 121)
	require.NoError(t, err)

	// the older commit record should exist
	_, commitTS, _, _ = CheckTxnStatus(pk(), 120, 130, 130, false, store)
	require.Equal(t, uint64(121), commitTS)
	_, commitTS, _, _ = CheckTxnStatus(pk(), 110, 130, 130, false, store)
	require.Equal(t, uint64(111), commitTS)
	_, commitTS, _, _ = CheckTxnStatus(pk(), 100, 130, 130, false, store)
	require.Equal(t, uint64(101), commitTS)

	getVal, err := store.newReqCtx().getDBReader().Get(pk(), 90)
	require.NoError(t, err)
	require.Nil(t, getVal)
	getVal, err = store.newReqCtx().getDBReader().Get(pk(), 110)
	require.NoError(t, err)
	require.Nil(t, getVal)
	getVal, err = store.newReqCtx().getDBReader().Get(pk(), 111)
	require.NoError(t, err)
	require.Equal(t, val2, getVal)
	getVal, err = store.newReqCtx().getDBReader().Get(pk(), 130)
	require.NoError(t, err)
	require.Equal(t, val2, getVal)
}

func TestMvccTxnRead(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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

func TestTxnPrewrite(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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

func TestPrewriteInsert(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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

func TestRollbackKey(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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

func TestCleanup(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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

func TestCommit(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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

func TestMinCommitTs(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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

func TestPessimisticLock(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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
	require.NoError(t, PrewriteOptimistic(k, k, v, 40, lockTTL, 40, false, [][]byte{}, store))
	MustLocked(k, true, store)
	MustCommit(k, 40, 41, store)
	MustUnLocked(k, store)
}

func TestResolveCommit(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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
	err := store.MvccStore.ResolveLock(store.newReqCtx(), [][]byte{sk}, 2, 3)
	require.NoError(t, err)
	skLock := store.MvccStore.getLock(store.newReqCtx(), sk)
	require.NotNil(t, skLock)
	err = store.MvccStore.ResolveLock(store.newReqCtx(), [][]byte{sk}, 1, 2)
	require.NoError(t, err)

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
	require.NoError(t, err)
	err = kvTxn.Commit()
	require.NoError(t, err)
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

func TestBatchGet(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()
	MustLoad(100, 101, store, "ta:1", "tb:2", "tc:3")
	MustPrewritePut([]byte("ta"), []byte("ta"), []byte("0"), 103, store)
	keys := [][]byte{[]byte("ta"), []byte("tb"), []byte("tc")}
	pairs := store.MvccStore.BatchGet(store.newReqCtx(), keys, 104)
	require.Len(t, pairs, 3)
	require.NotNil(t, pairs[0].Error)
	require.Equal(t, "2", string(pairs[1].Value))
	require.Equal(t, "3", string(pairs[2].Value))
}

func TestCommitPessimisticLock(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()
	k := []byte("ta")
	MustAcquirePessimisticLock(k, k, 10, 10, store)
	MustCommitErr(k, 20, 30, store)
	MustCommit(k, 10, 20, store)
	MustGet(k, 30, store)
}

func TestOpCheckNotExist(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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

func TestPessimisticLockForce(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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

func TestScanSampleStep(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()
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
	require.Len(t, pairs, 80)
	for i, pair := range pairs {
		require.True(t, bytes.Equal(genScanSampleStepKey(100+i*sampleStep), pair.Key))
	}
	scanReq.Limit = 20
	pairs = store.MvccStore.Scan(store.newReqCtx(), scanReq)
	require.Len(t, pairs, 20)
	for i, pair := range pairs {
		require.True(t, bytes.Equal(genScanSampleStepKey(100+i*sampleStep), pair.Key))
	}
}

func genScanSampleStepKey(i int) []byte {
	return []byte(fmt.Sprintf("t%0.4d", i))
}

func TestAsyncCommitPrewrite(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

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
	require.Equal(t, uint32(2), pkLock.LockHdr.SecondaryNum)
	require.Equal(t, 0, bytes.Compare(pkLock.Secondaries[0], secKey1))
	require.Equal(t, 0, bytes.Compare(pkLock.Secondaries[1], secKey2))
	require.True(t, pkLock.UseAsyncCommit)
	require.Greater(t, pkLock.MinCommitTS, uint64(0))

	secLock := store.MvccStore.getLock(store.newReqCtx(), secKey2)
	require.Equal(t, uint32(0), secLock.LockHdr.SecondaryNum)
	require.Equal(t, 0, len(secLock.Secondaries))
	require.True(t, secLock.UseAsyncCommit)
	require.Greater(t, secLock.MinCommitTS, uint64(0))
	require.Equal(t, 0, bytes.Compare(secLock.Value, secVal2))
}

func TestAccessCommittedLocks(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

	k0 := []byte("t0")
	v0 := []byte("v0")
	MustLoad(10, 20, store, "t0:v0")
	// delete
	MustPrewriteDelete(k0, k0, 30, store)
	MustGetErr(k0, 40, store)
	// meet lock
	val, err := kvGet(k0, 40, []uint64{20}, nil, store)
	require.Error(store.t, err)
	require.Nil(store.t, val)
	val, err = kvGet(k0, 40, []uint64{20}, []uint64{20}, store)
	require.Error(store.t, err)
	require.Nil(store.t, val)
	// ignore lock
	val, err = kvGet(k0, 40, []uint64{30}, nil, store)
	require.NoError(store.t, err)
	require.Equal(store.t, v0, val)
	// access lock
	val, err = kvGet(k0, 40, nil, []uint64{30}, store)
	require.NoError(store.t, err)
	require.Nil(store.t, val)

	k1 := []byte("t1")
	v1 := []byte("v1")
	// put
	MustPrewritePut(k1, k1, v1, 50, store)
	// ignore lock
	val, err = kvGet(k1, 60, []uint64{50}, nil, store)
	require.NoError(store.t, err)
	require.Len(store.t, val, 0)
	// access lock
	val, err = kvGet(k1, 60, nil, []uint64{50}, store)
	require.NoError(store.t, err)
	require.Equal(store.t, v1, val)

	// locked
	k2 := []byte("t2")
	v2 := []byte("v2")
	MustPrewritePut(k2, k2, v2, 70, store)

	// lock for ingore
	k3 := []byte("t3")
	v3 := []byte("v3")
	MustPrewritePut(k3, k3, v3, 80, store)

	// No lock
	k4 := []byte("t4")
	v4 := []byte("v4")
	MustLoad(80, 90, store, "t4:v4")

	keys := [][]byte{k0, k1, k2, k3, k4}
	expected := []struct {
		key []byte
		val []byte
		err bool
	}{{k1, v1, false}, {k2, nil, true}, {k4, v4, false}}
	reqCtx := store.newReqCtx()
	reqCtx.rpcCtx.ResolvedLocks = []uint64{80}
	reqCtx.rpcCtx.CommittedLocks = []uint64{30, 50}
	pairs := store.MvccStore.BatchGet(reqCtx, keys, 100)
	require.Equal(store.t, len(expected), len(pairs))
	for i, pair := range pairs {
		e := expected[i]
		require.Equal(store.t, pair.Key, e.key)
		require.Equal(store.t, pair.Value, e.val)
		if e.err {
			require.NotNil(store.t, pair.Error)
		} else {
			require.Nil(store.t, pair.Error)
		}
	}

	scanReq := &kvrpcpb.ScanRequest{
		StartKey: []byte("t0"),
		EndKey:   []byte("t5"),
		Limit:    100,
		Version:  100,
	}
	pairs = store.MvccStore.Scan(reqCtx, scanReq)
	require.Equal(store.t, len(expected), len(pairs))
	for i, pair := range pairs {
		e := expected[i]
		require.Equal(store.t, pair.Key, e.key)
		require.Equal(store.t, pair.Value, e.val)
		if e.err {
			require.NotNil(store.t, pair.Error)
		} else {
			require.Nil(store.t, pair.Error)
		}
	}
}

func TestTiKVRCRead(t *testing.T) {
	store, clean := NewTestStore("basic_optimistic_db", "basic_optimistic_log", t)
	defer clean()

	k1 := []byte("t1")
	k2, v2 := []byte("t2"), []byte("v2")
	k3, v3 := []byte("t3"), []byte("v3")
	k4, v4 := []byte("t4"), []byte("v4")
	MustLoad(10, 20, store, "t1:v1", "t2:v2", "t3:v3")
	// write to be read
	MustPrewritePut(k1, k1, []byte("v11"), 30, store)
	MustCommit(k1, 30, 40, store)
	// lock to be ignored
	MustPrewritePut(k2, k2, v2, 50, store)
	MustPrewriteDelete(k3, k3, 60, store)
	MustPrewritePut(k4, k4, v4, 70, store)

	expected := map[string][]byte{string(k1): []byte("v11"), string(k2): v2, string(k3): v3, string(k4): nil}

	reqCtx := store.newReqCtx()
	reqCtx.rpcCtx.IsolationLevel = kvrpcpb.IsolationLevel_RC
	// get
	for k, v := range expected {
		res, err := store.MvccStore.Get(reqCtx, []byte(k), 80)
		require.NoError(t, err)
		require.Equal(t, res, v)
	}
	// batch get
	pairs := store.MvccStore.BatchGet(reqCtx, [][]byte{k1, k2, k3, k4}, 80)
	require.Equal(t, len(pairs), 3)
	for _, pair := range pairs {
		v, ok := expected[string(pair.Key)]
		require.True(t, ok)
		require.Nil(t, pair.Error)
		require.Equal(t, pair.Value, v)
	}
	// scan
	pairs = store.MvccStore.Scan(reqCtx, &kvrpcpb.ScanRequest{
		StartKey: []byte("t1"),
		EndKey:   []byte("t4"),
		Limit:    100,
		Version:  80,
	})
	require.Equal(t, len(pairs), 3)
	for _, pair := range pairs {
		v, ok := expected[string(pair.Key)]
		require.True(t, ok)
		require.Nil(t, pair.Error)
		require.Equal(t, pair.Value, v)
	}
}

func TestAssertion(t *testing.T) {
	store, clean := NewTestStore("TestAssertion", "TestAssertion", t)
	defer clean()

	// Prepare
	MustPrewriteOptimistic([]byte("k1"), []byte("k1"), []byte("v1"), 1, 100, 0, store)
	MustPrewriteOptimistic([]byte("k1"), []byte("k2"), []byte("v2"), 1, 100, 0, store)
	MustPrewriteOptimistic([]byte("k1"), []byte("k3"), []byte("v3"), 1, 100, 0, store)
	MustCommit([]byte("k1"), 1, 2, store)
	MustCommit([]byte("k2"), 1, 2, store)
	MustCommit([]byte("k3"), 1, 2, store)

	checkAssertionFailedError := func(err error, disable bool, startTs uint64, key []byte, assertion kvrpcpb.Assertion, existingStartTs uint64, existingCommitTs uint64) {
		t.Logf("Check error: %+q", err)
		if disable {
			require.Nil(t, err)
			return
		}
		require.NotNil(t, err)
		e, ok := errors.Cause(err).(*kverrors.ErrAssertionFailed)
		require.True(t, ok)
		require.Equal(t, startTs, e.StartTS)
		require.Equal(t, key, e.Key)
		require.Equal(t, assertion, e.Assertion)
		require.Equal(t, existingStartTs, e.ExistingStartTS)
		require.Equal(t, existingCommitTs, e.ExistingCommitTS)
	}

	for _, disable := range []bool{false, true} {
		level := kvrpcpb.AssertionLevel_Strict
		if disable {
			level = kvrpcpb.AssertionLevel_Off
		}
		// Test with optimistic transaction
		err := PrewriteOptimisticWithAssertion([]byte("k1"), []byte("k1"), []byte("v1"), 10, 100, 0, false, nil,
			kvrpcpb.Assertion_NotExist, level, store)
		checkAssertionFailedError(err, disable, 10, []byte("k1"), kvrpcpb.Assertion_NotExist, 1, 2)
		err = PrewriteOptimisticWithAssertion([]byte("k11"), []byte("k11"), []byte("v11"), 10, 100, 0, false, nil,
			kvrpcpb.Assertion_Exist, level, store)
		checkAssertionFailedError(err, disable, 10, []byte("k11"), kvrpcpb.Assertion_Exist, 0, 0)

		// Test with pessimistic transaction
		MustAcquirePessimisticLock([]byte("k2"), []byte("k2"), 10, 10, store)
		err = PrewritePessimisticWithAssertion([]byte("k2"), []byte("k2"), []byte("v2"), 10, 100, []bool{true}, 10,
			kvrpcpb.Assertion_NotExist, level, store)
		checkAssertionFailedError(err, disable, 10, []byte("k2"), kvrpcpb.Assertion_NotExist, 1, 2)
		MustAcquirePessimisticLock([]byte("k22"), []byte("k22"), 10, 10, store)
		err = PrewritePessimisticWithAssertion([]byte("k22"), []byte("k22"), []byte("v22"), 10, 100, []bool{true}, 10,
			kvrpcpb.Assertion_Exist, level, store)
		checkAssertionFailedError(err, disable, 10, []byte("k22"), kvrpcpb.Assertion_Exist, 0, 0)

		// Test with pessimistic transaction (non-pessimistic-lock)
		err = PrewritePessimisticWithAssertion([]byte("pk"), []byte("k3"), []byte("v3"), 10, 100, []bool{false}, 10,
			kvrpcpb.Assertion_NotExist, level, store)
		checkAssertionFailedError(err, disable, 10, []byte("k3"), kvrpcpb.Assertion_NotExist, 1, 2)
		err = PrewritePessimisticWithAssertion([]byte("pk"), []byte("k33"), []byte("v33"), 10, 100, []bool{false}, 10,
			kvrpcpb.Assertion_Exist, level, store)
		checkAssertionFailedError(err, disable, 10, []byte("k33"), kvrpcpb.Assertion_Exist, 0, 0)
	}

	for _, k := range [][]byte{
		[]byte("k1"),
		[]byte("k11"),
		[]byte("k2"),
		[]byte("k22"),
		[]byte("k3"),
		[]byte("k33"),
	} {
		MustRollbackKey(k, 10, store)
	}

	// Test assertion passes
	// Test with optimistic transaction
	err := PrewriteOptimisticWithAssertion([]byte("k1"), []byte("k1"), []byte("v1"), 20, 100, 0, false, nil,
		kvrpcpb.Assertion_Exist, kvrpcpb.AssertionLevel_Strict, store)
	require.Nil(t, err)
	err = PrewriteOptimisticWithAssertion([]byte("k11"), []byte("k11"), []byte("v11"), 20, 100, 0, false, nil,
		kvrpcpb.Assertion_NotExist, kvrpcpb.AssertionLevel_Strict, store)
	require.Nil(t, err)

	// Test with pessimistic transaction
	MustAcquirePessimisticLock([]byte("k2"), []byte("k2"), 20, 10, store)
	err = PrewritePessimisticWithAssertion([]byte("k2"), []byte("k2"), []byte("v2"), 20, 100, []bool{true}, 10,
		kvrpcpb.Assertion_Exist, kvrpcpb.AssertionLevel_Strict, store)
	require.Nil(t, err)
	MustAcquirePessimisticLock([]byte("k22"), []byte("k22"), 20, 10, store)
	err = PrewritePessimisticWithAssertion([]byte("k22"), []byte("k22"), []byte("v22"), 20, 100, []bool{true}, 10,
		kvrpcpb.Assertion_NotExist, kvrpcpb.AssertionLevel_Strict, store)
	require.Nil(t, err)

	// Test with pessimistic transaction (non-pessimistic-lock)
	err = PrewritePessimisticWithAssertion([]byte("pk"), []byte("k3"), []byte("v3"), 20, 100, []bool{false}, 10,
		kvrpcpb.Assertion_Exist, kvrpcpb.AssertionLevel_Strict, store)
	require.Nil(t, err)
	err = PrewritePessimisticWithAssertion([]byte("pk"), []byte("k33"), []byte("v33"), 20, 100, []bool{false}, 10,
		kvrpcpb.Assertion_NotExist, kvrpcpb.AssertionLevel_Strict, store)
	require.Nil(t, err)
}

func getConflictErr(res []*kvrpcpb.KvPair) *kvrpcpb.WriteConflict {
	for _, pair := range res {
		if pair.Error != nil && pair.Error.Conflict != nil {
			return pair.Error.Conflict
		}
	}
	return nil
}

func TestRcReadCheckTS(t *testing.T) {
	store, clean := NewTestStore("TestRcReadCheckTS", "TestRcReadCheckTS", t)
	defer clean()

	// Prepare.
	k1 := []byte("tk1")
	v1 := []byte("v1")
	MustPrewriteOptimistic(k1, k1, v1, 1, 100, 0, store)
	MustCommit(k1, 1, 2, store)

	k2 := []byte("tk2")
	v2 := []byte("v2")
	MustPrewriteOptimistic(k2, k2, v2, 5, 100, 0, store)
	MustCommit(k2, 5, 6, store)

	k3 := []byte("tk3")
	v3 := []byte("v3")
	MustPrewriteOptimistic(k3, k3, v3, 10, 100, 0, store)

	// Test point get with RcReadCheckTS.
	reqCtx := store.newReqCtx()
	reqCtx.rpcCtx.ResolvedLocks = nil
	reqCtx.rpcCtx.CommittedLocks = nil
	reqCtx.rpcCtx.IsolationLevel = kvrpcpb.IsolationLevel_RCCheckTS
	val, err := store.MvccStore.Get(reqCtx, k1, 3)
	require.Nil(t, err)
	require.Equal(t, v1, val)

	_, err = store.MvccStore.Get(reqCtx, k2, 3)
	require.NotNil(t, err)
	e, ok := errors.Cause(err).(*kverrors.ErrConflict)
	require.True(t, ok)
	require.Equal(t, uint64(3), e.StartTS)
	require.Equal(t, uint64(5), e.ConflictTS)
	require.Equal(t, uint64(6), e.ConflictCommitTS)

	_, err = store.MvccStore.Get(reqCtx, k3, 3)
	require.NotNil(t, err)
	e, ok = errors.Cause(err).(*kverrors.ErrConflict)
	require.True(t, ok)
	require.Equal(t, uint64(3), e.StartTS)
	require.Equal(t, uint64(10), e.ConflictTS)

	// Test scan and reverse scan.
	scanReq := &kvrpcpb.ScanRequest{
		Context:  reqCtx.rpcCtx,
		StartKey: []byte("a"),
		Limit:    100,
		Version:  3,
		EndKey:   []byte("z"),
	}

	// The error is reported from more recent version.
	scanRes := store.MvccStore.Scan(reqCtx, scanReq)
	conflictErr := getConflictErr(scanRes)
	require.NotNil(t, conflictErr)
	require.Equal(t, uint64(3), conflictErr.StartTs)
	require.Equal(t, uint64(5), conflictErr.ConflictTs)
	require.Equal(t, uint64(6), conflictErr.ConflictCommitTs)

	// The error is reported from lock.
	scanReq.Version = 15
	scanRes = store.MvccStore.Scan(reqCtx, scanReq)
	conflictErr = getConflictErr(scanRes)
	require.NotNil(t, conflictErr)
	require.Equal(t, uint64(15), conflictErr.StartTs)
	require.Equal(t, uint64(10), conflictErr.ConflictTs)

	// Test reverse scan.
	scanReq.Version = 3
	scanReq.Reverse = true
	scanRes = store.MvccStore.Scan(reqCtx, scanReq)
	conflictErr = getConflictErr(scanRes)
	require.NotNil(t, conflictErr)

	scanReq.Version = 15
	scanRes = store.MvccStore.Scan(reqCtx, scanReq)
	conflictErr = getConflictErr(scanRes)
	require.NotNil(t, conflictErr)
}
