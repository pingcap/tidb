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
	"context"
	"math"
	"slices"
	"time"

	"github.com/pingcap/badger"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/kverrors"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"go.uber.org/zap"
)

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
					Reason:           kvrpcpb.WriteConflict_Optimistic,
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

	expectedForUpdateTSMap := make(map[int]uint64, len(req.GetForUpdateTsConstraints()))
	for _, constraint := range req.GetForUpdateTsConstraints() {
		index := int(constraint.Index)
		if index >= len(mutations) {
			return errors.Errorf("prewrite request invalid: for_update_ts constraint set for index %v while %v mutations were given", index, len(mutations))
		}

		expectedForUpdateTSMap[index] = constraint.ExpectedForUpdateTs
	}

	reader := reqCtx.getDBReader()
	txn := reader.GetTxn()

	for i, m := range mutations {
		if m.Op == kvrpcpb.Op_CheckNotExists {
			return kverrors.ErrInvalidOp{Op: m.Op}
		}
		lock := store.getLock(reqCtx, m.Key)
		isPessimisticLock := len(req.PessimisticActions) > 0 && req.PessimisticActions[i] == kvrpcpb.PrewriteRequest_DO_PESSIMISTIC_CHECK
		needConstraintCheck := len(req.PessimisticActions) > 0 && req.PessimisticActions[i] == kvrpcpb.PrewriteRequest_DO_CONSTRAINT_CHECK
		lockExists := lock != nil
		lockMatch := lockExists && lock.StartTS == startTS
		lockConstraintPasses := true
		if expectedForUpdateTS, ok := expectedForUpdateTSMap[i]; ok {
			if lock.ForUpdateTS != expectedForUpdateTS {
				lockConstraintPasses = false
			}
		}
		if isPessimisticLock {
			valid := lockExists && lockMatch && lockConstraintPasses
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
		} else if needConstraintCheck {
			item, err := txn.Get(m.Key)
			if err != nil && err != badger.ErrKeyNotFound {
				return errors.Trace(err)
			}
			// check conflict
			if item != nil {
				userMeta := mvcc.DBUserMeta(item.UserMeta())
				if userMeta.CommitTS() > startTS {
					return &kverrors.ErrConflict{
						StartTS:          startTS,
						ConflictTS:       userMeta.StartTS(),
						ConflictCommitTS: userMeta.CommitTS(),
						Key:              item.KeyCopy(nil),
						Reason:           kvrpcpb.WriteConflict_LazyUniquenessCheck,
					}
				}
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

// Flush implements the MVCCStore interface.
func (store *MVCCStore) Flush(reqCtx *requestCtx, req *kvrpcpb.FlushRequest) error {
	mutations := req.GetMutations()
	if !slices.IsSortedFunc(mutations, func(i, j *kvrpcpb.Mutation) int {
		return bytes.Compare(i.Key, j.Key)
	}) {
		return errors.New("mutations are not sorted")
	}

	regCtx := reqCtx.regCtx
	hashVals := mutationsToHashVals(mutations)
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	startTS := req.StartTs
	// Only check the PK's status first.
	for _, m := range mutations {
		lock, err := store.checkConflictInLockStore(reqCtx, m, startTS)
		if err != nil {
			return err
		}
		if lock != nil {
			continue
		}
		if bytes.Equal(m.Key, req.PrimaryKey) {
			status := store.checkExtraTxnStatus(reqCtx, m.Key, startTS)
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
					Reason:           kvrpcpb.WriteConflict_Optimistic,
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
	}

	dummyPrewriteReq := &kvrpcpb.PrewriteRequest{
		PrimaryLock:  req.PrimaryKey,
		StartVersion: startTS,
	}
	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)
	for i, m := range mutations {
		if m.Op == kvrpcpb.Op_CheckNotExists {
			continue
		}
		lock, err1 := store.buildPrewriteLock(reqCtx, m, items[i], dummyPrewriteReq)
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
	buf = buf[:0]
	return encoder.Encode(time.UTC, colIDs, datums, nil, buf)
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
	// In the write path, remove the keyspace prefix
	// to ensure compatibility with the key parsing implemented in the mock.
	tempKey := rowcodec.RemoveKeyspacePrefix(m.Key)
	if rowcodec.IsRowKey(tempKey) && lock.Op == uint8(kvrpcpb.Op_Put) {
		if !rowcodec.IsNewFormat(m.Value) {
			reqCtx.buf, err = encodeFromOldRow(m.Value, reqCtx.buf)
			if err != nil {
				log.Error("encode data failed", zap.Binary("value", m.Value), zap.Binary("key", m.Key), zap.Stringer("op", m.Op), zap.Error(err))
				return nil, err
			}

			lock.Value = slices.Clone(reqCtx.buf)
		}
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
