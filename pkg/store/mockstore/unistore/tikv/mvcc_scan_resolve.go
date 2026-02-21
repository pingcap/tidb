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
	"cmp"
	"context"
	"math"
	"slices"
	"sync/atomic"
	"time"

	"github.com/pingcap/badger"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/lockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/kverrors"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/mvcc"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"go.uber.org/zap"
)

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
	if lock.StartTS <= maxTS {
		locks = append(locks, lock.ToLockInfo(slices.Clone(it.Key())))
	}
	return locks
}

// scanPessimisticLocks returns matching pessimistic locks.
func (store *MVCCStore) scanPessimisticLocks(reqCtx *requestCtx, startTS uint64, forUpdateTS uint64) ([]*kvrpcpb.LockInfo, error) {
	var locks []*kvrpcpb.LockInfo
	it := store.lockStore.NewIterator()
	for it.Seek(reqCtx.regCtx.RawStart()); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), reqCtx.regCtx.RawEnd()) {
			return locks, nil
		}
		lock := mvcc.DecodeLock(it.Value())
		if lock.Op == uint8(kvrpcpb.Op_PessimisticLock) && lock.StartTS == startTS && lock.ForUpdateTS <= forUpdateTS {
			locks = append(locks, lock.ToLockInfo(slices.Clone(it.Key())))
		}
	}
	return locks, nil
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
	return time.UnixMilli(int64(ts >> 18))
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
	slices.SortFunc(mvccInfo.Writes, func(i, j *kvrpcpb.MvccWrite) int {
		return cmp.Compare(j.CommitTs, i.CommitTs)
	})
	mvccInfo.Values = make([]*kvrpcpb.MvccValue, len(mvccInfo.Writes))
	for i := range mvccInfo.Writes {
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

func doesNeedLock(item *badger.Item,
	req *kvrpcpb.PessimisticLockRequest) (bool, error) {
	if req.LockOnlyIfExists {
		if item == nil {
			return false, nil
		}
		val, err := item.Value()
		if err != nil {
			return false, err
		}
		if len(val) == 0 {
			return false, nil
		}
	}
	return true, nil
}
