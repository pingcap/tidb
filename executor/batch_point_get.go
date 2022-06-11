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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	driver "github.com/pingcap/tidb/store/driver/txn"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil/consistency"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

// BatchPointGetExec executes a bunch of point select queries.
type BatchPointGetExec struct {
	baseExecutor

	tblInfo          *model.TableInfo
	idxInfo          *model.IndexInfo
	handles          []kv.Handle
	physIDs          []int64
	partExpr         *tables.PartitionExpr
	partPos          int
	singlePart       bool
	partTblID        int64
	idxVals          [][]types.Datum
	startTS          uint64
	readReplicaScope string
	isStaleness      bool
	snapshotTS       uint64
	txn              kv.Transaction
	lock             bool
	waitTime         int64
	inited           uint32
	values           [][]byte
	index            int
	rowDecoder       *rowcodec.ChunkDecoder
	keepOrder        bool
	desc             bool
	batchGetter      kv.BatchGetter

	columns []*model.ColumnInfo
	// virtualColumnIndex records all the indices of virtual columns and sort them in definition
	// to make sure we can compute the virtual column in right order.
	virtualColumnIndex []int

	// virtualColumnRetFieldTypes records the RetFieldTypes of virtual columns.
	virtualColumnRetFieldTypes []*types.FieldType

	snapshot   kv.Snapshot
	stats      *runtimeStatsWithSnapshot
	cacheTable kv.MemBuffer
}

// buildVirtualColumnInfo saves virtual column indices and sort them in definition order
func (e *BatchPointGetExec) buildVirtualColumnInfo() {
	e.virtualColumnIndex = buildVirtualColumnIndex(e.Schema(), e.columns)
	if len(e.virtualColumnIndex) > 0 {
		e.virtualColumnRetFieldTypes = make([]*types.FieldType, len(e.virtualColumnIndex))
		for i, idx := range e.virtualColumnIndex {
			e.virtualColumnRetFieldTypes[i] = e.schema.Columns[idx].RetType
		}
	}
}

// Open implements the Executor interface.
func (e *BatchPointGetExec) Open(context.Context) error {
	e.snapshotTS = e.startTS
	sessVars := e.ctx.GetSessionVars()
	txnCtx := sessVars.TxnCtx
	stmtCtx := sessVars.StmtCtx
	if e.lock {
		e.snapshotTS = txnCtx.GetForUpdateTS()
	}
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return err
	}
	e.txn = txn
	var snapshot kv.Snapshot
	if txn.Valid() && txnCtx.StartTS == txnCtx.GetForUpdateTS() && txnCtx.StartTS == e.snapshotTS {
		// We can safely reuse the transaction snapshot if startTS is equal to forUpdateTS.
		// The snapshot may contains cache that can reduce RPC call.
		snapshot = txn.GetSnapshot()
	} else {
		snapshot = e.ctx.GetSnapshotWithTS(e.snapshotTS)
	}
	if e.ctx.GetSessionVars().StmtCtx.RCCheckTS {
		snapshot.SetOption(kv.IsolationLevel, kv.RCCheckTS)
	}
	if e.cacheTable != nil {
		snapshot = cacheTableSnapshot{snapshot, e.cacheTable}
	}
	if e.runtimeStats != nil {
		snapshotStats := &txnsnapshot.SnapshotRuntimeStats{}
		e.stats = &runtimeStatsWithSnapshot{
			SnapshotRuntimeStats: snapshotStats,
		}
		snapshot.SetOption(kv.CollectRuntimeStats, snapshotStats)
		stmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.stats)
	}
	replicaReadType := e.ctx.GetSessionVars().GetReplicaRead()
	if replicaReadType.IsFollowerRead() && !e.ctx.GetSessionVars().StmtCtx.RCCheckTS {
		snapshot.SetOption(kv.ReplicaRead, replicaReadType)
	}
	snapshot.SetOption(kv.TaskID, stmtCtx.TaskID)
	snapshot.SetOption(kv.ReadReplicaScope, e.readReplicaScope)
	snapshot.SetOption(kv.IsStalenessReadOnly, e.isStaleness)
	failpoint.Inject("assertBatchPointReplicaOption", func(val failpoint.Value) {
		assertScope := val.(string)
		if replicaReadType.IsClosestRead() && assertScope != e.readReplicaScope {
			panic("batch point get replica option fail")
		}
	})

	if replicaReadType.IsClosestRead() && e.readReplicaScope != kv.GlobalTxnScope {
		snapshot.SetOption(kv.MatchStoreLabels, []*metapb.StoreLabel{
			{
				Key:   placement.DCLabelKey,
				Value: e.readReplicaScope,
			},
		})
	}
	setOptionForTopSQL(stmtCtx, snapshot)
	var batchGetter kv.BatchGetter = snapshot
	if txn.Valid() {
		lock := e.tblInfo.Lock
		if e.lock {
			batchGetter = driver.NewBufferBatchGetter(txn.GetMemBuffer(), &PessimisticLockCacheGetter{txnCtx: txnCtx}, snapshot)
		} else if lock != nil && (lock.Tp == model.TableLockRead || lock.Tp == model.TableLockReadOnly) && e.ctx.GetSessionVars().EnablePointGetCache {
			batchGetter = newCacheBatchGetter(e.ctx, e.tblInfo.ID, snapshot)
		} else {
			batchGetter = driver.NewBufferBatchGetter(txn.GetMemBuffer(), nil, snapshot)
		}
	}
	e.snapshot = snapshot
	e.batchGetter = batchGetter
	return nil
}

// CacheTable always use memBuffer in session as snapshot.
// cacheTableSnapshot inherits kv.Snapshot and override the BatchGet methods and Get methods.
type cacheTableSnapshot struct {
	kv.Snapshot
	memBuffer kv.MemBuffer
}

func (s cacheTableSnapshot) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	values := make(map[string][]byte)
	if s.memBuffer == nil {
		return values, nil
	}

	for _, key := range keys {
		val, err := s.memBuffer.Get(ctx, key)
		if kv.ErrNotExist.Equal(err) {
			continue
		}

		if err != nil {
			return nil, err
		}

		if len(val) == 0 {
			continue
		}

		values[string(key)] = val
	}

	return values, nil
}

func (s cacheTableSnapshot) Get(ctx context.Context, key kv.Key) ([]byte, error) {
	return s.memBuffer.Get(ctx, key)
}

// MockNewCacheTableSnapShot only serves for test.
func MockNewCacheTableSnapShot(snapshot kv.Snapshot, memBuffer kv.MemBuffer) *cacheTableSnapshot {
	return &cacheTableSnapshot{snapshot, memBuffer}
}

// Close implements the Executor interface.
func (e *BatchPointGetExec) Close() error {
	if e.runtimeStats != nil && e.snapshot != nil {
		e.snapshot.SetOption(kv.CollectRuntimeStats, nil)
	}
	e.inited = 0
	e.index = 0
	return nil
}

// Next implements the Executor interface.
func (e *BatchPointGetExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if atomic.CompareAndSwapUint32(&e.inited, 0, 1) {
		if err := e.initialize(ctx); err != nil {
			return err
		}
		if e.lock {
			e.updateDeltaForTableID(e.tblInfo.ID)
		}
	}

	if e.index >= len(e.values) {
		return nil
	}
	for !req.IsFull() && e.index < len(e.values) {
		handle, val := e.handles[e.index], e.values[e.index]
		err := DecodeRowValToChunk(e.base().ctx, e.schema, e.tblInfo, handle, val, req, e.rowDecoder)
		if err != nil {
			return err
		}
		e.index++
	}

	err := FillVirtualColumnValue(e.virtualColumnRetFieldTypes, e.virtualColumnIndex, e.schema, e.columns, e.ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func datumsContainNull(vals []types.Datum) bool {
	for _, val := range vals {
		if val.IsNull() {
			return true
		}
	}
	return false
}

func (e *BatchPointGetExec) initialize(ctx context.Context) error {
	var handleVals map[string][]byte
	var indexKeys []kv.Key
	var err error
	batchGetter := e.batchGetter
	rc := e.ctx.GetSessionVars().IsPessimisticReadConsistency()
	if e.idxInfo != nil && !isCommonHandleRead(e.tblInfo, e.idxInfo) {
		// `SELECT a, b FROM t WHERE (a, b) IN ((1, 2), (1, 2), (2, 1), (1, 2))` should not return duplicated rows
		dedup := make(map[hack.MutableString]struct{})
		toFetchIndexKeys := make([]kv.Key, 0, len(e.idxVals))
		for _, idxVals := range e.idxVals {
			// For all x, 'x IN (null)' evaluate to null, so the query get no result.
			if datumsContainNull(idxVals) {
				continue
			}

			physID, err := getPhysID(e.tblInfo, e.partExpr, idxVals[e.partPos].GetInt64())
			if err != nil {
				continue
			}

			// If this BatchPointGetExec is built only for the specific table partition, skip those filters not matching this partition.
			if e.singlePart && e.partTblID != physID {
				continue
			}
			idxKey, err1 := EncodeUniqueIndexKey(e.ctx, e.tblInfo, e.idxInfo, idxVals, physID)
			if err1 != nil && !kv.ErrNotExist.Equal(err1) {
				return err1
			}
			if idxKey == nil {
				continue
			}
			s := hack.String(idxKey)
			if _, found := dedup[s]; found {
				continue
			}
			dedup[s] = struct{}{}
			toFetchIndexKeys = append(toFetchIndexKeys, idxKey)
		}
		if e.keepOrder {
			sort.Slice(toFetchIndexKeys, func(i int, j int) bool {
				if e.desc {
					return toFetchIndexKeys[i].Cmp(toFetchIndexKeys[j]) > 0
				}
				return toFetchIndexKeys[i].Cmp(toFetchIndexKeys[j]) < 0
			})
		}

		// lock all keys in repeatable read isolation.
		// for read consistency, only lock exist keys,
		// indexKeys will be generated after getting handles.
		if !rc {
			indexKeys = toFetchIndexKeys
		} else {
			indexKeys = make([]kv.Key, 0, len(toFetchIndexKeys))
		}

		// SELECT * FROM t WHERE x IN (null), in this case there is no key.
		if len(toFetchIndexKeys) == 0 {
			return nil
		}

		// Fetch all handles.
		handleVals, err = batchGetter.BatchGet(ctx, toFetchIndexKeys)
		if err != nil {
			return err
		}

		e.handles = make([]kv.Handle, 0, len(toFetchIndexKeys))
		if e.tblInfo.Partition != nil {
			e.physIDs = make([]int64, 0, len(toFetchIndexKeys))
		}
		for _, key := range toFetchIndexKeys {
			handleVal := handleVals[string(key)]
			if len(handleVal) == 0 {
				continue
			}
			handle, err1 := tablecodec.DecodeHandleInUniqueIndexValue(handleVal, e.tblInfo.IsCommonHandle)
			if err1 != nil {
				return err1
			}
			e.handles = append(e.handles, handle)
			if rc {
				indexKeys = append(indexKeys, key)
			}
			if e.tblInfo.Partition != nil {
				pid := tablecodec.DecodeTableID(key)
				e.physIDs = append(e.physIDs, pid)
				if e.lock {
					e.updateDeltaForTableID(pid)
				}
			}
		}

		// The injection is used to simulate following scenario:
		// 1. Session A create a point get query but pause before second time `GET` kv from backend
		// 2. Session B create an UPDATE query to update the record that will be obtained in step 1
		// 3. Then point get retrieve data from backend after step 2 finished
		// 4. Check the result
		failpoint.InjectContext(ctx, "batchPointGetRepeatableReadTest-step1", func() {
			if ch, ok := ctx.Value("batchPointGetRepeatableReadTest").(chan struct{}); ok {
				// Make `UPDATE` continue
				close(ch)
			}
			// Wait `UPDATE` finished
			failpoint.InjectContext(ctx, "batchPointGetRepeatableReadTest-step2", nil)
		})
	} else if e.keepOrder {
		less := func(i int, j int) bool {
			if e.desc {
				return e.handles[i].Compare(e.handles[j]) > 0
			}
			return e.handles[i].Compare(e.handles[j]) < 0

		}
		if e.tblInfo.PKIsHandle && mysql.HasUnsignedFlag(e.tblInfo.GetPkColInfo().GetFlag()) {
			uintComparator := func(i, h kv.Handle) int {
				if !i.IsInt() || !h.IsInt() {
					panic(fmt.Sprintf("both handles need be IntHandle, but got %T and %T ", i, h))
				}
				ihVal := uint64(i.IntValue())
				hVal := uint64(h.IntValue())
				if ihVal > hVal {
					return 1
				}
				if ihVal < hVal {
					return -1
				}
				return 0
			}
			less = func(i int, j int) bool {
				if e.desc {
					return uintComparator(e.handles[i], e.handles[j]) > 0
				}
				return uintComparator(e.handles[i], e.handles[j]) < 0
			}
		}
		sort.Slice(e.handles, less)
	}

	keys := make([]kv.Key, 0, len(e.handles))
	newHandles := make([]kv.Handle, 0, len(e.handles))
	for i, handle := range e.handles {
		var tID int64
		if len(e.physIDs) > 0 {
			tID = e.physIDs[i]
		} else {
			if handle.IsInt() {
				tID, err = getPhysID(e.tblInfo, e.partExpr, handle.IntValue())
				if err != nil {
					continue
				}
			} else {
				_, d, err1 := codec.DecodeOne(handle.EncodedCol(e.partPos))
				if err1 != nil {
					return err1
				}
				tID, err = getPhysID(e.tblInfo, e.partExpr, d.GetInt64())
				if err != nil {
					continue
				}
			}
		}
		// If this BatchPointGetExec is built only for the specific table partition, skip those handles not matching this partition.
		if e.singlePart && e.partTblID != tID {
			continue
		}
		key := tablecodec.EncodeRowKeyWithHandle(tID, handle)
		keys = append(keys, key)
		newHandles = append(newHandles, handle)
	}
	e.handles = newHandles

	var values map[string][]byte
	// Lock keys (include exists and non-exists keys) before fetch all values for Repeatable Read Isolation.
	if e.lock && !rc {
		lockKeys := make([]kv.Key, len(keys)+len(indexKeys))
		copy(lockKeys, keys)
		copy(lockKeys[len(keys):], indexKeys)
		err = LockKeys(ctx, e.ctx, e.waitTime, lockKeys...)
		if err != nil {
			return err
		}
		// Change the unique index LOCK into PUT record.
		if len(indexKeys) > 0 {
			if !e.txn.Valid() {
				return kv.ErrInvalidTxn
			}
			membuf := e.txn.GetMemBuffer()
			for _, idxKey := range indexKeys {
				handleVal := handleVals[string(idxKey)]
				if len(handleVal) == 0 {
					continue
				}
				err = membuf.Set(idxKey, handleVal)
				if err != nil {
					return err
				}
			}
		}
	}
	// Fetch all values.
	values, err = batchGetter.BatchGet(ctx, keys)
	if err != nil {
		return err
	}
	handles := make([]kv.Handle, 0, len(values))
	var existKeys []kv.Key
	if e.lock && rc {
		existKeys = make([]kv.Key, 0, 2*len(values))
	}
	changeLockToPutIdxKeys := make([]kv.Key, 0, len(indexKeys))
	e.values = make([][]byte, 0, len(values))
	for i, key := range keys {
		val := values[string(key)]
		if len(val) == 0 {
			if e.idxInfo != nil && (!e.tblInfo.IsCommonHandle || !e.idxInfo.Primary) &&
				!e.ctx.GetSessionVars().StmtCtx.WeakConsistency {
				return (&consistency.Reporter{
					HandleEncode: func(_ kv.Handle) kv.Key {
						return key
					},
					IndexEncode: func(_ *consistency.RecordData) kv.Key {
						return indexKeys[i]
					},
					Tbl:  e.tblInfo,
					Idx:  e.idxInfo,
					Sctx: e.ctx,
				}).ReportLookupInconsistent(ctx,
					1, 0,
					e.handles[i:i+1],
					e.handles,
					[]consistency.RecordData{{}},
				)
			}
			continue
		}
		e.values = append(e.values, val)
		handles = append(handles, e.handles[i])
		if e.lock && rc {
			existKeys = append(existKeys, key)
			// when e.handles is set in builder directly, index should be primary key and the plan is CommonHandleRead
			// with clustered index enabled, indexKeys is empty in this situation
			// lock primary key for clustered index table is redundant
			if len(indexKeys) != 0 {
				existKeys = append(existKeys, indexKeys[i])
				changeLockToPutIdxKeys = append(changeLockToPutIdxKeys, indexKeys[i])
			}
		}
	}
	// Lock exists keys only for Read Committed Isolation.
	if e.lock && rc {
		err = LockKeys(ctx, e.ctx, e.waitTime, existKeys...)
		if err != nil {
			return err
		}
		if len(changeLockToPutIdxKeys) > 0 {
			if !e.txn.Valid() {
				return kv.ErrInvalidTxn
			}
			for _, idxKey := range changeLockToPutIdxKeys {
				membuf := e.txn.GetMemBuffer()
				handleVal := handleVals[string(idxKey)]
				if len(handleVal) == 0 {
					return kv.ErrNotExist
				}
				err = membuf.Set(idxKey, handleVal)
				if err != nil {
					return err
				}
			}
		}
	}
	e.handles = handles
	return nil
}

// LockKeys locks the keys for pessimistic transaction.
func LockKeys(ctx context.Context, seCtx sessionctx.Context, lockWaitTime int64, keys ...kv.Key) error {
	txnCtx := seCtx.GetSessionVars().TxnCtx
	lctx := newLockCtx(seCtx.GetSessionVars(), lockWaitTime, len(keys))
	if txnCtx.IsPessimistic {
		lctx.InitReturnValues(len(keys))
	}
	err := doLockKeys(ctx, seCtx, lctx, keys...)
	if err != nil {
		return err
	}
	if txnCtx.IsPessimistic {
		// When doLockKeys returns without error, no other goroutines access the map,
		// it's safe to read it without mutex.
		for _, key := range keys {
			if v, ok := lctx.GetValueNotLocked(key); ok {
				txnCtx.SetPessimisticLockCache(key, v)
			}
		}
	}
	return nil
}

// PessimisticLockCacheGetter implements the kv.Getter interface.
// It is used as a middle cache to construct the BufferedBatchGetter.
type PessimisticLockCacheGetter struct {
	txnCtx *variable.TransactionContext
}

// Get implements the kv.Getter interface.
func (getter *PessimisticLockCacheGetter) Get(_ context.Context, key kv.Key) ([]byte, error) {
	val, ok := getter.txnCtx.GetKeyInPessimisticLockCache(key)
	if ok {
		return val, nil
	}
	return nil, kv.ErrNotExist
}

func getPhysID(tblInfo *model.TableInfo, partitionExpr *tables.PartitionExpr, intVal int64) (int64, error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return tblInfo.ID, nil
	}

	if partitionExpr == nil {
		return tblInfo.ID, nil
	}

	switch pi.Type {
	case model.PartitionTypeHash:
		partIdx := mathutil.Abs(intVal % int64(pi.Num))
		return pi.Definitions[partIdx].ID, nil
	case model.PartitionTypeRange:
		// we've check the type assertions in func TryFastPlan
		col, ok := partitionExpr.Expr.(*expression.Column)
		if !ok {
			return 0, errors.Errorf("unsupported partition type in BatchGet")
		}
		unsigned := mysql.HasUnsignedFlag(col.GetType().GetFlag())
		ranges := partitionExpr.ForRangePruning
		length := len(ranges.LessThan)
		partIdx := sort.Search(length, func(i int) bool {
			return ranges.Compare(i, intVal, unsigned) > 0
		})
		if partIdx >= 0 && partIdx < length {
			return pi.Definitions[partIdx].ID, nil
		}
	case model.PartitionTypeList:
		isNull := false // we've guaranteed this in the build process of either TryFastPlan or buildBatchPointGet
		partIdx := partitionExpr.ForListPruning.LocatePartition(intVal, isNull)
		if partIdx >= 0 {
			return pi.Definitions[partIdx].ID, nil
		}
	}

	return 0, errors.Errorf("dual partition")
}

type cacheBatchGetter struct {
	ctx      sessionctx.Context
	tid      int64
	snapshot kv.Snapshot
}

func (b *cacheBatchGetter) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	cacheDB := b.ctx.GetStore().GetMemCache()
	vals := make(map[string][]byte)
	for _, key := range keys {
		val, err := cacheDB.UnionGet(ctx, b.tid, b.snapshot, key)
		if err != nil {
			if !kv.ErrNotExist.Equal(err) {
				return nil, err
			}
			continue
		}
		vals[string(key)] = val
	}
	return vals, nil
}

func newCacheBatchGetter(ctx sessionctx.Context, tid int64, snapshot kv.Snapshot) *cacheBatchGetter {
	return &cacheBatchGetter{ctx, tid, snapshot}
}
