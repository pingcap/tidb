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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"sort"
	"sync/atomic"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/math"
	"github.com/pingcap/tidb/util/rowcodec"
)

// BatchPointGetExec executes a bunch of point select queries.
type BatchPointGetExec struct {
	baseExecutor

	tblInfo     *model.TableInfo
	idxInfo     *model.IndexInfo
	handles     []kv.Handle
	physIDs     []int64
	partPos     int
	idxVals     [][]types.Datum
	startTS     uint64
	snapshotTS  uint64
	txn         kv.Transaction
	lock        bool
	waitTime    int64
	inited      uint32
	values      [][]byte
	index       int
	rowDecoder  *rowcodec.ChunkDecoder
	keepOrder   bool
	desc        bool
	batchGetter kv.BatchGetter

	columns []*model.ColumnInfo
	// virtualColumnIndex records all the indices of virtual columns and sort them in definition
	// to make sure we can compute the virtual column in right order.
	virtualColumnIndex []int

	// virtualColumnRetFieldTypes records the RetFieldTypes of virtual columns.
	virtualColumnRetFieldTypes []*types.FieldType

	snapshot kv.Snapshot
	stats    *pointGetRuntimeStats
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
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	if e.lock {
		e.snapshotTS = txnCtx.GetForUpdateTS()
	}
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return err
	}
	e.txn = txn
	var snapshot kv.Snapshot
	if txn.Valid() && txnCtx.StartTS == txnCtx.GetForUpdateTS() {
		// We can safely reuse the transaction snapshot if startTS is equal to forUpdateTS.
		// The snapshot may contains cache that can reduce RPC call.
		snapshot = txn.GetSnapshot()
	} else {
		snapshot, err = e.ctx.GetStore().GetSnapshot(kv.Version{Ver: e.snapshotTS})
		if err != nil {
			return err
		}
	}
	if e.runtimeStats != nil {
		snapshotStats := &tikv.SnapshotRuntimeStats{}
		e.stats = &pointGetRuntimeStats{
			BasicRuntimeStats:    e.runtimeStats,
			SnapshotRuntimeStats: snapshotStats,
		}
		snapshot.SetOption(kv.CollectRuntimeStats, snapshotStats)
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.stats)
	}
	if e.ctx.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}
	snapshot.SetOption(kv.TaskID, e.ctx.GetSessionVars().StmtCtx.TaskID)
	var batchGetter kv.BatchGetter = snapshot
	if txn.Valid() {
		batchGetter = kv.NewBufferBatchGetter(txn.GetMemBuffer(), &PessimisticLockCacheGetter{txnCtx: txnCtx}, snapshot)
	}
	e.snapshot = snapshot
	e.batchGetter = batchGetter
	return nil
}

// Close implements the Executor interface.
func (e *BatchPointGetExec) Close() error {
	if e.runtimeStats != nil && e.snapshot != nil {
		e.snapshot.DelOption(kv.CollectRuntimeStats)
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
	if e.idxInfo != nil && !isCommonHandleRead(e.tblInfo, e.idxInfo) {
		// `SELECT a, b FROM t WHERE (a, b) IN ((1, 2), (1, 2), (2, 1), (1, 2))` should not return duplicated rows
		dedup := make(map[hack.MutableString]struct{})
		keys := make([]kv.Key, 0, len(e.idxVals))
		for _, idxVals := range e.idxVals {
			// For all x, 'x IN (null)' evaluate to null, so the query get no result.
			if datumsContainNull(idxVals) {
				continue
			}

			physID := getPhysID(e.tblInfo, idxVals[e.partPos].GetInt64())
			idxKey, err1 := EncodeUniqueIndexKey(e.ctx, e.tblInfo, e.idxInfo, idxVals, physID)
			if err1 != nil && !kv.ErrNotExist.Equal(err1) {
				return err1
			}
			s := hack.String(idxKey)
			if _, found := dedup[s]; found {
				continue
			}
			dedup[s] = struct{}{}
			keys = append(keys, idxKey)
		}
		if e.keepOrder {
			sort.Slice(keys, func(i int, j int) bool {
				if e.desc {
					return keys[i].Cmp(keys[j]) > 0
				}
				return keys[i].Cmp(keys[j]) < 0
			})
		}
		indexKeys = keys

		// SELECT * FROM t WHERE x IN (null), in this case there is no key.
		if len(keys) == 0 {
			return nil
		}

		// Fetch all handles.
		handleVals, err = batchGetter.BatchGet(ctx, keys)
		if err != nil {
			return err
		}

		e.handles = make([]kv.Handle, 0, len(keys))
		if e.tblInfo.Partition != nil {
			e.physIDs = make([]int64, 0, len(keys))
		}
		for _, key := range keys {
			handleVal := handleVals[string(key)]
			if len(handleVal) == 0 {
				continue
			}
			handle, err1 := tablecodec.DecodeHandleInUniqueIndexValue(handleVal, e.tblInfo.IsCommonHandle)
			if err1 != nil {
				return err1
			}
			e.handles = append(e.handles, handle)
			if e.tblInfo.Partition != nil {
				e.physIDs = append(e.physIDs, tablecodec.DecodeTableID(key))
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
		sort.Slice(e.handles, func(i int, j int) bool {
			if e.desc {
				return e.handles[i].Compare(e.handles[j]) > 0
			}
			return e.handles[i].Compare(e.handles[j]) < 0
		})
	}

	keys := make([]kv.Key, len(e.handles))
	for i, handle := range e.handles {
		var tID int64
		if len(e.physIDs) > 0 {
			tID = e.physIDs[i]
		} else {
			if handle.IsInt() {
				tID = getPhysID(e.tblInfo, handle.IntValue())
			} else {
				_, d, err1 := codec.DecodeOne(handle.EncodedCol(e.partPos))
				if err1 != nil {
					return err1
				}
				tID = getPhysID(e.tblInfo, d.GetInt64())
			}
		}
		key := tablecodec.EncodeRowKeyWithHandle(tID, handle)
		keys[i] = key
	}

	var values map[string][]byte
	rc := e.ctx.GetSessionVars().IsPessimisticReadConsistency()
	// Lock keys (include exists and non-exists keys) before fetch all values for Repeatable Read Isolation.
	if e.lock && !rc {
		lockKeys := make([]kv.Key, len(keys), len(keys)+len(indexKeys))
		copy(lockKeys, keys)
		for _, idxKey := range indexKeys {
			// lock the non-exist index key, using len(val) in case BatchGet result contains some zero len entries
			if val := handleVals[string(idxKey)]; len(val) == 0 {
				lockKeys = append(lockKeys, idxKey)
			}
		}
		err = LockKeys(ctx, e.ctx, e.waitTime, lockKeys...)
		if err != nil {
			return err
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
		existKeys = make([]kv.Key, 0, len(values))
	}
	e.values = make([][]byte, 0, len(values))
	for i, key := range keys {
		val := values[string(key)]
		if len(val) == 0 {
			if e.idxInfo != nil && (!e.tblInfo.IsCommonHandle || !e.idxInfo.Primary) {
				return kv.ErrNotExist.GenWithStack("inconsistent extra index %s, handle %d not found in table",
					e.idxInfo.Name.O, e.handles[i])
			}
			continue
		}
		e.values = append(e.values, val)
		handles = append(handles, e.handles[i])
		if e.lock && rc {
			existKeys = append(existKeys, key)
		}
	}
	// Lock exists keys only for Read Committed Isolation.
	if e.lock && rc {
		err = LockKeys(ctx, e.ctx, e.waitTime, existKeys...)
		if err != nil {
			return err
		}
	}
	e.handles = handles
	return nil
}

// LockKeys locks the keys for pessimistic transaction.
func LockKeys(ctx context.Context, seCtx sessionctx.Context, lockWaitTime int64, keys ...kv.Key) error {
	txnCtx := seCtx.GetSessionVars().TxnCtx
	lctx := newLockCtx(seCtx.GetSessionVars(), lockWaitTime)
	if txnCtx.IsPessimistic {
		lctx.ReturnValues = true
		lctx.Values = make(map[string]kv.ReturnedValue, len(keys))
	}
	err := doLockKeys(ctx, seCtx, lctx, keys...)
	if err != nil {
		return err
	}
	if txnCtx.IsPessimistic {
		// When doLockKeys returns without error, no other goroutines access the map,
		// it's safe to read it without mutex.
		for _, key := range keys {
			rv := lctx.Values[string(key)]
			if !rv.AlreadyLocked {
				txnCtx.SetPessimisticLockCache(key, rv.Value)
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

func getPhysID(tblInfo *model.TableInfo, intVal int64) int64 {
	pi := tblInfo.Partition
	if pi == nil {
		return tblInfo.ID
	}
	partIdx := math.Abs(intVal % int64(pi.Num))
	return pi.Definitions[partIdx].ID
}
