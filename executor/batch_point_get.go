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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/math"
	"github.com/pingcap/tidb/util/rowcodec"
)

// BatchPointGetExec executes a bunch of point select queries.
type BatchPointGetExec struct {
	baseExecutor

	tblInfo    *model.TableInfo
	idxInfo    *model.IndexInfo
	handles    []int64
	physIDs    []int64
	partPos    int
	idxVals    [][]types.Datum
	startTS    uint64
	snapshotTS uint64
	txn        kv.Transaction
	lock       bool
	waitTime   int64
	inited     bool
	values     [][]byte
	index      int
	rowDecoder *rowcodec.ChunkDecoder
	keepOrder  bool
	desc       bool

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
	return nil
}

// Close implements the Executor interface.
func (e *BatchPointGetExec) Close() error {
	if e.runtimeStats != nil && e.snapshot != nil {
		e.snapshot.DelOption(kv.CollectRuntimeStats)
	}
	e.inited = false
	e.index = 0
	return nil
}

// Next implements the Executor interface.
func (e *BatchPointGetExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.inited {
		if err := e.initialize(ctx); err != nil {
			return err
		}
		e.inited = true
	}
	if e.index >= len(e.values) {
		return nil
	}
	for !req.IsFull() && e.index < len(e.values) {
		handle, val := e.handles[e.index], e.values[e.index]
		err := decodeRowValToChunk(e.base(), e.tblInfo, handle, val, req, e.rowDecoder)
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

func (e *BatchPointGetExec) initialize(ctx context.Context) error {
	e.snapshotTS = e.startTS
	sessVars := e.ctx.GetSessionVars()
	txnCtx := sessVars.TxnCtx
	if e.lock {
		e.snapshotTS = txnCtx.GetForUpdateTS()
	}
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return err
	}
	e.txn = txn
	var snapshot kv.Snapshot
	if sessVars.InTxn() && txnCtx.StartTS == txnCtx.GetForUpdateTS() {
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
		e.snapshot = snapshot
	}
	if e.ctx.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}
	snapshot.SetOption(kv.TaskID, e.ctx.GetSessionVars().StmtCtx.TaskID)
	var batchGetter kv.BatchGetter = snapshot
	if sessVars.InTxn() {
		batchGetter = kv.NewBufferBatchGetter(txn.GetMemBuffer(), &PessimisticLockCacheGetter{txnCtx: txnCtx}, snapshot)
	}

	var handleVals map[string][]byte
	var indexKeys []kv.Key
	if e.idxInfo != nil {
		// `SELECT a, b FROM t WHERE (a, b) IN ((1, 2), (1, 2), (2, 1), (1, 2))` should not return duplicated rows
		dedup := make(map[hack.MutableString]struct{})
		keys := make([]kv.Key, 0, len(e.idxVals))
		for _, idxVals := range e.idxVals {
			physID := getPhysID(e.tblInfo, idxVals[e.partPos].GetInt64())
			idxKey, hasNull, err1 := encodeIndexKey(e.base(), e.tblInfo, e.idxInfo, idxVals, physID)
			if hasNull {
				continue
			}
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

		// Fetch all handles.
		handleVals, err = batchGetter.BatchGet(ctx, keys)
		if err != nil {
			return err
		}

		e.handles = make([]int64, 0, len(keys))
		if e.tblInfo.Partition != nil {
			e.physIDs = make([]int64, 0, len(keys))
		}
		for _, key := range keys {
			handleVal := handleVals[string(key)]
			if len(handleVal) == 0 {
				continue
			}
			handle, err1 := tables.DecodeHandle(handleVal)
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
				return e.handles[i] > e.handles[j]
			}
			return e.handles[i] < e.handles[j]
		})
	}

	keys := make([]kv.Key, len(e.handles))
	for i, handle := range e.handles {
		var tID int64
		if len(e.physIDs) > 0 {
			tID = e.physIDs[i]
		} else {
			tID = getPhysID(e.tblInfo, handle)
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
		err = e.lockKeys(ctx, lockKeys)
		if err != nil {
			return err
		}
	}
	// Fetch all values.
	values, err = batchGetter.BatchGet(ctx, keys)
	if err != nil {
		return err
	}
	handles := make([]int64, 0, len(values))
	var existKeys []kv.Key
	if e.lock && rc {
		existKeys = make([]kv.Key, 0, len(values))
	}
	e.values = make([][]byte, 0, len(values))
	for i, key := range keys {
		val := values[string(key)]
		if len(val) == 0 {
			if e.idxInfo != nil {
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
		err = e.lockKeys(ctx, existKeys)
		if err != nil {
			return err
		}
	}
	e.handles = handles
	return nil
}

func (e *BatchPointGetExec) lockKeys(ctx context.Context, keys []kv.Key) error {
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	lctx := newLockCtx(e.ctx.GetSessionVars(), e.waitTime)
	if txnCtx.IsPessimistic {
		lctx.ReturnValues = true
		lctx.Values = make(map[string]kv.ReturnedValue, len(keys))
	}
	err := doLockKeys(ctx, e.ctx, lctx, keys...)
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

func getPhysID(tblInfo *model.TableInfo, val int64) int64 {
	pi := tblInfo.Partition
	if pi == nil {
		return tblInfo.ID
	}
	partIdx := math.Abs(val % int64(pi.Num))
	return pi.Definitions[partIdx].ID
}
