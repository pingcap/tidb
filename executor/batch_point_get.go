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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/rowcodec"
)

// BatchPointGetExec executes a bunch of point select queries.
type BatchPointGetExec struct {
	baseExecutor

	tblInfo    *model.TableInfo
	idxInfo    *model.IndexInfo
	handles    []int64
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
}

// Open implements the Executor interface.
func (e *BatchPointGetExec) Open(context.Context) error {
	return nil
}

// Close implements the Executor interface.
func (e *BatchPointGetExec) Close() error {
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
	return nil
}

func (e *BatchPointGetExec) initialize(ctx context.Context) error {
	e.snapshotTS = e.startTS
	if e.lock {
		e.snapshotTS = e.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	}
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return err
	}
	e.txn = txn
	snapshot, err := e.ctx.GetStore().GetSnapshot(kv.Version{Ver: e.snapshotTS})
	if err != nil {
		return err
	}
	if e.ctx.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}
	var batchGetter kv.BatchGetter = snapshot
	if txn.Valid() {
		batchGetter = kv.NewBufferBatchGetter(txn.GetMemBuffer(), snapshot)
	}

	var handleVals map[string][]byte
	if e.idxInfo != nil {
		// `SELECT a, b FROM t WHERE (a, b) IN ((1, 2), (1, 2), (2, 1), (1, 2))` should not return duplicated rows
		dedup := make(map[hack.MutableString]struct{})
		keys := make([]kv.Key, 0, len(e.idxVals))
		for _, idxVals := range e.idxVals {
			idxKey, err1 := encodeIndexKey(e.base(), e.tblInfo, e.idxInfo, idxVals, e.tblInfo.ID)
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

		// Fetch all handles.
		handleVals, err = batchGetter.BatchGet(ctx, keys)
		if err != nil {
			return err
		}

		e.handles = make([]int64, 0, len(keys))
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
	}

	keys := make([]kv.Key, len(e.handles))
	for i, handle := range e.handles {
		key := tablecodec.EncodeRowKeyWithHandle(e.tblInfo.ID, handle)
		keys[i] = key
	}

	// Fetch all values.
	var values map[string][]byte
	if e.lock {
		values, err = e.lockKeys(ctx, keys)
		if err != nil {
			return err
		}
	}
	if values == nil {
		// Fetch all values.
		values, err = batchGetter.BatchGet(ctx, keys)
		if err != nil {
			return err
		}
	}
	handles := make([]int64, 0, len(values))
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
	}
	e.handles = handles
	return nil
}

func (e *BatchPointGetExec) lockKeys(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	lctx := newLockCtx(e.ctx.GetSessionVars(), e.waitTime)
	if txnCtx.IsPessimistic {
		lctx.ReturnValues = true
		lctx.Values = make(map[string][]byte, len(keys))
		// pre-fill the values of already locked keys.
		for _, key := range keys {
			if val, ok := txnCtx.GetKeyInPessimisticLockCache(key); ok {
				lctx.Values[string(key)] = val
			}
		}
	}
	err := doLockKeys(ctx, e.ctx, lctx, keys...)
	if err != nil {
		return nil, err
	}
	if txnCtx.IsPessimistic {
		// When doLockKeys returns without error, no other goroutines access the map,
		// it's safe to read it without mutex.
		for _, key := range keys {
			txnCtx.SetPessimisticLockCache(key, lctx.Values[string(key)])
		}
		// Overwrite with buffer value.
		buffer := e.txn.GetMemBuffer()
		for _, key := range keys {
			val, err1 := buffer.Get(ctx, key)
			if err1 != kv.ErrNotExist {
				lctx.Values[string(key)] = val
			}
		}
		return lctx.Values, nil
	}
	return nil, nil
}
