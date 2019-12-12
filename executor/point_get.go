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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
)

func (b *executorBuilder) buildPointGet(p *plannercore.PointGetPlan) Executor {
	if b.ctx.GetSessionVars().IsPessimisticReadConsistency() {
		if err := b.refreshForUpdateTS(); err != nil {
			b.err = err
			return nil
		}
	}
	startTS, err := b.getStartTS()
	if err != nil {
		b.err = err
		return nil
	}
	e := &PointGetExecutor{
		baseExecutor: newBaseExecutor(b.ctx, p.Schema(), p.ExplainID()),
	}
	e.base().initCap = 1
	e.base().maxChunkSize = 1
	b.isSelectForUpdate = p.IsForUpdate
	e.Init(p, startTS)
	return e
}

// PointGetExecutor executes point select query.
type PointGetExecutor struct {
	baseExecutor

	tblInfo      *model.TableInfo
	handle       int64
	idxInfo      *model.IndexInfo
	partInfo     *model.PartitionDefinition
	idxKey       kv.Key
	txn          kv.Transaction
	retryCnt     int
	idxVals      []types.Datum
	startTS      uint64
	snapshot     kv.Snapshot
	snapshotTS   uint64
	done         bool
	lock         bool
	lockWaitTime int64
	rowDecoder   *rowcodec.ChunkDecoder
}

// Init set fields needed for PointGetExecutor reuse, this does NOT change baseExecutor field
func (e *PointGetExecutor) Init(p *plannercore.PointGetPlan, startTs uint64) {
	decoder := newRowDecoder(e.ctx, p.Schema(), p.TblInfo)
	e.tblInfo = p.TblInfo
	e.handle = p.Handle
	e.idxInfo = p.IndexInfo
	e.idxVals = p.IndexValues
	e.startTS = startTs
	e.done = false
	e.lock = p.Lock
	e.lockWaitTime = p.LockWaitTime
	e.rowDecoder = decoder
	e.partInfo = p.PartitionInfo
}

// Open implements the Executor interface.
func (e *PointGetExecutor) Open(context.Context) error {
	return nil
}

// Close implements the Executor interface.
func (e *PointGetExecutor) Close() error {
	return nil
}

// Next implements the Executor interface.
func (e *PointGetExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	e.snapshotTS = e.startTS
	if e.lock {
		e.snapshotTS = e.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	}
	var tblID int64
	if e.partInfo != nil {
		tblID = e.partInfo.ID
	} else {
		tblID = e.tblInfo.ID
	}
	if e.idxInfo != nil {
		idxKey, err1 := encodeIndexKey(e.base(), e.tblInfo, e.idxInfo, e.idxVals, tblID)
		if err1 != nil && !kv.ErrNotExist.Equal(err1) {
			return err1
		}
		e.idxKey = idxKey
		handleVal, err1 := e.get(ctx, idxKey)
		if err1 != nil && !kv.ErrNotExist.Equal(err1) {
			return err1
		}
		if len(handleVal) == 0 {
			return e.lockKeyIfNeeded(ctx, idxKey)
		}
		e.handle, err1 = tables.DecodeHandle(handleVal)
		if err1 != nil {
			return err1
		}

		// The injection is used to simulate following scenario:
		// 1. Session A create a point get query but pause before second time `GET` kv from backend
		// 2. Session B create an UPDATE query to update the record that will be obtained in step 1
		// 3. Then point get retrieve data from backend after step 2 finished
		// 4. Check the result
		failpoint.InjectContext(ctx, "pointGetRepeatableReadTest-step1", func() {
			if ch, ok := ctx.Value("pointGetRepeatableReadTest").(chan struct{}); ok {
				// Make `UPDATE` continue
				close(ch)
			}
			// Wait `UPDATE` finished
			failpoint.InjectContext(ctx, "pointGetRepeatableReadTest-step2", nil)
		})
	}

	key := tablecodec.EncodeRowKeyWithHandle(tblID, e.handle)
	if e.lock && e.ctx.GetSessionVars().TxnCtx.IsPessimistic {
		return e.forcePessimisticLock(ctx, key, req)
	}
	val, err := e.get(ctx, key)
	if err != nil && !kv.ErrNotExist.Equal(err) {
		return err
	}
	err = e.lockKeyIfNeeded(ctx, key)
	if err != nil {
		return err
	}
	if len(val) == 0 {
		if e.idxInfo != nil {
			return kv.ErrNotExist.GenWithStack("inconsistent extra index %s, handle %d not found in table",
				e.idxInfo.Name.O, e.handle)
		}
		return nil
	}
	return decodeRowValToChunk(e.base(), e.tblInfo, e.handle, val, req, e.rowDecoder)
}

func (e *PointGetExecutor) lockKeyIfNeeded(ctx context.Context, key []byte) error {
	if e.lock {
		return doLockKeys(ctx, e.ctx, newLockCtx(e.ctx.GetSessionVars(), e.lockWaitTime), key)
	}
	return nil
}

func (e *PointGetExecutor) get(ctx context.Context, key kv.Key) (val []byte, err error) {
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if txn.Valid() && e.snapshotTS == e.startTS {
		var ok bool
		val, ok = e.ctx.GetSessionVars().TxnCtx.GetKeyInPessimisticLockCache(key)
		if ok {
			return
		}
		// We can safely use the snapshot in the txn and utilize the cache.
		return txn.Get(ctx, key)
	}
	if txn.Valid() && !txn.IsReadOnly() {
		// We cannot use txn.Get directly here because the snapshot in txn and the snapshot of e.snapshot is
		// different for pessimistic transaction.
		val, err = txn.GetMemBuffer().Get(ctx, key)
		if err == nil {
			return val, err
		}
		if !kv.IsErrNotFound(err) {
			return nil, err
		}
		var ok bool
		val, ok = e.ctx.GetSessionVars().TxnCtx.GetKeyInPessimisticLockCache(key)
		if ok {
			return
		}
		// fallthrough to snapshot get.
	}
	if e.snapshot == nil {
		e.snapshot, err = e.ctx.GetStore().GetSnapshot(kv.Version{Ver: e.snapshotTS})
		if err != nil {
			return nil, err
		}
		if e.ctx.GetSessionVars().GetReplicaRead().IsFollowerRead() {
			e.snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
		}
	}
	return e.snapshot.Get(ctx, key)
}

// forcePessimisticLock gets the latest value by force pessimistic lock.
// If the value does not match the index, errIndexColumnChanged is returned.
func (e *PointGetExecutor) forcePessimisticLock(ctx context.Context, key kv.Key, chk *chunk.Chunk) error {
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.ForUpdate = true
	// Lock keys only once when finished fetching all results.
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	e.txn = txn
	lockCtx := newLockCtx(e.ctx.GetSessionVars(), e.lockWaitTime)
	// Set Force to true will lock the key even if the value has changed since the for update ts, the latest value
	// will be returned.
	lockCtx.Force = true
	err = txn.LockKeys(ctx, lockCtx, key)
	if err != nil {
		return err
	}
	if lockCtx.AlreadyLocked {
		var val []byte
		val, err = e.get(ctx, key)
		if err != nil && !kv.ErrNotExist.Equal(err) {
			return err
		}
		if len(val) == 0 {
			return nil
		}
		return decodeRowValToChunk(e.base(), e.tblInfo, e.handle, val, chk, e.rowDecoder)
	}
	err = UpdateForUpdateTS(e.ctx, lockCtx.ValueCommitTS)
	if err != nil {
		return err
	}
	if e.idxKey == nil {
		txnCtx.SetPessimisticLockCache(key, lockCtx.Value)
		if len(lockCtx.Value) == 0 {
			return nil
		}
		return decodeRowValToChunk(e.base(), e.tblInfo, e.handle, lockCtx.Value, chk, e.rowDecoder)
	}
	if len(lockCtx.Value) == 0 {
		// The key is deleted after we get the handle from the unique index.
		return kv.ErrWriteConflict.FastGenByArgs(e.startTS, 0, 0, key)
	}
	err = decodeRowValToChunk(e.base(), e.tblInfo, e.handle, lockCtx.Value, chk, e.rowDecoder)
	if err != nil {
		return err
	}
	err = e.checkUniqueIndexColumn(chk, txn)
	if err != nil {
		return err
	}
	txnCtx.SetPessimisticLockCache(key, lockCtx.Value)
	// Also cache the index key since the row is locked it doesn't change.
	txnCtx.SetPessimisticLockCache(e.idxKey, tables.EncodeHandle(e.handle))
	return nil
}

func (e *PointGetExecutor) checkUniqueIndexColumn(chk *chunk.Chunk, txn kv.Transaction) error {
	// FIXME: unique index forcePessimisticLock need extra check for index row consistency.
	// The unique index of the row may change after we get the handle,
	// so we need to decode the row and check if the unique index columns in the row has changed.
	// If it has changed, we need to rollback the key and return write conflict error.
	return nil
}

func encodeIndexKey(e *baseExecutor, tblInfo *model.TableInfo, idxInfo *model.IndexInfo, idxVals []types.Datum, tID int64) (_ []byte, err error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	for i := range idxVals {
		colInfo := tblInfo.Columns[idxInfo.Columns[i].Offset]
		// table.CastValue will append 0x0 if the string value's length is smaller than the BINARY column's length.
		// So we don't use CastValue for string value for now.
		// TODO: merge two if branch.
		if colInfo.Tp == mysql.TypeString || colInfo.Tp == mysql.TypeVarString || colInfo.Tp == mysql.TypeVarchar {
			var str string
			str, err = idxVals[i].ToString()
			idxVals[i].SetString(str)
		} else {
			idxVals[i], err = table.CastValue(e.ctx, idxVals[i], colInfo)
		}
		if err != nil {
			return nil, err
		}
	}

	encodedIdxVals, err := codec.EncodeKey(sc, nil, idxVals...)
	if err != nil {
		return nil, err
	}
	return tablecodec.EncodeIndexSeekKey(tID, idxInfo.ID, encodedIdxVals), nil
}

func decodeRowValToChunk(e *baseExecutor, tblInfo *model.TableInfo, handle int64, rowVal []byte, chk *chunk.Chunk, rd *rowcodec.ChunkDecoder) error {
	if rowcodec.IsNewFormat(rowVal) {
		return rd.DecodeToChunk(rowVal, handle, chk)
	}
	return decodeOldRowValToChunk(e, tblInfo, handle, rowVal, chk)
}

func decodeOldRowValToChunk(e *baseExecutor, tblInfo *model.TableInfo, handle int64, rowVal []byte, chk *chunk.Chunk) error {
	if len(rowVal) == 0 {
		return nil
	}
	colID2CutPos := make(map[int64]int, e.schema.Len())
	for _, col := range e.schema.Columns {
		if _, ok := colID2CutPos[col.ID]; !ok {
			colID2CutPos[col.ID] = len(colID2CutPos)
		}
	}
	cutVals, err := tablecodec.CutRowNew(rowVal, colID2CutPos)
	if err != nil {
		return err
	}
	if cutVals == nil {
		cutVals = make([][]byte, len(colID2CutPos))
	}
	decoder := codec.NewDecoder(chk, e.ctx.GetSessionVars().Location())
	for i, col := range e.schema.Columns {
		if tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.RetType.Flag) {
			chk.AppendInt64(i, handle)
			continue
		}
		if col.ID == model.ExtraHandleID {
			chk.AppendInt64(i, handle)
			continue
		}
		cutPos := colID2CutPos[col.ID]
		if len(cutVals[cutPos]) == 0 {
			colInfo := getColInfoByID(tblInfo, col.ID)
			d, err1 := table.GetColOriginDefaultValue(e.ctx, colInfo)
			if err1 != nil {
				return err1
			}
			chk.AppendDatum(i, &d)
			continue
		}
		_, err = decoder.DecodeOne(cutVals[cutPos], i, col.RetType)
		if err != nil {
			return err
		}
	}
	return nil
}

func getColInfoByID(tbl *model.TableInfo, colID int64) *model.ColumnInfo {
	for _, col := range tbl.Columns {
		if col.ID == colID {
			return col
		}
	}
	return nil
}
