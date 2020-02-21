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
	idxVals      []types.Datum
	startTS      uint64
	snapshot     kv.Snapshot
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
	snapshotTS := e.startTS
	if e.lock {
		snapshotTS = e.ctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	}
	var err error
	e.snapshot, err = e.ctx.GetStore().GetSnapshot(kv.Version{Ver: snapshotTS})
	if err != nil {
		return err
	}
	if e.ctx.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		e.snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
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
	if txn != nil && txn.Valid() && !txn.IsReadOnly() {
		// We cannot use txn.Get directly here because the snapshot in txn and the snapshot of e.snapshot may be
		// different for pessimistic transaction.
		val, err = txn.GetMemBuffer().Get(ctx, key)
		if err == nil {
			return val, err
		}
		if !kv.IsErrNotFound(err) {
			return nil, err
		}
		// fallthrough to snapshot get.
	}
	return e.snapshot.Get(ctx, key)
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
			idxVals[i].SetString(str, colInfo.FieldType.Collate, colInfo.Flen)
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
