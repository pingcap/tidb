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
)

func (b *executorBuilder) buildPointGet(p *plannercore.PointGetPlan) Executor {
	startTS, err := b.getStartTS()
	if err != nil {
		b.err = err
		return nil
	}
	e := &PointGetExecutor{
		baseExecutor: newBaseExecutor(b.ctx, p.Schema(), p.ExplainID()),
		tblInfo:      p.TblInfo,
		idxInfo:      p.IndexInfo,
		idxVals:      p.IndexValues,
		handle:       p.Handle,
		startTS:      startTS,
		lock:         p.Lock,
		lockWaitTime: p.LockWaitTime,
	}
	b.isSelectForUpdate = p.IsForUpdate
	e.base().initCap = 1
	e.base().maxChunkSize = 1
	return e
}

// PointGetExecutor executes point select query.
type PointGetExecutor struct {
	baseExecutor

	tblInfo      *model.TableInfo
	handle       int64
	idxInfo      *model.IndexInfo
	idxVals      []types.Datum
	startTS      uint64
	snapshot     kv.Snapshot
	done         bool
	lock         bool
	lockWaitTime int64
	snapValExist bool
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
	if e.idxInfo != nil {
		idxKey, err1 := e.encodeIndexKey()
		if err1 != nil && !kv.ErrNotExist.Equal(err1) {
			return err1
		}

		handleVal, err1 := e.get(idxKey)
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

	key := tablecodec.EncodeRowKeyWithHandle(e.tblInfo.ID, e.handle)
	val, err := e.get(key)
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
	return e.decodeRowValToChunk(val, req)
}

func (e *PointGetExecutor) lockKeyIfNeeded(ctx context.Context, key []byte) error {
	if e.lock {
		lockCtx := newLockCtx(e.ctx.GetSessionVars(), e.lockWaitTime)
		lockCtx.PointGetLock = &e.snapValExist
		return doLockKeys(ctx, e.ctx, lockCtx, key)
	}
	return nil
}

func (e *PointGetExecutor) encodeIndexKey() (_ []byte, err error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	for i := range e.idxVals {
		colInfo := e.tblInfo.Columns[e.idxInfo.Columns[i].Offset]
		if colInfo.Tp == mysql.TypeString || colInfo.Tp == mysql.TypeVarString || colInfo.Tp == mysql.TypeVarchar {
			var str string
			str, err = e.idxVals[i].ToString()
			e.idxVals[i].SetString(str)
		} else {
			e.idxVals[i], err = table.CastValue(e.ctx, e.idxVals[i], colInfo)
		}
		if err != nil {
			return nil, err
		}
	}

	encodedIdxVals, err := codec.EncodeKey(sc, nil, e.idxVals...)
	if err != nil {
		return nil, err
	}
	return tablecodec.EncodeIndexSeekKey(e.tblInfo.ID, e.idxInfo.ID, encodedIdxVals), nil
}

func (e *PointGetExecutor) get(key kv.Key) (val []byte, err error) {
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if txn != nil && txn.Valid() && !txn.IsReadOnly() {
		// We cannot use txn.Get directly here because the snapshot in txn and the snapshot of e.snapshot may be
		// different for pessimistic transaction.
		val, err = txn.GetMemBuffer().Get(key)
		if err == nil {
			return val, err
		}
		if !kv.IsErrNotFound(err) {
			return nil, err
		}
		// fallthrough to snapshot get.
	}
	snapVal, snapErr := e.snapshot.Get(key)
	if snapErr == nil && len(snapVal) > 0 {
		e.snapValExist = true
	}
	return snapVal, snapErr
}

func (e *PointGetExecutor) decodeRowValToChunk(rowVal []byte, chk *chunk.Chunk) error {
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
		if e.tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.RetType.Flag) {
			chk.AppendInt64(i, e.handle)
			continue
		}
		if col.ID == model.ExtraHandleID {
			chk.AppendInt64(i, e.handle)
			continue
		}
		cutPos := colID2CutPos[col.ID]
		if len(cutVals[cutPos]) == 0 {
			colInfo := getColInfoByID(e.tblInfo, col.ID)
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
