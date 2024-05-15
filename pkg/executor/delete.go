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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

// DeleteExec represents a delete executor.
// See https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteExec struct {
	exec.BaseExecutor

	IsMultiTable bool
	tblID2Table  map[int64]table.Table

	// tblColPosInfos stores relationship between column ordinal to its table handle.
	// the columns ordinals is present in ordinal range format, @see plannercore.TblColPosInfos
	tblColPosInfos plannercore.TblColPosInfoSlice
	memTracker     *memory.Tracker
	// fkChecks contains the foreign key checkers. the map is tableID -> []*FKCheckExec
	fkChecks map[int64][]*FKCheckExec
	// fkCascades contains the foreign key cascade. the map is tableID -> []*FKCascadeExec
	fkCascades map[int64][]*FKCascadeExec
}

// Next implements the Executor Next interface.
func (e *DeleteExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.IsMultiTable {
		return e.deleteMultiTablesByChunk(ctx)
	}
	return e.deleteSingleTableByChunk(ctx)
}

func (e *DeleteExec) deleteOneRow(tbl table.Table, handleCols util.HandleCols, isExtraHandle bool, row []types.Datum) error {
	end := len(row)
	if isExtraHandle {
		end--
	}
	handle, err := handleCols.BuildHandleByDatums(row)
	if err != nil {
		return err
	}
	err = e.removeRow(e.Ctx(), tbl, handle, row[:end])
	if err != nil {
		return err
	}
	return nil
}

func (e *DeleteExec) deleteSingleTableByChunk(ctx context.Context) error {
	var (
		tbl           table.Table
		isExtrahandle bool
		handleCols    util.HandleCols
		rowCount      int
	)
	for _, info := range e.tblColPosInfos {
		tbl = e.tblID2Table[info.TblID]
		handleCols = info.HandleCols
		if !tbl.Meta().IsCommonHandle {
			isExtrahandle = handleCols.IsInt() && handleCols.GetCol(0).ID == model.ExtraHandleID
		}
	}

	batchDMLSize := e.Ctx().GetSessionVars().DMLBatchSize
	// If tidb_batch_delete is ON and not in a transaction, we could use BatchDelete mode.
	batchDelete := e.Ctx().GetSessionVars().BatchDelete && !e.Ctx().GetSessionVars().InTxn() &&
		variable.EnableBatchDML.Load() && batchDMLSize > 0
	fields := exec.RetTypes(e.Children(0))
	chk := exec.TryNewCacheChunk(e.Children(0))
	columns := e.Children(0).Schema().Columns
	if len(columns) != len(fields) {
		logutil.BgLogger().Error("schema columns and fields mismatch",
			zap.Int("len(columns)", len(columns)),
			zap.Int("len(fields)", len(fields)))
		// Should never run here, so the error code is not defined.
		return errors.New("schema columns and fields mismatch")
	}
	memUsageOfChk := int64(0)
	for {
		e.memTracker.Consume(-memUsageOfChk)
		iter := chunk.NewIterator4Chunk(chk)
		err := exec.Next(ctx, e.Children(0), chk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
		memUsageOfChk = chk.MemoryUsage()
		e.memTracker.Consume(memUsageOfChk)
		for chunkRow := iter.Begin(); chunkRow != iter.End(); chunkRow = iter.Next() {
			if batchDelete && rowCount >= batchDMLSize {
				if err := e.doBatchDelete(ctx); err != nil {
					return err
				}
				rowCount = 0
			}

			datumRow := make([]types.Datum, 0, len(fields))
			for i, field := range fields {
				if columns[i].ID == model.ExtraPidColID || columns[i].ID == model.ExtraPhysTblID {
					continue
				}

				datum := chunkRow.GetDatum(i, field)
				datumRow = append(datumRow, datum)
			}

			err = e.deleteOneRow(tbl, handleCols, isExtrahandle, datumRow)
			if err != nil {
				return err
			}
			rowCount++
		}
		chk = chunk.Renew(chk, e.MaxChunkSize())
		if txn, _ := e.Ctx().Txn(false); txn != nil {
			if err := txn.MayFlush(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *DeleteExec) doBatchDelete(ctx context.Context) error {
	txn, err := e.Ctx().Txn(false)
	if err != nil {
		return exeerrors.ErrBatchInsertFail.GenWithStack("BatchDelete failed with error: %v", err)
	}
	e.memTracker.Consume(-int64(txn.Size()))
	e.Ctx().StmtCommit(ctx)
	if err := sessiontxn.NewTxnInStmt(ctx, e.Ctx()); err != nil {
		// We should return a special error for batch insert.
		return exeerrors.ErrBatchInsertFail.GenWithStack("BatchDelete failed with error: %v", err)
	}
	return nil
}

func (e *DeleteExec) composeTblRowMap(tblRowMap tableRowMapType, colPosInfos []plannercore.TblColPosInfo, joinedRow []types.Datum) error {
	// iterate all the joined tables, and got the corresponding rows in joinedRow.
	for _, info := range colPosInfos {
		if unmatchedOuterRow(info, joinedRow) {
			continue
		}
		if tblRowMap[info.TblID] == nil {
			tblRowMap[info.TblID] = kv.NewMemAwareHandleMap[[]types.Datum]()
		}
		handle, err := info.HandleCols.BuildHandleByDatums(joinedRow)
		if err != nil {
			return err
		}
		// tblRowMap[info.TblID][handle] hold the row datas binding to this table and this handle.
		row, exist := tblRowMap[info.TblID].Get(handle)
		if !exist {
			row = make([]types.Datum, info.End-info.Start)
		}
		for i, d := range joinedRow[info.Start:info.End] {
			d.Copy(&row[i])
		}
		memDelta := tblRowMap[info.TblID].Set(handle, row)
		if !exist {
			memDelta += types.EstimatedMemUsage(joinedRow, 1)
			memDelta += int64(handle.ExtraMemSize())
		}
		e.memTracker.Consume(memDelta)
	}
	return nil
}

func (e *DeleteExec) deleteMultiTablesByChunk(ctx context.Context) error {
	colPosInfos := e.tblColPosInfos
	tblRowMap := make(tableRowMapType)
	fields := exec.RetTypes(e.Children(0))
	chk := exec.TryNewCacheChunk(e.Children(0))
	memUsageOfChk := int64(0)
	joinedDatumRowBuffer := make([]types.Datum, len(fields))
	for {
		e.memTracker.Consume(-memUsageOfChk)
		iter := chunk.NewIterator4Chunk(chk)
		err := exec.Next(ctx, e.Children(0), chk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
		memUsageOfChk = chk.MemoryUsage()
		e.memTracker.Consume(memUsageOfChk)

		for joinedChunkRow := iter.Begin(); joinedChunkRow != iter.End(); joinedChunkRow = iter.Next() {
			joinedDatumRowBuffer = joinedChunkRow.GetDatumRowWithBuffer(fields, joinedDatumRowBuffer)
			err := e.composeTblRowMap(tblRowMap, colPosInfos, joinedDatumRowBuffer)
			if err != nil {
				return err
			}
		}
		chk = exec.TryNewCacheChunk(e.Children(0))
		if txn, _ := e.Ctx().Txn(false); txn != nil {
			if err := txn.MayFlush(); err != nil {
				return err
			}
		}
	}

	return e.removeRowsInTblRowMap(tblRowMap)
}

func (e *DeleteExec) removeRowsInTblRowMap(tblRowMap tableRowMapType) error {
	for id, rowMap := range tblRowMap {
		var err error
		rowMap.Range(func(h kv.Handle, val []types.Datum) bool {
			err = e.removeRow(e.Ctx(), e.tblID2Table[id], h, val)
			return err == nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *DeleteExec) removeRow(ctx sessionctx.Context, t table.Table, h kv.Handle, data []types.Datum) error {
	err := t.RemoveRecord(ctx.GetTableCtx(), h, data)
	if err != nil {
		return err
	}
	tid := t.Meta().ID
	err = onRemoveRowForFK(ctx, data, e.fkChecks[tid], e.fkCascades[tid])
	if err != nil {
		return err
	}
	ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	return nil
}

func onRemoveRowForFK(ctx sessionctx.Context, data []types.Datum, fkChecks []*FKCheckExec, fkCascades []*FKCascadeExec) error {
	sc := ctx.GetSessionVars().StmtCtx
	for _, fkc := range fkChecks {
		err := fkc.deleteRowNeedToCheck(sc, data)
		if err != nil {
			return err
		}
	}
	for _, fkc := range fkCascades {
		err := fkc.onDeleteRow(sc, data)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *DeleteExec) Close() error {
	defer e.memTracker.ReplaceBytesUsed(0)
	return exec.Close(e.Children(0))
}

// Open implements the Executor Open interface.
func (e *DeleteExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	return exec.Open(ctx, e.Children(0))
}

// GetFKChecks implements WithForeignKeyTrigger interface.
func (e *DeleteExec) GetFKChecks() []*FKCheckExec {
	fkChecks := []*FKCheckExec{}
	for _, fkcs := range e.fkChecks {
		fkChecks = append(fkChecks, fkcs...)
	}
	return fkChecks
}

// GetFKCascades implements WithForeignKeyTrigger interface.
func (e *DeleteExec) GetFKCascades() []*FKCascadeExec {
	fkCascades := []*FKCascadeExec{}
	for _, fkcs := range e.fkCascades {
		fkCascades = append(fkCascades, fkcs...)
	}
	return fkCascades
}

// HasFKCascades implements WithForeignKeyTrigger interface.
func (e *DeleteExec) HasFKCascades() bool {
	return len(e.fkCascades) > 0
}

// tableRowMapType is a map for unique (Table, Row) pair. key is the tableID.
// the key in map[int64]Row is the joined table handle, which represent a unique reference row.
// the value in map[int64]Row is the deleting row.
type tableRowMapType map[int64]*kv.MemAwareHandleMap[[]types.Datum]
