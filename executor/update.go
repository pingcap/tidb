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
	"fmt"
	"runtime/trace"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
)

// UpdateExec represents a new update executor.
type UpdateExec struct {
	baseExecutor

	OrderedList []*expression.Assignment

	// updatedRowKeys is a map for unique (Table, handle) pair.
	// The value is true if the row is changed, or false otherwise
	updatedRowKeys map[int64]*kv.HandleMap
	tblID2table    map[int64]table.Table

	matched uint64 // a counter of matched rows during update
	// tblColPosInfos stores relationship between column ordinal to its table handle.
	// the columns ordinals is present in ordinal range format, @see plannercore.TblColPosInfos
	tblColPosInfos            plannercore.TblColPosInfoSlice
	evalBuffer                chunk.MutRow
	allAssignmentsAreConstant bool
	drained                   bool
	memTracker                *memory.Tracker
}

func (e *UpdateExec) exec(ctx context.Context, schema *expression.Schema, row, newData []types.Datum) error {
	defer trace.StartRegion(ctx, "UpdateExec").End()
	assignFlag, err := plannercore.GetUpdateColumns(e.ctx, e.OrderedList, schema.Len())
	if err != nil {
		return err
	}
	if e.updatedRowKeys == nil {
		e.updatedRowKeys = make(map[int64]*kv.HandleMap)
	}
	for _, content := range e.tblColPosInfos {
		tbl := e.tblID2table[content.TblID]
		if e.updatedRowKeys[content.TblID] == nil {
			e.updatedRowKeys[content.TblID] = kv.NewHandleMap()
		}
		var handle kv.Handle
		handle, err = content.HandleCols.BuildHandleByDatums(row)
		if err != nil {
			return err
		}

		oldData := row[content.Start:content.End]
		newTableData := newData[content.Start:content.End]
		updatable := false
		flags := assignFlag[content.Start:content.End]
		for _, flag := range flags {
			if flag {
				updatable = true
				break
			}
		}
		if !updatable {
			// If there's nothing to update, we can just skip current row
			continue
		}
		var changed bool
		v, ok := e.updatedRowKeys[content.TblID].Get(handle)
		if !ok {
			// Row is matched for the first time, increment `matched` counter
			e.matched++
		} else {
			changed = v.(bool)
		}
		if changed {
			// Each matched row is updated once, even if it matches the conditions multiple times.
			continue
		}

		// Update row
		changed, err1 := updateRecord(ctx, e.ctx, handle, oldData, newTableData, flags, tbl, false, e.memTracker)
		if err1 == nil {
			e.updatedRowKeys[content.TblID].Set(handle, changed)
			continue
		}

		sc := e.ctx.GetSessionVars().StmtCtx
		if kv.ErrKeyExists.Equal(err1) && sc.DupKeyAsWarning {
			sc.AppendWarning(err1)
			continue
		}
		return err1
	}
	return nil
}

// canNotUpdate checks the handle of a record to decide whether that record
// can not be updated. The handle is NULL only when it is the inner side of an
// outer join: the outer row can not match any inner rows, and in this scenario
// the inner handle field is filled with a NULL value.
//
// This fixes: https://github.com/pingcap/tidb/issues/7176.
func (e *UpdateExec) canNotUpdate(handle types.Datum) bool {
	return handle.IsNull()
}

// Next implements the Executor Next interface.
func (e *UpdateExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.drained {
		numRows, err := e.updateRows(ctx)
		if err != nil {
			return err
		}
		e.drained = true
		e.ctx.GetSessionVars().StmtCtx.AddRecordRows(uint64(numRows))
	}
	return nil
}

func (e *UpdateExec) updateRows(ctx context.Context) (int, error) {
	fields := retTypes(e.children[0])
	colsInfo := make([]*table.Column, len(fields))
	for _, content := range e.tblColPosInfos {
		tbl := e.tblID2table[content.TblID]
		for i, c := range tbl.WritableCols() {
			colsInfo[content.Start+i] = c
		}
	}
	globalRowIdx := 0
	chk := newFirstChunk(e.children[0])
	if !e.allAssignmentsAreConstant {
		e.evalBuffer = chunk.MutRowFromTypes(fields)
	}
	composeFunc := e.fastComposeNewRow
	if !e.allAssignmentsAreConstant {
		composeFunc = e.composeNewRow
	}
	memUsageOfChk := int64(0)
	totalNumRows := 0
	for {
		e.memTracker.Consume(-memUsageOfChk)
		err := Next(ctx, e.children[0], chk)
		if err != nil {
			return 0, err
		}

		if chk.NumRows() == 0 {
			break
		}
		memUsageOfChk = chk.MemoryUsage()
		e.memTracker.Consume(memUsageOfChk)
		for rowIdx := 0; rowIdx < chk.NumRows(); rowIdx++ {
			chunkRow := chk.GetRow(rowIdx)
			datumRow := chunkRow.GetDatumRow(fields)
			newRow, err1 := composeFunc(globalRowIdx, datumRow, colsInfo)
			if err1 != nil {
				return 0, err1
			}
			if err := e.exec(ctx, e.children[0].Schema(), datumRow, newRow); err != nil {
				return 0, err
			}
		}
		totalNumRows += chk.NumRows()
		chk = chunk.Renew(chk, e.maxChunkSize)
	}
	return totalNumRows, nil
}

func (e *UpdateExec) handleErr(colName model.CIStr, rowIdx int, err error) error {
	if err == nil {
		return nil
	}

	if types.ErrDataTooLong.Equal(err) {
		return resetErrDataTooLong(colName.O, rowIdx+1, err)
	}

	if types.ErrOverflow.Equal(err) {
		return types.ErrWarnDataOutOfRange.GenWithStackByArgs(colName.O, rowIdx+1)
	}

	return err
}

func (e *UpdateExec) fastComposeNewRow(rowIdx int, oldRow []types.Datum, cols []*table.Column) ([]types.Datum, error) {
	newRowData := types.CloneRow(oldRow)
	for _, assign := range e.OrderedList {
		handleIdx, handleFound := e.tblColPosInfos.FindHandle(assign.Col.Index)
		if handleFound && e.canNotUpdate(oldRow[handleIdx]) {
			continue
		}

		con := assign.Expr.(*expression.Constant)
		val, err := con.Eval(emptyRow)
		if err = e.handleErr(assign.ColName, rowIdx, err); err != nil {
			return nil, err
		}

		// info of `_tidb_rowid` column is nil.
		// No need to cast `_tidb_rowid` column value.
		if cols[assign.Col.Index] != nil {
			val, err = table.CastValue(e.ctx, val, cols[assign.Col.Index].ColumnInfo, false, false)
			if err = e.handleErr(assign.ColName, rowIdx, err); err != nil {
				return nil, err
			}
		}

		val.Copy(&newRowData[assign.Col.Index])
	}
	return newRowData, nil
}

func (e *UpdateExec) composeNewRow(rowIdx int, oldRow []types.Datum, cols []*table.Column) ([]types.Datum, error) {
	newRowData := types.CloneRow(oldRow)
	e.evalBuffer.SetDatums(newRowData...)
	for _, assign := range e.OrderedList {
		handleIdx, handleFound := e.tblColPosInfos.FindHandle(assign.Col.Index)
		if handleFound && e.canNotUpdate(oldRow[handleIdx]) {
			continue
		}
		val, err := assign.Expr.Eval(e.evalBuffer.ToRow())
		if err = e.handleErr(assign.ColName, rowIdx, err); err != nil {
			return nil, err
		}

		// info of `_tidb_rowid` column is nil.
		// No need to cast `_tidb_rowid` column value.
		if cols[assign.Col.Index] != nil {
			val, err = table.CastValue(e.ctx, val, cols[assign.Col.Index].ColumnInfo, false, false)
			if err = e.handleErr(assign.ColName, rowIdx, err); err != nil {
				return nil, err
			}
		}

		val.Copy(&newRowData[assign.Col.Index])
		e.evalBuffer.SetDatum(assign.Col.Index, val)
	}
	return newRowData, nil
}

// Close implements the Executor Close interface.
func (e *UpdateExec) Close() error {
	e.setMessage()
	return e.children[0].Close()
}

// Open implements the Executor Open interface.
func (e *UpdateExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	return e.children[0].Open(ctx)
}

// setMessage sets info message(ERR_UPDATE_INFO) generated by UPDATE statement
func (e *UpdateExec) setMessage() {
	stmtCtx := e.ctx.GetSessionVars().StmtCtx
	numMatched := e.matched
	numChanged := stmtCtx.UpdatedRows()
	numWarnings := stmtCtx.WarningCount()
	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUpdateInfo], numMatched, numChanged, numWarnings)
	stmtCtx.SetMessage(msg)
}
