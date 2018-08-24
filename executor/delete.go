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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

// DeleteExec represents a delete executor.
// See https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteExec struct {
	baseExecutor

	SelectExec Executor

	Tables       []*ast.TableName
	IsMultiTable bool
	tblID2Table  map[int64]table.Table
	// tblMap is the table map value is an array which contains table aliases.
	// Table ID may not be unique for deleting multiple tables, for statements like
	// `delete from t as t1, t as t2`, the same table has two alias, we have to identify a table
	// by its alias instead of ID.
	tblMap map[int64][]*ast.TableName

	finished bool
}

// Next implements the Executor Next interface.
func (e *DeleteExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.finished {
		return nil
	}
	defer func() {
		e.finished = true
	}()

	if e.IsMultiTable {
		return errors.Trace(e.deleteMultiTablesByChunk(ctx))
	}
	return errors.Trace(e.deleteSingleTableByChunk(ctx))
}

// matchingDeletingTable checks whether this column is from the table which is in the deleting list.
func (e *DeleteExec) matchingDeletingTable(tableID int64, col *expression.Column) bool {
	names, ok := e.tblMap[tableID]
	if !ok {
		return false
	}
	for _, n := range names {
		if (col.DBName.L == "" || col.DBName.L == n.Schema.L) && col.TblName.L == n.Name.L {
			return true
		}
	}
	return false
}

func (e *DeleteExec) deleteOneRow(tbl table.Table, handleCol *expression.Column, row []types.Datum) error {
	end := len(row)
	if handleIsExtra(handleCol) {
		end--
	}
	handle := row[handleCol.Index].GetInt64()
	err := e.removeRow(e.ctx, tbl, handle, row[:end])
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (e *DeleteExec) deleteSingleTableByChunk(ctx context.Context) error {
	var (
		id        int64
		tbl       table.Table
		handleCol *expression.Column
		rowCount  int
	)
	for i, t := range e.tblID2Table {
		id, tbl = i, t
		handleCol = e.children[0].Schema().TblID2Handle[id][0]
		break
	}

	// If tidb_batch_delete is ON and not in a transaction, we could use BatchDelete mode.
	batchDelete := e.ctx.GetSessionVars().BatchDelete && !e.ctx.GetSessionVars().InTxn()
	batchDMLSize := e.ctx.GetSessionVars().DMLBatchSize
	fields := e.children[0].retTypes()
	for {
		chk := e.children[0].newChunk()
		iter := chunk.NewIterator4Chunk(chk)

		err := e.children[0].Next(ctx, chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}

		for chunkRow := iter.Begin(); chunkRow != iter.End(); chunkRow = iter.Next() {
			if batchDelete && rowCount >= batchDMLSize {
				e.ctx.StmtCommit()
				if err = e.ctx.NewTxn(); err != nil {
					// We should return a special error for batch insert.
					return ErrBatchInsertFail.Gen("BatchDelete failed with error: %v", err)
				}
				rowCount = 0
			}

			datumRow := chunkRow.GetDatumRow(fields)
			err = e.deleteOneRow(tbl, handleCol, datumRow)
			if err != nil {
				return errors.Trace(err)
			}
			rowCount++
		}
	}

	return nil
}

func (e *DeleteExec) initialMultiTableTblMap() {
	e.tblMap = make(map[int64][]*ast.TableName, len(e.Tables))
	for _, t := range e.Tables {
		e.tblMap[t.TableInfo.ID] = append(e.tblMap[t.TableInfo.ID], t)
	}
}

func (e *DeleteExec) getColPosInfos(schema *expression.Schema) []tblColPosInfo {
	var colPosInfos []tblColPosInfo
	// Extract the columns' position information of this table in the delete's schema, together with the table id
	// and its handle's position in the schema.
	for id, cols := range schema.TblID2Handle {
		tbl := e.tblID2Table[id]
		for _, col := range cols {
			if !e.matchingDeletingTable(id, col) {
				continue
			}
			offset := getTableOffset(schema, col)
			end := offset + len(tbl.Cols())
			colPosInfos = append(colPosInfos, tblColPosInfo{tblID: id, colBeginIndex: offset, colEndIndex: end, handleIndex: col.Index})
		}
	}
	return colPosInfos
}

func (e *DeleteExec) composeTblRowMap(tblRowMap tableRowMapType, colPosInfos []tblColPosInfo, joinedRow []types.Datum) {
	// iterate all the joined tables, and got the copresonding rows in joinedRow.
	for _, info := range colPosInfos {
		if tblRowMap[info.tblID] == nil {
			tblRowMap[info.tblID] = make(map[int64][]types.Datum)
		}
		handle := joinedRow[info.handleIndex].GetInt64()
		// tblRowMap[info.tblID][handle] hold the row datas binding to this table and this handle.
		tblRowMap[info.tblID][handle] = joinedRow[info.colBeginIndex:info.colEndIndex]
	}
}

func (e *DeleteExec) deleteMultiTablesByChunk(ctx context.Context) error {
	if len(e.Tables) == 0 {
		return nil
	}

	e.initialMultiTableTblMap()
	colPosInfos := e.getColPosInfos(e.children[0].Schema())
	tblRowMap := make(tableRowMapType)
	fields := e.children[0].retTypes()
	for {
		chk := e.children[0].newChunk()
		iter := chunk.NewIterator4Chunk(chk)

		err := e.children[0].Next(ctx, chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}

		for joinedChunkRow := iter.Begin(); joinedChunkRow != iter.End(); joinedChunkRow = iter.Next() {
			joinedDatumRow := joinedChunkRow.GetDatumRow(fields)
			e.composeTblRowMap(tblRowMap, colPosInfos, joinedDatumRow)
		}
	}

	return errors.Trace(e.removeRowsInTblRowMap(tblRowMap))
}

func (e *DeleteExec) removeRowsInTblRowMap(tblRowMap tableRowMapType) error {
	for id, rowMap := range tblRowMap {
		for handle, data := range rowMap {
			err := e.removeRow(e.ctx, e.tblID2Table[id], handle, data)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func (e *DeleteExec) removeRow(ctx sessionctx.Context, t table.Table, h int64, data []types.Datum) error {
	err := t.RemoveRecord(ctx, h, data)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	colSize := make(map[int64]int64)
	for id, col := range t.Cols() {
		val := -int64(len(data[id].GetBytes()))
		if val != 0 {
			colSize[col.ID] = val
		}
	}
	ctx.GetSessionVars().TxnCtx.UpdateDeltaForTable(t.Meta().ID, -1, 1, colSize)
	return nil
}

// Close implements the Executor Close interface.
func (e *DeleteExec) Close() error {
	return e.SelectExec.Close()
}

// Open implements the Executor Open interface.
func (e *DeleteExec) Open(ctx context.Context) error {
	return e.SelectExec.Open(ctx)
}

type tblColPosInfo struct {
	tblID         int64
	colBeginIndex int
	colEndIndex   int
	handleIndex   int
}

// tableRowMapType is a map for unique (Table, Row) pair. key is the tableID.
// the key in map[int64]Row is the joined table handle, which represent a unique reference row.
// the value in map[int64]Row is the deleting row.
type tableRowMapType map[int64]map[int64][]types.Datum
