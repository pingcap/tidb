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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

// UpdateExec represents a new update executor.
type UpdateExec struct {
	baseExecutor

	SelectExec  Executor
	OrderedList []*expression.Assignment

	// updatedRowKeys is a map for unique (Table, handle) pair.
	updatedRowKeys map[int64]map[int64]struct{}
	tblID2table    map[int64]table.Table

	rows        [][]types.Datum // The rows fetched from TableExec.
	newRowsData [][]types.Datum // The new values to be set.
	fetched     bool
	cursor      int
	// columns2Handle stores relationship between column ordinal to its table handle.
	// the columns ordinals is present in ordinal range format, @see executor.cols2Handle
	columns2Handle cols2HandleSlice
}

func (e *UpdateExec) exec(schema *expression.Schema) ([]types.Datum, error) {
	assignFlag, err := e.getUpdateColumns(schema.Len())
	if err != nil {
		return nil, errors.Trace(err)
	}
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	if e.updatedRowKeys == nil {
		e.updatedRowKeys = make(map[int64]map[int64]struct{})
	}
	row := e.rows[e.cursor]
	newData := e.newRowsData[e.cursor]
	for id, cols := range schema.TblID2Handle {
		tbl := e.tblID2table[id]
		if e.updatedRowKeys[id] == nil {
			e.updatedRowKeys[id] = make(map[int64]struct{})
		}
		for _, col := range cols {
			offset := getTableOffset(schema, col)
			end := offset + len(tbl.WritableCols())
			handleDatum := row[col.Index]
			if e.canNotUpdate(handleDatum) {
				continue
			}
			handle := row[col.Index].GetInt64()
			oldData := row[offset:end]
			newTableData := newData[offset:end]
			flags := assignFlag[offset:end]
			_, ok := e.updatedRowKeys[id][handle]
			if ok {
				// Each matched row is updated once, even if it matches the conditions multiple times.
				continue
			}

			// Update row
			changed, _, _, _, err1 := updateRecord(e.ctx, handle, oldData, newTableData, flags, tbl, false)
			if err1 == nil {
				if changed {
					e.updatedRowKeys[id][handle] = struct{}{}
				}
				continue
			}

			sc := e.ctx.GetSessionVars().StmtCtx
			if kv.ErrKeyExists.Equal(err1) && sc.DupKeyAsWarning {
				sc.AppendWarning(err1)
				continue
			}
			return nil, errors.Trace(err1)
		}
	}
	e.cursor++
	return []types.Datum{}, nil
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
func (e *UpdateExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if !e.fetched {
		err := e.fetchChunkRows(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		e.fetched = true

		for {
			row, err := e.exec(e.children[0].Schema())
			if err != nil {
				return errors.Trace(err)
			}

			// once "row == nil" there is no more data waiting to be updated,
			// the execution of UpdateExec is finished.
			if row == nil {
				break
			}
		}
	}

	return nil
}

func (e *UpdateExec) fetchChunkRows(ctx context.Context) error {
	fields := e.children[0].retTypes()
	globalRowIdx := 0
	for {
		chk := e.children[0].newChunk()
		err := e.children[0].Next(ctx, chk)
		if err != nil {
			return errors.Trace(err)
		}

		if chk.NumRows() == 0 {
			break
		}

		for rowIdx := 0; rowIdx < chk.NumRows(); rowIdx++ {
			chunkRow := chk.GetRow(rowIdx)
			datumRow := chunkRow.GetDatumRow(fields)
			newRow, err1 := e.composeNewRow(globalRowIdx, datumRow)
			if err1 != nil {
				return errors.Trace(err1)
			}
			e.rows = append(e.rows, datumRow)
			e.newRowsData = append(e.newRowsData, newRow)
			globalRowIdx++
		}
	}
	return nil
}

func (e *UpdateExec) handleErr(colName model.CIStr, rowIdx int, err error) error {
	if err == nil {
		return nil
	}

	if types.ErrDataTooLong.Equal(err) {
		return resetErrDataTooLong(colName.O, rowIdx+1, err)
	}

	return errors.Trace(err)
}

func (e *UpdateExec) composeNewRow(rowIdx int, oldRow []types.Datum) ([]types.Datum, error) {
	newRowData := types.CopyRow(oldRow)
	for _, assign := range e.OrderedList {
		handleIdx, handleFound := e.columns2Handle.findHandle(int32(assign.Col.Index))
		if handleFound && e.canNotUpdate(oldRow[handleIdx]) {
			continue
		}
		val, err := assign.Expr.Eval(chunk.MutRowFromDatums(newRowData).ToRow())

		if err1 := e.handleErr(assign.Col.ColName, rowIdx, err); err1 != nil {
			return nil, errors.Trace(err1)
		}
		newRowData[assign.Col.Index] = val
	}
	return newRowData, nil
}

// Close implements the Executor Close interface.
func (e *UpdateExec) Close() error {
	return e.SelectExec.Close()
}

// Open implements the Executor Open interface.
func (e *UpdateExec) Open(ctx context.Context) error {
	return e.SelectExec.Open(ctx)
}

func (e *UpdateExec) getUpdateColumns(schemaLen int) ([]bool, error) {
	assignFlag := make([]bool, schemaLen)
	for _, v := range e.OrderedList {
		idx := v.Col.Index
		assignFlag[idx] = true
	}
	return assignFlag, nil
}
