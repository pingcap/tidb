// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

var (
	_ Executor = &UpdateExec{}
	_ Executor = &DeleteExec{}
	_ Executor = &InsertExec{}
	_ Executor = &ReplaceExec{}
	_ Executor = &LoadData{}
)

// updateRecord updates the row specified by the handle `h`, from `oldData` to `newData`.
// `modified` means which columns are really modified. It's used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
// ignoreErr indicate that update statement has the `IGNORE` modifier, in this situation, update statement will not update
// the keys which cause duplicate conflicts and ignore the error.
// The return values:
//     1. changed (bool) : does the update really change the row values. e.g. update set i = 1 where i = 1;
//     2. handleChanged (bool) : is the handle changed after the update.
//     3. newHandle (int64) : if handleChanged == true, the newHandle means the new handle after update.
//     4. err (error) : error in the update.
func updateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, modified []bool, t table.Table,
	onDup, ignoreErr bool) (bool, bool, int64, error) {
	var sc = ctx.GetSessionVars().StmtCtx
	var changed, handleChanged = false, false
	// onUpdateSpecified is for "UPDATE SET ts_field = old_value", the
	// timestamp field is explicitly set, but not changed in fact.
	var onUpdateSpecified = make(map[int]bool)
	var newHandle int64

	// We can iterate on public columns not writable columns,
	// because all of them are sorted by their `Offset`, which
	// causes all writable columns are after public columns.
	for i, col := range t.Cols() {
		if modified[i] {
			// Cast changed fields with respective columns.
			v, err := table.CastValue(ctx, newData[i], col.ToInfo())
			if err != nil {
				return false, handleChanged, newHandle, errors.Trace(err)
			}
			newData[i] = v
		}

		// Rebase auto increment id if the field is changed.
		if mysql.HasAutoIncrementFlag(col.Flag) {
			if newData[i].IsNull() {
				return false, handleChanged, newHandle, errors.Errorf("Column '%v' cannot be null", col.Name.O)
			}
			val, errTI := newData[i].ToInt64(sc)
			if errTI != nil {
				return false, handleChanged, newHandle, errors.Trace(errTI)
			}
			err := t.RebaseAutoID(ctx, val, true)
			if err != nil {
				return false, handleChanged, newHandle, errors.Trace(err)
			}
		}
		cmp, err := newData[i].CompareDatum(sc, &oldData[i])
		if err != nil {
			return false, handleChanged, newHandle, errors.Trace(err)
		}
		if cmp != 0 {
			changed = true
			modified[i] = true
			if col.IsPKHandleColumn(t.Meta()) {
				handleChanged = true
				newHandle = newData[i].GetInt64()
			}
		} else {
			if mysql.HasOnUpdateNowFlag(col.Flag) && modified[i] {
				// It's for "UPDATE t SET ts = ts" and ts is a timestamp.
				onUpdateSpecified[i] = true
			}
			modified[i] = false
		}
	}

	// Check the not-null constraints.
	err := table.CheckNotNull(t.Cols(), newData)
	if err != nil {
		return false, handleChanged, newHandle, errors.Trace(err)
	}

	if !changed {
		// See https://dev.mysql.com/doc/refman/5.7/en/mysql-real-connect.html  CLIENT_FOUND_ROWS
		if ctx.GetSessionVars().ClientCapability&mysql.ClientFoundRows > 0 {
			sc.AddAffectedRows(1)
		}
		return false, handleChanged, newHandle, nil
	}

	// Fill values into on-update-now fields, only if they are really changed.
	for i, col := range t.Cols() {
		if mysql.HasOnUpdateNowFlag(col.Flag) && !modified[i] && !onUpdateSpecified[i] {
			v, errGT := expression.GetTimeValue(ctx, strings.ToUpper(ast.CurrentTimestamp), col.Tp, col.Decimal)
			if errGT != nil {
				return false, handleChanged, newHandle, errors.Trace(errGT)
			}
			newData[i] = v
			modified[i] = true
		}
	}

	if handleChanged {
		skipHandleCheck := false
		if ignoreErr {
			// if the new handle exists. `UPDATE IGNORE` will avoid removing record, and do nothing.
			if err = tables.CheckHandleExists(ctx, t, newHandle); err != nil {
				return false, handleChanged, newHandle, errors.Trace(err)
			}
			skipHandleCheck = true
		}
		err = t.RemoveRecord(ctx, h, oldData)
		if err != nil {
			return false, handleChanged, newHandle, errors.Trace(err)
		}
		newHandle, err = t.AddRecord(ctx, newData, skipHandleCheck)
	} else {
		// Update record to new value and update index.
		err = t.UpdateRecord(ctx, h, oldData, newData, modified)
	}
	if err != nil {
		return false, handleChanged, newHandle, errors.Trace(err)
	}

	tid := t.Meta().ID
	ctx.StmtAddDirtyTableOP(DirtyTableDeleteRow, tid, h, nil)
	ctx.StmtAddDirtyTableOP(DirtyTableAddRow, tid, h, newData)

	if onDup {
		sc.AddAffectedRows(2)
	} else {
		sc.AddAffectedRows(1)
	}

	ctx.GetSessionVars().TxnCtx.UpdateDeltaForTable(t.Meta().ID, 0, 1)
	return true, handleChanged, newHandle, nil
}

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

type tblColPosInfo struct {
	tblID         int64
	colBeginIndex int
	colEndIndex   int
	handleIndex   int
}

// tableRowMapType is a map for unique (Table, Row) pair. key is the tableID.
// the key in map[int64]Row is the joined table handle, which represent a unique reference row.
// the value in map[int64]Row is the deleting row.
type tableRowMapType map[int64]map[int64]Row

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

func (e *DeleteExec) deleteOneRow(tbl table.Table, handleCol *expression.Column, row Row) error {
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

func (e *DeleteExec) composeTblRowMap(tblRowMap tableRowMapType, colPosInfos []tblColPosInfo, joinedRow Row) {
	// iterate all the joined tables, and got the copresonding rows in joinedRow.
	for _, info := range colPosInfos {
		if tblRowMap[info.tblID] == nil {
			tblRowMap[info.tblID] = make(map[int64]Row)
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

const (
	// DirtyTableAddRow is the constant for dirty table operation type.
	DirtyTableAddRow = iota
	// DirtyTableDeleteRow is the constant for dirty table operation type.
	DirtyTableDeleteRow
	// DirtyTableTruncate is the constant for dirty table operation type.
	DirtyTableTruncate
)

func (e *DeleteExec) removeRow(ctx sessionctx.Context, t table.Table, h int64, data []types.Datum) error {
	err := t.RemoveRecord(ctx, h, data)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.StmtAddDirtyTableOP(DirtyTableDeleteRow, t.Meta().ID, h, nil)
	ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	ctx.GetSessionVars().TxnCtx.UpdateDeltaForTable(t.Meta().ID, -1, 1)
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

// NewLoadDataInfo returns a LoadDataInfo structure, and it's only used for tests now.
func NewLoadDataInfo(ctx sessionctx.Context, row []types.Datum, tbl table.Table, cols []*table.Column) *LoadDataInfo {
	insertVal := &InsertValues{baseExecutor: newBaseExecutor(ctx, nil, "InsertValues"), Table: tbl}
	return &LoadDataInfo{
		row:       row,
		insertVal: insertVal,
		Table:     tbl,
		Ctx:       ctx,
		columns:   cols,
	}
}

// LoadDataInfo saves the information of loading data operation.
type LoadDataInfo struct {
	row       []types.Datum
	insertVal *InsertValues

	Path       string
	Table      table.Table
	FieldsInfo *ast.FieldsClause
	LinesInfo  *ast.LinesClause
	Ctx        sessionctx.Context
	columns    []*table.Column
}

// SetMaxRowsInBatch sets the max number of rows to insert in a batch.
func (e *LoadDataInfo) SetMaxRowsInBatch(limit uint64) {
	e.insertVal.maxRowsInBatch = limit
}

// getValidData returns prevData and curData that starts from starting symbol.
// If the data doesn't have starting symbol, prevData is nil and curData is curData[len(curData)-startingLen+1:].
// If curData size less than startingLen, curData is returned directly.
func (e *LoadDataInfo) getValidData(prevData, curData []byte) ([]byte, []byte) {
	startingLen := len(e.LinesInfo.Starting)
	if startingLen == 0 {
		return prevData, curData
	}

	prevLen := len(prevData)
	if prevLen > 0 {
		// starting symbol in the prevData
		idx := strings.Index(string(prevData), e.LinesInfo.Starting)
		if idx != -1 {
			return prevData[idx:], curData
		}

		// starting symbol in the middle of prevData and curData
		restStart := curData
		if len(curData) >= startingLen {
			restStart = curData[:startingLen-1]
		}
		prevData = append(prevData, restStart...)
		idx = strings.Index(string(prevData), e.LinesInfo.Starting)
		if idx != -1 {
			return prevData[idx:prevLen], curData
		}
	}

	// starting symbol in the curData
	idx := strings.Index(string(curData), e.LinesInfo.Starting)
	if idx != -1 {
		return nil, curData[idx:]
	}

	// no starting symbol
	if len(curData) >= startingLen {
		curData = curData[len(curData)-startingLen+1:]
	}
	return nil, curData
}

// getLine returns a line, curData, the next data start index and a bool value.
// If it has starting symbol the bool is true, otherwise is false.
func (e *LoadDataInfo) getLine(prevData, curData []byte) ([]byte, []byte, bool) {
	startingLen := len(e.LinesInfo.Starting)
	prevData, curData = e.getValidData(prevData, curData)
	if prevData == nil && len(curData) < startingLen {
		return nil, curData, false
	}

	prevLen := len(prevData)
	terminatedLen := len(e.LinesInfo.Terminated)
	curStartIdx := 0
	if prevLen < startingLen {
		curStartIdx = startingLen - prevLen
	}
	endIdx := -1
	if len(curData) >= curStartIdx {
		endIdx = strings.Index(string(curData[curStartIdx:]), e.LinesInfo.Terminated)
	}
	if endIdx == -1 {
		// no terminated symbol
		if len(prevData) == 0 {
			return nil, curData, true
		}

		// terminated symbol in the middle of prevData and curData
		curData = append(prevData, curData...)
		endIdx = strings.Index(string(curData[startingLen:]), e.LinesInfo.Terminated)
		if endIdx != -1 {
			nextDataIdx := startingLen + endIdx + terminatedLen
			return curData[startingLen : startingLen+endIdx], curData[nextDataIdx:], true
		}
		// no terminated symbol
		return nil, curData, true
	}

	// terminated symbol in the curData
	nextDataIdx := curStartIdx + endIdx + terminatedLen
	if len(prevData) == 0 {
		return curData[curStartIdx : curStartIdx+endIdx], curData[nextDataIdx:], true
	}

	// terminated symbol in the curData
	prevData = append(prevData, curData[:nextDataIdx]...)
	endIdx = strings.Index(string(prevData[startingLen:]), e.LinesInfo.Terminated)
	if endIdx >= prevLen {
		return prevData[startingLen : startingLen+endIdx], curData[nextDataIdx:], true
	}

	// terminated symbol in the middle of prevData and curData
	lineLen := startingLen + endIdx + terminatedLen
	return prevData[startingLen : startingLen+endIdx], curData[lineLen-prevLen:], true
}

// InsertData inserts data into specified table according to the specified format.
// If it has the rest of data isn't completed the processing, then is returns without completed data.
// If the number of inserted rows reaches the batchRows, then the second return value is true.
// If prevData isn't nil and curData is nil, there are no other data to deal with and the isEOF is true.
func (e *LoadDataInfo) InsertData(prevData, curData []byte) ([]byte, bool, error) {
	// TODO: support enclosed and escape.
	if len(prevData) == 0 && len(curData) == 0 {
		return nil, false, nil
	}

	var line []byte
	var isEOF, hasStarting, reachLimit bool
	if len(prevData) > 0 && len(curData) == 0 {
		isEOF = true
		prevData, curData = curData, prevData
	}
	rows := make([][]types.Datum, 0, e.insertVal.maxRowsInBatch)
	for len(curData) > 0 {
		line, curData, hasStarting = e.getLine(prevData, curData)
		prevData = nil

		// If it doesn't find the terminated symbol and this data isn't the last data,
		// the data can't be inserted.
		if line == nil && !isEOF {
			break
		}
		// If doesn't find starting symbol, this data can't be inserted.
		if !hasStarting {
			if isEOF {
				curData = nil
			}
			break
		}
		if line == nil && isEOF {
			line = curData[len(e.LinesInfo.Starting):]
			curData = nil
		}

		cols, err := GetFieldsFromLine(line, e.FieldsInfo)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		rows = append(rows, e.colsToRow(cols))
		e.insertVal.rowCount++
		if e.insertVal.maxRowsInBatch != 0 && e.insertVal.rowCount%e.insertVal.maxRowsInBatch == 0 {
			reachLimit = true
			log.Infof("This insert rows has reached the batch %d, current total rows %d",
				e.insertVal.maxRowsInBatch, e.insertVal.rowCount)
			break
		}
	}
	rows, err := batchMarkDupRows(e.Ctx, e.Table, rows)
	if err != nil {
		return nil, reachLimit, errors.Trace(err)
	}
	for _, row := range rows {
		e.insertData(row)
	}
	if e.insertVal.lastInsertID != 0 {
		e.insertVal.ctx.GetSessionVars().SetLastInsertID(e.insertVal.lastInsertID)
	}

	return curData, reachLimit, nil
}

// GetFieldsFromLine splits line according to fieldsInfo, this function is exported for testing.
func GetFieldsFromLine(line []byte, fieldsInfo *ast.FieldsClause) ([]string, error) {
	var sep []byte
	if fieldsInfo.Enclosed != 0 {
		if line[0] != fieldsInfo.Enclosed || line[len(line)-1] != fieldsInfo.Enclosed {
			return nil, errors.Errorf("line %s should begin and end with %c", string(line), fieldsInfo.Enclosed)
		}
		line = line[1 : len(line)-1]
		sep = make([]byte, 0, len(fieldsInfo.Terminated)+2)
		sep = append(sep, fieldsInfo.Enclosed)
		sep = append(sep, fieldsInfo.Terminated...)
		sep = append(sep, fieldsInfo.Enclosed)
	} else {
		sep = []byte(fieldsInfo.Terminated)
	}
	rawCols := bytes.Split(line, sep)
	cols := escapeCols(rawCols)
	return cols, nil
}

func escapeCols(strs [][]byte) []string {
	ret := make([]string, len(strs))
	for i, v := range strs {
		output := escape(v)
		ret[i] = string(output)
	}
	return ret
}

// escape handles escape characters when running load data statement.
// TODO: escape need to be improved, it should support ESCAPED BY to specify
// the escape character and handle \N escape.
// See http://dev.mysql.com/doc/refman/5.7/en/load-data.html
func escape(str []byte) []byte {
	pos := 0
	for i := 0; i < len(str); i++ {
		c := str[i]
		if c == '\\' && i+1 < len(str) {
			c = escapeChar(str[i+1])
			i++
		}

		str[pos] = c
		pos++
	}
	return str[:pos]
}

func escapeChar(c byte) byte {
	switch c {
	case '0':
		return 0
	case 'b':
		return '\b'
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	case 'Z':
		return 26
	case '\\':
		return '\\'
	}
	return c
}

func (e *LoadDataInfo) colsToRow(cols []string) types.DatumRow {
	for i := 0; i < len(e.row); i++ {
		if i >= len(cols) {
			e.row[i].SetString("")
			continue
		}
		e.row[i].SetString(cols[i])
	}
	row, err := e.insertVal.fillRowData(e.columns, e.row, true)
	if err != nil {
		warnLog := fmt.Sprintf("Load Data: insert data:%v failed:%v", e.row, errors.ErrorStack(err))
		e.insertVal.handleLoadDataWarnings(err, warnLog)
		return nil
	}
	return row
}

func (e *LoadDataInfo) insertData(row types.DatumRow) {
	if row == nil {
		return
	}
	_, err := e.Table.AddRecord(e.insertVal.ctx, row, false)
	if err != nil {
		warnLog := fmt.Sprintf("Load Data: insert data:%v failed:%v", row, errors.ErrorStack(err))
		e.insertVal.handleLoadDataWarnings(err, warnLog)
	}
}

func (e *InsertValues) handleLoadDataWarnings(err error, logInfo string) {
	sc := e.ctx.GetSessionVars().StmtCtx
	sc.AppendWarning(err)
	log.Warn(logInfo)
}

// LoadData represents a load data executor.
type LoadData struct {
	baseExecutor

	IsLocal      bool
	loadDataInfo *LoadDataInfo
}

// loadDataVarKeyType is a dummy type to avoid naming collision in context.
type loadDataVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k loadDataVarKeyType) String() string {
	return "load_data_var"
}

// LoadDataVarKey is a variable key for load data.
const LoadDataVarKey loadDataVarKeyType = 0

// Next implements the Executor Next interface.
func (e *LoadData) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	// TODO: support load data without local field.
	if !e.IsLocal {
		return errors.New("Load Data: don't support load data without local field")
	}
	// TODO: support lines terminated is "".
	if len(e.loadDataInfo.LinesInfo.Terminated) == 0 {
		return errors.New("Load Data: don't support load data terminated is nil")
	}

	sctx := e.loadDataInfo.insertVal.ctx
	val := sctx.Value(LoadDataVarKey)
	if val != nil {
		sctx.SetValue(LoadDataVarKey, nil)
		return errors.New("Load Data: previous load data option isn't closed normal")
	}
	if e.loadDataInfo.Path == "" {
		return errors.New("Load Data: infile path is empty")
	}
	sctx.SetValue(LoadDataVarKey, e.loadDataInfo)

	return nil
}

// Close implements the Executor Close interface.
func (e *LoadData) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *LoadData) Open(ctx context.Context) error {
	return nil
}

type defaultVal struct {
	val types.Datum
	// We evaluate the default value lazily. The valid indicates whether the val is evaluated.
	valid bool
}

// InsertValues is the data to insert.
type InsertValues struct {
	baseExecutor

	rowCount              uint64
	maxRowsInBatch        uint64
	lastInsertID          uint64
	needFillDefaultValues bool
	hasExtraHandle        bool

	SelectExec Executor

	Table     table.Table
	Columns   []*ast.ColumnName
	Lists     [][]expression.Expression
	Setlist   []*expression.Assignment
	IsPrepare bool

	GenColumns []*ast.ColumnName
	GenExprs   []expression.Expression

	// colDefaultVals is used to store casted default value.
	// Because not every insert statement needs colDefaultVals, so we will init the buffer lazily.
	colDefaultVals []defaultVal
}

// InsertExec represents an insert executor.
type InsertExec struct {
	*InsertValues

	OnDuplicate []*expression.Assignment

	Priority  mysql.PriorityEnum
	IgnoreErr bool

	finished bool
	rowCount int

	// For duplicate key update
	uniqueKeysInRows [][]keyWithDupError
	dupKeyValues     map[string][]byte
	dupOldRowValues  map[string][]byte
}

func (e *InsertExec) exec(ctx context.Context, rows [][]types.Datum) (Row, error) {
	// If tidb_batch_insert is ON and not in a transaction, we could use BatchInsert mode.
	sessVars := e.ctx.GetSessionVars()
	defer sessVars.CleanBuffers()

	e.rowCount = 0
	if !sessVars.ImportingData {
		sessVars.GetWriteStmtBufs().BufStore = kv.NewBufferStore(e.ctx.Txn(), kv.TempTxnMemBufCap)
	}

	// If `ON DUPLICATE KEY UPDATE` is specified, and no `IGNORE` keyword,
	// the to-be-insert rows will be check on duplicate keys and update to the new rows.
	if len(e.OnDuplicate) > 0 && !e.IgnoreErr {
		var err error
		rows, err = e.batchUpdateDupRows(rows, e.OnDuplicate)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	// If you use the IGNORE keyword, duplicate-key error that occurs while executing the INSERT statement are ignored.
	// For example, without IGNORE, a row that duplicates an existing UNIQUE index or PRIMARY KEY value in
	// the table causes a duplicate-key error and the statement is aborted. With IGNORE, the row is discarded and no error occurs.
	// However, if the `on duplicate update` is also specified, the duplicated row will be updated.
	// Using BatchGet in insert ignore to mark rows as duplicated before we add records to the table.
	if len(e.OnDuplicate) == 0 && e.IgnoreErr {
		var err error
		rows, err = batchMarkDupRows(e.ctx, e.Table, rows)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for _, row := range rows {
		// duplicate row will be marked as nil in batchMarkDupRows if
		// IgnoreErr is true. For IgnoreErr is false, it is a protection.
		if row == nil {
			continue
		}
		if err := e.checkBatchLimit(); err != nil {
			return nil, errors.Trace(err)
		}
		if len(e.OnDuplicate) == 0 && !e.IgnoreErr {
			e.ctx.Txn().SetOption(kv.PresumeKeyNotExists, nil)
		}
		h, err := e.Table.AddRecord(e.ctx, row, false)
		e.ctx.Txn().DelOption(kv.PresumeKeyNotExists)
		if err == nil {
			if !sessVars.ImportingData {
				e.ctx.StmtAddDirtyTableOP(DirtyTableAddRow, e.Table.Meta().ID, h, row)
			}
			e.rowCount++
			continue
		}
		if kv.ErrKeyExists.Equal(err) {
			// TODO: Use batch get to speed up `insert ignore on duplicate key update`.
			if len(e.OnDuplicate) > 0 && e.IgnoreErr {
				data, err1 := e.Table.RowWithCols(e.ctx, h, e.Table.WritableCols())
				if err1 != nil {
					return nil, errors.Trace(err1)
				}
				if _, _, _, err = e.doDupRowUpdate(h, data, row, e.OnDuplicate); err != nil {
					return nil, errors.Trace(err)
				}
				e.rowCount++
				continue
			}
		}
		return nil, errors.Trace(err)
	}

	if e.lastInsertID != 0 {
		sessVars.SetLastInsertID(e.lastInsertID)
	}
	e.finished = true
	return nil, nil
}

type keyWithDupError struct {
	isRecordKey bool
	key         kv.Key
	dupErr      error
	handle      int64
	newRowValue []byte
}

// genNewHandles generates the handles of to-be-insert rows.
func genNewHandles(ctx sessionctx.Context, t table.Table, rows [][]types.Datum) ([]int64, error) {
	handles := make([]int64, 0, len(rows))
	if t.Meta().PKIsHandle {
		var handleCol *table.Column
		for _, col := range t.Cols() {
			if col.IsPKHandleColumn(t.Meta()) {
				handleCol = col
				break
			}
		}
		for _, row := range rows {
			handles = append(handles, row[handleCol.Offset].GetInt64())
		}
	} else {
		for range rows {
			handle, err := t.AllocAutoID(ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			handles = append(handles, handle)
		}
	}
	return handles, nil
}

// batchGetOldValues gets the values of storage in batch.
func batchGetOldValues(ctx sessionctx.Context, t table.Table, handles []int64) (map[string][]byte, error) {
	batchKeys := make([]kv.Key, 0, len(handles))
	for _, handle := range handles {
		batchKeys = append(batchKeys, t.RecordKey(handle))
	}
	values, err := kv.BatchGetValues(ctx.Txn(), batchKeys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return values, nil
}

// encodeNewRow encodes a new row to value.
func encodeNewRow(ctx sessionctx.Context, t table.Table, row []types.Datum) ([]byte, error) {
	colIDs := make([]int64, 0, len(row))
	skimmedRow := make([]types.Datum, 0, len(row))
	for _, col := range t.WritableCols() {
		if !tables.CanSkip(t.Meta(), col, row[col.Offset]) {
			colIDs = append(colIDs, col.ID)
			skimmedRow = append(skimmedRow, row[col.Offset])
		}
	}
	newRowValue, err := tablecodec.EncodeRow(ctx.GetSessionVars().StmtCtx, skimmedRow, colIDs, nil, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newRowValue, nil
}

// getKeysNeedCheck gets keys converted from to-be-insert rows to record keys and unique index keys,
// which need to be checked whether they are duplicate keys.
func getKeysNeedCheck(ctx sessionctx.Context, t table.Table, rows [][]types.Datum) ([][]keyWithDupError,
	error) {
	nUnique := 0
	for _, v := range t.WritableIndices() {
		if v.Meta().Unique {
			nUnique++
		}
	}
	rowWithKeys := make([][]keyWithDupError, 0, len(rows))

	// Get the new handles.
	handles, err := genNewHandles(ctx, t, rows)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for i, row := range rows {
		keysWithErr := make([]keyWithDupError, 0, nUnique+1)
		newRowValue, err := encodeNewRow(ctx, t, row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Append record keys and errors.
		if t.Meta().PKIsHandle {
			keysWithErr = append(keysWithErr, keyWithDupError{
				true,
				t.RecordKey(handles[i]),
				kv.ErrKeyExists.FastGen("Duplicate entry '%d' for key 'PRIMARY'", handles[i]),
				handles[i],
				newRowValue,
			})
		}

		// append unique keys and errors
		for _, v := range t.WritableIndices() {
			if !v.Meta().Unique {
				continue
			}
			colVals, err1 := v.FetchValues(row, nil)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			key, distinct, err2 := v.GenIndexKey(ctx.GetSessionVars().StmtCtx,
				colVals, handles[i], nil)
			if err2 != nil {
				return nil, errors.Trace(err2)
			}
			// Skip the non-distinct keys.
			if !distinct {
				continue
			}
			keysWithErr = append(keysWithErr, keyWithDupError{
				false,
				key,
				kv.ErrKeyExists.FastGen("Duplicate entry '%d' for key '%s'", handles[i], v.Meta().Name),
				handles[i],
				newRowValue,
			})
		}
		rowWithKeys = append(rowWithKeys, keysWithErr)
	}
	return rowWithKeys, nil
}

// batchGetInsertKeys uses batch-get to fetch all key-value pairs to be checked for ignore or duplicate key update.
func batchGetInsertKeys(ctx sessionctx.Context, t table.Table, newRows [][]types.Datum) ([][]keyWithDupError, map[string][]byte, error) {
	// Get keys need to be checked.
	keysInRows, err := getKeysNeedCheck(ctx, t, newRows)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	// Batch get values.
	nKeys := 0
	for _, v := range keysInRows {
		nKeys += len(v)
	}
	batchKeys := make([]kv.Key, 0, nKeys)
	for _, v := range keysInRows {
		for _, k := range v {
			batchKeys = append(batchKeys, k.key)
		}
	}
	values, err := kv.BatchGetValues(ctx.Txn(), batchKeys)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return keysInRows, values, nil
}

// checkBatchLimit check the batchSize limitation.
func (e *InsertExec) checkBatchLimit() error {
	sessVars := e.ctx.GetSessionVars()
	batchInsert := sessVars.BatchInsert && !sessVars.InTxn()
	batchSize := sessVars.DMLBatchSize
	if batchInsert && e.rowCount >= batchSize {
		e.ctx.StmtCommit()
		if err := e.ctx.NewTxn(); err != nil {
			// We should return a special error for batch insert.
			return ErrBatchInsertFail.Gen("BatchInsert failed with error: %v", err)
		}
		e.rowCount = 0
		if !sessVars.ImportingData {
			sessVars.GetWriteStmtBufs().BufStore = kv.NewBufferStore(e.ctx.Txn(), kv.TempTxnMemBufCap)
		}
	}
	return nil
}

// initDupOldRowValue initializes dupOldRowValues which contain the to-be-updated rows from storage.
func (e *InsertExec) initDupOldRowValue(newRows [][]types.Datum) (err error) {
	e.dupOldRowValues = make(map[string][]byte, len(newRows))
	handles := make([]int64, 0, len(newRows))
	for _, keysInRow := range e.uniqueKeysInRows {
		for _, k := range keysInRow {
			if val, found := e.dupKeyValues[string(k.key)]; found {
				var handle int64
				handle, err = decodeOldHandle(k, val)
				if err != nil {
					return errors.Trace(err)
				}
				handles = append(handles, handle)
				break
			}
		}
	}
	e.dupOldRowValues, err = batchGetOldValues(e.ctx, e.Table, handles)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// decodeOldHandle decode old handle by key-value pair.
// The key-value pair should only be a table record or a distinct index record.
// If the key is a record key, decode handle from the key, else decode handle from the value.
func decodeOldHandle(k keyWithDupError, value []byte) (oldHandle int64, err error) {
	if k.isRecordKey {
		oldHandle, err = tablecodec.DecodeRowKey(k.key)
	} else {
		oldHandle, err = tables.DecodeHandle(value)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return oldHandle, nil
}

// updateDupRow updates a duplicate row to a new row.
func (e *InsertExec) updateDupRow(keys []keyWithDupError, k keyWithDupError, val []byte, newRow []types.Datum, onDuplicate []*expression.Assignment) (err error) {
	oldHandle, err := decodeOldHandle(k, val)
	if err != nil {
		return errors.Trace(err)
	}

	// Get the table record row from storage for update.
	oldValue, ok := e.dupOldRowValues[string(e.Table.RecordKey(oldHandle))]
	if !ok {
		return errors.NotFoundf("can not be duplicated row, due to old row not found. handle %d", oldHandle)
	}
	oldRow, err := tables.DecodeRawRowData(e.ctx, e.Table.Meta(), oldHandle, e.Table.WritableCols(), oldValue)
	if err != nil {
		return errors.Trace(err)
	}

	// Do update row.
	updatedRow, handleChanged, newHandle, err := e.doDupRowUpdate(oldHandle, oldRow, newRow, onDuplicate)
	if err != nil {
		return errors.Trace(err)
	}
	return e.updateDupKeyValues(keys, oldHandle, newHandle, handleChanged, updatedRow)
}

// updateDupKeyValues updates the dupKeyValues for further duplicate key check.
func (e *InsertExec) updateDupKeyValues(keys []keyWithDupError, oldHandle int64,
	newHandle int64, handleChanged bool, updatedRow []types.Datum) error {
	// There is only one row per update.
	fillBackKeysInRows, err := getKeysNeedCheck(e.ctx, e.Table, [][]types.Datum{updatedRow})
	if err != nil {
		return errors.Trace(err)
	}
	// Delete key-values belong to the old row.
	for _, del := range keys {
		delete(e.dupKeyValues, string(del.key))
	}
	// Fill back new key-values of the updated row.
	if handleChanged {
		delete(e.dupOldRowValues, string(e.Table.RecordKey(oldHandle)))
		e.fillBackKeys(fillBackKeysInRows[0], newHandle)
	} else {
		e.fillBackKeys(fillBackKeysInRows[0], oldHandle)
	}
	return nil
}

// batchUpdateDupRows updates multi-rows in batch if they are duplicate with rows in table.
func (e *InsertExec) batchUpdateDupRows(newRows [][]types.Datum, onDuplicate []*expression.Assignment) ([][]types.Datum, error) {
	var err error
	e.uniqueKeysInRows, e.dupKeyValues, err = batchGetInsertKeys(e.ctx, e.Table, newRows)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Batch get the to-be-updated rows in storage.
	err = e.initDupOldRowValue(newRows)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for i, keysInRow := range e.uniqueKeysInRows {
		for _, k := range keysInRow {
			if val, found := e.dupKeyValues[string(k.key)]; found {
				err := e.updateDupRow(keysInRow, k, val, newRows[i], e.OnDuplicate)
				if err != nil {
					return nil, errors.Trace(err)
				}
				// Clean up row for latest add record operation.
				newRows[i] = nil
				break
			}
		}
		// If row was checked with no duplicate keys,
		// it should be add to values map for the further row check.
		// There may be duplicate keys inside the insert statement.
		if newRows[i] == nil {
			for _, k := range keysInRow {
				e.dupKeyValues[string(k.key)] = k.newRowValue
			}
		}
	}
	return newRows, nil
}

// fillBackKeys fills the updated key-value pair to the dupKeyValues for further check.
func (e *InsertExec) fillBackKeys(fillBackKeysInRow []keyWithDupError, handle int64) {
	e.dupOldRowValues[string(e.Table.RecordKey(handle))] = fillBackKeysInRow[0].newRowValue
	for _, insert := range fillBackKeysInRow {
		if insert.isRecordKey {
			e.dupKeyValues[string(e.Table.RecordKey(handle))] = insert.newRowValue
		} else {
			e.dupKeyValues[string(insert.key)] = tables.EncodeHandle(handle)
		}
	}
}

// batchMarkDupRows marks rows with duplicate errors as nil.
// All duplicate rows were marked and appended as duplicate warnings
// to the statement context in batch.
func batchMarkDupRows(ctx sessionctx.Context, t table.Table, rows [][]types.Datum) ([][]types.Datum, error) {
	rowWithKeys, values, err := batchGetInsertKeys(ctx, t, rows)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// append warnings and get no duplicated error rows
	for i, v := range rowWithKeys {
		for _, k := range v {
			if _, found := values[string(k.key)]; found {
				// If duplicate keys were found in BatchGet, mark row = nil.
				rows[i] = nil
				ctx.GetSessionVars().StmtCtx.AppendWarning(k.dupErr)
				break
			}
		}
		// If row was checked with no duplicate keys,
		// it should be add to values map for the further row check.
		// There may be duplicate keys inside the insert statement.
		if rows[i] != nil {
			for _, k := range v {
				values[string(k.key)] = []byte{}
			}
		}
	}
	// this statement was already been checked
	ctx.GetSessionVars().StmtCtx.BatchCheck = true
	return rows, nil
}

// Next implements Exec Next interface.
func (e *InsertExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.finished {
		return nil
	}
	cols, err := e.getColumns(e.Table.Cols())
	if err != nil {
		return errors.Trace(err)
	}

	var rows [][]types.Datum
	if len(e.children) > 0 && e.children[0] != nil {
		rows, err = e.getRowsSelectChunk(ctx, cols, e.IgnoreErr)
	} else {
		rows, err = e.getRows(cols, e.IgnoreErr)
	}
	if err != nil {
		return errors.Trace(err)
	}

	_, err = e.exec(ctx, rows)
	return errors.Trace(err)
}

// Close implements the Executor Close interface.
func (e *InsertExec) Close() error {
	e.ctx.GetSessionVars().CurrInsertValues = nil
	if e.SelectExec != nil {
		return e.SelectExec.Close()
	}
	return nil
}

// Open implements the Executor Close interface.
func (e *InsertExec) Open(ctx context.Context) error {
	if e.SelectExec != nil {
		return e.SelectExec.Open(ctx)
	}
	return nil
}

// getColumns gets the explicitly specified columns of an insert statement. There are three cases:
// There are three types of insert statements:
// 1 insert ... values(...)  --> name type column
// 2 insert ... set x=y...   --> set type column
// 3 insert ... (select ..)  --> name type column
// See https://dev.mysql.com/doc/refman/5.7/en/insert.html
func (e *InsertValues) getColumns(tableCols []*table.Column) ([]*table.Column, error) {
	var cols []*table.Column
	var err error

	if len(e.Setlist) > 0 {
		// Process `set` type column.
		columns := make([]string, 0, len(e.Setlist))
		for _, v := range e.Setlist {
			columns = append(columns, v.Col.ColName.O)
		}
		for _, v := range e.GenColumns {
			columns = append(columns, v.Name.O)
		}
		cols, err = table.FindCols(tableCols, columns, e.Table.Meta().PKIsHandle)
		if err != nil {
			return nil, errors.Errorf("INSERT INTO %s: %s", e.Table.Meta().Name.O, err)
		}
		if len(cols) == 0 {
			return nil, errors.Errorf("INSERT INTO %s: empty column", e.Table.Meta().Name.O)
		}
	} else if len(e.Columns) > 0 {
		// Process `name` type column.
		columns := make([]string, 0, len(e.Columns))
		for _, v := range e.Columns {
			columns = append(columns, v.Name.O)
		}
		for _, v := range e.GenColumns {
			columns = append(columns, v.Name.O)
		}
		cols, err = table.FindCols(tableCols, columns, e.Table.Meta().PKIsHandle)
		if err != nil {
			return nil, errors.Errorf("INSERT INTO %s: %s", e.Table.Meta().Name.O, err)
		}
	} else {
		// If e.Columns are empty, use all columns instead.
		cols = tableCols
	}
	for _, col := range cols {
		if col.Name.L == model.ExtraHandleName.L {
			e.hasExtraHandle = true
			break
		}
	}

	// Check column whether is specified only once.
	err = table.CheckOnce(cols)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cols, nil
}

func (e *InsertValues) lazilyInitColDefaultValBuf() (ok bool) {
	if e.colDefaultVals != nil {
		return true
	}

	// only if values count of insert statement is more than one, use colDefaultVals to store
	// casted default values has benefits.
	if len(e.Lists) > 1 {
		e.colDefaultVals = make([]defaultVal, len(e.Table.Cols()))
		return true
	}

	return false
}

func (e *InsertValues) fillValueList() error {
	if len(e.Setlist) > 0 {
		if len(e.Lists) > 0 {
			return errors.Errorf("INSERT INTO %s: set type should not use values", e.Table)
		}
		l := make([]expression.Expression, 0, len(e.Setlist))
		for _, v := range e.Setlist {
			l = append(l, v.Expr)
		}
		e.Lists = append(e.Lists, l)
	}
	return nil
}

func (e *InsertValues) checkValueCount(insertValueCount, valueCount, genColsCount, num int, cols []*table.Column) error {
	// TODO: This check should be done in plan builder.
	if insertValueCount != valueCount {
		// "insert into t values (), ()" is valid.
		// "insert into t values (), (1)" is not valid.
		// "insert into t values (1), ()" is not valid.
		// "insert into t values (1,2), (1)" is not valid.
		// So the value count must be same for all insert list.
		return ErrWrongValueCountOnRow.GenByArgs(num + 1)
	}
	if valueCount == 0 && len(e.Columns) > 0 {
		// "insert into t (c1) values ()" is not valid.
		return ErrWrongValueCountOnRow.GenByArgs(num + 1)
	} else if valueCount > 0 {
		explicitSetLen := 0
		if len(e.Columns) != 0 {
			explicitSetLen = len(e.Columns)
		} else {
			explicitSetLen = len(e.Setlist)
		}
		if explicitSetLen > 0 && valueCount+genColsCount != len(cols) {
			return ErrWrongValueCountOnRow.GenByArgs(num + 1)
		} else if explicitSetLen == 0 && valueCount != len(cols) {
			return ErrWrongValueCountOnRow.GenByArgs(num + 1)
		}
	}
	return nil
}

func (e *InsertValues) getRows(cols []*table.Column, ignoreErr bool) (rows [][]types.Datum, err error) {
	// process `insert|replace ... set x=y...`
	if err = e.fillValueList(); err != nil {
		return nil, errors.Trace(err)
	}

	rows = make([][]types.Datum, len(e.Lists))
	length := len(e.Lists[0])
	for i, list := range e.Lists {
		if err = e.checkValueCount(length, len(list), len(e.GenColumns), i, cols); err != nil {
			return nil, errors.Trace(err)
		}
		e.rowCount = uint64(i)
		rows[i], err = e.getRow(cols, list, i, ignoreErr)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return
}

// resetErrDataTooLong reset ErrDataTooLong error msg.
// types.ErrDataTooLong is produced in types.ProduceStrWithSpecifiedTp, there is no column info in there,
// so we reset the error msg here, and wrap old err with errors.Wrap.
func resetErrDataTooLong(colName string, rowIdx int, err error) error {
	newErr := types.ErrDataTooLong.Gen("Data too long for column '%v' at row %v", colName, rowIdx)
	return errors.Wrap(err, newErr)
}

func (e *InsertValues) handleErr(col *table.Column, rowIdx int, err error, ignoreErr bool) error {
	if err == nil {
		return nil
	}

	if types.ErrDataTooLong.Equal(err) {
		return resetErrDataTooLong(col.Name.O, rowIdx+1, err)
	}

	return e.filterErr(err, ignoreErr)
}

// getRow eval the insert statement. Because the value of column may calculated based on other column,
// it use fillDefaultValues to init the empty row before eval expressions when needFillDefaultValues is true.
func (e *InsertValues) getRow(cols []*table.Column, list []expression.Expression, rowIdx int, ignoreErr bool) ([]types.Datum, error) {
	rowLen := len(e.Table.Cols())
	if e.hasExtraHandle {
		rowLen++
	}
	row := make(types.DatumRow, rowLen)
	hasValue := make([]bool, rowLen)

	if e.needFillDefaultValues {
		if err := e.fillDefaultValues(row, hasValue, ignoreErr); err != nil {
			return nil, errors.Trace(err)
		}
	}

	for i, expr := range list {
		val, err := expr.Eval(row)
		if err = e.handleErr(cols[i], rowIdx, err, ignoreErr); err != nil {
			return nil, errors.Trace(err)
		}
		val, err = table.CastValue(e.ctx, val, cols[i].ToInfo())
		if err = e.handleErr(cols[i], rowIdx, err, ignoreErr); err != nil {
			return nil, errors.Trace(err)
		}

		offset := cols[i].Offset
		row[offset], hasValue[offset] = val, true
	}

	return e.fillGenColData(cols, len(list), hasValue, row, ignoreErr)
}

// fillDefaultValues fills a row followed by these rules:
//     1. for nullable and no default value column, use NULL.
//     2. for nullable and have default value column, use it's default value.
//     3. for not null column, use zero value even in strict mode.
//     4. for auto_increment column, use zero value.
//     5. for generated column, use NULL.
func (e *InsertValues) fillDefaultValues(row []types.Datum, hasValue []bool, ignoreErr bool) error {
	for i, c := range e.Table.Cols() {
		var err error
		if c.IsGenerated() {
			continue
		} else if mysql.HasAutoIncrementFlag(c.Flag) {
			row[i] = table.GetZeroValue(c.ToInfo())
		} else {
			row[i], err = e.getColDefaultValue(i, c)
			hasValue[c.Offset] = true
			if table.ErrNoDefaultValue.Equal(err) {
				row[i] = table.GetZeroValue(c.ToInfo())
				hasValue[c.Offset] = false
			} else if err = e.filterErr(err, ignoreErr); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func (e *InsertValues) getRowsSelectChunk(ctx context.Context, cols []*table.Column, ignoreErr bool) ([][]types.Datum, error) {
	// process `insert|replace into ... select ... from ...`
	selectExec := e.children[0]
	if selectExec.Schema().Len() != len(cols) {
		return nil, ErrWrongValueCountOnRow.GenByArgs(1)
	}
	var rows [][]types.Datum
	fields := selectExec.retTypes()
	for {
		chk := selectExec.newChunk()
		iter := chunk.NewIterator4Chunk(chk)

		err := selectExec.Next(ctx, chk)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}

		for innerChunkRow := iter.Begin(); innerChunkRow != iter.End(); innerChunkRow = iter.Next() {
			innerRow := innerChunkRow.GetDatumRow(fields)
			e.rowCount = uint64(len(rows))
			row, err := e.fillRowData(cols, innerRow, ignoreErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (e *InsertValues) fillRowData(cols []*table.Column, vals []types.Datum, ignoreErr bool) ([]types.Datum, error) {
	row := make([]types.Datum, len(e.Table.Cols()))
	hasValue := make([]bool, len(e.Table.Cols()))
	for i, v := range vals {
		casted, err := table.CastValue(e.ctx, v, cols[i].ToInfo())
		if err = e.filterErr(err, ignoreErr); err != nil {
			return nil, errors.Trace(err)
		}

		offset := cols[i].Offset
		row[offset] = casted
		hasValue[offset] = true
	}

	return e.fillGenColData(cols, len(vals), hasValue, row, ignoreErr)
}

func (e *InsertValues) fillGenColData(cols []*table.Column, valLen int, hasValue []bool, row types.DatumRow, ignoreErr bool) ([]types.Datum, error) {
	err := e.initDefaultValues(row, hasValue, ignoreErr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, expr := range e.GenExprs {
		var val types.Datum
		val, err = expr.Eval(row)
		if err = e.filterErr(err, ignoreErr); err != nil {
			return nil, errors.Trace(err)
		}
		val, err = table.CastValue(e.ctx, val, cols[valLen+i].ToInfo())
		if err != nil {
			return nil, errors.Trace(err)
		}
		offset := cols[valLen+i].Offset
		row[offset] = val
	}

	if err = table.CheckNotNull(e.Table.Cols(), row); err != nil {
		return nil, errors.Trace(err)
	}
	return row, nil
}

func (e *InsertValues) filterErr(err error, ignoreErr bool) error {
	if err == nil {
		return nil
	}
	if !ignoreErr {
		return errors.Trace(err)
	}

	warnLog := fmt.Sprintf("ignore err:%v", errors.ErrorStack(err))
	e.handleLoadDataWarnings(err, warnLog)
	return nil
}

func (e *InsertValues) getColDefaultValue(idx int, col *table.Column) (d types.Datum, err error) {
	if e.colDefaultVals != nil && e.colDefaultVals[idx].valid {
		return e.colDefaultVals[idx].val, nil
	}

	defaultVal, err := table.GetColDefaultValue(e.ctx, col.ToInfo())
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if initialized := e.lazilyInitColDefaultValBuf(); initialized {
		e.colDefaultVals[idx].val = defaultVal
		e.colDefaultVals[idx].valid = true
	}

	return defaultVal, nil
}

// initDefaultValues fills generated columns, auto_increment column and empty column.
// For NOT NULL column, it will return error or use zero value based on sql_mode.
func (e *InsertValues) initDefaultValues(row []types.Datum, hasValue []bool, ignoreErr bool) error {
	strictSQL := e.ctx.GetSessionVars().StrictSQLMode

	for i, c := range e.Table.Cols() {
		var needDefaultValue bool
		if !hasValue[i] {
			needDefaultValue = true
		} else if mysql.HasNotNullFlag(c.Flag) && row[i].IsNull() && !strictSQL {
			needDefaultValue = true
			// TODO: Append Warning ErrColumnCantNull.
		}
		if mysql.HasAutoIncrementFlag(c.Flag) || c.IsGenerated() {
			// Just leave generated column as null. It will be calculated later
			// but before we check whether the column can be null or not.
			needDefaultValue = false
			if !hasValue[i] {
				row[i].SetNull()
			}
		}
		if needDefaultValue {
			var err error
			row[i], err = e.getColDefaultValue(i, c)
			if e.filterErr(err, ignoreErr) != nil {
				return errors.Trace(err)
			}
		}

		// Adjust the value if this column has auto increment flag.
		if mysql.HasAutoIncrementFlag(c.Flag) {
			if err := e.adjustAutoIncrementDatum(row, i, c, ignoreErr); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (e *InsertValues) adjustAutoIncrementDatum(row []types.Datum, i int, c *table.Column, ignoreErr bool) error {
	retryInfo := e.ctx.GetSessionVars().RetryInfo
	if retryInfo.Retrying {
		id, err := retryInfo.GetCurrAutoIncrementID()
		if err != nil {
			return errors.Trace(err)
		}
		if mysql.HasUnsignedFlag(c.Flag) {
			row[i].SetUint64(uint64(id))
		} else {
			row[i].SetInt64(id)
		}
		return nil
	}

	var err error
	var recordID int64
	if !row[i].IsNull() {
		recordID, err = row[i].ToInt64(e.ctx.GetSessionVars().StmtCtx)
		if e.filterErr(errors.Trace(err), ignoreErr) != nil {
			return errors.Trace(err)
		}
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		err = e.Table.RebaseAutoID(e.ctx, recordID, true)
		if err != nil {
			return errors.Trace(err)
		}
		e.ctx.GetSessionVars().InsertID = uint64(recordID)
		if mysql.HasUnsignedFlag(c.Flag) {
			row[i].SetUint64(uint64(recordID))
		} else {
			row[i].SetInt64(recordID)
		}
		retryInfo.AddAutoIncrementID(recordID)
		return nil
	}

	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	if row[i].IsNull() || e.ctx.GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		recordID, err = e.Table.AllocAutoID(e.ctx)
		if e.filterErr(errors.Trace(err), ignoreErr) != nil {
			return errors.Trace(err)
		}
		// It's compatible with mysql. So it sets last insert id to the first row.
		if e.rowCount == 0 {
			e.lastInsertID = uint64(recordID)
		}
	}

	if mysql.HasUnsignedFlag(c.Flag) {
		row[i].SetUint64(uint64(recordID))
	} else {
		row[i].SetInt64(recordID)
	}
	retryInfo.AddAutoIncrementID(recordID)

	// the value of row[i] is adjusted by autoid, so we need to cast it again.
	casted, err := table.CastValue(e.ctx, row[i], c.ToInfo())
	if err != nil {
		return errors.Trace(err)
	}
	row[i] = casted
	return nil
}

// doDupRowUpdate updates the duplicate row.
// TODO: Report rows affected and last insert id.
func (e *InsertExec) doDupRowUpdate(handle int64, oldRow []types.Datum, newRow []types.Datum,
	cols []*expression.Assignment) ([]types.Datum, bool, int64, error) {
	assignFlag := make([]bool, len(e.Table.WritableCols()))
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	e.ctx.GetSessionVars().CurrInsertValues = types.DatumRow(newRow)
	newData := make(types.DatumRow, len(oldRow))
	copy(newData, oldRow)
	for _, col := range cols {
		val, err1 := col.Expr.Eval(newData)
		if err1 != nil {
			return nil, false, 0, errors.Trace(err1)
		}
		newData[col.Col.Index] = val
		assignFlag[col.Col.Index] = true
	}
	_, handleChanged, newHandle, err := updateRecord(e.ctx, handle, oldRow, newData, assignFlag, e.Table, true, false)
	if err != nil {
		return nil, false, 0, errors.Trace(err)
	}
	e.rowCount++
	if err := e.checkBatchLimit(); err != nil {
		return nil, false, 0, errors.Trace(err)
	}
	return newData, handleChanged, newHandle, nil
}

// ReplaceExec represents a replace executor.
type ReplaceExec struct {
	*InsertValues
	Priority int
	finished bool
}

// Close implements the Executor Close interface.
func (e *ReplaceExec) Close() error {
	if e.SelectExec != nil {
		return e.SelectExec.Close()
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *ReplaceExec) Open(ctx context.Context) error {
	if e.SelectExec != nil {
		return e.SelectExec.Open(ctx)
	}
	return nil
}

func (e *ReplaceExec) exec(ctx context.Context, rows [][]types.Datum) (Row, error) {
	/*
	 * MySQL uses the following algorithm for REPLACE (and LOAD DATA ... REPLACE):
	 *  1. Try to insert the new row into the table
	 *  2. While the insertion fails because a duplicate-key error occurs for a primary key or unique index:
	 *  3. Delete from the table the conflicting row that has the duplicate key value
	 *  4. Try again to insert the new row into the table
	 * See http://dev.mysql.com/doc/refman/5.7/en/replace.html
	 *
	 * For REPLACE statements, the affected-rows value is 2 if the new row replaced an old row,
	 * because in this case, one row was inserted after the duplicate was deleted.
	 * See http://dev.mysql.com/doc/refman/5.7/en/mysql-affected-rows.html
	 */
	idx := 0
	rowsLen := len(rows)
	sc := e.ctx.GetSessionVars().StmtCtx
	for {
		if idx >= rowsLen {
			break
		}
		row := rows[idx]
		h, err1 := e.Table.AddRecord(e.ctx, row, false)
		if err1 == nil {
			e.ctx.StmtAddDirtyTableOP(DirtyTableAddRow, e.Table.Meta().ID, h, row)
			idx++
			continue
		}
		if err1 != nil && !kv.ErrKeyExists.Equal(err1) {
			return nil, errors.Trace(err1)
		}
		oldRow, err1 := e.Table.Row(e.ctx, h)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		rowUnchanged, err1 := types.EqualDatums(sc, oldRow, row)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if rowUnchanged {
			// If row unchanged, we do not need to do insert.
			e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
			idx++
			continue
		}
		// Remove current row and try replace again.
		err1 = e.Table.RemoveRecord(e.ctx, h, oldRow)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		e.ctx.StmtAddDirtyTableOP(DirtyTableDeleteRow, e.Table.Meta().ID, h, nil)
		e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	}

	if e.lastInsertID != 0 {
		e.ctx.GetSessionVars().SetLastInsertID(e.lastInsertID)
	}
	e.finished = true
	return nil, nil
}

// Next implements the Executor Next interface.
func (e *ReplaceExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.finished {
		return nil
	}
	cols, err := e.getColumns(e.Table.Cols())
	if err != nil {
		return errors.Trace(err)
	}

	var rows [][]types.Datum
	if len(e.children) > 0 && e.children[0] != nil {
		rows, err = e.getRowsSelectChunk(ctx, cols, false)
	} else {
		rows, err = e.getRows(cols, false)
	}
	if err != nil {
		return errors.Trace(err)
	}

	_, err = e.exec(ctx, rows)
	return errors.Trace(err)
}

// UpdateExec represents a new update executor.
type UpdateExec struct {
	baseExecutor

	SelectExec  Executor
	OrderedList []*expression.Assignment
	IgnoreErr   bool

	// updatedRowKeys is a map for unique (Table, handle) pair.
	updatedRowKeys map[int64]map[int64]struct{}
	tblID2table    map[int64]table.Table

	rows        []Row           // The rows fetched from TableExec.
	newRowsData [][]types.Datum // The new values to be set.
	fetched     bool
	cursor      int
}

func (e *UpdateExec) exec(ctx context.Context, schema *expression.Schema) (Row, error) {
	assignFlag, err := getUpdateColumns(e.OrderedList, schema.Len())
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
			changed, _, _, err1 := updateRecord(e.ctx, handle, oldData, newTableData, flags, tbl, false, e.IgnoreErr)
			if err1 == nil {
				if changed {
					e.updatedRowKeys[id][handle] = struct{}{}
				}
				continue
			}

			if kv.ErrKeyExists.Equal(err1) && e.IgnoreErr {
				e.ctx.GetSessionVars().StmtCtx.AppendWarning(err1)
				continue
			}
			return nil, errors.Trace(err1)
		}
	}
	e.cursor++
	return Row{}, nil
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
			row, err := e.exec(ctx, e.children[0].Schema())
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

func getUpdateColumns(assignList []*expression.Assignment, schemaLen int) ([]bool, error) {
	assignFlag := make([]bool, schemaLen)
	for _, v := range assignList {
		idx := v.Col.Index
		assignFlag[idx] = true
	}
	return assignFlag, nil
}

func (e *UpdateExec) fetchChunkRows(ctx context.Context) error {
	fields := e.children[0].retTypes()
	globalRowIdx := 0
	for {
		chk := chunk.NewChunk(fields)
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

func (e *UpdateExec) composeNewRow(rowIdx int, oldRow Row) (Row, error) {
	newRowData := oldRow.Copy()
	for _, assign := range e.OrderedList {
		val, err := assign.Expr.Eval(newRowData)

		if err1 := e.handleErr(assign.Col.ColName, rowIdx, err); err1 != nil {
			return nil, errors.Trace(err1)
		}
		newRowData[assign.Col.Index] = val
	}
	return newRowData, nil
}

func getTableOffset(schema *expression.Schema, handleCol *expression.Column) int {
	for i, col := range schema.Columns {
		if col.DBName.L == handleCol.DBName.L && col.TblName.L == handleCol.TblName.L {
			return i
		}
	}
	panic("Couldn't get column information when do update/delete")
}

// Close implements the Executor Close interface.
func (e *UpdateExec) Close() error {
	return e.SelectExec.Close()
}

// Open implements the Executor Open interface.
func (e *UpdateExec) Open(ctx context.Context) error {
	return e.SelectExec.Open(ctx)
}
