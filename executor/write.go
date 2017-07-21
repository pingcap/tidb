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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ Executor = &UpdateExec{}
	_ Executor = &DeleteExec{}
	_ Executor = &InsertExec{}
	_ Executor = &ReplaceExec{}
	_ Executor = &LoadData{}
)

func updateRecord(ctx context.Context, h int64, oldData, newData []types.Datum, assignFlag []bool, t table.Table, onDuplicateUpdate bool) (bool, error) {
	cols := t.WritableCols()
	touched := make(map[int]bool, len(cols))
	assignExists := false
	sc := ctx.GetSessionVars().StmtCtx
	var newHandle types.Datum
	for i, hasSetExpr := range assignFlag {
		if !hasSetExpr {
			if onDuplicateUpdate {
				newData[i] = oldData[i]
			}
			continue
		}
		col := cols[i]
		if col.IsPKHandleColumn(t.Meta()) {
			newHandle = newData[i]
		}
		if mysql.HasAutoIncrementFlag(col.Flag) {
			if newData[i].IsNull() {
				return false, errors.Errorf("Column '%v' cannot be null", col.Name.O)
			}
			val, err := newData[i].ToInt64(sc)
			if err != nil {
				return false, errors.Trace(err)
			}
			t.RebaseAutoID(val, true)
		}
		casted, err := table.CastValue(ctx, newData[i], col.ToInfo())
		if err != nil {
			return false, errors.Trace(err)
		}
		newData[i] = casted
		touched[i] = true
		assignExists = true
	}

	// If no assign list for this table, no need to update.
	if !assignExists {
		return false, nil
	}

	if err := table.CheckNotNull(cols, newData); err != nil {
		return false, errors.Trace(err)
	}

	// If row is not changed, we should do nothing.
	rowChanged := false
	for i := range oldData {
		if !touched[i] {
			continue
		}

		n, err := newData[i].CompareDatum(sc, oldData[i])
		if err != nil {
			return false, errors.Trace(err)
		}
		if n != 0 {
			rowChanged = true
			break
		}
	}
	if !rowChanged {
		// See https://dev.mysql.com/doc/refman/5.7/en/mysql-real-connect.html  CLIENT_FOUND_ROWS
		if ctx.GetSessionVars().ClientCapability&mysql.ClientFoundRows > 0 {
			sc.AddAffectedRows(1)
		}
		return false, nil
	}

	var err error
	if !newHandle.IsNull() {
		err = t.RemoveRecord(ctx, h, oldData)
		if err != nil {
			return false, errors.Trace(err)
		}
		_, err = t.AddRecord(ctx, newData)
	} else {
		// Update record to new value and update index.
		err = t.UpdateRecord(ctx, h, oldData, newData, touched)
	}
	if err != nil {
		return false, errors.Trace(err)
	}
	dirtyDB := getDirtyDB(ctx)
	tid := t.Meta().ID
	dirtyDB.deleteRow(tid, h)
	dirtyDB.addRow(tid, h, newData)

	// Record affected rows.
	if !onDuplicateUpdate {
		sc.AddAffectedRows(1)
	} else {
		sc.AddAffectedRows(2)
	}
	ctx.GetSessionVars().TxnCtx.UpdateDeltaForTable(t.Meta().ID, 0, 1)
	return true, nil
}

// DeleteExec represents a delete executor.
// See https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteExec struct {
	baseExecutor

	SelectExec Executor

	Tables       []*ast.TableName
	IsMultiTable bool

	finished bool
}

// Schema implements the Executor Schema interface.
func (e *DeleteExec) Schema() *expression.Schema {
	return expression.NewSchema()
}

// Next implements the Executor Next interface.
func (e *DeleteExec) Next() (*Row, error) {
	if e.finished {
		return nil, nil
	}
	defer func() {
		e.finished = true
	}()

	if e.IsMultiTable {
		return nil, e.deleteMultiTables()
	}
	return nil, e.deleteSingleTable()
}

func (e *DeleteExec) deleteMultiTables() error {
	if len(e.Tables) == 0 {
		return nil
	}

	// Table ID may not be unique for deleting multiple tables, for statements like
	// `delete from t as t1, t as t2`, the same table has two alias, we have to identify a table
	// by its alias instead of ID, so the table map value is an array which contains table aliases.
	tblMap := make(map[int64][]string, len(e.Tables))
	for _, t := range e.Tables {
		tblMap[t.TableInfo.ID] = append(tblMap[t.TableInfo.ID], t.Name.L)
	}

	// Map for unique (Table, Row) pair.
	tblRowMap := make(map[table.Table]map[int64][]types.Datum)
	for {
		joinedRow, err := e.SelectExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if joinedRow == nil {
			break
		}

		for _, entry := range joinedRow.RowKeys {
			if !isMatchTableName(entry, tblMap) {
				continue
			}
			if tblRowMap[entry.Tbl] == nil {
				tblRowMap[entry.Tbl] = make(map[int64][]types.Datum)
			}
			offset := getTableOffset(e.SelectExec.Schema(), entry)
			data := joinedRow.Data[offset : offset+len(entry.Tbl.WritableCols())]
			tblRowMap[entry.Tbl][entry.Handle] = data
		}
	}
	for t, rowMap := range tblRowMap {
		for handle, data := range rowMap {
			err := e.removeRow(e.ctx, t, handle, data)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func isMatchTableName(entry *RowKeyEntry, tblMap map[int64][]string) bool {
	names, ok := tblMap[entry.Tbl.Meta().ID]
	if !ok {
		return false
	}
	for _, n := range names {
		if n == entry.TableName {
			return true
		}
	}

	return false
}

func (e *DeleteExec) deleteSingleTable() error {
	for {
		row, err := e.SelectExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		rowKey := row.RowKeys[0]
		err = e.removeRow(e.ctx, rowKey.Tbl, rowKey.Handle, row.Data)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *DeleteExec) removeRow(ctx context.Context, t table.Table, h int64, data []types.Datum) error {
	err := t.RemoveRecord(ctx, h, data)
	if err != nil {
		return errors.Trace(err)
	}
	getDirtyDB(ctx).deleteRow(t.Meta().ID, h)
	ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	ctx.GetSessionVars().TxnCtx.UpdateDeltaForTable(t.Meta().ID, -1, 1)
	return nil
}

// Close implements the Executor Close interface.
func (e *DeleteExec) Close() error {
	return e.SelectExec.Close()
}

// Open implements the Executor Open interface.
func (e *DeleteExec) Open() error {
	return e.SelectExec.Open()
}

// NewLoadDataInfo returns a LoadDataInfo structure, and it's only used for tests now.
func NewLoadDataInfo(row []types.Datum, ctx context.Context, tbl table.Table, cols []*table.Column) *LoadDataInfo {
	return &LoadDataInfo{
		row:       row,
		insertVal: &InsertValues{ctx: ctx, Table: tbl},
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
	Ctx        context.Context
	columns    []*table.Column
}

// SetBatchCount sets the number of rows to insert in a batch.
func (e *LoadDataInfo) SetBatchCount(limit int64) {
	e.insertVal.batchRows = limit
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
		e.insertData(cols)
		e.insertVal.currRow++
		if e.insertVal.batchRows != 0 && e.insertVal.currRow%e.insertVal.batchRows == 0 {
			reachLimit = true
			log.Infof("This insert rows has reached the batch %d, current total rows %d",
				e.insertVal.batchRows, e.insertVal.currRow)
			break
		}
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

func (e *LoadDataInfo) insertData(cols []string) {
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
		return
	}
	_, err = e.Table.AddRecord(e.insertVal.ctx, row)
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
func (e *LoadData) Next() (*Row, error) {
	// TODO: support load data without local field.
	if !e.IsLocal {
		return nil, errors.New("Load Data: don't support load data without local field")
	}
	// TODO: support lines terminated is "".
	if len(e.loadDataInfo.LinesInfo.Terminated) == 0 {
		return nil, errors.New("Load Data: don't support load data terminated is nil")
	}

	ctx := e.loadDataInfo.insertVal.ctx
	val := ctx.Value(LoadDataVarKey)
	if val != nil {
		ctx.SetValue(LoadDataVarKey, nil)
		return nil, errors.New("Load Data: previous load data option isn't closed normal")
	}
	if e.loadDataInfo.Path == "" {
		return nil, errors.New("Load Data: infile path is empty")
	}
	ctx.SetValue(LoadDataVarKey, e.loadDataInfo)

	return nil, nil
}

// Schema implements the Executor Schema interface.
func (e *LoadData) Schema() *expression.Schema {
	return expression.NewSchema()
}

// Close implements the Executor Close interface.
func (e *LoadData) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *LoadData) Open() error {
	return nil
}

// InsertValues is the data to insert.
type InsertValues struct {
	currRow      int64
	batchRows    int64
	lastInsertID uint64
	ctx          context.Context
	SelectExec   Executor

	Table     table.Table
	Columns   []*ast.ColumnName
	Lists     [][]expression.Expression
	Setlist   []*expression.Assignment
	IsPrepare bool
}

// InsertExec represents an insert executor.
type InsertExec struct {
	*InsertValues

	OnDuplicate []*expression.Assignment

	Priority mysql.PriorityEnum
	Ignore   bool

	finished bool
}

// Schema implements the Executor Schema interface.
func (e *InsertExec) Schema() *expression.Schema {
	return expression.NewSchema()
}

// BatchInsertSize is the batch size of auto-splitted insert data.
// This will be used when tidb_batch_insert is set to ON.
var BatchInsertSize = 20000

// Next implements the Executor Next interface.
func (e *InsertExec) Next() (*Row, error) {
	if e.finished {
		return nil, nil
	}
	cols, err := e.getColumns(e.Table.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}
	txn := e.ctx.Txn()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	var rows [][]types.Datum
	if e.SelectExec != nil {
		rows, err = e.getRowsSelect(cols)
	} else {
		rows, err = e.getRows(cols)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	// If tidb_batch_insert is ON and not in a transaction, we could use BatchInsert mode.
	batchInsert := e.ctx.GetSessionVars().BatchInsert && !e.ctx.GetSessionVars().InTxn()
	rowCount := 0

	for _, row := range rows {
		if batchInsert && rowCount >= BatchInsertSize {
			err = e.ctx.NewTxn()
			if err != nil {
				// We should return a special error for batch insert.
				return nil, ErrBatchInsertFail.Gen("BatchInsert failed with error: %v", err)
			}
			txn = e.ctx.Txn()
			rowCount = 0
		}
		if len(e.OnDuplicate) == 0 && !e.Ignore {
			txn.SetOption(kv.PresumeKeyNotExists, nil)
		}
		h, err := e.Table.AddRecord(e.ctx, row)
		txn.DelOption(kv.PresumeKeyNotExists)
		if err == nil {
			getDirtyDB(e.ctx).addRow(e.Table.Meta().ID, h, row)
			rowCount++
			continue
		}

		if terror.ErrorEqual(err, kv.ErrKeyExists) {
			// If you use the IGNORE keyword, duplicate-key error that occurs while executing the INSERT statement are ignored.
			// For example, without IGNORE, a row that duplicates an existing UNIQUE index or PRIMARY KEY value in
			// the table causes a duplicate-key error and the statement is aborted. With IGNORE, the row is discarded and no error occurs.
			if e.Ignore {
				continue
			}
			if len(e.OnDuplicate) > 0 {
				if err = e.onDuplicateUpdate(row, h, e.OnDuplicate); err != nil {
					return nil, errors.Trace(err)
				}
				continue
			}
		}
		return nil, errors.Trace(err)
	}

	if e.lastInsertID != 0 {
		e.ctx.GetSessionVars().SetLastInsertID(e.lastInsertID)
	}
	e.finished = true
	return nil, nil
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
func (e *InsertExec) Open() error {
	if e.SelectExec != nil {
		return e.SelectExec.Open()
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

		cols, err = table.FindCols(tableCols, columns)
		if err != nil {
			return nil, errors.Errorf("INSERT INTO %s: %s", e.Table.Meta().Name.O, err)
		}

		if len(cols) == 0 {
			return nil, errors.Errorf("INSERT INTO %s: empty column", e.Table.Meta().Name.O)
		}
	} else {
		// Process `name` type column.
		columns := make([]string, 0, len(e.Columns))
		for _, v := range e.Columns {
			columns = append(columns, v.Name.O)
		}
		cols, err = table.FindCols(tableCols, columns)
		if err != nil {
			return nil, errors.Errorf("INSERT INTO %s: %s", e.Table.Meta().Name.O, err)
		}

		// If cols are empty, use all columns instead.
		if len(cols) == 0 {
			cols = tableCols
		}
	}

	// Check column whether is specified only once.
	err = table.CheckOnce(cols)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cols, nil
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

func (e *InsertValues) checkValueCount(insertValueCount, valueCount, num int, cols []*table.Column) error {
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
	} else if valueCount > 0 && valueCount != len(cols) {
		return ErrWrongValueCountOnRow.GenByArgs(num + 1)
	}
	return nil
}

func (e *InsertValues) getRows(cols []*table.Column) (rows [][]types.Datum, err error) {
	// process `insert|replace ... set x=y...`
	if err = e.fillValueList(); err != nil {
		return nil, errors.Trace(err)
	}

	rows = make([][]types.Datum, len(e.Lists))
	length := len(e.Lists[0])
	for i, list := range e.Lists {
		if err = e.checkValueCount(length, len(list), i, cols); err != nil {
			return nil, errors.Trace(err)
		}
		e.currRow = int64(i)
		rows[i], err = e.getRow(cols, list)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return
}

func (e *InsertValues) getRow(cols []*table.Column, list []expression.Expression) ([]types.Datum, error) {
	vals := make([]types.Datum, len(list))
	for i, expr := range list {
		val, err := expr.Eval(nil)
		vals[i] = val
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return e.fillRowData(cols, vals, false)
}

func (e *InsertValues) getRowsSelect(cols []*table.Column) ([][]types.Datum, error) {
	// process `insert|replace into ... select ... from ...`
	if e.SelectExec.Schema().Len() != len(cols) {
		return nil, ErrWrongValueCountOnRow.GenByArgs(1)
	}
	var rows [][]types.Datum
	for {
		innerRow, err := e.SelectExec.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if innerRow == nil {
			break
		}
		e.currRow = int64(len(rows))
		row, err := e.fillRowData(cols, innerRow.Data, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *InsertValues) fillRowData(cols []*table.Column, vals []types.Datum, ignoreErr bool) ([]types.Datum, error) {
	row := make([]types.Datum, len(e.Table.Cols()))
	hasValue := make([]bool, len(e.Table.Cols()))
	for i, v := range vals {
		offset := cols[i].Offset
		row[offset] = v
		hasValue[offset] = true
	}
	err := e.initDefaultValues(row, hasValue, ignoreErr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = table.CastValues(e.ctx, row, cols, ignoreErr); err != nil {
		return nil, errors.Trace(err)
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

func (e *InsertValues) initDefaultValues(row []types.Datum, hasValue []bool, ignoreErr bool) error {
	var defaultValueCols []*table.Column
	strictSQL := e.ctx.GetSessionVars().StrictSQLMode

	for i, c := range e.Table.Cols() {
		var needDefaultValue bool
		if !hasValue[i] {
			needDefaultValue = true
		} else if mysql.HasNotNullFlag(c.Flag) && row[i].IsNull() && !strictSQL {
			needDefaultValue = true
			// TODO: Append Warning ErrColumnCantNull.
		}
		if mysql.HasAutoIncrementFlag(c.Flag) {
			needDefaultValue = false
		}
		if needDefaultValue {
			var err error
			row[i], err = table.GetColDefaultValue(e.ctx, c.ToInfo())
			if e.filterErr(err, ignoreErr) != nil {
				return errors.Trace(err)
			}
			defaultValueCols = append(defaultValueCols, c)
		}

		// Adjust the value if this column has auto increment flag.
		if mysql.HasAutoIncrementFlag(c.Flag) {
			if err := e.adjustAutoIncrementDatum(row, i, c, ignoreErr); err != nil {
				return errors.Trace(err)
			}
		}
	}
	if err := table.CastValues(e.ctx, row, defaultValueCols, ignoreErr); err != nil {
		return errors.Trace(err)
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
		row[i].SetInt64(id)
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
		e.Table.RebaseAutoID(recordID, true)
		e.ctx.GetSessionVars().InsertID = uint64(recordID)
		row[i].SetInt64(recordID)
		retryInfo.AddAutoIncrementID(recordID)
		return nil
	}

	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	if row[i].IsNull() || e.ctx.GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		recordID, err = e.Table.AllocAutoID()
		if e.filterErr(errors.Trace(err), ignoreErr) != nil {
			return errors.Trace(err)
		}
		// It's compatible with mysql. So it sets last insert id to the first row.
		if e.currRow == 0 {
			e.lastInsertID = uint64(recordID)
		}
	}

	row[i].SetInt64(recordID)
	retryInfo.AddAutoIncrementID(recordID)
	return nil
}

// onDuplicateUpdate updates the duplicate row.
// TODO: Report rows affected and last insert id.
func (e *InsertExec) onDuplicateUpdate(row []types.Datum, h int64, cols []*expression.Assignment) error {
	data, err := e.Table.Row(e.ctx, h)
	if err != nil {
		return errors.Trace(err)
	}
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	e.ctx.GetSessionVars().CurrInsertValues = row

	// evaluate assignment
	assignFlag := make([]bool, len(e.Table.Cols()))
	newData := make([]types.Datum, len(data))
	copy(newData, data)
	for _, col := range cols {
		val, err1 := col.Expr.Eval(newData)
		if err1 != nil {
			return errors.Trace(err1)
		}
		newData[col.Col.Index] = val
		assignFlag[col.Col.Index] = true
	}
	if _, err = updateRecord(e.ctx, h, data, newData, assignFlag, e.Table, true); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func findColumnByName(t table.Table, tableName, colName string) (*table.Column, error) {
	if len(tableName) > 0 && !strings.EqualFold(tableName, t.Meta().Name.O) {
		return nil, errors.Errorf("unknown field %s.%s", tableName, colName)
	}

	c := table.FindCol(t.Cols(), colName)
	if c == nil {
		return nil, errors.Errorf("unknown field %s", colName)
	}
	return c, nil
}

// ReplaceExec represents a replace executor.
type ReplaceExec struct {
	*InsertValues
	Priority int
	finished bool
}

// Schema implements the Executor Schema interface.
func (e *ReplaceExec) Schema() *expression.Schema {
	return expression.NewSchema()
}

// Close implements the Executor Close interface.
func (e *ReplaceExec) Close() error {
	if e.SelectExec != nil {
		return e.SelectExec.Close()
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *ReplaceExec) Open() error {
	if e.SelectExec != nil {
		return e.SelectExec.Open()
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *ReplaceExec) Next() (*Row, error) {
	if e.finished {
		return nil, nil
	}
	cols, err := e.getColumns(e.Table.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}

	var rows [][]types.Datum
	if e.SelectExec != nil {
		rows, err = e.getRowsSelect(cols)
	} else {
		rows, err = e.getRows(cols)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

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
		h, err1 := e.Table.AddRecord(e.ctx, row)
		if err1 == nil {
			getDirtyDB(e.ctx).addRow(e.Table.Meta().ID, h, row)
			idx++
			continue
		}
		if err1 != nil && !terror.ErrorEqual(err1, kv.ErrKeyExists) {
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
		getDirtyDB(e.ctx).deleteRow(e.Table.Meta().ID, h)
		e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	}

	if e.lastInsertID != 0 {
		e.ctx.GetSessionVars().SetLastInsertID(e.lastInsertID)
	}
	e.finished = true
	return nil, nil
}

// UpdateExec represents a new update executor.
type UpdateExec struct {
	baseExecutor

	SelectExec  Executor
	OrderedList []*expression.Assignment

	// updatedRowKeys is a map for unique (Table, handle) pair.
	updatedRowKeys map[table.Table]map[int64]struct{}

	rows        []*Row          // The rows fetched from TableExec.
	newRowsData [][]types.Datum // The new values to be set.
	fetched     bool
	cursor      int
}

// Next implements the Executor Next interface.
func (e *UpdateExec) Next() (*Row, error) {
	if !e.fetched {
		err := e.fetchRows()
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.fetched = true
	}

	assignFlag, err := getUpdateColumns(e.OrderedList, e.SelectExec.Schema().Len())
	if err != nil {
		return nil, errors.Trace(err)
	}
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	if e.updatedRowKeys == nil {
		e.updatedRowKeys = make(map[table.Table]map[int64]struct{})
	}
	row := e.rows[e.cursor]
	newData := e.newRowsData[e.cursor]
	for _, entry := range row.RowKeys {
		tbl := entry.Tbl
		if e.updatedRowKeys[tbl] == nil {
			e.updatedRowKeys[tbl] = make(map[int64]struct{})
		}
		offset := getTableOffset(e.SelectExec.Schema(), entry)
		end := offset + len(tbl.WritableCols())
		handle := entry.Handle
		oldData := row.Data[offset:end]
		newTableData := newData[offset:end]
		flags := assignFlag[offset:end]
		_, ok := e.updatedRowKeys[tbl][handle]
		if ok {
			// Each matched row is updated once, even if it matches the conditions multiple times.
			continue
		}
		// Update row
		changed, err1 := updateRecord(e.ctx, handle, oldData, newTableData, flags, tbl, false)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if changed {
			e.updatedRowKeys[tbl][handle] = struct{}{}
		}
	}
	e.cursor++
	return &Row{}, nil
}

func getUpdateColumns(assignList []*expression.Assignment, schemaLen int) ([]bool, error) {
	assignFlag := make([]bool, schemaLen)
	for _, v := range assignList {
		idx := v.Col.Index
		if v != nil {
			assignFlag[idx] = true
		}
	}
	return assignFlag, nil
}

func (e *UpdateExec) fetchRows() error {
	for {
		row, err := e.SelectExec.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			return nil
		}
		newRowData := make([]types.Datum, len(row.Data))
		copy(newRowData, row.Data)
		for _, assign := range e.OrderedList {
			val, err := assign.Expr.Eval(newRowData)
			if err != nil {
				return errors.Trace(err)
			}
			newRowData[assign.Col.Index] = val
		}
		e.rows = append(e.rows, row)
		e.newRowsData = append(e.newRowsData, newRowData)
	}
}

func getTableOffset(schema *expression.Schema, entry *RowKeyEntry) int {
	for i := 0; i < schema.Len(); i++ {
		s := schema.Columns[i]
		if s.TblName.L == entry.TableName {
			return i
		}
	}
	return 0
}

// Close implements the Executor Close interface.
func (e *UpdateExec) Close() error {
	return e.SelectExec.Close()
}

// Open implements the Executor Open interface.
func (e *UpdateExec) Open() error {
	return e.SelectExec.Open()
}
