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
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

var (
	null          = []byte("NULL")
	taskQueueSize = 16 // the maximum number of pending tasks to commit in queue
)

// LoadDataExec represents a load data executor.
type LoadDataExec struct {
	baseExecutor

	IsLocal      bool
	OnDuplicate  ast.OnDuplicateKeyHandlingType
	loadDataInfo *LoadDataInfo
}

var insertValuesLabel fmt.Stringer = stringutil.StringerStr("InsertValues")

// NewLoadDataInfo returns a LoadDataInfo structure, and it's only used for tests now.
func NewLoadDataInfo(ctx sessionctx.Context, row []types.Datum, tbl table.Table, cols []*table.Column) *LoadDataInfo {
	insertVal := &InsertValues{baseExecutor: newBaseExecutor(ctx, nil, insertValuesLabel, 0), Table: tbl}
	return &LoadDataInfo{
		row:          row,
		InsertValues: insertVal,
		Table:        tbl,
		Ctx:          ctx,
	}
}

// Next implements the Executor Next interface.
func (e *LoadDataExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	// TODO: support load data without local field.
	if !e.IsLocal {
		return errors.New("Load Data: don't support load data without local field")
	}
	// TODO: support load data with replace field.
	if e.OnDuplicate == ast.OnDuplicateKeyHandlingReplace {
		return errors.New("Load Data: don't support load data with replace field")
	}
	// TODO: support lines terminated is "".
	if len(e.loadDataInfo.LinesInfo.Terminated) == 0 {
		return errors.New("Load Data: don't support load data terminated is nil")
	}

	sctx := e.loadDataInfo.ctx
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
func (e *LoadDataExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *LoadDataExec) Open(ctx context.Context) error {
	if e.loadDataInfo.insertColumns != nil {
		e.loadDataInfo.initEvalBuffer()
	}
	return nil
}

// CommitTask is used for fetching data from data preparing routine into committing routine.
type CommitTask struct {
	cnt  uint64
	rows [][]types.Datum
}

// LoadDataInfo saves the information of loading data operation.
type LoadDataInfo struct {
	*InsertValues

	row         []types.Datum
	Path        string
	Table       table.Table
	FieldsInfo  *ast.FieldsClause
	LinesInfo   *ast.LinesClause
	IgnoreLines uint64
	Ctx         sessionctx.Context
	rows        [][]types.Datum
	Drained     bool

	ColumnAssignments  []*ast.Assignment
	ColumnsAndUserVars []*ast.ColumnNameOrUserVar
	FieldMappings      []*FieldMapping

	commitTaskQueue chan CommitTask
	StopCh          chan struct{}
	QuitCh          chan struct{}
}

// FieldMapping inticates the relationship between input field and table column or user variable
type FieldMapping struct {
	Column  *table.Column
	UserVar *ast.VariableExpr
}

// initLoadColumns sets columns which the input fields loaded to.
func (e *LoadDataInfo) initLoadColumns(columnNames []string) error {
	var cols []*table.Column
	var missingColName string
	var err error
	tableCols := e.Table.Cols()

	if len(columnNames) != len(tableCols) {
		for _, v := range e.ColumnAssignments {
			columnNames = append(columnNames, v.Column.Name.O)
		}

		cols, missingColName = table.FindCols(tableCols, columnNames, e.Table.Meta().PKIsHandle)
		if missingColName != "" {
			return errors.Errorf("LOAD DATA INTO %s: unknown column %s", e.Table.Meta().Name.O, missingColName)
		}
	} else {
		cols = tableCols
	}

	for _, col := range cols {
		if !col.IsGenerated() {
			e.insertColumns = append(e.insertColumns, col)
		}
		if col.Name.L == model.ExtraHandleName.L {
			if !e.ctx.GetSessionVars().AllowWriteRowID {
				return errors.Errorf("load data statement for _tidb_rowid are not supported.")
			}
			e.hasExtraHandle = true
			break
		}
	}

	// Check column whether is specified only once.
	err = table.CheckOnce(cols)
	if err != nil {
		return err
	}

	return nil
}

// initFieldMappings make a field mapping slice to implicitly map input field to table column or user defined variable
// the slice's order is the same as the order of the input fields.
// Returns a slice of same ordered column names without user defined variable names.
func (e *LoadDataInfo) initFieldMappings() []string {
	columns := make([]string, 0, len(e.ColumnsAndUserVars)+len(e.ColumnAssignments))
	tableCols := e.Table.Cols()

	if len(e.ColumnsAndUserVars) == 0 {
		for _, v := range tableCols {
			fieldMapping := &FieldMapping{
				Column: v,
			}
			e.FieldMappings = append(e.FieldMappings, fieldMapping)
			columns = append(columns, v.Name.O)
		}

		return columns
	}

	var column *table.Column

	for _, v := range e.ColumnsAndUserVars {
		if v.ColumnName != nil {
			column = table.FindCol(tableCols, v.ColumnName.Name.O)
			columns = append(columns, v.ColumnName.Name.O)
		} else {
			column = nil
		}

		fieldMapping := &FieldMapping{
			Column:  column,
			UserVar: v.UserVar,
		}
		e.FieldMappings = append(e.FieldMappings, fieldMapping)
	}

	return columns
}

// GetRows getter for rows
func (e *LoadDataInfo) GetRows() [][]types.Datum {
	return e.rows
}

// GetCurBatchCnt getter for curBatchCnt
func (e *LoadDataInfo) GetCurBatchCnt() uint64 {
	return e.curBatchCnt
}

// CloseTaskQueue preparing routine to inform commit routine no more data
func (e *LoadDataInfo) CloseTaskQueue() {
	close(e.commitTaskQueue)
}

// InitQueues initialize task queue and error report queue
func (e *LoadDataInfo) InitQueues() {
	e.commitTaskQueue = make(chan CommitTask, taskQueueSize)
	e.StopCh = make(chan struct{}, 2)
	e.QuitCh = make(chan struct{})
}

// StartStopWatcher monitor StopCh to force quit
func (e *LoadDataInfo) StartStopWatcher() {
	go func() {
		<-e.StopCh
		close(e.QuitCh)
	}()
}

// ForceQuit let commit quit directly
func (e *LoadDataInfo) ForceQuit() {
	e.StopCh <- struct{}{}
}

// MakeCommitTask produce commit task with data in LoadDataInfo.rows LoadDataInfo.curBatchCnt
func (e *LoadDataInfo) MakeCommitTask() CommitTask {
	return CommitTask{e.curBatchCnt, e.rows}
}

// EnqOneTask feed one batch commit task to commit work
func (e *LoadDataInfo) EnqOneTask(ctx context.Context) error {
	var err error
	if e.curBatchCnt > 0 {
		sendOk := false
		for !sendOk {
			select {
			case e.commitTaskQueue <- e.MakeCommitTask():
				sendOk = true
			case <-e.QuitCh:
				err = errors.New("EnqOneTask forced to quit")
				logutil.Logger(ctx).Error("EnqOneTask forced to quit, possible commitWork error")
				return err
			}
		}
		// reset rows buffer, will reallocate buffer but NOT reuse
		e.SetMaxRowsInBatch(e.maxRowsInBatch)
	}
	return err
}

// CommitOneTask insert Data from LoadDataInfo.rows, then make commit and refresh txn
func (e *LoadDataInfo) CommitOneTask(ctx context.Context, task CommitTask) error {
	var err error
	defer func() {
		if err != nil {
			e.Ctx.StmtRollback()
		}
	}()
	err = e.CheckAndInsertOneBatch(ctx, task.rows, task.cnt)
	if err != nil {
		logutil.Logger(ctx).Error("commit error CheckAndInsert", zap.Error(err))
		return err
	}
	failpoint.Inject("commitOneTaskErr", func() error {
		return errors.New("mock commit one task error")
	})
	e.Ctx.StmtCommit()
	// Make sure that there are no retries when committing.
	if err = e.Ctx.RefreshTxnCtx(ctx); err != nil {
		logutil.Logger(ctx).Error("commit error refresh", zap.Error(err))
		return err
	}
	return err
}

// CommitWork commit batch sequentially
func (e *LoadDataInfo) CommitWork(ctx context.Context) error {
	var err error
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("CommitWork panicked",
				zap.Reflect("r", r),
				zap.Stack("stack"))
		}
		if err != nil || r != nil {
			e.ForceQuit()
		}
		if err != nil {
			e.ctx.StmtRollback()
		}
	}()
	var tasks uint64
	var end = false
	for !end {
		select {
		case <-e.QuitCh:
			err = errors.New("commit forced to quit")
			logutil.Logger(ctx).Error("commit forced to quit, possible preparation failed")
			return err
		case commitTask, ok := <-e.commitTaskQueue:
			if ok {
				start := time.Now()
				err = e.CommitOneTask(ctx, commitTask)
				if err != nil {
					break
				}
				tasks++
				logutil.Logger(ctx).Info("commit one task success",
					zap.Duration("commit time usage", time.Since(start)),
					zap.Uint64("keys processed", commitTask.cnt),
					zap.Uint64("tasks processed", tasks),
					zap.Int("tasks in queue", len(e.commitTaskQueue)))
			} else {
				end = true
			}
		}
		if err != nil {
			logutil.Logger(ctx).Error("load data commit work error", zap.Error(err))
			break
		}
		if atomic.CompareAndSwapUint32(&e.Ctx.GetSessionVars().Killed, 1, 0) {
			logutil.Logger(ctx).Info("load data query interrupted quit data processing")
			err = ErrQueryInterrupted
			break
		}
	}
	return err
}

// SetMaxRowsInBatch sets the max number of rows to insert in a batch.
func (e *LoadDataInfo) SetMaxRowsInBatch(limit uint64) {
	e.maxRowsInBatch = limit
	e.rows = make([][]types.Datum, 0, limit)
	e.curBatchCnt = 0
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
		idx := strings.Index(string(hack.String(prevData)), e.LinesInfo.Starting)
		if idx != -1 {
			return prevData[idx:], curData
		}

		// starting symbol in the middle of prevData and curData
		restStart := curData
		if len(curData) >= startingLen {
			restStart = curData[:startingLen-1]
		}
		prevData = append(prevData, restStart...)
		idx = strings.Index(string(hack.String(prevData)), e.LinesInfo.Starting)
		if idx != -1 {
			return prevData[idx:prevLen], curData
		}
	}

	// starting symbol in the curData
	idx := strings.Index(string(hack.String(curData)), e.LinesInfo.Starting)
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
		endIdx = strings.Index(string(hack.String(curData[curStartIdx:])), e.LinesInfo.Terminated)
	}
	if endIdx == -1 {
		// no terminated symbol
		if len(prevData) == 0 {
			return nil, curData, true
		}

		// terminated symbol in the middle of prevData and curData
		curData = append(prevData, curData...)
		endIdx = strings.Index(string(hack.String(curData[startingLen:])), e.LinesInfo.Terminated)
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
	endIdx = strings.Index(string(hack.String(prevData[startingLen:])), e.LinesInfo.Terminated)
	if endIdx >= prevLen {
		return prevData[startingLen : startingLen+endIdx], curData[nextDataIdx:], true
	}

	// terminated symbol in the middle of prevData and curData
	lineLen := startingLen + endIdx + terminatedLen
	return prevData[startingLen : startingLen+endIdx], curData[lineLen-prevLen:], true
}

// InsertData inserts data into specified table according to the specified format.
// If it has the rest of data isn't completed the processing, then it returns without completed data.
// If the number of inserted rows reaches the batchRows, then the second return value is true.
// If prevData isn't nil and curData is nil, there are no other data to deal with and the isEOF is true.
func (e *LoadDataInfo) InsertData(ctx context.Context, prevData, curData []byte) ([]byte, bool, error) {
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

		if e.IgnoreLines > 0 {
			e.IgnoreLines--
			continue
		}
		cols, err := e.getFieldsFromLine(line)
		if err != nil {
			return nil, false, err
		}
		// rowCount will be used in fillRow(), last insert ID will be assigned according to the rowCount = 1.
		// So should add first here.
		e.rowCount++
		e.rows = append(e.rows, e.colsToRow(ctx, cols))
		e.curBatchCnt++
		if e.maxRowsInBatch != 0 && e.rowCount%e.maxRowsInBatch == 0 {
			reachLimit = true
			logutil.Logger(ctx).Info("batch limit hit when inserting rows", zap.Int("maxBatchRows", e.maxChunkSize),
				zap.Uint64("totalRows", e.rowCount))
			break
		}
	}
	return curData, reachLimit, nil
}

// CheckAndInsertOneBatch is used to commit one transaction batch full filled data
func (e *LoadDataInfo) CheckAndInsertOneBatch(ctx context.Context, rows [][]types.Datum, cnt uint64) error {
	var err error
	if cnt == 0 {
		return err
	}
	e.ctx.GetSessionVars().StmtCtx.AddRecordRows(cnt)
	err = e.batchCheckAndInsert(ctx, rows[0:cnt], e.addRecordLD)
	if err != nil {
		return err
	}
	return err
}

// SetMessage sets info message(ERR_LOAD_INFO) generated by LOAD statement, it is public because of the special way that
// LOAD statement is handled.
func (e *LoadDataInfo) SetMessage() {
	stmtCtx := e.ctx.GetSessionVars().StmtCtx
	numRecords := stmtCtx.RecordRows()
	numDeletes := 0
	numSkipped := numRecords - stmtCtx.CopiedRows()
	numWarnings := stmtCtx.WarningCount()
	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrLoadInfo], numRecords, numDeletes, numSkipped, numWarnings)
	e.ctx.GetSessionVars().StmtCtx.SetMessage(msg)
}

func (e *LoadDataInfo) colsToRow(ctx context.Context, cols []field) []types.Datum {
	row := make([]types.Datum, 0, len(e.insertColumns))

	for i := 0; i < len(e.FieldMappings); i++ {
		if i >= len(cols) {
			if e.FieldMappings[i].Column == nil {
				sessionVars := e.Ctx.GetSessionVars()
				sessionVars.SetUserVar(e.FieldMappings[i].UserVar.Name, "", mysql.DefaultCollationName)
				continue
			}

			// If some columns is missing and their type is time and has not null flag, they should be set as current time.
			if types.IsTypeTime(e.FieldMappings[i].Column.Tp) && mysql.HasNotNullFlag(e.FieldMappings[i].Column.Flag) {
				row = append(row, types.NewTimeDatum(types.CurrentTime(e.FieldMappings[i].Column.Tp)))
				continue
			}

			row = append(row, types.NewDatum(nil))
			continue
		}

		if e.FieldMappings[i].Column == nil {
			sessionVars := e.Ctx.GetSessionVars()
			sessionVars.SetUserVar(e.FieldMappings[i].UserVar.Name, string(cols[i].str), mysql.DefaultCollationName)
			continue
		}

		// The field with only "\N" in it is handled as NULL in the csv file.
		// See http://dev.mysql.com/doc/refman/5.7/en/load-data.html
		if cols[i].maybeNull && string(cols[i].str) == "N" {
			row = append(row, types.NewDatum(nil))
			continue
		}

		row = append(row, types.NewDatum(string(cols[i].str)))
	}
	for i := 0; i < len(e.ColumnAssignments); i++ {
		// eval expression of `SET` clause
		d, err := expression.EvalAstExpr(e.Ctx, e.ColumnAssignments[i].Expr)
		if err != nil {
			e.handleWarning(err)
			return nil
		}
		row = append(row, d)
	}

	// a new row buffer will be allocated in getRow
	newRow, err := e.getRow(ctx, row)
	if err != nil {
		e.handleWarning(err)
		return nil
	}

	return newRow
}

func (e *LoadDataInfo) addRecordLD(ctx context.Context, row []types.Datum) error {
	if row == nil {
		return nil
	}
	err := e.addRecord(ctx, row)
	if err != nil {
		e.handleWarning(err)
		return err
	}
	return nil
}

type field struct {
	str       []byte
	maybeNull bool
	enclosed  bool
}

type fieldWriter struct {
	pos           int
	ReadBuf       []byte
	OutputBuf     []byte
	term          string
	enclosedChar  byte
	fieldTermChar byte
	isEnclosed    bool
	isLineStart   bool
	isFieldStart  bool
}

func (w *fieldWriter) Init(enclosedChar byte, fieldTermChar byte, readBuf []byte, term string) {
	w.isEnclosed = false
	w.isLineStart = true
	w.isFieldStart = true
	w.ReadBuf = readBuf
	w.enclosedChar = enclosedChar
	w.fieldTermChar = fieldTermChar
	w.term = term
}

func (w *fieldWriter) putback() {
	w.pos--
}

func (w *fieldWriter) getChar() (bool, byte) {
	if w.pos < len(w.ReadBuf) {
		ret := w.ReadBuf[w.pos]
		w.pos++
		return true, ret
	}
	return false, 0
}

func (w *fieldWriter) isTerminator() bool {
	chkpt, isterm := w.pos, true
	for i := 1; i < len(w.term); i++ {
		flag, ch := w.getChar()
		if !flag || ch != w.term[i] {
			isterm = false
			break
		}
	}
	if !isterm {
		w.pos = chkpt
		return false
	}
	return true
}

func (w *fieldWriter) outputField(enclosed bool) field {
	var fild []byte
	start := 0
	if enclosed {
		start = 1
	}
	for i := start; i < len(w.OutputBuf); i++ {
		fild = append(fild, w.OutputBuf[i])
	}
	if len(fild) == 0 {
		fild = []byte("")
	}
	w.OutputBuf = w.OutputBuf[0:0]
	w.isEnclosed = false
	w.isFieldStart = true
	return field{fild, false, enclosed}
}

func (w *fieldWriter) GetField() (bool, field) {
	// The first return value implies whether fieldWriter read the last character of line.
	if w.isLineStart {
		_, ch := w.getChar()
		if ch == w.enclosedChar {
			w.isEnclosed = true
			w.isFieldStart, w.isLineStart = false, false
			w.OutputBuf = append(w.OutputBuf, ch)
		} else {
			w.putback()
		}
	}
	for {
		flag, ch := w.getChar()
		if !flag {
			ret := w.outputField(false)
			return true, ret
		}
		if ch == w.enclosedChar && w.isFieldStart {
			// If read enclosed char at field start.
			w.isEnclosed = true
			w.OutputBuf = append(w.OutputBuf, ch)
			w.isLineStart, w.isFieldStart = false, false
			continue
		}
		w.isLineStart, w.isFieldStart = false, false
		if ch == w.fieldTermChar && !w.isEnclosed {
			// If read filed terminate char.
			if w.isTerminator() {
				ret := w.outputField(false)
				return false, ret
			}
			w.OutputBuf = append(w.OutputBuf, ch)
		} else if ch == w.enclosedChar && w.isEnclosed {
			// If read enclosed char, look ahead.
			flag, ch = w.getChar()
			if !flag {
				ret := w.outputField(true)
				return true, ret
			} else if ch == w.enclosedChar {
				w.OutputBuf = append(w.OutputBuf, ch)
				continue
			} else if ch == w.fieldTermChar {
				// If the next char is fieldTermChar, look ahead.
				if w.isTerminator() {
					ret := w.outputField(true)
					return false, ret
				}
				w.OutputBuf = append(w.OutputBuf, ch)
			} else {
				// If there is no terminator behind enclosedChar, put the char back.
				w.OutputBuf = append(w.OutputBuf, w.enclosedChar)
				w.putback()
			}
		} else if ch == '\\' {
			// TODO: escape only support '\'
			// When the escaped character is interpreted as if
			// it was not escaped, backslash is ignored.
			flag, ch = w.getChar()
			if flag {
				w.OutputBuf = append(w.OutputBuf, '\\')
				w.OutputBuf = append(w.OutputBuf, ch)
			}
		} else {
			w.OutputBuf = append(w.OutputBuf, ch)
		}
	}
}

// getFieldsFromLine splits line according to fieldsInfo.
func (e *LoadDataInfo) getFieldsFromLine(line []byte) ([]field, error) {
	var (
		reader fieldWriter
		fields []field
	)

	if len(line) == 0 {
		str := []byte("")
		fields = append(fields, field{str, false, false})
		return fields, nil
	}

	reader.Init(e.FieldsInfo.Enclosed, e.FieldsInfo.Terminated[0], line, e.FieldsInfo.Terminated)
	for {
		eol, f := reader.GetField()
		f = f.escape()
		if bytes.Equal(f.str, null) && !f.enclosed {
			f.str = []byte{'N'}
			f.maybeNull = true
		}
		fields = append(fields, f)
		if eol {
			break
		}
	}
	return fields, nil
}

// escape handles escape characters when running load data statement.
// See http://dev.mysql.com/doc/refman/5.7/en/load-data.html
// TODO: escape only support '\' as the `ESCAPED BY` character, it should support specify characters.
func (f *field) escape() field {
	pos := 0
	for i := 0; i < len(f.str); i++ {
		c := f.str[i]
		if i+1 < len(f.str) && f.str[i] == '\\' {
			c = f.escapeChar(f.str[i+1])
			i++
		}

		f.str[pos] = c
		pos++
	}
	return field{f.str[:pos], f.maybeNull, f.enclosed}
}

func (f *field) escapeChar(c byte) byte {
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
	case 'N':
		f.maybeNull = true
		return c
	case '\\':
		return c
	default:
		return c
	}
}

// loadDataVarKeyType is a dummy type to avoid naming collision in context.
type loadDataVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k loadDataVarKeyType) String() string {
	return "load_data_var"
}

// LoadDataVarKey is a variable key for load data.
const LoadDataVarKey loadDataVarKeyType = 0
