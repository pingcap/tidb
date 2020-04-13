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
	"strings"
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

var (
	null = []byte("NULL")
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
	insertVal := &InsertValues{baseExecutor: newBaseExecutor(ctx, nil, insertValuesLabel), Table: tbl}
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
}

// SetMaxRowsInBatch sets the max number of rows to insert in a batch.
func (e *LoadDataInfo) SetMaxRowsInBatch(limit uint64) {
	e.maxRowsInBatch = limit
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
	rows := make([][]types.Datum, 0, e.maxRowsInBatch)
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
		rows = append(rows, e.colsToRow(ctx, cols))
		if e.maxRowsInBatch != 0 && e.rowCount%e.maxRowsInBatch == 0 {
			reachLimit = true
			logutil.Logger(context.Background()).Info("batch limit hit when inserting rows", zap.Int("maxBatchRows", e.maxChunkSize),
				zap.Uint64("totalRows", e.rowCount))
			break
		}
	}
	e.ctx.GetSessionVars().StmtCtx.AddRecordRows(uint64(len(rows)))
	err := e.batchCheckAndInsert(rows, e.addRecordLD)
	if err != nil {
		return nil, reachLimit, err
	}
	return curData, reachLimit, nil
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
	totalCols := e.Table.Cols()
	for i := 0; i < len(e.row); i++ {
		if i >= len(cols) {
			// If some columns is missing and their type is time and has not null flag, they should be set as current time.
			if types.IsTypeTime(totalCols[i].Tp) && mysql.HasNotNullFlag(totalCols[i].Flag) {
				e.row[i].SetMysqlTime(types.CurrentTime(totalCols[i].Tp))
				continue
			}
			e.row[i].SetNull()
			continue
		}
		// The field with only "\N" in it is handled as NULL in the csv file.
		// See http://dev.mysql.com/doc/refman/5.7/en/load-data.html
		if cols[i].maybeNull && string(cols[i].str) == "N" {
			e.row[i].SetNull()
		} else {
			e.row[i].SetString(string(cols[i].str))
		}
	}
	row, err := e.getRow(ctx, e.row)
	if err != nil {
		e.handleWarning(err)
		return nil
	}
	return row
}

func (e *LoadDataInfo) addRecordLD(row []types.Datum) (int64, error) {
	if row == nil {
		return 0, nil
	}
	h, err := e.addRecord(row)
	if err != nil {
		e.handleWarning(err)
		return 0, err
	}
	return h, nil
}

type field struct {
	str       []byte
	maybeNull bool
	enclosed  bool
}

type fieldWriter struct {
	pos           int
	enclosedChar  byte
	fieldTermChar byte
	term          string
	isEnclosed    bool
	isLineStart   bool
	isFieldStart  bool
	ReadBuf       []byte
	OutputBuf     []byte
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
			w.OutputBuf = append(w.OutputBuf, ch)
			flag, ch = w.getChar()
			if flag {
				if ch == w.enclosedChar {
					w.OutputBuf = append(w.OutputBuf, ch)
				} else {
					w.putback()
				}
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
