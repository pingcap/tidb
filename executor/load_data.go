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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	log "github.com/sirupsen/logrus"
)

// LoadDataExec represents a load data executor.
type LoadDataExec struct {
	baseExecutor

	IsLocal      bool
	loadDataInfo *LoadDataInfo
}

// NewLoadDataInfo returns a LoadDataInfo structure, and it's only used for tests now.
func NewLoadDataInfo(ctx sessionctx.Context, row []types.Datum, tbl table.Table, cols []*table.Column) *LoadDataInfo {
	insertVal := &InsertValues{baseExecutor: newBaseExecutor(ctx, nil, "InsertValues"), Table: tbl}
	return &LoadDataInfo{
		row:          row,
		InsertValues: insertVal,
		Table:        tbl,
		Ctx:          ctx,
	}
}

// Next implements the Executor Next interface.
func (e *LoadDataExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	req.GrowAndReset(e.maxChunkSize)
	// TODO: support load data without local field.
	if !e.IsLocal {
		return errors.New("Load Data: don't support load data without local field")
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
func (e *LoadDataInfo) InsertData(prevData, curData []byte) ([]byte, bool, error) {
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
			return nil, false, errors.Trace(err)
		}
		rows = append(rows, e.colsToRow(cols))
		e.rowCount++
		if e.maxRowsInBatch != 0 && e.rowCount%e.maxRowsInBatch == 0 {
			reachLimit = true
			log.Infof("This insert rows has reached the batch %d, current total rows %d",
				e.maxRowsInBatch, e.rowCount)
			break
		}
	}
	e.ctx.GetSessionVars().StmtCtx.AddRecordRows(uint64(len(rows)))
	err := e.batchCheckAndInsert(rows, e.addRecordLD)
	if err != nil {
		return nil, reachLimit, errors.Trace(err)
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

func (e *LoadDataInfo) colsToRow(cols []field) []types.Datum {
	for i := 0; i < len(e.row); i++ {
		if i >= len(cols) {
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
	row, err := e.getRow(e.row)
	if err != nil {
		e.handleWarning(err,
			fmt.Sprintf("Load Data: insert data:%v failed:%v", e.row, errors.ErrorStack(err)))
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
		e.handleWarning(err,
			fmt.Sprintf("Load Data: insert data:%v failed:%v", e.row, errors.ErrorStack(err)))
	}
	return h, nil
}

type field struct {
	str       []byte
	maybeNull bool
	enclosed  bool
}

// getFieldsFromLine splits line according to fieldsInfo.
func (e *LoadDataInfo) getFieldsFromLine(line []byte) ([]field, error) {
	var (
		// fields slice store fields which are not processed by escape()
		fields []field
		// ret slice store final results
		ret []field
		// buf is buffer to process field.str
		buf []byte
	)

	// enclosed is flag to represent whether is start from a enclosed character
	// start is flag to represent whether is start of word
	enclosed, start := false, true
	pos := 0

	enclosedChar := e.FieldsInfo.Enclosed
	fieldTermChar := e.FieldsInfo.Terminated[0]

	if len(line) == 0 {
		// If line is an empty string
		ret = append(ret, field{[]byte(""), false, false})
		return ret, nil
	}
	// getchar
	ch := line[pos]
	pos++
	// start of line
	if ch == enclosedChar {
		// If started with enclosed char, set enclosed flag to true
		enclosed = true
		start = false
		buf = append(buf, ch)
	} else {
		// Else roll back
		pos--
	}
	for {
		if pos == len(line) {
			// If read the end of line
			var fild []byte
			for i := 0; i < len(buf); i++ {
				fild = append(fild, buf[i])
			}
			if len(fild) == 0 {
				fild = []byte("")
			}
			fields = append(fields, field{fild, false, false})
			enclosed = false
			// clear buffer
			buf = buf[0:0]
			break
		}
		// getchar
		ch = line[pos]
		pos++
		// If is at start of line and has enclosed char, set flag
		if ch == enclosedChar && start {
			start = false
			enclosed = true
			buf = append(buf, ch)
			continue
		} else if start {
			start = false
		}
		// In order to judge whether is at terminate of word, save current pos and char.
		chkpt := pos
		curChar := ch
		if ch == fieldTermChar && !enclosed {
			isTerm, i := true, 0
			// judge whether is at the end of word.
			for i = 1; i < len(e.FieldsInfo.Terminated); i++ {
				if pos >= len(line) {
					isTerm = false
					break
				}
				ch = line[pos]
				pos++
				if ch != e.FieldsInfo.Terminated[i] {
					isTerm = false
					break
				}
			}
			if isTerm {
				// If it is the end or word, save field and clear buffer.
				var fild []byte
				for i := 0; i < len(buf); i++ {
					fild = append(fild, buf[i])
				}
				if len(fild) == 0 {
					fild = []byte("")
				}
				fields = append(fields, field{fild, false, false})
				buf = buf[0:0]
				enclosed = false
				start = true
				continue
			}
			// if not at the ned, roll back.
			pos = chkpt
			buf = append(buf, curChar)
		} else if ch == enclosedChar && enclosed {
			// If it is enclosed and has a enclosed char currently
			if pos == len(line) {
				// If reach the end of line
				var fild []byte
				for i := 1; i < len(buf); i++ {
					fild = append(fild, buf[i])
				}
				if len(fild) == 0 {
					fild = []byte("")
				}
				fields = append(fields, field{fild, false, true})
				enclosed = false
				start = true
				buf = buf[0:0]
				break
			}
			if pos < len(line) {
				// getchar
				ch = line[pos]
				pos++
			}
			if ch == enclosedChar {
				// Remove dupplicated
				buf = append(buf, ch)
				continue
			}
			if ch == fieldTermChar {
				// If reach the end of field
				chkpt := pos
				curChar := ch
				isTerm, i := true, 0
				for i = 1; i < len(e.FieldsInfo.Terminated); i++ {
					if pos >= len(line) {
						isTerm = false
						break
					}
					ch = line[pos]
					pos++
					if ch != e.FieldsInfo.Terminated[i] {
						isTerm = false
						break
					}
				}
				if isTerm {
					var fild []byte
					for i := 1; i < len(buf); i++ {
						fild = append(fild, buf[i])
					}
					if len(fild) == 0 {
						fild = []byte("")
					}
					fields = append(fields, field{fild, false, true})
					buf = buf[0:0]
					enclosed = false
					start = true
					continue
				}
				// If not terminated, roll back
				pos = chkpt
				buf = append(buf, curChar)
			}
			// roll back
			pos--
		} else if ch == '\\' {
			// deal with \"
			buf = append(buf, ch)
			if pos < len(line) {
				ch = line[pos]
				pos++
				if ch == enclosedChar {
					buf = append(buf, ch)
				} else {
					pos--
				}
			}
		} else {
			// For regular character
			buf = append(buf, ch)
		}
	}
	for _, v := range fields {
		f := v.escape()
		if string(f.str) == "NULL" && !f.enclosed {
			f.str = []byte{'N'}
			f.maybeNull = true
		}
		ret = append(ret, f)
	}
	return ret, nil
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
