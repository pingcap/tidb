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
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
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
		columns:      cols,
	}
}

// Next implements the Executor Next interface.
func (e *LoadDataExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
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
	return nil
}

// LoadDataInfo saves the information of loading data operation.
type LoadDataInfo struct {
	*InsertValues

	row        []types.Datum
	Path       string
	Table      table.Table
	FieldsInfo *ast.FieldsClause
	LinesInfo  *ast.LinesClause
	Ctx        sessionctx.Context
	columns    []*table.Column
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
// If it has the rest of data isn't completed the processing, then is returns without completed data.
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
	err := e.batchCheckAndInsert(rows, e.insertData)
	if err != nil {
		return nil, reachLimit, errors.Trace(err)
	}
	if e.lastInsertID != 0 {
		e.ctx.GetSessionVars().SetLastInsertID(e.lastInsertID)
	}

	return curData, reachLimit, nil
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
	row, err := e.fillRowData(e.columns, e.row)
	if err != nil {
		e.handleWarning(err,
			fmt.Sprintf("Load Data: insert data:%v failed:%v", e.row, errors.ErrorStack(err)))
		return nil
	}
	return row
}

func (e *LoadDataInfo) insertData(row []types.Datum) (int64, error) {
	if row == nil {
		return 0, nil
	}
	h, err := e.Table.AddRecord(e.ctx, row, false)
	if err != nil {
		e.handleWarning(err,
			fmt.Sprintf("Load Data: insert data:%v failed:%v", e.row, errors.ErrorStack(err)))
	}
	return h, nil
}

type field struct {
	str       []byte
	maybeNull bool
}

// getFieldsFromLine splits line according to fieldsInfo.
func (e *LoadDataInfo) getFieldsFromLine(line []byte) ([]field, error) {
	var sep []byte
	if e.FieldsInfo.Enclosed != 0 {
		if line[0] != e.FieldsInfo.Enclosed || line[len(line)-1] != e.FieldsInfo.Enclosed {
			return nil, errors.Errorf("line %s should begin and end with %c", string(line), e.FieldsInfo.Enclosed)
		}
		line = line[1 : len(line)-1]
		sep = make([]byte, 0, len(e.FieldsInfo.Terminated)+2)
		sep = append(sep, e.FieldsInfo.Enclosed)
		sep = append(sep, e.FieldsInfo.Terminated...)
		sep = append(sep, e.FieldsInfo.Enclosed)
	} else {
		sep = []byte(e.FieldsInfo.Terminated)
	}
	rawCols := bytes.Split(line, sep)
	fields := make([]field, 0, len(rawCols))
	for _, v := range rawCols {
		f := field{v, false}
		fields = append(fields, f.escape())
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
	return field{f.str[:pos], f.maybeNull}
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
