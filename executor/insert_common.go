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
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"go.uber.org/zap"
)

// InsertValues is the data to insert.
// nolint:structcheck
type InsertValues struct {
	baseExecutor

	rowCount       uint64
	curBatchCnt    uint64
	maxRowsInBatch uint64
	lastInsertID   uint64

	SelectExec Executor

	Table   table.Table
	Columns []*ast.ColumnName
	Lists   [][]expression.Expression
	SetList []*expression.Assignment

	GenExprs []expression.Expression

	insertColumns []*table.Column

	// colDefaultVals is used to store casted default value.
	// Because not every insert statement needs colDefaultVals, so we will init the buffer lazily.
	colDefaultVals  []defaultVal
	evalBuffer      chunk.MutRow
	evalBufferTypes []*types.FieldType

	allAssignmentsAreConstant bool

	hasRefCols     bool
	hasExtraHandle bool

	// lazyFillAutoID indicates whatever had been filled the autoID lazily to datum. This is used for being compatible with JDBC using getGeneratedKeys().
	// `insert|replace values` can guarantee consecutive autoID in a batch.
	// Other statements like `insert select from` don't guarantee consecutive autoID.
	// https://dev.mysql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html
	lazyFillAutoID bool
	memTracker     *memory.Tracker

	rowLen int

	stats *InsertRuntimeStat

	// isLoadData indicates whatever current goroutine is use for generating batch data. LoadData use two goroutines. One for generate batch data,
	// The other one for commit task, which will invalid txn.
	// We use mutex to protect routine from using invalid txn.
	isLoadData bool
	txnInUse   sync.Mutex
}

type defaultVal struct {
	val types.Datum
	// valid indicates whether the val is evaluated. We evaluate the default value lazily.
	valid bool
}

type insertCommon interface {
	insertCommon() *InsertValues
	exec(ctx context.Context, rows [][]types.Datum) error
}

func (e *InsertValues) insertCommon() *InsertValues {
	return e
}

func (e *InsertValues) exec(_ context.Context, _ [][]types.Datum) error {
	panic("derived should overload exec function")
}

// initInsertColumns sets the explicitly specified columns of an insert statement. There are three cases:
// There are three types of insert statements:
// 1 insert ... values(...)  --> name type column
// 2 insert ... set x=y...   --> set type column
// 3 insert ... (select ..)  --> name type column
// See https://dev.mysql.com/doc/refman/5.7/en/insert.html
func (e *InsertValues) initInsertColumns() error {
	var cols []*table.Column
	var missingColIdx int
	var err error

	tableCols := e.Table.Cols()

	if len(e.SetList) > 0 {
		// Process `set` type column.
		columns := make([]string, 0, len(e.SetList))
		for _, v := range e.SetList {
			columns = append(columns, v.ColName.L)
		}
		cols, missingColIdx = table.FindColumns(tableCols, columns, e.Table.Meta().PKIsHandle)
		if missingColIdx >= 0 {
			return errors.Errorf("INSERT INTO %s: unknown column %s",
				e.Table.Meta().Name.O, e.SetList[missingColIdx].ColName.O)
		}
		if len(cols) == 0 {
			return errors.Errorf("INSERT INTO %s: empty column", e.Table.Meta().Name.O)
		}
	} else if len(e.Columns) > 0 {
		// Process `name` type column.
		columns := make([]string, 0, len(e.Columns))
		for _, v := range e.Columns {
			columns = append(columns, v.Name.L)
		}
		cols, missingColIdx = table.FindColumns(tableCols, columns, e.Table.Meta().PKIsHandle)
		if missingColIdx >= 0 {
			return errors.Errorf("INSERT INTO %s: unknown column %s",
				e.Table.Meta().Name.O, e.Columns[missingColIdx].Name.O)
		}
	} else {
		// If e.Columns are empty, use all columns instead.
		cols = tableCols
	}
	for _, col := range cols {
		if !col.IsGenerated() {
			e.insertColumns = append(e.insertColumns, col)
		}
		if col.Name.L == model.ExtraHandleName.L {
			if !e.ctx.GetSessionVars().AllowWriteRowID {
				return errors.Errorf("insert, update and replace statements for _tidb_rowid are not supported")
			}
			e.hasExtraHandle = true
		}
	}

	// Check column whether is specified only once.
	err = table.CheckOnce(cols)
	if err != nil {
		return err
	}
	return nil
}

func (e *InsertValues) initEvalBuffer() {
	numCols := len(e.Table.Cols())
	if e.hasExtraHandle {
		numCols++
	}
	e.evalBufferTypes = make([]*types.FieldType, numCols)
	for i, col := range e.Table.Cols() {
		e.evalBufferTypes[i] = &col.FieldType
	}
	if e.hasExtraHandle {
		e.evalBufferTypes[len(e.evalBufferTypes)-1] = types.NewFieldType(mysql.TypeLonglong)
	}
	e.evalBuffer = chunk.MutRowFromTypes(e.evalBufferTypes)
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

func (e *InsertValues) processSetList() error {
	if len(e.SetList) > 0 {
		if len(e.Lists) > 0 {
			return errors.Errorf("INSERT INTO %s: set type should not use values", e.Table)
		}
		l := make([]expression.Expression, 0, len(e.SetList))
		for _, v := range e.SetList {
			l = append(l, v.Expr)
		}
		e.Lists = append(e.Lists, l)
	}
	return nil
}

// insertRows processes `insert|replace into values ()` or `insert|replace into set x=y`
func insertRows(ctx context.Context, base insertCommon) (err error) {
	e := base.insertCommon()
	// For `insert|replace into set x=y`, process the set list here.
	if err = e.processSetList(); err != nil {
		return err
	}
	sessVars := e.ctx.GetSessionVars()
	batchSize := sessVars.DMLBatchSize
	batchInsert := sessVars.BatchInsert && !sessVars.InTxn() && variable.EnableBatchDML.Load() && batchSize > 0

	e.lazyFillAutoID = true
	evalRowFunc := e.fastEvalRow
	if !e.allAssignmentsAreConstant {
		evalRowFunc = e.evalRow
	}

	rows := make([][]types.Datum, 0, len(e.Lists))
	memUsageOfRows := int64(0)
	memTracker := e.memTracker
	for i, list := range e.Lists {
		e.rowCount++
		var row []types.Datum
		row, err = evalRowFunc(ctx, list, i)
		if err != nil {
			return err
		}
		rows = append(rows, row)
		if batchInsert && e.rowCount%uint64(batchSize) == 0 {
			memUsageOfRows = types.EstimatedMemUsage(rows[0], len(rows))
			memTracker.Consume(memUsageOfRows)
			// Before batch insert, fill the batch allocated autoIDs.
			rows, err = e.lazyAdjustAutoIncrementDatum(ctx, rows)
			if err != nil {
				return err
			}
			if err = base.exec(ctx, rows); err != nil {
				return err
			}
			rows = rows[:0]
			memTracker.Consume(-memUsageOfRows)
			memUsageOfRows = 0
			if err = e.doBatchInsert(ctx); err != nil {
				return err
			}
		}
	}
	if len(rows) != 0 {
		memUsageOfRows = types.EstimatedMemUsage(rows[0], len(rows))
		memTracker.Consume(memUsageOfRows)
	}
	// Fill the batch allocated autoIDs.
	rows, err = e.lazyAdjustAutoIncrementDatum(ctx, rows)
	if err != nil {
		return err
	}
	err = base.exec(ctx, rows)
	if err != nil {
		return err
	}
	memTracker.Consume(-memUsageOfRows)
	return nil
}

func (e *InsertValues) handleErr(col *table.Column, val *types.Datum, rowIdx int, err error) error {
	if err == nil {
		return nil
	}

	// Convert the error with full messages.
	var (
		colTp   byte
		colName string
	)
	if col != nil {
		colTp = col.GetType()
		colName = col.Name.String()
	}

	if types.ErrDataTooLong.Equal(err) {
		err = resetErrDataTooLong(colName, rowIdx+1, err)
	} else if types.ErrOverflow.Equal(err) {
		err = types.ErrWarnDataOutOfRange.GenWithStackByArgs(colName, rowIdx+1)
	} else if types.ErrTruncated.Equal(err) {
		err = types.ErrTruncated.GenWithStackByArgs(colName, rowIdx+1)
	} else if types.ErrTruncatedWrongVal.Equal(err) && (colTp == mysql.TypeDuration || colTp == mysql.TypeDatetime || colTp == mysql.TypeDate || colTp == mysql.TypeTimestamp) {
		valStr, err1 := val.ToString()
		if err1 != nil {
			logutil.BgLogger().Debug("time truncated error", zap.Error(err1))
		}
		err = errTruncateWrongInsertValue.GenWithStackByArgs(types.TypeStr(colTp), valStr, colName, rowIdx+1)
	} else if types.ErrTruncatedWrongVal.Equal(err) || types.ErrWrongValue.Equal(err) {
		valStr, err1 := val.ToString()
		if err1 != nil {
			logutil.BgLogger().Debug("truncated/wrong value error", zap.Error(err1))
		}
		err = table.ErrTruncatedWrongValueForField.GenWithStackByArgs(types.TypeStr(colTp), valStr, colName, rowIdx+1)
	} else if types.ErrWarnDataOutOfRange.Equal(err) {
		err = types.ErrWarnDataOutOfRange.GenWithStackByArgs(colName, rowIdx+1)
	}

	if !e.ctx.GetSessionVars().StmtCtx.DupKeyAsWarning {
		return err
	}
	// TODO: should not filter all types of errors here.
	e.handleWarning(err)
	return nil
}

// evalRow evaluates a to-be-inserted row. The value of the column may base on another column,
// so we use setValueForRefColumn to fill the empty row some default values when needFillDefaultValues is true.
func (e *InsertValues) evalRow(ctx context.Context, list []expression.Expression, rowIdx int) ([]types.Datum, error) {
	rowLen := len(e.Table.Cols())
	if e.hasExtraHandle {
		rowLen++
	}
	row := make([]types.Datum, rowLen)
	hasValue := make([]bool, rowLen)

	// For statements like `insert into t set a = b + 1`.
	if e.hasRefCols {
		if err := e.setValueForRefColumn(row, hasValue); err != nil {
			return nil, err
		}
	}

	e.evalBuffer.SetDatums(row...)
	for i, expr := range list {
		val, err := expr.Eval(e.evalBuffer.ToRow())
		if err != nil {
			return nil, err
		}
		val1, err := table.CastValue(e.ctx, val, e.insertColumns[i].ToInfo(), false, false)
		if err = e.handleErr(e.insertColumns[i], &val, rowIdx, err); err != nil {
			return nil, err
		}

		offset := e.insertColumns[i].Offset
		val1.Copy(&row[offset])
		hasValue[offset] = true
		e.evalBuffer.SetDatum(offset, val1)
	}
	// Row may lack of generated column, autoIncrement column, empty column here.
	return e.fillRow(ctx, row, hasValue)
}

var emptyRow chunk.Row

func (e *InsertValues) fastEvalRow(ctx context.Context, list []expression.Expression, rowIdx int) ([]types.Datum, error) {
	rowLen := len(e.Table.Cols())
	if e.hasExtraHandle {
		rowLen++
	}
	row := make([]types.Datum, rowLen)
	hasValue := make([]bool, rowLen)
	for i, expr := range list {
		con := expr.(*expression.Constant)
		val, err := con.Eval(emptyRow)
		if err = e.handleErr(e.insertColumns[i], &val, rowIdx, err); err != nil {
			return nil, err
		}
		val1, err := table.CastValue(e.ctx, val, e.insertColumns[i].ToInfo(), false, false)
		if err = e.handleErr(e.insertColumns[i], &val, rowIdx, err); err != nil {
			return nil, err
		}
		offset := e.insertColumns[i].Offset
		row[offset], hasValue[offset] = val1, true
	}
	return e.fillRow(ctx, row, hasValue)
}

// setValueForRefColumn set some default values for the row to eval the row value with other columns,
// it follows these rules:
//     1. for nullable and no default value column, use NULL.
//     2. for nullable and have default value column, use it's default value.
//     3. for not null column, use zero value even in strict mode.
//     4. for auto_increment column, use zero value.
//     5. for generated column, use NULL.
func (e *InsertValues) setValueForRefColumn(row []types.Datum, hasValue []bool) error {
	for i, c := range e.Table.Cols() {
		d, err := e.getColDefaultValue(i, c)
		if err == nil {
			row[i] = d
			if !mysql.HasAutoIncrementFlag(c.GetFlag()) {
				// It is an interesting behavior in MySQL.
				// If the value of auto ID is not explicit, MySQL use 0 value for auto ID when it is
				// evaluated by another column, but it should be used once only.
				// When we fill it as an auto ID column, it should be set as it used to be.
				// So just keep `hasValue` false for auto ID, and the others set true.
				hasValue[c.Offset] = true
			}
		} else if table.ErrNoDefaultValue.Equal(err) {
			row[i] = table.GetZeroValue(c.ToInfo())
			hasValue[c.Offset] = false
		} else if e.handleErr(c, &d, 0, err) != nil {
			return err
		}
	}
	return nil
}

func insertRowsFromSelect(ctx context.Context, base insertCommon) error {
	// process `insert|replace into ... select ... from ...`
	e := base.insertCommon()
	selectExec := e.children[0]
	fields := retTypes(selectExec)
	chk := newFirstChunk(selectExec)
	iter := chunk.NewIterator4Chunk(chk)
	rows := make([][]types.Datum, 0, chk.Capacity())

	sessVars := e.ctx.GetSessionVars()
	batchSize := sessVars.DMLBatchSize
	batchInsert := sessVars.BatchInsert && !sessVars.InTxn() && variable.EnableBatchDML.Load() && batchSize > 0
	memUsageOfRows := int64(0)
	memUsageOfExtraCols := int64(0)
	memTracker := e.memTracker
	extraColsInSel := make([][]types.Datum, 0, chk.Capacity())
	// In order to ensure the correctness of the `transaction write throughput` SLI statistics,
	// just ignore the transaction which contain `insert|replace into ... select ... from ...` statement.
	e.ctx.GetTxnWriteThroughputSLI().SetInvalid()
	for {
		err := Next(ctx, selectExec, chk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
		chkMemUsage := chk.MemoryUsage()
		memTracker.Consume(chkMemUsage)
		for innerChunkRow := iter.Begin(); innerChunkRow != iter.End(); innerChunkRow = iter.Next() {
			innerRow := innerChunkRow.GetDatumRow(fields)
			e.rowCount++
			row, err := e.getRow(ctx, innerRow)
			if err != nil {
				return err
			}
			extraColsInSel = append(extraColsInSel, innerRow[e.rowLen:])
			rows = append(rows, row)
			if batchInsert && e.rowCount%uint64(batchSize) == 0 {
				memUsageOfRows = types.EstimatedMemUsage(rows[0], len(rows))
				memUsageOfExtraCols = types.EstimatedMemUsage(extraColsInSel[0], len(extraColsInSel))
				memTracker.Consume(memUsageOfRows + memUsageOfExtraCols)
				e.ctx.GetSessionVars().CurrInsertBatchExtraCols = extraColsInSel
				if err = base.exec(ctx, rows); err != nil {
					return err
				}
				rows = rows[:0]
				extraColsInSel = extraColsInSel[:0]
				memTracker.Consume(-memUsageOfRows)
				memTracker.Consume(-memUsageOfExtraCols)
				memUsageOfRows = 0
				if err = e.doBatchInsert(ctx); err != nil {
					return err
				}
			}
		}

		if len(rows) != 0 {
			memUsageOfRows = types.EstimatedMemUsage(rows[0], len(rows))
			memUsageOfExtraCols = types.EstimatedMemUsage(extraColsInSel[0], len(extraColsInSel))
			memTracker.Consume(memUsageOfRows + memUsageOfExtraCols)
			e.ctx.GetSessionVars().CurrInsertBatchExtraCols = extraColsInSel
		}
		err = base.exec(ctx, rows)
		if err != nil {
			return err
		}
		rows = rows[:0]
		extraColsInSel = extraColsInSel[:0]
		memTracker.Consume(-memUsageOfRows)
		memTracker.Consume(-memUsageOfExtraCols)
		memTracker.Consume(-chkMemUsage)
	}
	return nil
}

func (e *InsertValues) doBatchInsert(ctx context.Context) error {
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return ErrBatchInsertFail.GenWithStack("BatchInsert failed with error: %v", err)
	}
	e.memTracker.Consume(-int64(txn.Size()))
	e.ctx.StmtCommit()
	if err := sessiontxn.NewTxnInStmt(ctx, e.ctx); err != nil {
		// We should return a special error for batch insert.
		return ErrBatchInsertFail.GenWithStack("BatchInsert failed with error: %v", err)
	}
	return nil
}

// getRow gets the row which from `insert into select from` or `load data`.
// The input values from these two statements are datums instead of
// expressions which are used in `insert into set x=y`.
func (e *InsertValues) getRow(ctx context.Context, vals []types.Datum) ([]types.Datum, error) {
	row := make([]types.Datum, len(e.Table.Cols()))
	hasValue := make([]bool, len(e.Table.Cols()))
	for i := 0; i < e.rowLen; i++ {
		casted, err := table.CastValue(e.ctx, vals[i], e.insertColumns[i].ToInfo(), false, false)
		if e.handleErr(nil, &vals[i], 0, err) != nil {
			return nil, err
		}

		offset := e.insertColumns[i].Offset
		row[offset] = casted
		hasValue[offset] = true
	}

	return e.fillRow(ctx, row, hasValue)
}

// getColDefaultValue gets the column default value.
func (e *InsertValues) getColDefaultValue(idx int, col *table.Column) (d types.Datum, err error) {
	if !col.DefaultIsExpr && e.colDefaultVals != nil && e.colDefaultVals[idx].valid {
		return e.colDefaultVals[idx].val, nil
	}

	var defaultVal types.Datum
	if col.DefaultIsExpr && col.DefaultExpr != nil {
		defaultVal, err = table.EvalColDefaultExpr(e.ctx, col.ToInfo(), col.DefaultExpr)
	} else {
		defaultVal, err = table.GetColDefaultValue(e.ctx, col.ToInfo())
	}
	if err != nil {
		return types.Datum{}, err
	}
	if initialized := e.lazilyInitColDefaultValBuf(); initialized && !col.DefaultIsExpr {
		e.colDefaultVals[idx].val = defaultVal
		e.colDefaultVals[idx].valid = true
	}

	return defaultVal, nil
}

// fillColValue fills the column value if it is not set in the insert statement.
func (e *InsertValues) fillColValue(ctx context.Context, datum types.Datum, idx int, column *table.Column, hasValue bool) (types.Datum,
	error) {
	if mysql.HasAutoIncrementFlag(column.GetFlag()) {
		if e.lazyFillAutoID {
			// Handle hasValue info in autoIncrement column previously for lazy handle.
			if !hasValue {
				datum.SetNull()
			}
			// Store the plain datum of autoIncrement column directly for lazy handle.
			return datum, nil
		}
		d, err := e.adjustAutoIncrementDatum(ctx, datum, hasValue, column)
		if err != nil {
			return types.Datum{}, err
		}
		return d, nil
	}
	tblInfo := e.Table.Meta()
	if ddl.IsAutoRandomColumnID(tblInfo, column.ID) {
		d, err := e.adjustAutoRandomDatum(ctx, datum, hasValue, column)
		if err != nil {
			return types.Datum{}, err
		}
		return d, nil
	}
	if column.ID == model.ExtraHandleID && hasValue {
		d, err := e.adjustImplicitRowID(ctx, datum, hasValue, column)
		if err != nil {
			return types.Datum{}, err
		}
		return d, nil
	}
	if !hasValue {
		d, err := e.getColDefaultValue(idx, column)
		if e.handleErr(column, &datum, 0, err) != nil {
			return types.Datum{}, err
		}
		return d, nil
	}
	return datum, nil
}

// fillRow fills generated columns, auto_increment column and empty column.
// For NOT NULL column, it will return error or use zero value based on sql_mode.
// When lazyFillAutoID is true, fill row will lazily handle auto increment datum for lazy batch allocation.
// `insert|replace values` can guarantee consecutive autoID in a batch.
// Other statements like `insert select from` don't guarantee consecutive autoID.
// https://dev.mysql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html
func (e *InsertValues) fillRow(ctx context.Context, row []types.Datum, hasValue []bool) ([]types.Datum, error) {
	gCols := make([]*table.Column, 0)
	tCols := e.Table.Cols()
	if e.hasExtraHandle {
		col := &table.Column{}
		col.ColumnInfo = model.NewExtraHandleColInfo()
		col.ColumnInfo.Offset = len(tCols)
		tCols = append(tCols, col)
	}
	for i, c := range tCols {
		var err error
		// Evaluate the generated columns later after real columns set
		if c.IsGenerated() {
			gCols = append(gCols, c)
		} else {
			// Get the default value for all no value columns, the auto increment column is different from the others.
			if row[i], err = e.fillColValue(ctx, row[i], i, c, hasValue[i]); err != nil {
				return nil, err
			}
			if !e.lazyFillAutoID || (e.lazyFillAutoID && !mysql.HasAutoIncrementFlag(c.GetFlag())) {
				if err = c.HandleBadNull(&row[i], e.ctx.GetSessionVars().StmtCtx); err != nil {
					return nil, err
				}
			}
		}
	}
	for i, gCol := range gCols {
		colIdx := gCol.ColumnInfo.Offset
		val, err := e.GenExprs[i].Eval(chunk.MutRowFromDatums(row).ToRow())
		if e.ctx.GetSessionVars().StmtCtx.HandleTruncate(err) != nil {
			return nil, err
		}
		row[colIdx], err = table.CastValue(e.ctx, val, gCol.ToInfo(), false, false)
		if err != nil {
			return nil, err
		}
		// Handle the bad null error.
		if err = gCol.HandleBadNull(&row[colIdx], e.ctx.GetSessionVars().StmtCtx); err != nil {
			return nil, err
		}
	}
	return row, nil
}

// isAutoNull can help judge whether a datum is AutoIncrement Null quickly.
// This used to help lazyFillAutoIncrement to find consecutive N datum backwards for batch autoID alloc.
func (e *InsertValues) isAutoNull(ctx context.Context, d types.Datum, col *table.Column) bool {
	var err error
	var recordID int64
	if !d.IsNull() {
		recordID, err = getAutoRecordID(d, &col.FieldType, true)
		if err != nil {
			return false
		}
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		return false
	}
	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	if d.IsNull() || e.ctx.GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		return true
	}
	return false
}

func findAutoIncrementColumn(t table.Table) (col *table.Column, offsetInRow int, found bool) {
	for i, c := range t.Cols() {
		if mysql.HasAutoIncrementFlag(c.GetFlag()) {
			return c, i, true
		}
	}
	return nil, -1, false
}

func setDatumAutoIDAndCast(ctx sessionctx.Context, d *types.Datum, id int64, col *table.Column) error {
	d.SetAutoID(id, col.GetFlag())
	var err error
	*d, err = table.CastValue(ctx, *d, col.ToInfo(), false, false)
	if err == nil && d.GetInt64() < id {
		// Auto ID is out of range, the truncated ID is possible to duplicate with an existing ID.
		// To prevent updating unrelated rows in the REPLACE statement, it is better to throw an error.
		return autoid.ErrAutoincReadFailed
	}
	return err
}

// lazyAdjustAutoIncrementDatum is quite similar to adjustAutoIncrementDatum
// except it will cache auto increment datum previously for lazy batch allocation of autoID.
func (e *InsertValues) lazyAdjustAutoIncrementDatum(ctx context.Context, rows [][]types.Datum) ([][]types.Datum, error) {
	// Not in lazyFillAutoID mode means no need to fill.
	if !e.lazyFillAutoID {
		return rows, nil
	}
	col, idx, found := findAutoIncrementColumn(e.Table)
	if !found {
		return rows, nil
	}
	retryInfo := e.ctx.GetSessionVars().RetryInfo
	rowCount := len(rows)
	for processedIdx := 0; processedIdx < rowCount; processedIdx++ {
		autoDatum := rows[processedIdx][idx]

		var err error
		var recordID int64
		if !autoDatum.IsNull() {
			recordID, err = getAutoRecordID(autoDatum, &col.FieldType, true)
			if err != nil {
				return nil, err
			}
		}
		// Use the value if it's not null and not 0.
		if recordID != 0 {
			err = e.Table.Allocators(e.ctx).Get(autoid.RowIDAllocType).Rebase(ctx, recordID, true)
			if err != nil {
				return nil, err
			}
			e.ctx.GetSessionVars().StmtCtx.InsertID = uint64(recordID)
			retryInfo.AddAutoIncrementID(recordID)
			continue
		}

		// Change NULL to auto id.
		// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
		if autoDatum.IsNull() || e.ctx.GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
			// Consume the auto IDs in RetryInfo first.
			for retryInfo.Retrying && processedIdx < rowCount {
				nextID, ok := retryInfo.GetCurrAutoIncrementID()
				if !ok {
					break
				}
				err = setDatumAutoIDAndCast(e.ctx, &rows[processedIdx][idx], nextID, col)
				if err != nil {
					return nil, err
				}
				processedIdx++
				if processedIdx == rowCount {
					return rows, nil
				}
			}
			// Find consecutive num.
			start := processedIdx
			cnt := 1
			for processedIdx+1 < rowCount && e.isAutoNull(ctx, rows[processedIdx+1][idx], col) {
				processedIdx++
				cnt++
			}
			// AllocBatchAutoIncrementValue allocates batch N consecutive autoIDs.
			// The max value can be derived from adding the increment value to min for cnt-1 times.
			min, increment, err := table.AllocBatchAutoIncrementValue(ctx, e.Table, e.ctx, cnt)
			if e.handleErr(col, &autoDatum, cnt, err) != nil {
				return nil, err
			}
			// It's compatible with mysql setting the first allocated autoID to lastInsertID.
			// Cause autoID may be specified by user, judge only the first row is not suitable.
			if e.lastInsertID == 0 {
				e.lastInsertID = uint64(min)
			}
			// Assign autoIDs to rows.
			for j := 0; j < cnt; j++ {
				offset := j + start
				id := int64(uint64(min) + uint64(j)*uint64(increment))
				err = setDatumAutoIDAndCast(e.ctx, &rows[offset][idx], id, col)
				if err != nil {
					return nil, err
				}
				retryInfo.AddAutoIncrementID(id)
			}
			continue
		}

		err = setDatumAutoIDAndCast(e.ctx, &rows[processedIdx][idx], recordID, col)
		if err != nil {
			return nil, err
		}
		retryInfo.AddAutoIncrementID(recordID)
	}
	return rows, nil
}

func (e *InsertValues) adjustAutoIncrementDatum(ctx context.Context, d types.Datum, hasValue bool, c *table.Column) (types.Datum, error) {
	retryInfo := e.ctx.GetSessionVars().RetryInfo
	if retryInfo.Retrying {
		id, ok := retryInfo.GetCurrAutoIncrementID()
		if ok {
			err := setDatumAutoIDAndCast(e.ctx, &d, id, c)
			if err != nil {
				return types.Datum{}, err
			}
			return d, nil
		}
	}

	var err error
	var recordID int64
	if !hasValue {
		d.SetNull()
	}
	if !d.IsNull() {
		recordID, err = getAutoRecordID(d, &c.FieldType, true)
		if err != nil {
			return types.Datum{}, err
		}
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		err = e.Table.Allocators(e.ctx).Get(autoid.RowIDAllocType).Rebase(ctx, recordID, true)
		if err != nil {
			return types.Datum{}, err
		}
		e.ctx.GetSessionVars().StmtCtx.InsertID = uint64(recordID)
		retryInfo.AddAutoIncrementID(recordID)
		return d, nil
	}

	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	if d.IsNull() || e.ctx.GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		recordID, err = table.AllocAutoIncrementValue(ctx, e.Table, e.ctx)
		if e.handleErr(c, &d, 0, err) != nil {
			return types.Datum{}, err
		}
		// It's compatible with mysql setting the first allocated autoID to lastInsertID.
		// Cause autoID may be specified by user, judge only the first row is not suitable.
		if e.lastInsertID == 0 {
			e.lastInsertID = uint64(recordID)
		}
	}

	err = setDatumAutoIDAndCast(e.ctx, &d, recordID, c)
	if err != nil {
		return types.Datum{}, err
	}
	retryInfo.AddAutoIncrementID(recordID)
	return d, nil
}

func getAutoRecordID(d types.Datum, target *types.FieldType, isInsert bool) (int64, error) {
	var recordID int64
	switch target.GetType() {
	case mysql.TypeFloat, mysql.TypeDouble:
		f := d.GetFloat64()
		if isInsert {
			recordID = int64(math.Round(f))
		} else {
			recordID = int64(f)
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		recordID = d.GetInt64()
	default:
		return 0, errors.Errorf("unexpected field type [%v]", target.GetType())
	}

	return recordID, nil
}

func (e *InsertValues) adjustAutoRandomDatum(ctx context.Context, d types.Datum, hasValue bool, c *table.Column) (types.Datum, error) {
	retryInfo := e.ctx.GetSessionVars().RetryInfo
	if retryInfo.Retrying {
		autoRandomID, ok := retryInfo.GetCurrAutoRandomID()
		if ok {
			err := setDatumAutoIDAndCast(e.ctx, &d, autoRandomID, c)
			if err != nil {
				return types.Datum{}, err
			}
			return d, nil
		}
	}

	var err error
	var recordID int64
	if !hasValue {
		d.SetNull()
	}
	if !d.IsNull() {
		recordID, err = getAutoRecordID(d, &c.FieldType, true)
		if err != nil {
			return types.Datum{}, err
		}
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		if !e.ctx.GetSessionVars().AllowAutoRandExplicitInsert {
			return types.Datum{}, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomExplicitInsertDisabledErrMsg)
		}
		err = e.rebaseAutoRandomID(ctx, recordID, &c.FieldType)
		if err != nil {
			return types.Datum{}, err
		}
		e.ctx.GetSessionVars().StmtCtx.InsertID = uint64(recordID)
		err = setDatumAutoIDAndCast(e.ctx, &d, recordID, c)
		if err != nil {
			return types.Datum{}, err
		}
		retryInfo.AddAutoRandomID(recordID)
		return d, nil
	}

	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	if d.IsNull() || e.ctx.GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		recordID, err = e.allocAutoRandomID(ctx, &c.FieldType)
		if err != nil {
			return types.Datum{}, err
		}
		// It's compatible with mysql setting the first allocated autoID to lastInsertID.
		// Cause autoID may be specified by user, judge only the first row is not suitable.
		if e.lastInsertID == 0 {
			e.lastInsertID = uint64(recordID)
		}
	}

	err = setDatumAutoIDAndCast(e.ctx, &d, recordID, c)
	if err != nil {
		return types.Datum{}, err
	}
	retryInfo.AddAutoRandomID(recordID)
	return d, nil
}

// allocAutoRandomID allocates a random id for primary key column. It assumes tableInfo.AutoRandomBits > 0.
func (e *InsertValues) allocAutoRandomID(ctx context.Context, fieldType *types.FieldType) (int64, error) {
	alloc := e.Table.Allocators(e.ctx).Get(autoid.AutoRandomType)
	tableInfo := e.Table.Meta()
	increment := e.ctx.GetSessionVars().AutoIncrementIncrement
	offset := e.ctx.GetSessionVars().AutoIncrementOffset
	_, autoRandomID, err := alloc.Alloc(ctx, 1, int64(increment), int64(offset))
	if err != nil {
		return 0, err
	}
	layout := autoid.NewShardIDLayout(fieldType, tableInfo.AutoRandomBits)
	if tables.OverflowShardBits(autoRandomID, tableInfo.AutoRandomBits, layout.TypeBitsLength, layout.HasSignBit) {
		return 0, autoid.ErrAutoRandReadFailed
	}
	if e.isLoadData {
		e.txnInUse.Lock()
		defer e.txnInUse.Unlock()
	}
	_, err = e.ctx.Txn(true)
	if err != nil {
		return 0, err
	}
	shard := e.ctx.GetSessionVars().TxnCtx.GetShard(tableInfo.AutoRandomBits, layout.TypeBitsLength, layout.HasSignBit, 1)
	autoRandomID |= shard
	return autoRandomID, nil
}

func (e *InsertValues) rebaseAutoRandomID(ctx context.Context, recordID int64, fieldType *types.FieldType) error {
	if recordID < 0 {
		return nil
	}
	alloc := e.Table.Allocators(e.ctx).Get(autoid.AutoRandomType)
	tableInfo := e.Table.Meta()

	layout := autoid.NewShardIDLayout(fieldType, tableInfo.AutoRandomBits)
	autoRandomID := layout.IncrementalMask() & recordID

	return alloc.Rebase(ctx, autoRandomID, true)
}

func (e *InsertValues) adjustImplicitRowID(ctx context.Context, d types.Datum, hasValue bool, c *table.Column) (types.Datum, error) {
	var err error
	var recordID int64
	if !hasValue {
		d.SetNull()
	}
	if !d.IsNull() {
		recordID = d.GetInt64()
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		if !e.ctx.GetSessionVars().AllowWriteRowID {
			return types.Datum{}, errors.Errorf("insert, update and replace statements for _tidb_rowid are not supported")
		}
		err = e.rebaseImplicitRowID(ctx, recordID)
		if err != nil {
			return types.Datum{}, err
		}
		d.SetInt64(recordID)
		return d, nil
	}
	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	if d.IsNull() || e.ctx.GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		_, err := e.ctx.Txn(true)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		intHandle, err := tables.AllocHandle(ctx, e.ctx, e.Table)
		if err != nil {
			return types.Datum{}, err
		}
		recordID = intHandle.IntValue()
	}
	err = setDatumAutoIDAndCast(e.ctx, &d, recordID, c)
	if err != nil {
		return types.Datum{}, err
	}
	return d, nil
}

func (e *InsertValues) rebaseImplicitRowID(ctx context.Context, recordID int64) error {
	if recordID < 0 {
		return nil
	}
	alloc := e.Table.Allocators(e.ctx).Get(autoid.RowIDAllocType)
	tableInfo := e.Table.Meta()

	layout := autoid.NewShardIDLayout(types.NewFieldType(mysql.TypeLonglong), tableInfo.ShardRowIDBits)
	newTiDBRowIDBase := layout.IncrementalMask() & recordID

	return alloc.Rebase(ctx, newTiDBRowIDBase, true)
}

func (e *InsertValues) handleWarning(err error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	sc.AppendWarning(err)
}

func (e *InsertValues) collectRuntimeStatsEnabled() bool {
	if e.runtimeStats != nil {
		if e.stats == nil {
			snapshotStats := &txnsnapshot.SnapshotRuntimeStats{}
			e.stats = &InsertRuntimeStat{
				BasicRuntimeStats:     e.runtimeStats,
				SnapshotRuntimeStats:  snapshotStats,
				AllocatorRuntimeStats: autoid.NewAllocatorRuntimeStats(),
			}
			e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.stats)
		}
		return true
	}
	return false
}

// batchCheckAndInsert checks rows with duplicate errors.
// All duplicate rows will be ignored and appended as duplicate warnings.
func (e *InsertValues) batchCheckAndInsert(ctx context.Context, rows [][]types.Datum,
	addRecord func(ctx context.Context, row []types.Datum) error,
	replace bool) error {
	// all the rows will be checked, so it is safe to set BatchCheck = true
	e.ctx.GetSessionVars().StmtCtx.BatchCheck = true
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("InsertValues.batchCheckAndInsert", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(ctx, span1)
	}
	start := time.Now()
	// Get keys need to be checked.
	toBeCheckedRows, err := getKeysNeedCheck(ctx, e.ctx, e.Table, rows)
	if err != nil {
		return err
	}

	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	setOptionForTopSQL(e.ctx.GetSessionVars().StmtCtx, txn)
	if e.collectRuntimeStatsEnabled() {
		if snapshot := txn.GetSnapshot(); snapshot != nil {
			snapshot.SetOption(kv.CollectRuntimeStats, e.stats.SnapshotRuntimeStats)
			defer snapshot.SetOption(kv.CollectRuntimeStats, nil)
		}
	}
	prefetchStart := time.Now()
	// Fill cache using BatchGet, the following Get requests don't need to visit TiKV.
	// Temporary table need not to do prefetch because its all data are stored in the memory.
	if e.Table.Meta().TempTableType == model.TempTableNone {
		if _, err = prefetchUniqueIndices(ctx, txn, toBeCheckedRows); err != nil {
			return err
		}
	}

	if e.stats != nil {
		e.stats.Prefetch += time.Since(prefetchStart)
	}

	// append warnings and get no duplicated error rows
	for i, r := range toBeCheckedRows {
		if r.ignored {
			continue
		}
		skip := false
		if r.handleKey != nil {
			_, err := txn.Get(ctx, r.handleKey.newKey)
			if err == nil {
				if replace {
					err2 := e.removeRow(ctx, txn, r)
					if err2 != nil {
						return err2
					}
				} else {
					e.ctx.GetSessionVars().StmtCtx.AppendWarning(r.handleKey.dupErr)
					continue
				}
			} else if !kv.IsErrNotFound(err) {
				return err
			}
		}
		for _, uk := range r.uniqueKeys {
			_, err := txn.Get(ctx, uk.newKey)
			if err == nil {
				// If duplicate keys were found in BatchGet, mark row = nil.
				e.ctx.GetSessionVars().StmtCtx.AppendWarning(uk.dupErr)
				skip = true
				break
			}
			if !kv.IsErrNotFound(err) {
				return err
			}
		}

		// If row was checked with no duplicate keys,
		// it should be add to values map for the further row check.
		// There may be duplicate keys inside the insert statement.
		if !skip {
			e.ctx.GetSessionVars().StmtCtx.AddCopiedRows(1)
			err = addRecord(ctx, rows[i])
			if err != nil {
				return err
			}
		}
	}
	if e.stats != nil {
		e.stats.CheckInsertTime += time.Since(start)
	}
	return nil
}

func (e *InsertValues) removeRow(ctx context.Context, txn kv.Transaction, r toBeCheckedRow) error {
	handle, err := tablecodec.DecodeRowKey(r.handleKey.newKey)
	if err != nil {
		return err
	}

	newRow := r.row
	oldRow, err := getOldRow(ctx, e.ctx, txn, r.t, handle, e.GenExprs)
	if err != nil {
		logutil.BgLogger().Error("get old row failed when replace",
			zap.String("handle", handle.String()),
			zap.String("toBeInsertedRow", types.DatumsToStrNoErr(r.row)))
		if kv.IsErrNotFound(err) {
			err = errors.NotFoundf("can not be duplicated row, due to old row not found. handle %s", handle)
		}
		return err
	}

	identical, err := e.equalDatumsAsBinary(oldRow, newRow)
	if err != nil {
		return err
	}
	if identical {
		return nil
	}

	err = r.t.RemoveRecord(e.ctx, handle, oldRow)
	if err != nil {
		return err
	}
	e.ctx.GetSessionVars().StmtCtx.AddDeletedRows(1)

	return nil
}

// equalDatumsAsBinary compare if a and b contains the same datum values in binary collation.
func (e *InsertValues) equalDatumsAsBinary(a []types.Datum, b []types.Datum) (bool, error) {
	if len(a) != len(b) {
		return false, nil
	}
	for i, ai := range a {
		v, err := ai.Compare(e.ctx.GetSessionVars().StmtCtx, &b[i], collate.GetBinaryCollator())
		if err != nil {
			return false, errors.Trace(err)
		}
		if v != 0 {
			return false, nil
		}
	}
	return true, nil
}

func (e *InsertValues) addRecord(ctx context.Context, row []types.Datum) error {
	return e.addRecordWithAutoIDHint(ctx, row, 0)
}

func (e *InsertValues) addRecordWithAutoIDHint(ctx context.Context, row []types.Datum, reserveAutoIDCount int) (err error) {
	vars := e.ctx.GetSessionVars()
	if !vars.ConstraintCheckInPlace {
		vars.PresumeKeyNotExists = true
	}
	if reserveAutoIDCount > 0 {
		_, err = e.Table.AddRecord(e.ctx, row, table.WithCtx(ctx), table.WithReserveAutoIDHint(reserveAutoIDCount))
	} else {
		_, err = e.Table.AddRecord(e.ctx, row, table.WithCtx(ctx))
	}
	vars.PresumeKeyNotExists = false
	if err != nil {
		return err
	}
	vars.StmtCtx.AddAffectedRows(1)
	if e.lastInsertID != 0 {
		vars.SetLastInsertID(e.lastInsertID)
	}
	return nil
}

// InsertRuntimeStat record the stat about insert and check
type InsertRuntimeStat struct {
	*execdetails.BasicRuntimeStats
	*txnsnapshot.SnapshotRuntimeStats
	*autoid.AllocatorRuntimeStats
	CheckInsertTime time.Duration
	Prefetch        time.Duration
}

func (e *InsertRuntimeStat) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	var allocatorStatsStr string
	if e.AllocatorRuntimeStats != nil {
		allocatorStatsStr = e.AllocatorRuntimeStats.String()
	}
	if e.CheckInsertTime == 0 {
		// For replace statement.
		if allocatorStatsStr != "" {
			buf.WriteString(allocatorStatsStr)
		}
		if e.Prefetch > 0 && e.SnapshotRuntimeStats != nil {
			if buf.Len() > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString("prefetch: ")
			buf.WriteString(execdetails.FormatDuration(e.Prefetch))
			buf.WriteString(", rpc: {")
			buf.WriteString(e.SnapshotRuntimeStats.String())
			buf.WriteString("}")
			return buf.String()
		}
		return ""
	}
	if allocatorStatsStr != "" {
		buf.WriteString("prepare: {total: ")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.BasicRuntimeStats.GetTime()) - e.CheckInsertTime))
		buf.WriteString(", ")
		buf.WriteString(allocatorStatsStr)
		buf.WriteString("}, ")
	} else {
		buf.WriteString("prepare: ")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.BasicRuntimeStats.GetTime()) - e.CheckInsertTime))
		buf.WriteString(", ")
	}
	if e.Prefetch > 0 {
		buf.WriteString(fmt.Sprintf("check_insert: {total_time: %v, mem_insert_time: %v, prefetch: %v",
			execdetails.FormatDuration(e.CheckInsertTime),
			execdetails.FormatDuration(e.CheckInsertTime-e.Prefetch),
			execdetails.FormatDuration(e.Prefetch)))
		if e.SnapshotRuntimeStats != nil {
			if rpc := e.SnapshotRuntimeStats.String(); len(rpc) > 0 {
				buf.WriteString(fmt.Sprintf(", rpc:{%s}", rpc))
			}
		}
		buf.WriteString("}")
	} else {
		buf.WriteString(fmt.Sprintf("insert:%v", execdetails.FormatDuration(e.CheckInsertTime)))
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *InsertRuntimeStat) Clone() execdetails.RuntimeStats {
	newRs := &InsertRuntimeStat{
		CheckInsertTime: e.CheckInsertTime,
		Prefetch:        e.Prefetch,
	}
	if e.SnapshotRuntimeStats != nil {
		snapshotStats := e.SnapshotRuntimeStats.Clone()
		newRs.SnapshotRuntimeStats = snapshotStats
	}
	if e.BasicRuntimeStats != nil {
		basicStats := e.BasicRuntimeStats.Clone()
		newRs.BasicRuntimeStats = basicStats.(*execdetails.BasicRuntimeStats)
	}
	if e.AllocatorRuntimeStats != nil {
		newRs.AllocatorRuntimeStats = e.AllocatorRuntimeStats.Clone()
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (e *InsertRuntimeStat) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*InsertRuntimeStat)
	if !ok {
		return
	}
	if tmp.SnapshotRuntimeStats != nil {
		if e.SnapshotRuntimeStats == nil {
			snapshotStats := tmp.SnapshotRuntimeStats.Clone()
			e.SnapshotRuntimeStats = snapshotStats
		} else {
			e.SnapshotRuntimeStats.Merge(tmp.SnapshotRuntimeStats)
		}
	}
	if tmp.BasicRuntimeStats != nil {
		if e.BasicRuntimeStats == nil {
			basicStats := tmp.BasicRuntimeStats.Clone()
			e.BasicRuntimeStats = basicStats.(*execdetails.BasicRuntimeStats)
		} else {
			e.BasicRuntimeStats.Merge(tmp.BasicRuntimeStats)
		}
	}
	if tmp.AllocatorRuntimeStats != nil {
		if e.AllocatorRuntimeStats == nil {
			e.AllocatorRuntimeStats = tmp.AllocatorRuntimeStats.Clone()
		} else {
			e.AllocatorRuntimeStats.Merge(tmp.AllocatorRuntimeStats)
		}
	}
	e.Prefetch += tmp.Prefetch
	e.CheckInsertTime += tmp.CheckInsertTime
}

// Tp implements the RuntimeStats interface.
func (e *InsertRuntimeStat) Tp() int {
	return execdetails.TpInsertRuntimeStat
}
