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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

// InsertValues is the data to insert.
// nolint:structcheck
type InsertValues struct {
	exec.BaseExecutor

	rowCount       uint64
	curBatchCnt    uint64
	maxRowsInBatch uint64
	lastInsertID   uint64

	SelectExec exec.Executor

	Table   table.Table
	Columns []*ast.ColumnName
	Lists   [][]expression.Expression

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

	// fkChecks contains the foreign key checkers.
	fkChecks   []*FKCheckExec
	fkCascades []*FKCascadeExec

	ignoreErr bool
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

func (*InsertValues) exec(context.Context, [][]types.Datum) error {
	panic("derived should overload exec function")
}

// initInsertColumns sets the explicitly specified columns of an insert statement. There are three cases:
// There are three types of insert statements:
// 1 insert ... values(...)  --> name type column
// 2 insert ... set x=y...   --> set type column
// 3 insert ... (select ..)  --> name type column
// set type is converted to name type in the optimizer
// See https://dev.mysql.com/doc/refman/5.7/en/insert.html
func (e *InsertValues) initInsertColumns() error {
	var cols []*table.Column
	var missingColIdx int
	var err error

	tableCols := e.Table.Cols()

	if len(e.Columns) > 0 {
		// Process `name` and `set` type column.
		columns := make([]string, 0, len(e.Columns))
		for _, v := range e.Columns {
			columns = append(columns, v.Name.L)
		}
		cols, missingColIdx = table.FindColumns(tableCols, columns, e.Table.Meta().PKIsHandle)
		if missingColIdx >= 0 {
			return errors.Errorf(
				"INSERT INTO %s: unknown column %s",
				e.Table.Meta().Name.O, e.Columns[missingColIdx].Name.O,
			)
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
			if !e.Ctx().GetSessionVars().AllowWriteRowID {
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
		e.evalBufferTypes[i] = &(col.FieldType)
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

// insertRows processes `insert|replace into values ()` or `insert|replace into set x=y`
func insertRows(ctx context.Context, base insertCommon) (err error) {
	e := base.insertCommon()
	sessVars := e.Ctx().GetSessionVars()
	batchSize := sessVars.DMLBatchSize
	batchInsert := sessVars.BatchInsert && !sessVars.InTxn() && vardef.EnableBatchDML.Load() && batchSize > 0

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

func completeInsertErr(col *model.ColumnInfo, val *types.Datum, rowIdx int, err error) error {
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
		err = types.ErrWarnDataOutOfRange.FastGenByArgs(colName, rowIdx+1)
	} else if types.ErrTruncated.Equal(err) {
		err = types.ErrTruncated.FastGenByArgs(colName, rowIdx+1)
	} else if types.ErrTruncatedWrongVal.Equal(err) && (colTp == mysql.TypeDuration || colTp == mysql.TypeDatetime || colTp == mysql.TypeDate || colTp == mysql.TypeTimestamp) {
		valStr, err1 := val.ToString()
		if err1 != nil {
			logutil.BgLogger().Debug("time truncated error", zap.Error(err1))
		}
		err = exeerrors.ErrTruncateWrongInsertValue.FastGenByArgs(types.TypeStr(colTp), valStr, colName, rowIdx+1)
	} else if types.ErrTruncatedWrongVal.Equal(err) || types.ErrWrongValue.Equal(err) {
		valStr, err1 := val.ToString()
		if err1 != nil {
			logutil.BgLogger().Debug("truncated/wrong value error", zap.Error(err1))
		}
		err = table.ErrTruncatedWrongValueForField.FastGenByArgs(types.TypeStr(colTp), valStr, colName, rowIdx+1)
	} else if types.ErrWarnDataOutOfRange.Equal(err) {
		err = types.ErrWarnDataOutOfRange.FastGenByArgs(colName, rowIdx+1)
	}
	return err
}

func completeLoadErr(col *model.ColumnInfo, rowIdx int, err error) error {
	var (
		colName string
	)
	if col != nil {
		colName = col.Name.String()
	}

	if types.ErrDataTooLong.Equal(err) {
		err = types.ErrTruncated.FastGen("Data truncated for column '%v' at row %v", colName, rowIdx)
	}
	return err
}

func (e *InsertValues) handleErr(col *table.Column, val *types.Datum, rowIdx int, err error) error {
	if err == nil {
		return nil
	}

	// Convert the error with full messages.
	var c *model.ColumnInfo
	if col != nil {
		c = col.ColumnInfo
	}
	if e.Ctx().GetSessionVars().StmtCtx.InLoadDataStmt {
		err = completeLoadErr(c, rowIdx, err)
	} else {
		err = completeInsertErr(c, val, rowIdx, err)
	}
	if col != nil && col.GetType() == mysql.TypeTimestamp &&
		types.ErrTimestampInDSTTransition.Equal(err) {
		newErr := exeerrors.ErrTruncateWrongInsertValue.FastGenByArgs(types.TypeStr(col.GetType()), val.GetString(), col.Name.O, rowIdx+1)
		// IGNORE takes precedence over STRICT mode.
		if !e.ignoreErr && e.Ctx().GetSessionVars().SQLMode.HasStrictMode() {
			return newErr
		}
		// timestamp already adjusted to end of DST transition, convert error to warning
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(newErr)
		return nil
	}

	// TODO: should not filter all types of errors here.
	if err != nil {
		ec := e.Ctx().GetSessionVars().StmtCtx.ErrCtx()
		return errors.AddStack(ec.HandleErrorWithAlias(kv.ErrKeyExists, err, err))
	}
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
	sctx := e.Ctx()
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	sc := sctx.GetSessionVars().StmtCtx
	warnCnt := int(sc.WarningCount())
	for i, expr := range list {
		val, err := expr.Eval(evalCtx, e.evalBuffer.ToRow())
		if err != nil {
			return nil, err
		}
		val1, err := table.CastValue(sctx, val, e.insertColumns[i].ToInfo(), false, false)
		if err = e.handleErr(e.insertColumns[i], &val, rowIdx, err); err != nil {
			return nil, err
		}
		if newWarnings := sc.TruncateWarnings(warnCnt); len(newWarnings) > 0 {
			for k := range newWarnings {
				newWarnings[k].Err = completeInsertErr(e.insertColumns[i].ColumnInfo, &val, rowIdx, newWarnings[k].Err)
			}
			sc.AppendWarnings(newWarnings)
			warnCnt += len(newWarnings)
		}

		offset := e.insertColumns[i].Offset
		val1.Copy(&row[offset])
		hasValue[offset] = true
		e.evalBuffer.SetDatum(offset, val1)
	}
	// Row may lack of generated column, autoIncrement column, empty column here.
	return e.fillRow(ctx, row, hasValue, rowIdx)
}

var emptyRow chunk.Row

func (e *InsertValues) fastEvalRow(ctx context.Context, list []expression.Expression, rowIdx int) (
	[]types.Datum, error,
) {
	rowLen := len(e.Table.Cols())
	if e.hasExtraHandle {
		rowLen++
	}
	row := make([]types.Datum, rowLen)
	hasValue := make([]bool, rowLen)
	sctx := e.Ctx()
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	sc := sctx.GetSessionVars().StmtCtx
	warnCnt := int(sc.WarningCount())
	for i, expr := range list {
		con := expr.(*expression.Constant)
		val, err := con.Eval(evalCtx, emptyRow)
		if err = e.handleErr(e.insertColumns[i], &val, rowIdx, err); err != nil {
			return nil, err
		}
		val1, err := table.CastValue(sctx, val, e.insertColumns[i].ToInfo(), false, false)
		if err = e.handleErr(e.insertColumns[i], &val, rowIdx, err); err != nil {
			return nil, err
		}
		if newWarnings := sc.TruncateWarnings(warnCnt); len(newWarnings) > 0 {
			for k := range newWarnings {
				newWarnings[k].Err = completeInsertErr(e.insertColumns[i].ColumnInfo, &val, rowIdx, newWarnings[k].Err)
			}
			sc.AppendWarnings(newWarnings)
			warnCnt += len(newWarnings)
		}
		offset := e.insertColumns[i].Offset
		row[offset], hasValue[offset] = val1, true
	}
	return e.fillRow(ctx, row, hasValue, rowIdx)
}

// setValueForRefColumn set some default values for the row to eval the row value with other columns,
// it follows these rules:
//  1. for nullable and no default value column, use NULL.
//  2. for nullable and have default value column, use it's default value.
//  3. for not null column, use zero value even in strict mode.
//  4. for auto_increment column, use zero value.
//  5. for generated column, use NULL.
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

