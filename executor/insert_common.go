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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// InsertValues is the data to insert.
type InsertValues struct {
	baseExecutor
	batchChecker

	rowCount       uint64
	maxRowsInBatch uint64
	lastInsertID   uint64
	hasRefCols     bool
	hasExtraHandle bool

	SelectExec Executor

	Table   table.Table
	Columns []*ast.ColumnName
	Lists   [][]expression.Expression
	SetList []*expression.Assignment

	GenColumns []*ast.ColumnName
	GenExprs   []expression.Expression

	insertColumns []*table.Column

	// colDefaultVals is used to store casted default value.
	// Because not every insert statement needs colDefaultVals, so we will init the buffer lazily.
	colDefaultVals  []defaultVal
	evalBuffer      chunk.MutRow
	evalBufferTypes []*types.FieldType

	// Fill the autoID lazily to datum. This is used for being compatible with JDBC using getGeneratedKeys().
	// `insert|replace values` can guarantee consecutive autoID in a batch.
	// Other statements like `insert select from` don't guarantee consecutive autoID.
	// https://dev.mysql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html
	lazyFillAutoID bool
}

type defaultVal struct {
	val types.Datum
	// valid indicates whether the val is evaluated. We evaluate the default value lazily.
	valid bool
}

// initInsertColumns sets the explicitly specified columns of an insert statement. There are three cases:
// There are three types of insert statements:
// 1 insert ... values(...)  --> name type column
// 2 insert ... set x=y...   --> set type column
// 3 insert ... (select ..)  --> name type column
// See https://dev.mysql.com/doc/refman/5.7/en/insert.html
func (e *InsertValues) initInsertColumns() error {
	var cols []*table.Column
	var err error

	tableCols := e.Table.Cols()

	if len(e.SetList) > 0 {
		// Process `set` type column.
		columns := make([]string, 0, len(e.SetList))
		for _, v := range e.SetList {
			columns = append(columns, v.Col.ColName.O)
		}
		cols, err = table.FindCols(tableCols, columns, e.Table.Meta().PKIsHandle)
		if err != nil {
			return errors.Errorf("INSERT INTO %s: %s", e.Table.Meta().Name.O, err)
		}
		if len(cols) == 0 {
			return errors.Errorf("INSERT INTO %s: empty column", e.Table.Meta().Name.O)
		}
	} else if len(e.Columns) > 0 {
		// Process `name` type column.
		columns := make([]string, 0, len(e.Columns))
		for _, v := range e.Columns {
			columns = append(columns, v.Name.O)
		}
		cols, err = table.FindCols(tableCols, columns, e.Table.Meta().PKIsHandle)
		if err != nil {
			return errors.Errorf("INSERT INTO %s: %s", e.Table.Meta().Name.O, err)
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
				return errors.Errorf("insert, update and replace statements for _tidb_rowid are not supported.")
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
func (e *InsertValues) insertRows(ctx context.Context, exec func(ctx context.Context, rows [][]types.Datum) error) (err error) {
	// For `insert|replace into set x=y`, process the set list here.
	if err = e.processSetList(); err != nil {
		return err
	}
	sessVars := e.ctx.GetSessionVars()
	batchInsert := (sessVars.BatchInsert && !sessVars.InTxn()) || config.GetGlobalConfig().EnableBatchDML

	e.lazyFillAutoID = true

	rows := make([][]types.Datum, 0, len(e.Lists))
	for i, list := range e.Lists {
		e.rowCount++
		row, err := e.evalRow(ctx, list, i)
		if err != nil {
			return err
		}
		rows = append(rows, row)
		if batchInsert && int(e.rowCount)%sessVars.DMLBatchSize == 0 {
			// Before batch insert, fill the batch allocated autoIDs.
			rows, err = e.lazyAdjustAutoIncrementDatum(ctx, rows)
			if err != nil {
				return err
			}
			if err = exec(ctx, rows); err != nil {
				return err
			}
			if err = batchDMLCommit(ctx, e.ctx); err != nil {
				return err
			}
			rows = rows[:0]
		}
	}
	// Fill the batch allocated autoIDs.
	rows, err = e.lazyAdjustAutoIncrementDatum(ctx, rows)
	if err != nil {
		return err
	}
	return exec(ctx, rows)
}

func (e *InsertValues) handleErr(col *table.Column, val *types.Datum, rowIdx int, err error) error {
	if err == nil {
		return nil
	}

	if types.ErrDataTooLong.Equal(err) {
		return resetErrDataTooLong(col.Name.O, rowIdx+1, err)
	}

	if types.ErrOverflow.Equal(err) {
		return types.ErrWarnDataOutOfRange.GenWithStackByArgs(col.Name.O, rowIdx+1)
	}
	if types.ErrTruncated.Equal(err) {
		valStr, err1 := val.ToString()
		if err1 != nil {
			logutil.Logger(context.Background()).Warn("truncate error", zap.Error(err1))
		}
		return table.ErrTruncatedWrongValueForField.GenWithStackByArgs(types.TypeStr(col.Tp), valStr, col.Name.O, rowIdx+1)
	}
	return e.filterErr(err)
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
		if err = e.handleErr(e.insertColumns[i], &val, rowIdx, err); err != nil {
			return nil, err
		}
		val1, err := table.CastValue(e.ctx, val, e.insertColumns[i].ToInfo())
		if err = e.handleErr(e.insertColumns[i], &val, rowIdx, err); err != nil {
			return nil, err
		}

		offset := e.insertColumns[i].Offset
		row[offset], hasValue[offset] = *val1.Copy(), true
		e.evalBuffer.SetDatum(offset, val1)
	}
	// Row may lack of generated column, autoIncrement column, empty column here.
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
			if !mysql.HasAutoIncrementFlag(c.Flag) {
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
		} else if e.filterErr(err) != nil {
			return err
		}
	}
	return nil
}

func (e *InsertValues) insertRowsFromSelect(ctx context.Context, exec func(ctx context.Context, rows [][]types.Datum) error) error {
	// process `insert|replace into ... select ... from ...`
	selectExec := e.children[0]
	fields := retTypes(selectExec)
	chk := newFirstChunk(selectExec)
	iter := chunk.NewIterator4Chunk(chk)
	rows := make([][]types.Datum, 0, chk.Capacity())

	sessVars := e.ctx.GetSessionVars()
	if !sessVars.StrictSQLMode {
		// If StrictSQLMode is disabled and it is a insert-select statement, it also handle BadNullAsWarning.
		sessVars.StmtCtx.BadNullAsWarning = true
	}
	batchInsert := (sessVars.BatchInsert && !sessVars.InTxn()) || config.GetGlobalConfig().EnableBatchDML

	for {
		err := selectExec.Next(ctx, chk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}

		for innerChunkRow := iter.Begin(); innerChunkRow != iter.End(); innerChunkRow = iter.Next() {
			innerRow := types.CloneRow(innerChunkRow.GetDatumRow(fields))
			e.rowCount++
			row, err := e.getRow(ctx, innerRow)
			if err != nil {
				return err
			}
			rows = append(rows, row)
			if batchInsert && int(e.rowCount)%sessVars.DMLBatchSize == 0 {
				if err = exec(ctx, rows); err != nil {
					return err
				}
				if err = batchDMLCommit(ctx, e.ctx); err != nil {
					return err
				}
				rows = rows[:0]
			}
		}
	}
	return exec(ctx, rows)
}

// getRow gets the row which from `insert into select from` or `load data`.
// The input values from these two statements are datums instead of
// expressions which are used in `insert into set x=y`.
func (e *InsertValues) getRow(ctx context.Context, vals []types.Datum) ([]types.Datum, error) {
	row := make([]types.Datum, len(e.Table.Cols()))
	hasValue := make([]bool, len(e.Table.Cols()))
	for i, v := range vals {
		casted, err := table.CastValue(e.ctx, v, e.insertColumns[i].ToInfo())
		if e.filterErr(err) != nil {
			return nil, err
		}

		offset := e.insertColumns[i].Offset
		row[offset] = casted
		hasValue[offset] = true
	}

	return e.fillRow(ctx, row, hasValue)
}

func (e *InsertValues) filterErr(err error) error {
	if err == nil {
		return nil
	}
	if !e.ctx.GetSessionVars().StmtCtx.DupKeyAsWarning {
		return err
	}
	// TODO: should not filter all types of errors here.
	e.handleWarning(err)
	return nil
}

// getColDefaultValue gets the column default value.
func (e *InsertValues) getColDefaultValue(idx int, col *table.Column) (d types.Datum, err error) {
	if e.colDefaultVals != nil && e.colDefaultVals[idx].valid {
		return e.colDefaultVals[idx].val, nil
	}

	defaultVal, err := table.GetColDefaultValue(e.ctx, col.ToInfo())
	if err != nil {
		return types.Datum{}, err
	}
	if initialized := e.lazilyInitColDefaultValBuf(); initialized {
		e.colDefaultVals[idx].val = defaultVal
		e.colDefaultVals[idx].valid = true
	}

	return defaultVal, nil
}

// fillColValue fills the column value if it is not set in the insert statement.
func (e *InsertValues) fillColValue(ctx context.Context, datum types.Datum, idx int, column *table.Column, hasValue bool) (types.Datum,
	error) {
	if mysql.HasAutoIncrementFlag(column.Flag) {
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
<<<<<<< HEAD
=======
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
>>>>>>> f55e8f2bf... table: fix insert into _tidb_rowid panic and rebase it if needed (#22062)
	if !hasValue {
		d, err := e.getColDefaultValue(idx, column)
		if e.filterErr(err) != nil {
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
<<<<<<< HEAD
	gIdx := 0
	for i, c := range e.Table.Cols() {
=======
	gCols := make([]*table.Column, 0)
	tCols := e.Table.Cols()
	if e.hasExtraHandle {
		col := &table.Column{}
		col.ColumnInfo = model.NewExtraHandleColInfo()
		col.ColumnInfo.Offset = len(tCols)
		tCols = append(tCols, col)
	}
	for i, c := range tCols {
>>>>>>> f55e8f2bf... table: fix insert into _tidb_rowid panic and rebase it if needed (#22062)
		var err error
		// Get the default value for all no value columns, the auto increment column is different from the others.
		row[i], err = e.fillColValue(ctx, row[i], i, c, hasValue[i])
		if err != nil {
			return nil, err
		}

		// Evaluate the generated columns.
		if c.IsGenerated() {
			var val types.Datum
			val, err = e.GenExprs[gIdx].Eval(chunk.MutRowFromDatums(row).ToRow())
			gIdx++
			if e.filterErr(err) != nil {
				return nil, err
			}
			row[i], err = table.CastValue(e.ctx, val, c.ToInfo())
			if err != nil {
				return nil, err
			}
		}
		// Handle the bad null error. Cause generated column with `not null` flag will get default value datum in fillColValue
		// which should be override by generated expr first, then handle the bad null logic here.
		if !e.lazyFillAutoID || (e.lazyFillAutoID && !mysql.HasAutoIncrementFlag(c.Flag)) {
			if row[i], err = c.HandleBadNull(row[i], e.ctx.GetSessionVars().StmtCtx); err != nil {
				return nil, err
			}
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

func (e *InsertValues) hasAutoIncrementColumn() (int, bool) {
	colIdx := -1
	for i, c := range e.Table.Cols() {
		if mysql.HasAutoIncrementFlag(c.Flag) {
			colIdx = i
			break
		}
	}
	return colIdx, colIdx != -1
}

func (e *InsertValues) lazyAdjustAutoIncrementDatumInRetry(ctx context.Context, rows [][]types.Datum, colIdx int) ([][]types.Datum, error) {
	// Get the autoIncrement column.
	col := e.Table.Cols()[colIdx]
	// Consider the colIdx of autoIncrement in row are the same.
	length := len(rows)
	for i := 0; i < length; i++ {
		autoDatum := rows[i][colIdx]

		// autoID can be found in RetryInfo.
		retryInfo := e.ctx.GetSessionVars().RetryInfo
		if retryInfo.Retrying {
			id, err := retryInfo.GetCurrAutoIncrementID()
			if err != nil {
				return nil, err
			}
			autoDatum.SetAutoID(id, col.Flag)

			if autoDatum, err = col.HandleBadNull(autoDatum, e.ctx.GetSessionVars().StmtCtx); err != nil {
				return nil, err
			}
			rows[i][colIdx] = autoDatum
		}
	}
	return rows, nil
}

// lazyAdjustAutoIncrementDatum is quite similar to adjustAutoIncrementDatum
// except it will cache auto increment datum previously for lazy batch allocation of autoID.
func (e *InsertValues) lazyAdjustAutoIncrementDatum(ctx context.Context, rows [][]types.Datum) ([][]types.Datum, error) {
	// Not in lazyFillAutoID mode means no need to fill.
	if !e.lazyFillAutoID {
		return rows, nil
	}
	// No autoIncrement column means no need to fill.
	colIdx, ok := e.hasAutoIncrementColumn()
	if !ok {
		return rows, nil
	}
	// autoID can be found in RetryInfo.
	retryInfo := e.ctx.GetSessionVars().RetryInfo
	if retryInfo.Retrying {
		return e.lazyAdjustAutoIncrementDatumInRetry(ctx, rows, colIdx)
	}
	// Get the autoIncrement column.
	col := e.Table.Cols()[colIdx]
	// Consider the colIdx of autoIncrement in row are the same.
	length := len(rows)
	for i := 0; i < length; i++ {
		autoDatum := rows[i][colIdx]

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
			err = e.Table.RebaseAutoID(e.ctx, recordID, true)
			if err != nil {
				return nil, err
			}
			e.ctx.GetSessionVars().StmtCtx.InsertID = uint64(recordID)
			retryInfo.AddAutoIncrementID(recordID)
			rows[i][colIdx] = autoDatum
			continue
		}

		// Change NULL to auto id.
		// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
		if autoDatum.IsNull() || e.ctx.GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
			// Find consecutive num.
			start := i
			cnt := 1
			for i+1 < length && e.isAutoNull(ctx, rows[i+1][colIdx], col) {
				i++
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
				d := rows[offset][colIdx]

				id := int64(uint64(min) + uint64(j)*uint64(increment))
				d.SetAutoID(id, col.Flag)
				retryInfo.AddAutoIncrementID(id)

				// The value of d is adjusted by auto ID, so we need to cast it again.
				d, err := table.CastValue(e.ctx, d, col.ToInfo())
				if err != nil {
					return nil, err
				}
				rows[offset][colIdx] = d
			}
			continue
		}

		autoDatum.SetAutoID(recordID, col.Flag)
		retryInfo.AddAutoIncrementID(recordID)

		// the value of d is adjusted by auto ID, so we need to cast it again.
		autoDatum, err = table.CastValue(e.ctx, autoDatum, col.ToInfo())
		if err != nil {
			return nil, err
		}
		rows[i][colIdx] = autoDatum
	}
	return rows, nil
}

func (e *InsertValues) adjustAutoIncrementDatum(ctx context.Context, d types.Datum, hasValue bool, c *table.Column) (types.Datum, error) {
	retryInfo := e.ctx.GetSessionVars().RetryInfo
	if retryInfo.Retrying {
		id, err := retryInfo.GetCurrAutoIncrementID()
		if err != nil {
			return types.Datum{}, err
		}
		d.SetAutoID(id, c.Flag)
		return d, nil
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
		err = e.Table.RebaseAutoID(e.ctx, recordID, true)
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
		if e.filterErr(err) != nil {
			return types.Datum{}, err
		}
		// It's compatible with mysql setting the first allocated autoID to lastInsertID.
		// Cause autoID may be specified by user, judge only the first row is not suitable.
		if e.lastInsertID == 0 {
			e.lastInsertID = uint64(recordID)
		}
	}

	d.SetAutoID(recordID, c.Flag)
	retryInfo.AddAutoIncrementID(recordID)

	// the value of d is adjusted by auto ID, so we need to cast it again.
	casted, err := table.CastValue(e.ctx, d, c.ToInfo())
	if err != nil {
		return types.Datum{}, err
	}
	return casted, nil
}

func getAutoRecordID(d types.Datum, target *types.FieldType, isInsert bool) (int64, error) {
	var recordID int64

	switch target.Tp {
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
		return 0, errors.Errorf("unexpected field type [%v]", target.Tp)
	}

	return recordID, nil
}

<<<<<<< HEAD
=======
func (e *InsertValues) adjustAutoRandomDatum(ctx context.Context, d types.Datum, hasValue bool, c *table.Column) (types.Datum, error) {
	retryInfo := e.ctx.GetSessionVars().RetryInfo
	if retryInfo.Retrying {
		autoRandomID, ok := retryInfo.GetCurrAutoRandomID()
		if ok {
			d.SetAutoID(autoRandomID, c.Flag)
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
			return types.Datum{}, ddl.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomExplicitInsertDisabledErrMsg)
		}
		err = e.rebaseAutoRandomID(recordID, &c.FieldType)
		if err != nil {
			return types.Datum{}, err
		}
		e.ctx.GetSessionVars().StmtCtx.InsertID = uint64(recordID)
		d.SetAutoID(recordID, c.Flag)
		retryInfo.AddAutoRandomID(recordID)
		return d, nil
	}

	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	if d.IsNull() || e.ctx.GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		_, err := e.ctx.Txn(true)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
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
	_, autoRandomID, err := alloc.Alloc(ctx, tableInfo.ID, 1, int64(increment), int64(offset))
	if err != nil {
		return 0, err
	}
	layout := autoid.NewShardIDLayout(fieldType, tableInfo.AutoRandomBits)
	if tables.OverflowShardBits(autoRandomID, tableInfo.AutoRandomBits, layout.TypeBitsLength, layout.HasSignBit) {
		return 0, autoid.ErrAutoRandReadFailed
	}
	shard := e.ctx.GetSessionVars().TxnCtx.GetShard(tableInfo.AutoRandomBits, layout.TypeBitsLength, layout.HasSignBit, 1)
	autoRandomID |= shard
	return autoRandomID, nil
}

func (e *InsertValues) rebaseAutoRandomID(recordID int64, fieldType *types.FieldType) error {
	if recordID < 0 {
		return nil
	}
	alloc := e.Table.Allocators(e.ctx).Get(autoid.AutoRandomType)
	tableInfo := e.Table.Meta()

	layout := autoid.NewShardIDLayout(fieldType, tableInfo.AutoRandomBits)
	autoRandomID := layout.IncrementalMask() & recordID

	return alloc.Rebase(tableInfo.ID, autoRandomID, true)
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
			return types.Datum{}, errors.Errorf("insert, update and replace statements for _tidb_rowid are not supported.")
		}
		err = e.rebaseImplicitRowID(recordID)
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

func (e *InsertValues) rebaseImplicitRowID(recordID int64) error {
	if recordID < 0 {
		return nil
	}
	alloc := e.Table.Allocators(e.ctx).Get(autoid.RowIDAllocType)
	tableInfo := e.Table.Meta()

	layout := autoid.NewShardIDLayout(types.NewFieldType(mysql.TypeLonglong), tableInfo.ShardRowIDBits)
	newTiDBRowIDBase := layout.IncrementalMask() & recordID

	return alloc.Rebase(tableInfo.ID, newTiDBRowIDBase, true)
}

>>>>>>> f55e8f2bf... table: fix insert into _tidb_rowid panic and rebase it if needed (#22062)
func (e *InsertValues) handleWarning(err error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	sc.AppendWarning(err)
}

// batchCheckAndInsert checks rows with duplicate errors.
// All duplicate rows will be ignored and appended as duplicate warnings.
func (e *InsertValues) batchCheckAndInsert(rows [][]types.Datum, addRecord func(row []types.Datum) (int64, error)) error {
	// all the rows will be checked, so it is safe to set BatchCheck = true
	e.ctx.GetSessionVars().StmtCtx.BatchCheck = true

	// Get keys need to be checked.
	toBeCheckedRows, err := e.getKeysNeedCheck(e.ctx, e.Table, rows)
	if err != nil {
		return err
	}

	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}

	// Fill cache using BatchGet, the following Get requests don't need to visit TiKV.
	if _, err = prefetchUniqueIndices(txn, toBeCheckedRows); err != nil {
		return err
	}

	for i, r := range toBeCheckedRows {
		// skip := false
		if r.handleKey != nil {
			_, err := txn.Get(r.handleKey.newKV.key)
			if err == nil {
				e.ctx.GetSessionVars().StmtCtx.AppendWarning(r.handleKey.dupErr)
				continue
			}
			if !kv.IsErrNotFound(err) {
				return err
			}
		}
		for _, uk := range r.uniqueKeys {
			_, err := txn.Get(uk.newKV.key)
			if err == nil {
				// If duplicate keys were found in BatchGet, mark row = nil.
				rows[i] = nil
				e.ctx.GetSessionVars().StmtCtx.AppendWarning(uk.dupErr)
				break
			}
			if !kv.IsErrNotFound(err) {
				return err
			}
		}
		// If row was checked with no duplicate keys,
		// it should be add to values map for the further row check.
		// There may be duplicate keys inside the insert statement.
		if rows[i] != nil {
			e.ctx.GetSessionVars().StmtCtx.AddCopiedRows(1)
			_, err = addRecord(rows[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *InsertValues) addRecord(row []types.Datum) (int64, error) {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return 0, err
	}
	if !e.ctx.GetSessionVars().ConstraintCheckInPlace {
		txn.SetOption(kv.PresumeKeyNotExists, nil)
	}
	h, err := e.Table.AddRecord(e.ctx, row)
	txn.DelOption(kv.PresumeKeyNotExists)
	if err != nil {
		return 0, err
	}
	if e.lastInsertID != 0 {
		e.ctx.GetSessionVars().SetLastInsertID(e.lastInsertID)
	}
	return h, nil
}
