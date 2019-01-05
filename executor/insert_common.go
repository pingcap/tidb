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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	log "github.com/sirupsen/logrus"
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
		for _, v := range e.GenColumns {
			columns = append(columns, v.Name.O)
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
		for _, v := range e.GenColumns {
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
		if col.Name.L == model.ExtraHandleName.L {
			if !e.ctx.GetSessionVars().AllowWriteRowID {
				return errors.Errorf("insert, update and replace statements for _tidb_rowid are not supported.")
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
	e.insertColumns = cols
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
		return errors.Trace(err)
	}
	sessVars := e.ctx.GetSessionVars()
	batchInsert := sessVars.BatchInsert && !sessVars.InTxn()
	batchSize := sessVars.DMLBatchSize

	rows := make([][]types.Datum, 0, len(e.Lists))
	for i, list := range e.Lists {
		e.rowCount++
		row, err := e.evalRow(list, i)
		if err != nil {
			return errors.Trace(err)
		}
		rows = append(rows, row)
		if e.rowCount%uint64(batchSize) == 0 {
			if err = exec(ctx, rows); err != nil {
				return err
			}
			rows = rows[:0]
			if batchInsert {
				if err = e.doBatchInsert(ctx); err != nil {
					return err
				}
			}
		}
	}
	return errors.Trace(exec(ctx, rows))
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
			log.Warn(err1)
		}
		return table.ErrTruncatedWrongValueForField.GenWithStackByArgs(types.TypeStr(col.Tp), valStr, col.Name.O, rowIdx+1)
	}
	return e.filterErr(err)
}

// evalRow evaluates a to-be-inserted row. The value of the column may base on another column,
// so we use setValueForRefColumn to fill the empty row some default values when needFillDefaultValues is true.
func (e *InsertValues) evalRow(list []expression.Expression, rowIdx int) ([]types.Datum, error) {
	rowLen := len(e.Table.Cols())
	if e.hasExtraHandle {
		rowLen++
	}
	row := make([]types.Datum, rowLen)
	hasValue := make([]bool, rowLen)

	// For statements like `insert into t set a = b + 1`.
	if e.hasRefCols {
		if err := e.setValueForRefColumn(row, hasValue); err != nil {
			return nil, errors.Trace(err)
		}
	}

	e.evalBuffer.SetDatums(row...)
	for i, expr := range list {
		val, err := expr.Eval(e.evalBuffer.ToRow())
		if err = e.handleErr(e.insertColumns[i], &val, rowIdx, err); err != nil {
			return nil, errors.Trace(err)
		}
		val1, err := table.CastValue(e.ctx, val, e.insertColumns[i].ToInfo())
		if err = e.handleErr(e.insertColumns[i], &val, rowIdx, err); err != nil {
			return nil, errors.Trace(err)
		}

		offset := e.insertColumns[i].Offset
		row[offset], hasValue[offset] = *val1.Copy(), true
		e.evalBuffer.SetDatum(offset, val1)
	}

	return e.fillRow(row, hasValue)
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
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *InsertValues) insertRowsFromSelect(ctx context.Context, exec func(ctx context.Context, rows [][]types.Datum) error) error {
	// process `insert|replace into ... select ... from ...`
	selectExec := e.children[0]
	fields := selectExec.retTypes()
	chk := selectExec.newFirstChunk()
	iter := chunk.NewIterator4Chunk(chk)
	rows := make([][]types.Datum, 0, chk.Capacity())

	sessVars := e.ctx.GetSessionVars()
	batchInsert := sessVars.BatchInsert && !sessVars.InTxn()
	batchSize := sessVars.DMLBatchSize

	for {
		err := selectExec.Next(ctx, chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}

		for innerChunkRow := iter.Begin(); innerChunkRow != iter.End(); innerChunkRow = iter.Next() {
			innerRow := types.CopyRow(innerChunkRow.GetDatumRow(fields))
			e.rowCount++
			row, err := e.getRow(innerRow)
			if err != nil {
				return errors.Trace(err)
			}
			rows = append(rows, row)
			if e.rowCount%uint64(batchSize) == 0 {
				if err = exec(ctx, rows); err != nil {
					return errors.Trace(err)
				}
				rows = rows[:0]
				if batchInsert {
					if err = e.doBatchInsert(ctx); err != nil {
						return err
					}
				}
			}
		}
	}
	if err := exec(ctx, rows); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *InsertValues) doBatchInsert(ctx context.Context) error {
	sessVars := e.ctx.GetSessionVars()
	if err := e.ctx.StmtCommit(); err != nil {
		return errors.Trace(err)
	}
	if err := e.ctx.NewTxn(ctx); err != nil {
		// We should return a special error for batch insert.
		return ErrBatchInsertFail.GenWithStack("BatchInsert failed with error: %v", err)
	}
	if !sessVars.LightningMode {
		txn, err := e.ctx.Txn(true)
		if err != nil {
			return errors.Trace(err)
		}
		sessVars.GetWriteStmtBufs().BufStore = kv.NewBufferStore(txn, kv.TempTxnMemBufCap)
	}
	return nil
}

// getRow gets the row which from `insert into select from` or `load data`.
// The input values from these two statements are datums instead of
// expressions which are used in `insert into set x=y`.
func (e *InsertValues) getRow(vals []types.Datum) ([]types.Datum, error) {
	row := make([]types.Datum, len(e.Table.Cols()))
	hasValue := make([]bool, len(e.Table.Cols()))
	for i, v := range vals {
		casted, err := table.CastValue(e.ctx, v, e.insertColumns[i].ToInfo())
		if e.filterErr(err) != nil {
			return nil, errors.Trace(err)
		}

		offset := e.insertColumns[i].Offset
		row[offset] = casted
		hasValue[offset] = true
	}

	return e.fillRow(row, hasValue)
}

func (e *InsertValues) filterErr(err error) error {
	if err == nil {
		return nil
	}
	if !e.ctx.GetSessionVars().StmtCtx.DupKeyAsWarning {
		return err
	}
	// TODO: should not filter all types of errors here.
	e.handleWarning(err, fmt.Sprintf("ignore err:%v", errors.ErrorStack(err)))
	return nil
}

// getColDefaultValue gets the column default value.
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

// fillColValue fills the column value if it is not set in the insert statement.
func (e *InsertValues) fillColValue(datum types.Datum, idx int, column *table.Column, hasValue bool) (types.Datum,
	error) {
	if mysql.HasAutoIncrementFlag(column.Flag) {
		d, err := e.adjustAutoIncrementDatum(datum, hasValue, column)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		return d, nil
	}
	if !hasValue {
		d, err := e.getColDefaultValue(idx, column)
		if e.filterErr(err) != nil {
			return types.Datum{}, errors.Trace(err)
		}
		return d, nil
	}
	return datum, nil
}

// fillRow fills generated columns, auto_increment column and empty column.
// For NOT NULL column, it will return error or use zero value based on sql_mode.
func (e *InsertValues) fillRow(row []types.Datum, hasValue []bool) ([]types.Datum, error) {
	gIdx := 0
	for i, c := range e.Table.Cols() {
		var err error
		// Get the default value for all no value columns, the auto increment column is different from the others.
		row[i], err = e.fillColValue(row[i], i, c, hasValue[i])
		if err != nil {
			return nil, errors.Trace(err)
		}

		// Evaluate the generated columns.
		if c.IsGenerated() {
			var val types.Datum
			val, err = e.GenExprs[gIdx].Eval(chunk.MutRowFromDatums(row).ToRow())
			gIdx++
			if e.filterErr(err) != nil {
				return nil, errors.Trace(err)
			}
			row[i], err = table.CastValue(e.ctx, val, c.ToInfo())
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		// Handle the bad null error.
		if row[i], err = c.HandleBadNull(row[i], e.ctx.GetSessionVars().StmtCtx); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return row, nil
}

func (e *InsertValues) adjustAutoIncrementDatum(d types.Datum, hasValue bool, c *table.Column) (types.Datum, error) {
	retryInfo := e.ctx.GetSessionVars().RetryInfo
	if retryInfo.Retrying {
		id, err := retryInfo.GetCurrAutoIncrementID()
		if err != nil {
			return types.Datum{}, errors.Trace(err)
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
		sc := e.ctx.GetSessionVars().StmtCtx
		datum, err1 := d.ConvertTo(sc, &c.FieldType)
		if e.filterErr(err1) != nil {
			return types.Datum{}, err1
		}
		recordID = datum.GetInt64()
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		err = e.Table.RebaseAutoID(e.ctx, recordID, true)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		e.ctx.GetSessionVars().StmtCtx.InsertID = uint64(recordID)
		retryInfo.AddAutoIncrementID(recordID)
		d.SetAutoID(recordID, c.Flag)
		return d, nil
	}

	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	if d.IsNull() || e.ctx.GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		recordID, err = e.Table.AllocAutoID(e.ctx)
		if e.filterErr(err) != nil {
			return types.Datum{}, errors.Trace(err)
		}
		// It's compatible with mysql. So it sets last insert id to the first row.
		if e.rowCount == 1 {
			e.lastInsertID = uint64(recordID)
		}
	}

	d.SetAutoID(recordID, c.Flag)
	retryInfo.AddAutoIncrementID(recordID)

	// the value of d is adjusted by auto ID, so we need to cast it again.
	casted, err := table.CastValue(e.ctx, d, c.ToInfo())
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return casted, nil
}

func (e *InsertValues) handleWarning(err error, logInfo string) {
	sc := e.ctx.GetSessionVars().StmtCtx
	sc.AppendWarning(err)
	log.Warn(logInfo)
}

// batchCheckAndInsert checks rows with duplicate errors.
// All duplicate rows will be ignored and appended as duplicate warnings.
func (e *InsertValues) batchCheckAndInsert(rows [][]types.Datum, addRecord func(row []types.Datum) (int64, error)) error {
	// all the rows will be checked, so it is safe to set BatchCheck = true
	e.ctx.GetSessionVars().StmtCtx.BatchCheck = true
	err := e.batchGetInsertKeys(e.ctx, e.Table, rows)
	if err != nil {
		return errors.Trace(err)
	}
	// append warnings and get no duplicated error rows
	for i, r := range e.toBeCheckedRows {
		if r.handleKey != nil {
			if _, found := e.dupKVs[string(r.handleKey.newKV.key)]; found {
				rows[i] = nil
				e.ctx.GetSessionVars().StmtCtx.AppendWarning(r.handleKey.dupErr)
				continue
			}
		}
		for _, uk := range r.uniqueKeys {
			if _, found := e.dupKVs[string(uk.newKV.key)]; found {
				// If duplicate keys were found in BatchGet, mark row = nil.
				rows[i] = nil
				e.ctx.GetSessionVars().StmtCtx.AppendWarning(uk.dupErr)
				break
			}
		}
		// If row was checked with no duplicate keys,
		// it should be add to values map for the further row check.
		// There may be duplicate keys inside the insert statement.
		if rows[i] != nil {
			e.ctx.GetSessionVars().StmtCtx.AddCopiedRows(1)
			_, err = addRecord(rows[i])
			if err != nil {
				return errors.Trace(err)
			}
			if r.handleKey != nil {
				e.dupKVs[string(r.handleKey.newKV.key)] = r.handleKey.newKV.value
			}
			for _, uk := range r.uniqueKeys {
				e.dupKVs[string(uk.newKV.key)] = []byte{}
			}
		}
	}
	return nil
}

func (e *InsertValues) addRecord(row []types.Datum) (int64, error) {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !e.ctx.GetSessionVars().ConstraintCheckInPlace {
		txn.SetOption(kv.PresumeKeyNotExists, nil)
	}
	h, err := e.Table.AddRecord(e.ctx, row)
	txn.DelOption(kv.PresumeKeyNotExists)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if e.lastInsertID != 0 {
		e.ctx.GetSessionVars().SetLastInsertID(e.lastInsertID)
	}
	return h, nil
}
