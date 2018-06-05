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
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// InsertValues is the data to insert.
type InsertValues struct {
	baseExecutor

	rowCount              uint64
	maxRowsInBatch        uint64
	lastInsertID          uint64
	needFillDefaultValues bool
	hasExtraHandle        bool

	SelectExec Executor

	Table   table.Table
	Columns []*ast.ColumnName
	Lists   [][]expression.Expression
	SetList []*expression.Assignment

	GenColumns []*ast.ColumnName
	GenExprs   []expression.Expression

	// colDefaultVals is used to store casted default value.
	// Because not every insert statement needs colDefaultVals, so we will init the buffer lazily.
	colDefaultVals []defaultVal
}

type defaultVal struct {
	val types.Datum
	// We evaluate the default value lazily. The valid indicates whether the val is evaluated.
	valid bool
}

type keyWithDupError struct {
	isRecordKey bool
	key         kv.Key
	dupErr      error
	newRowValue []byte
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
			explicitSetLen = len(e.SetList)
		}
		if explicitSetLen > 0 && valueCount+genColsCount != len(cols) {
			return ErrWrongValueCountOnRow.GenByArgs(num + 1)
		} else if explicitSetLen == 0 && valueCount != len(cols) {
			return ErrWrongValueCountOnRow.GenByArgs(num + 1)
		}
	}
	return nil
}

func (e *InsertValues) getRows(cols []*table.Column) (rows []types.DatumRow, err error) {
	// process `insert|replace ... set x=y...`
	if err = e.fillValueList(); err != nil {
		return nil, errors.Trace(err)
	}

	rows = make([]types.DatumRow, len(e.Lists))
	length := len(e.Lists[0])
	for i, list := range e.Lists {
		if err = e.checkValueCount(length, len(list), len(e.GenColumns), i, cols); err != nil {
			return nil, errors.Trace(err)
		}
		e.rowCount = uint64(i)
		rows[i], err = e.getRow(cols, list, i)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return
}

func (e *InsertValues) handleErr(col *table.Column, rowIdx int, err error) error {
	if err == nil {
		return nil
	}

	if types.ErrDataTooLong.Equal(err) {
		return resetErrDataTooLong(col.Name.O, rowIdx+1, err)
	}

	if types.ErrOverflow.Equal(err) {
		return types.ErrWarnDataOutOfRange.GenByArgs(col.Name.O, int64(rowIdx+1))
	}
	return e.filterErr(err)
}

// getRow eval the insert statement. Because the value of column may calculated based on other column,
// it use fillDefaultValues to init the empty row before eval expressions when needFillDefaultValues is true.
func (e *InsertValues) getRow(cols []*table.Column, list []expression.Expression, rowIdx int) (types.DatumRow, error) {
	rowLen := len(e.Table.Cols())
	if e.hasExtraHandle {
		rowLen++
	}
	row := make(types.DatumRow, rowLen)
	hasValue := make([]bool, rowLen)

	if e.needFillDefaultValues {
		if err := e.fillDefaultValues(row, hasValue); err != nil {
			return nil, errors.Trace(err)
		}
	}

	for i, expr := range list {
		val, err := expr.Eval(row)
		if err = e.handleErr(cols[i], rowIdx, err); err != nil {
			return nil, errors.Trace(err)
		}
		val, err = table.CastValue(e.ctx, val, cols[i].ToInfo())
		if err = e.handleErr(cols[i], rowIdx, err); err != nil {
			return nil, errors.Trace(err)
		}

		offset := cols[i].Offset
		row[offset], hasValue[offset] = val, true
	}

	return e.fillGenColData(cols, len(list), hasValue, row)
}

// fillDefaultValues fills a row followed by these rules:
//     1. for nullable and no default value column, use NULL.
//     2. for nullable and have default value column, use it's default value.
//     3. for not null column, use zero value even in strict mode.
//     4. for auto_increment column, use zero value.
//     5. for generated column, use NULL.
func (e *InsertValues) fillDefaultValues(row types.DatumRow, hasValue []bool) error {
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
			} else if err = e.filterErr(err); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func (e *InsertValues) getRowsSelectChunk(ctx context.Context, cols []*table.Column) ([]types.DatumRow, error) {
	// process `insert|replace into ... select ... from ...`
	selectExec := e.children[0]
	if selectExec.Schema().Len() != len(cols) {
		return nil, ErrWrongValueCountOnRow.GenByArgs(1)
	}
	var rows []types.DatumRow
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
			row, err := e.fillRowData(cols, innerRow)
			if err != nil {
				return nil, errors.Trace(err)
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (e *InsertValues) fillRowData(cols []*table.Column, vals types.DatumRow) (types.DatumRow, error) {
	row := make(types.DatumRow, len(e.Table.Cols()))
	hasValue := make([]bool, len(e.Table.Cols()))
	for i, v := range vals {
		casted, err := table.CastValue(e.ctx, v, cols[i].ToInfo())
		if err = e.filterErr(err); err != nil {
			return nil, errors.Trace(err)
		}

		offset := cols[i].Offset
		row[offset] = casted
		hasValue[offset] = true
	}

	return e.fillGenColData(cols, len(vals), hasValue, row)
}

func (e *InsertValues) fillGenColData(cols []*table.Column, valLen int, hasValue []bool, row types.DatumRow) (types.DatumRow, error) {
	err := e.initDefaultValues(row, hasValue)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, expr := range e.GenExprs {
		var val types.Datum
		val, err = expr.Eval(row)
		if err = e.filterErr(err); err != nil {
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

func (e *InsertValues) filterErr(err error) error {
	if err == nil {
		return nil
	}
	if !e.ctx.GetSessionVars().StmtCtx.IgnoreErr {
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
func (e *InsertValues) initDefaultValues(row types.DatumRow, hasValue []bool) error {
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
			if e.filterErr(err) != nil {
				return errors.Trace(err)
			}
		}

		// Adjust the value if this column has auto increment flag.
		if mysql.HasAutoIncrementFlag(c.Flag) {
			if err := e.adjustAutoIncrementDatum(row, i, c); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (e *InsertValues) adjustAutoIncrementDatum(row types.DatumRow, i int, c *table.Column) error {
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
		if e.filterErr(errors.Trace(err)) != nil {
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
		if e.filterErr(errors.Trace(err)) != nil {
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

func (e *InsertValues) handleLoadDataWarnings(err error, logInfo string) {
	sc := e.ctx.GetSessionVars().StmtCtx
	sc.AppendWarning(err)
	log.Warn(logInfo)
}

// batchMarkDupRows marks rows with duplicate errors as nil.
// All duplicate rows were marked and appended as duplicate warnings
// to the statement context in batch.
func (e *InsertValues) batchMarkDupRows(rows []types.DatumRow) ([]types.DatumRow, error) {
	rowWithKeys, values, err := e.batchGetInsertKeys(rows)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// append warnings and get no duplicated error rows
	for i, v := range rowWithKeys {
		for _, k := range v {
			if _, found := values[string(k.key)]; found {
				// If duplicate keys were found in BatchGet, mark row = nil.
				rows[i] = nil
				e.ctx.GetSessionVars().StmtCtx.AppendWarning(k.dupErr)
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
	e.ctx.GetSessionVars().StmtCtx.BatchCheck = true
	return rows, nil
}

// batchGetOldValues gets the values of storage in batch.
func (e *InsertValues) batchGetOldValues(handles []int64) (map[string][]byte, error) {
	batchKeys := make([]kv.Key, 0, len(handles))
	for _, handle := range handles {
		batchKeys = append(batchKeys, e.Table.RecordKey(handle))
	}
	values, err := kv.BatchGetValues(e.ctx.Txn(), batchKeys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return values, nil
}

// encodeNewRow encodes a new row to value.
func (e *InsertValues) encodeNewRow(row types.DatumRow) ([]byte, error) {
	colIDs := make([]int64, 0, len(row))
	skimmedRow := make(types.DatumRow, 0, len(row))
	for _, col := range e.Table.Cols() {
		if !tables.CanSkip(e.Table.Meta(), col, row[col.Offset]) {
			colIDs = append(colIDs, col.ID)
			skimmedRow = append(skimmedRow, row[col.Offset])
		}
	}
	newRowValue, err := tablecodec.EncodeRow(e.ctx.GetSessionVars().StmtCtx, skimmedRow, colIDs, nil, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newRowValue, nil
}

// getKeysNeedCheck gets keys converted from to-be-insert rows to record keys and unique index keys,
// which need to be checked whether they are duplicate keys.
func (e *InsertValues) getKeysNeedCheck(rows []types.DatumRow) ([][]keyWithDupError,
	error) {
	nUnique := 0
	for _, v := range e.Table.WritableIndices() {
		if v.Meta().Unique {
			nUnique++
		}
	}
	rowWithKeys := make([][]keyWithDupError, 0, len(rows))

	var handleCol *table.Column
	// Get handle column if PK is handle.
	if e.Table.Meta().PKIsHandle {
		for _, col := range e.Table.Cols() {
			if col.IsPKHandleColumn(e.Table.Meta()) {
				handleCol = col
				break
			}
		}
	}

	for _, row := range rows {
		keysWithErr := make([]keyWithDupError, 0, nUnique+1)
		newRowValue, err := e.encodeNewRow(row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Append record keys and errors.
		if e.Table.Meta().PKIsHandle {
			handle := row[handleCol.Offset].GetInt64()
			keysWithErr = append(keysWithErr, keyWithDupError{
				true,
				e.Table.RecordKey(handle),
				kv.ErrKeyExists.FastGen("Duplicate entry '%d' for key 'PRIMARY'", handle),
				newRowValue,
			})
		}

		// append unique keys and errors
		for _, v := range e.Table.WritableIndices() {
			if !v.Meta().Unique {
				continue
			}
			colVals, err1 := v.FetchValues(row, nil)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			// Pass handle = 0 to GenIndexKey,
			// due to we only care about distinct key.
			key, distinct, err1 := v.GenIndexKey(e.ctx.GetSessionVars().StmtCtx,
				colVals, 0, nil)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			// Skip the non-distinct keys.
			if !distinct {
				continue
			}
			colValStr, err1 := types.DatumsToString(colVals)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			keysWithErr = append(keysWithErr, keyWithDupError{
				false,
				key,
				kv.ErrKeyExists.FastGen("Duplicate entry '%s' for key '%s'",
					colValStr, v.Meta().Name),
				newRowValue,
			})
		}
		rowWithKeys = append(rowWithKeys, keysWithErr)
	}
	return rowWithKeys, nil
}

// batchGetInsertKeys uses batch-get to fetch all key-value pairs to be checked for ignore or duplicate key update.
func (e *InsertValues) batchGetInsertKeys(newRows []types.DatumRow) ([][]keyWithDupError, map[string][]byte, error) {
	// Get keys need to be checked.
	keysInRows, err := e.getKeysNeedCheck(newRows)
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
	values, err := kv.BatchGetValues(e.ctx.Txn(), batchKeys)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return keysInRows, values, nil
}
