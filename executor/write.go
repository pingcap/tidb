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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
)

var (
	_ Executor = &UpdateExec{}
	_ Executor = &DeleteExec{}
	_ Executor = &InsertExec{}
	_ Executor = &ReplaceExec{}
	_ Executor = &LoadDataExec{}
)

// updateRecord updates the row specified by the handle `h`, from `oldData` to `newData`.
// `modified` means which columns are really modified. It's used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
// The return values:
//     1. changed (bool) : does the update really change the row values. e.g. update set i = 1 where i = 1;
//     2. handleChanged (bool) : is the handle changed after the update.
//     3. newHandle (int64) : if handleChanged == true, the newHandle means the new handle after update.
//     4. lastInsertID (uint64) : the lastInsertID should be set by the newData.
//     5. err (error) : error in the update.
func updateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, modified []bool, t table.Table,
	onDup bool) (bool, bool, int64, uint64, error) {
	var sc = ctx.GetSessionVars().StmtCtx
	var changed, handleChanged = false, false
	// onUpdateSpecified is for "UPDATE SET ts_field = old_value", the
	// timestamp field is explicitly set, but not changed in fact.
	var onUpdateSpecified = make(map[int]bool)
	var newHandle int64
	var lastInsertID uint64

	// We can iterate on public columns not writable columns,
	// because all of them are sorted by their `Offset`, which
	// causes all writable columns are after public columns.
	for i, col := range t.Cols() {
		if modified[i] {
			// Cast changed fields with respective columns.
			v, err := table.CastValue(ctx, newData[i], col.ToInfo())
			if err != nil {
				return false, handleChanged, newHandle, 0, errors.Trace(err)
			}
			newData[i] = v
		}

		if mysql.HasNotNullFlag(col.Flag) && newData[i].IsNull() && sc.BadNullAsWarning {
			var err error
			newData[i], err = table.GetColDefaultValue(ctx, col.ToInfo())
			if err != nil {
				return false, handleChanged, newHandle, 0, errors.Trace(err)
			}
		}
		// Rebase auto increment id if the field is changed.
		if mysql.HasAutoIncrementFlag(col.Flag) {
			if newData[i].IsNull() {
				return false, handleChanged, newHandle, 0, table.ErrColumnCantNull.GenByArgs(col.Name)
			}
			val, errTI := newData[i].ToInt64(sc)
			if errTI != nil {
				return false, handleChanged, newHandle, 0, errors.Trace(errTI)
			}
			lastInsertID = uint64(val)
			err := t.RebaseAutoID(ctx, val, true)
			if err != nil {
				return false, handleChanged, newHandle, 0, errors.Trace(err)
			}
		}
		cmp, err := newData[i].CompareDatum(sc, &oldData[i])
		if err != nil {
			return false, handleChanged, newHandle, 0, errors.Trace(err)
		}
		if cmp != 0 {
			changed = true
			modified[i] = true
			if col.IsPKHandleColumn(t.Meta()) {
				handleChanged = true
				newHandle = newData[i].GetInt64()
			}
		} else {
			if mysql.HasOnUpdateNowFlag(col.Flag) && modified[i] {
				// It's for "UPDATE t SET ts = ts" and ts is a timestamp.
				onUpdateSpecified[i] = true
			}
			modified[i] = false
		}
	}

	// Check the not-null constraints.
	err := table.CheckNotNull(t.Cols(), newData)
	if err != nil {
		return false, handleChanged, newHandle, 0, errors.Trace(err)
	}

	if !changed {
		// See https://dev.mysql.com/doc/refman/5.7/en/mysql-real-connect.html  CLIENT_FOUND_ROWS
		if ctx.GetSessionVars().ClientCapability&mysql.ClientFoundRows > 0 {
			sc.AddAffectedRows(1)
		}
		return false, handleChanged, newHandle, lastInsertID, nil
	}

	// Fill values into on-update-now fields, only if they are really changed.
	for i, col := range t.Cols() {
		if mysql.HasOnUpdateNowFlag(col.Flag) && !modified[i] && !onUpdateSpecified[i] {
			v, errGT := expression.GetTimeValue(ctx, strings.ToUpper(ast.CurrentTimestamp), col.Tp, col.Decimal)
			if errGT != nil {
				return false, handleChanged, newHandle, 0, errors.Trace(errGT)
			}
			newData[i] = v
			modified[i] = true
		}
	}

	if handleChanged {
		skipHandleCheck := false
		if sc.DupKeyAsWarning {
			// if the new handle exists. `UPDATE IGNORE` will avoid removing record, and do nothing.
			err = tables.CheckHandleExists(ctx, t, newHandle, newData)
			if err != nil {
				return false, handleChanged, newHandle, 0, errors.Trace(err)
			}
			skipHandleCheck = true
		}
		err = t.RemoveRecord(ctx, h, oldData)
		if err != nil {
			return false, handleChanged, newHandle, 0, errors.Trace(err)
		}
		newHandle, err = t.AddRecord(ctx, newData, skipHandleCheck)
	} else {
		// Update record to new value and update index.
		err = t.UpdateRecord(ctx, h, oldData, newData, modified)
	}
	if err != nil {
		return false, handleChanged, newHandle, 0, errors.Trace(err)
	}

	if onDup {
		sc.AddAffectedRows(2)
	} else {
		// if handleChanged == true, the `affectedRows` is calculated when add new record.
		if !handleChanged {
			sc.AddAffectedRows(1)
		}
	}
	colSize := make(map[int64]int64)
	for id, col := range t.Cols() {
		val := int64(len(newData[id].GetBytes()) - len(oldData[id].GetBytes()))
		if val != 0 {
			colSize[col.ID] = val
		}
	}
	ctx.GetSessionVars().TxnCtx.UpdateDeltaForTable(t.Meta().ID, 0, 1, colSize)
	return true, handleChanged, newHandle, lastInsertID, nil
}

// resetErrDataTooLong reset ErrDataTooLong error msg.
// types.ErrDataTooLong is produced in types.ProduceStrWithSpecifiedTp, there is no column info in there,
// so we reset the error msg here, and wrap old err with errors.Wrap.
func resetErrDataTooLong(colName string, rowIdx int, err error) error {
	newErr := types.ErrDataTooLong.Gen("Data too long for column '%v' at row %v", colName, rowIdx)
	return errors.Wrap(err, newErr)
}

func getTableOffset(schema *expression.Schema, handleCol *expression.Column) int {
	for i, col := range schema.Columns {
		if col.DBName.L == handleCol.DBName.L && col.TblName.L == handleCol.TblName.L {
			return i
		}
	}
	panic("Couldn't get column information when do update/delete")
}
