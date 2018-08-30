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
//     4. err (error) : error in the update.
func updateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, modified []bool, t table.Table,
	onDup bool) (bool, bool, int64, error) {
	sc := ctx.GetSessionVars().StmtCtx
	changed, handleChanged := false, false
	// onUpdateSpecified is for "UPDATE SET ts_field = old_value", the
	// timestamp field is explicitly set, but not changed in fact.
	onUpdateSpecified := make(map[int]bool)
	var newHandle int64

	// We can iterate on public columns not writable columns,
	// because all of them are sorted by their `Offset`, which
	// causes all writable columns are after public columns.

	// 1. Cast modified values.
	for i, col := range t.Cols() {
		if modified[i] {
			// Cast changed fields with respective columns.
			v, err := table.CastValue(ctx, newData[i], col.ToInfo())
			if err != nil {
				return false, false, 0, errors.Trace(err)
			}
			newData[i] = v
		}
	}

	// 2. Check null.
	for i, col := range t.Cols() {
		if err := col.CheckNotNull(newData[i]); err != nil {
			if sc.BadNullAsWarning {
				newData[i], err = table.GetColDefaultValue(ctx, col.ToInfo())
				if err != nil {
					return false, false, 0, errors.Trace(err)
				}
			} else {
				return false, false, 0, errors.Trace(err)
			}
		}
	}

	// 3. Compare datum, then handle some flags.
	for i, col := range t.Cols() {
		if cmp, err := newData[i].CompareDatum(sc, &oldData[i]); err != nil {
			return false, false, 0, errors.Trace(err)
		} else {
			if cmp != 0 {
				changed = true
				modified[i] = true
				// Rebase auto increment id if the field is changed.
				if mysql.HasAutoIncrementFlag(col.Flag) {
					if err := t.RebaseAutoID(ctx, newData[i].GetInt64(), true); err != nil {
						return false, false, 0, errors.Trace(err)
					}
				}
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
	}

	// If no changes, nothing to do, return directly.
	if !changed {
		// See https://dev.mysql.com/doc/refman/5.7/en/mysql-real-connect.html  CLIENT_FOUND_ROWS
		if ctx.GetSessionVars().ClientCapability&mysql.ClientFoundRows > 0 {
			sc.AddAffectedRows(1)
		}
		return false, false, 0, nil
	}

	// 4. Fill values into on-update-now fields, only if they are really changed.
	for i, col := range t.Cols() {
		if mysql.HasOnUpdateNowFlag(col.Flag) && !modified[i] && !onUpdateSpecified[i] {
			if v, err := expression.GetTimeValue(ctx, strings.ToUpper(ast.CurrentTimestamp), col.Tp, col.Decimal); err != nil {
				return false, false, 0, errors.Trace(err)
			} else {
				newData[i] = v
				modified[i] = true
			}
		}
	}

	// 5. If handle changed, remove the old then add the new record, otherwise update the record.
	var err error
	if handleChanged {
		skipHandleCheck := false
		if sc.DupKeyAsWarning {
			// For `UPDATE IGNORE`/`INSERT IGNORE ON DUPLICATE KEY UPDATE`
			// If the new handle exists, this will avoid to remove the record.
			err = tables.CheckHandleExists(ctx, t, newHandle, newData)
			if err != nil {
				return false, handleChanged, newHandle, errors.Trace(err)
			}
			skipHandleCheck = true
		}
		if err := t.RemoveRecord(ctx, h, oldData); err != nil {
			return false, false, 0, errors.Trace(err)
		}
		newHandle, err = t.AddRecord(ctx, newData, skipHandleCheck)
		if err != nil {
			return false, false, 0, errors.Trace(err)
		}
	} else {
		// Update record to new value and update index.
		err = t.UpdateRecord(ctx, h, oldData, newData, modified)
		if err != nil {
			return false, false, 0, errors.Trace(err)
		}
	}

	if onDup {
		sc.AddAffectedRows(2)
	} else {
		// if handleChanged == true, the `affectedRows` is calculated when add new record.
		if !handleChanged {
			sc.AddAffectedRows(1)
		}
	}

	// 6. Update delta for the statistics.
	colSize := make(map[int64]int64)
	for id, col := range t.Cols() {
		val := int64(len(newData[id].GetBytes()) - len(oldData[id].GetBytes()))
		if val != 0 {
			colSize[col.ID] = val
		}
	}
	ctx.GetSessionVars().TxnCtx.UpdateDeltaForTable(t.Meta().ID, 0, 1, colSize)
	return true, handleChanged, newHandle, nil
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
