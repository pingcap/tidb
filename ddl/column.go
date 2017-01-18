// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

func (d *ddl) adjustColumnOffset(columns []*model.ColumnInfo, indices []*model.IndexInfo, offset int, added bool) {
	offsetChanged := make(map[int]int)
	if added {
		for i := offset + 1; i < len(columns); i++ {
			offsetChanged[columns[i].Offset] = i
			columns[i].Offset = i
		}
		columns[offset].Offset = offset
	} else {
		for i := offset + 1; i < len(columns); i++ {
			offsetChanged[columns[i].Offset] = i - 1
			columns[i].Offset = i - 1
		}
		columns[offset].Offset = len(columns) - 1
	}

	// TODO: Index can't cover the add/remove column with offset now, we may check this later.

	// Update index column offset info.
	for _, idx := range indices {
		for _, col := range idx.Columns {
			newOffset, ok := offsetChanged[col.Offset]
			if ok {
				col.Offset = newOffset
			}
		}
	}
}

func (d *ddl) createColumnInfo(tblInfo *model.TableInfo, colInfo *model.ColumnInfo, pos *ast.ColumnPosition) (*model.ColumnInfo, int, error) {
	// Check column name duplicate.
	cols := tblInfo.Columns
	position := len(cols)

	// Get column position.
	if pos.Tp == ast.ColumnPositionFirst {
		position = 0
	} else if pos.Tp == ast.ColumnPositionAfter {
		c := findCol(cols, pos.RelativeColumn.Name.L)
		if c == nil {
			return nil, 0, infoschema.ErrColumnNotExists.GenByArgs(pos.RelativeColumn, tblInfo.Name)
		}

		// Insert position is after the mentioned column.
		position = c.Offset + 1
	}
	colInfo.ID = allocateColumnID(tblInfo)
	colInfo.State = model.StateNone
	// To support add column asynchronous, we should mark its offset as the last column.
	// So that we can use origin column offset to get value from row.
	colInfo.Offset = len(cols)

	// Insert col into the right place of the column list.
	newCols := make([]*model.ColumnInfo, 0, len(cols)+1)
	newCols = append(newCols, cols[:position]...)
	newCols = append(newCols, colInfo)
	newCols = append(newCols, cols[position:]...)

	tblInfo.Columns = newCols
	return colInfo, position, nil
}

func (d *ddl) onAddColumn(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return errors.Trace(err)
	}

	col := &model.ColumnInfo{}
	pos := &ast.ColumnPosition{}
	offset := 0
	err = job.DecodeArgs(col, pos, &offset)
	if err != nil {
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	columnInfo := findCol(tblInfo.Columns, col.Name.L)
	if columnInfo != nil {
		if columnInfo.State == model.StatePublic {
			// We already have a column with the same column name.
			job.State = model.JobCancelled
			return infoschema.ErrColumnExists.GenByArgs(col.Name)
		}
	} else {
		columnInfo, offset, err = d.createColumnInfo(tblInfo, col, pos)
		if err != nil {
			job.State = model.JobCancelled
			return errors.Trace(err)
		}
		// Set offset arg to job.
		if offset != 0 {
			job.Args = []interface{}{columnInfo, pos, offset}
		}
	}

	ver, err := updateSchemaVersion(t, job)
	if err != nil {
		return errors.Trace(err)
	}

	switch columnInfo.State {
	case model.StateNone:
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		columnInfo.State = model.StateDeleteOnly
		err = t.UpdateTable(schemaID, tblInfo)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		columnInfo.State = model.StateWriteOnly
		err = t.UpdateTable(schemaID, tblInfo)
	case model.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = model.StateWriteReorganization
		columnInfo.State = model.StateWriteReorganization
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		err = t.UpdateTable(schemaID, tblInfo)
	case model.StateWriteReorganization:
		// reorganization -> public
		// Get the current version for reorganization if we don't have it.
		reorgInfo, err := d.getReorgInfo(t, job)
		if err != nil || reorgInfo.first {
			// If we run reorg firstly, we should update the job snapshot version
			// and then run the reorg next time.
			return errors.Trace(err)
		}

		tbl, err := d.getTable(schemaID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}
		if columnInfo.DefaultValue != nil || mysql.HasNotNullFlag(columnInfo.Flag) {
			err = d.runReorgJob(func() error {
				return d.addTableColumn(tbl, columnInfo, reorgInfo, job)
			})
			if err != nil {
				// If the timeout happens, we should return.
				// Then check for the owner and re-wait job to finish.
				return errors.Trace(filterError(err, errWaitReorgTimeout))
			}
		}

		// Adjust column offset.
		d.adjustColumnOffset(tblInfo.Columns, tblInfo.Indices, offset, true)
		columnInfo.State = model.StatePublic
		if err = t.UpdateTable(schemaID, tblInfo); err != nil {
			return errors.Trace(err)
		}

		// Finish this job.
		job.SchemaState = model.StatePublic
		job.State = model.JobDone
		job.BinlogInfo.AddTableInfo(ver, tblInfo)
	default:
		err = ErrInvalidColumnState.Gen("invalid column state %v", columnInfo.State)
	}

	return errors.Trace(err)
}

func (d *ddl) onDropColumn(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return errors.Trace(err)
	}

	var colName model.CIStr
	err = job.DecodeArgs(&colName)
	if err != nil {
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	colInfo := findCol(tblInfo.Columns, colName.L)
	if colInfo == nil {
		job.State = model.JobCancelled
		return ErrCantDropFieldOrKey.Gen("column %s doesn't exist", colName)
	}

	if len(tblInfo.Columns) == 1 {
		job.State = model.JobCancelled
		return ErrCantRemoveAllFields.Gen("can't drop only column %s in table %s",
			colName, tblInfo.Name)
	}

	// We don't support dropping column with index covered now.
	if isColumnWithIndex(colName.L, tblInfo.Indices) {
		job.State = model.JobCancelled
		return errCantDropColWithIndex.Gen("can't drop column %s with index covered now", colName)
	}

	ver, err := updateSchemaVersion(t, job)
	if err != nil {
		return errors.Trace(err)
	}

	switch colInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		colInfo.State = model.StateWriteOnly
		// Set this column's offset to the last and reset all following columns' offsets.
		d.adjustColumnOffset(tblInfo.Columns, tblInfo.Indices, colInfo.Offset, false)
		err = t.UpdateTable(schemaID, tblInfo)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		colInfo.State = model.StateDeleteOnly
		err = t.UpdateTable(schemaID, tblInfo)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		colInfo.State = model.StateDeleteReorganization
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		err = t.UpdateTable(schemaID, tblInfo)
	case model.StateDeleteReorganization:
		// reorganization -> absent
		reorgInfo, err := d.getReorgInfo(t, job)
		if err != nil || reorgInfo.first {
			// If we run reorg firstly, we should update the job snapshot version
			// and then run the reorg next time.
			return errors.Trace(err)
		}

		// All reorganization jobs are done, drop this column.
		newColumns := make([]*model.ColumnInfo, 0, len(tblInfo.Columns))
		for _, col := range tblInfo.Columns {
			if col.Name.L != colName.L {
				newColumns = append(newColumns, col)
			}
		}
		tblInfo.Columns = newColumns
		if err = t.UpdateTable(schemaID, tblInfo); err != nil {
			return errors.Trace(err)
		}

		// Finish this job.
		job.SchemaState = model.StateNone
		job.State = model.JobDone
		job.BinlogInfo.AddTableInfo(ver, tblInfo)
	default:
		err = ErrInvalidTableState.Gen("invalid table state %v", tblInfo.State)
	}
	return errors.Trace(err)
}

// How to backfill column data in reorganization state?
//  1. Generate a snapshot with special version.
//  2. Traverse the snapshot, get every row in the table.
//  3. For one row, if the row has been already deleted, skip to next row.
//  4. If not deleted, check whether column data has existed, if existed, skip to next row.
//  5. If column data doesn't exist, backfill the column with default value and then continue to handle next row.
func (d *ddl) addTableColumn(t table.Table, columnInfo *model.ColumnInfo, reorgInfo *reorgInfo, job *model.Job) error {
	seekHandle := reorgInfo.Handle
	version := reorgInfo.SnapshotVer
	count := job.GetRowCount()
	ctx := d.newContext()

	colMeta := &columnMeta{
		colID:     columnInfo.ID,
		oldColMap: make(map[int64]*types.FieldType)}
	handles := make([]int64, 0, defaultBatchCnt)
	// Get column default value.
	var err error
	if columnInfo.DefaultValue != nil {
		colMeta.defaultVal, _, err = table.GetColDefaultValue(ctx, columnInfo)
		if err != nil {
			job.State = model.JobCancelled
			log.Errorf("[ddl] fatal: this case shouldn't happen, err:%v", err)
			return errors.Trace(err)
		}
	} else if mysql.HasNotNullFlag(columnInfo.Flag) {
		colMeta.defaultVal = table.GetZeroValue(columnInfo)
	}
	for _, col := range t.Meta().Columns {
		colMeta.oldColMap[col.ID] = &col.FieldType
	}

	for {
		startTime := time.Now()
		handles = handles[:0]
		err = d.iterateSnapshotRows(t, version, seekHandle,
			func(h int64, rowKey kv.Key, rawRecord []byte) (bool, error) {
				handles = append(handles, h)
				if len(handles) == defaultBatchCnt {
					return false, nil
				}
				return true, nil
			})
		if err != nil {
			return errors.Trace(err)
		} else if len(handles) == 0 {
			return nil
		}

		count += int64(len(handles))
		seekHandle = handles[len(handles)-1] + 1
		sub := time.Since(startTime).Seconds()
		err = d.backfillColumn(ctx, t, colMeta, handles, reorgInfo)
		if err != nil {
			log.Warnf("[ddl] added column for %v rows failed, take time %v", count, sub)
			return errors.Trace(err)
		}

		job.SetRowCount(count)
		batchHandleDataHistogram.WithLabelValues(batchAddCol).Observe(sub)
		log.Infof("[ddl] added column for %v rows, take time %v", count, sub)
	}
}

// backfillColumnInTxn deals with a part of backfilling column data in a Transaction.
// This part of the column data rows is defaultSmallBatchCnt.
func (d *ddl) backfillColumnInTxn(t table.Table, colMeta *columnMeta, handles []int64, txn kv.Transaction) (int64, error) {
	nextHandle := handles[0]
	for _, handle := range handles {
		log.Debug("[ddl] backfill column...", handle)
		rowKey := t.RecordKey(handle)
		rowVal, err := txn.Get(rowKey)
		if err != nil {
			if terror.ErrorEqual(err, kv.ErrNotExist) {
				// If row doesn't exist, skip it.
				continue
			}
			return 0, errors.Trace(err)
		}

		rowColumns, err := tablecodec.DecodeRow(rowVal, colMeta.oldColMap)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if _, ok := rowColumns[colMeta.colID]; ok {
			// The column is already added by update or insert statement, skip it.
			continue
		}

		newColumnIDs := make([]int64, 0, len(rowColumns)+1)
		newRow := make([]types.Datum, 0, len(rowColumns)+1)
		for colID, val := range rowColumns {
			newColumnIDs = append(newColumnIDs, colID)
			newRow = append(newRow, val)
		}
		newColumnIDs = append(newColumnIDs, colMeta.colID)
		newRow = append(newRow, colMeta.defaultVal)
		newRowVal, err := tablecodec.EncodeRow(newRow, newColumnIDs)
		if err != nil {
			return 0, errors.Trace(err)
		}
		err = txn.Set(rowKey, newRowVal)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	return nextHandle, nil
}

type columnMeta struct {
	colID      int64
	defaultVal types.Datum
	oldColMap  map[int64]*types.FieldType
}

func (d *ddl) backfillColumn(ctx context.Context, t table.Table, colMeta *columnMeta, handles []int64, reorgInfo *reorgInfo) error {
	var endIdx int
	for len(handles) > 0 {
		if len(handles) >= defaultSmallBatchCnt {
			endIdx = defaultSmallBatchCnt
		} else {
			endIdx = len(handles)
		}

		err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
			if err := d.isReorgRunnable(txn, ddlJobFlag); err != nil {
				return errors.Trace(err)
			}

			nextHandle, err1 := d.backfillColumnInTxn(t, colMeta, handles[:endIdx], txn)
			if err1 != nil {
				return errors.Trace(err1)
			}
			return errors.Trace(reorgInfo.UpdateHandle(txn, nextHandle))
		})

		if err != nil {
			return errors.Trace(err)
		}
		handles = handles[endIdx:]
	}

	return nil
}

func (d *ddl) onModifyColumn(t *meta.Meta, job *model.Job) error {
	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	newCol := &model.ColumnInfo{}
	oldColName := &model.CIStr{}
	err = job.DecodeArgs(newCol, oldColName)
	if err != nil {
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	oldCol := findCol(tblInfo.Columns, oldColName.L)
	if oldCol == nil || oldCol.State != model.StatePublic {
		job.State = model.JobCancelled
		return infoschema.ErrColumnNotExists.GenByArgs(newCol.Name, tblInfo.Name)
	}
	*oldCol = *newCol
	err = t.UpdateTable(job.SchemaID, tblInfo)
	if err != nil {
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	ver, err := updateSchemaVersion(t, job)
	if err != nil {
		return errors.Trace(err)
	}
	job.SchemaState = model.StatePublic
	job.State = model.JobDone
	job.BinlogInfo.AddTableInfo(ver, tblInfo)
	return nil
}

func isColumnWithIndex(colName string, indices []*model.IndexInfo) bool {
	for _, indexInfo := range indices {
		for _, col := range indexInfo.Columns {
			if col.Name.L == colName {
				return true
			}
		}
	}
	return false
}

func allocateColumnID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxColumnID++
	return tblInfo.MaxColumnID
}
