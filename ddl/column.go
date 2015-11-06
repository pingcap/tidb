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
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
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

	// TODO: index can't cover the add/remove column with offset now, we may check this later.

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

func (d *ddl) addColumn(tblInfo *model.TableInfo, colInfo *model.ColumnInfo, pos *ColumnPosition) (*model.ColumnInfo, int, error) {
	// Check column name duplicate.
	cols := tblInfo.Columns
	position := len(cols)

	// Get column position.
	if pos.Type == ColumnPositionFirst {
		position = 0
	} else if pos.Type == ColumnPositionAfter {
		c := findCol(cols, pos.RelativeColumn)
		if c == nil {
			return nil, 0, errors.Errorf("No such column: %v", pos.RelativeColumn)
		}

		// Insert position is after the mentioned column.
		position = c.Offset + 1
	}

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
	tblInfo, err := d.getTableInfo(t, job)
	if err != nil {
		return errors.Trace(err)
	}

	col := &model.ColumnInfo{}
	pos := &ColumnPosition{}
	offset := 0
	err = job.DecodeArgs(col, pos, &offset)
	if err != nil {
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	columnInfo := findCol(tblInfo.Columns, col.Name.L)
	if columnInfo != nil {
		if columnInfo.State == model.StatePublic {
			// we already have a column with same column name
			job.State = model.JobCancelled
			return errors.Errorf("ADD COLUMN: column already exist %s", col.Name.L)
		}
	} else {
		columnInfo, offset, err = d.addColumn(tblInfo, col, pos)
		if err != nil {
			job.State = model.JobCancelled
			return errors.Trace(err)
		}

		// Set offset arg to job.
		if offset != 0 {
			job.Args = []interface{}{columnInfo, pos, offset}
		}
	}

	_, err = t.GenSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	switch columnInfo.State {
	case model.StateNone:
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		columnInfo.State = model.StateDeleteOnly
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		columnInfo.State = model.StateWriteOnly
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = model.StateReorganization
		columnInfo.State = model.StateReorganization
		// initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		// initialize reorg handle to 0
		job.ReorgHandle = 0
		atomic.StoreInt64(&d.reorgHandle, 0)
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateReorganization:
		// reorganization -> public
		// get the current version for reorganization if we don't have
		if job.SnapshotVer == 0 {
			var ver kv.Version
			ver, err = d.store.CurrentVersion()
			if err != nil {
				return errors.Trace(err)
			}

			job.SnapshotVer = ver.Ver
		}

		tbl, err := d.getTable(t, schemaID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}

		err = d.runReorgJob(func() error {
			return d.backfillColumn(tbl, columnInfo, job.SnapshotVer, job.ReorgHandle)
		})

		// backfillColumn updates ReorgHandle after one batch.
		// so we update the job ReorgHandle here.
		job.ReorgHandle = atomic.LoadInt64(&d.reorgHandle)

		if terror.ErrorEqual(err, errWaitReorgTimeout) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		// Adjust column offset.
		d.adjustColumnOffset(tblInfo.Columns, tblInfo.Indices, offset, true)

		columnInfo.State = model.StatePublic

		if err = t.UpdateTable(schemaID, tblInfo); err != nil {
			return errors.Trace(err)
		}

		// finish this job
		job.SchemaState = model.StatePublic
		job.State = model.JobDone
		return nil
	default:
		return errors.Errorf("invalid column state %v", columnInfo.State)
	}
}

func (d *ddl) onDropColumn(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tblInfo, err := d.getTableInfo(t, job)
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
		return errors.Errorf("column %s doesn't exist", colName)
	}

	if len(tblInfo.Columns) == 1 {
		job.State = model.JobCancelled
		return errors.Errorf("can't drop only column %s in table %s", colName, tblInfo.Name)
	}

	// we don't support drop column with index covered now.
	// we must drop the index first, then drop the column.
	for _, indexInfo := range tblInfo.Indices {
		for _, col := range indexInfo.Columns {
			if col.Name.L == colName.L {
				job.State = model.JobCancelled
				return errors.Errorf("can't drop column %s with index %s covered now", colName, indexInfo.Name)
			}
		}
	}

	_, err = t.GenSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	switch colInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		colInfo.State = model.StateWriteOnly

		// set this column's offset to the last and reset all following columns' offset
		d.adjustColumnOffset(tblInfo.Columns, tblInfo.Indices, colInfo.Offset, false)

		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		colInfo.State = model.StateDeleteOnly
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateReorganization
		colInfo.State = model.StateReorganization
		// initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		// initialize reorg handle to 0
		job.ReorgHandle = 0
		atomic.StoreInt64(&d.reorgHandle, 0)
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateReorganization:
		// reorganization -> absent
		// get the current version for reorganization if we don't have
		if job.SnapshotVer == 0 {
			var ver kv.Version
			ver, err = d.store.CurrentVersion()
			if err != nil {
				return errors.Trace(err)
			}

			job.SnapshotVer = ver.Ver
		}

		tbl, err := d.getTable(t, schemaID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}

		err = d.runReorgJob(func() error {
			return d.dropTableColumn(tbl, colInfo, job.SnapshotVer, job.ReorgHandle)
		})

		job.ReorgHandle = atomic.LoadInt64(&d.reorgHandle)

		if terror.ErrorEqual(err, errWaitReorgTimeout) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		// all reorganization jobs done, drop this column
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

		// finish this job
		job.SchemaState = model.StateNone
		job.State = model.JobDone
		return nil
	default:
		return errors.Errorf("invalid table state %v", tblInfo.State)
	}
}

func (d *ddl) backfillColumn(t table.Table, columnInfo *model.ColumnInfo, version uint64, seekHandle int64) error {
	for {
		handles, err := d.getSnapshotRows(t, version, seekHandle)
		if err != nil {
			return errors.Trace(err)
		} else if len(handles) == 0 {
			return nil
		}

		seekHandle = handles[len(handles)-1] + 1
		// TODO: save seekHandle in reorganization job, so we can resume this job later from this handle.

		err = d.backfillColumnData(t, columnInfo, handles)
		if err != nil {
			return errors.Trace(err)
		}

		// update reorgHandle here after every successful batch.
		atomic.StoreInt64(&d.reorgHandle, seekHandle)
	}
}

func (d *ddl) backfillColumnData(t table.Table, columnInfo *model.ColumnInfo, handles []int64) error {
	for _, handle := range handles {
		log.Info("backfill column...", handle)

		err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
			// First check if row exists.
			exist, err := checkRowExist(txn, t, handle)
			if err != nil {
				return errors.Trace(err)
			} else if !exist {
				// If row doesn't exist, skip it.
				return nil
			}

			backfillKey := t.RecordKey(handle, &column.Col{ColumnInfo: *columnInfo})
			_, err = txn.Get(backfillKey)
			if err != nil && !kv.IsErrNotFound(err) {
				return errors.Trace(err)
			}

			value, _, err := tables.GetColDefaultValue(nil, columnInfo)
			if err != nil {
				return errors.Trace(err)
			}

			// must convert to the column field type.
			v, err := types.Convert(value, &columnInfo.FieldType)
			if err != nil {
				return errors.Trace(err)
			}

			err = t.SetColValue(txn, backfillKey, v)
			if err != nil {
				return errors.Trace(err)
			}

			return nil
		})

		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (d *ddl) dropTableColumn(t table.Table, colInfo *model.ColumnInfo, version uint64, seekHandle int64) error {
	col := &column.Col{ColumnInfo: *colInfo}
	for {
		handles, err := d.getSnapshotRows(t, version, seekHandle)
		if err != nil {
			return errors.Trace(err)
		} else if len(handles) == 0 {
			return nil
		}

		seekHandle = handles[len(handles)-1] + 1

		err = kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
			for _, h := range handles {
				key := t.RecordKey(h, col)
				if err := txn.Delete(key); err != nil {
					return errors.Trace(err)
				}
			}
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}

		// update reorgHandle here after every successful batch.
		atomic.StoreInt64(&d.reorgHandle, seekHandle)
	}
}
