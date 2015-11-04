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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/errors2"
)

func (d *ddl) adjustColumnOffset(columns []*model.ColumnInfo, indices []*model.IndexInfo, offset int) {
	offsetChanged := make(map[int]int)
	for i := offset; i < len(columns); i++ {
		offsetChanged[columns[i].Offset] = i
		columns[i].Offset = i
	}

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

func (d *ddl) addColumn(tblInfo *model.TableInfo, spec *AlterSpecification) (*model.ColumnInfo, int, error) {
	// Check column name duplicate.
	cols := tblInfo.Columns
	position := len(cols)

	// Get column position.
	if spec.Position.Type == ColumnPositionFirst {
		position = 0
	} else if spec.Position.Type == ColumnPositionAfter {
		c := findCol(tblInfo.Columns, spec.Position.RelativeColumn)
		if c == nil {
			return nil, 0, errors.Errorf("No such column: %v", spec.Column.Name)
		}

		// Insert position is after the mentioned column.
		position = c.Offset + 1
	}

	// TODO: set constraint
	col, _, err := d.buildColumnAndConstraint(position, spec.Column)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	colInfo := &col.ColumnInfo
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

func (d *ddl) onColumnAdd(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tblInfo, err := d.getTableInfo(t, job)
	if err != nil {
		return errors.Trace(err)
	}

	spec := &AlterSpecification{}
	offset := 0
	err = job.DecodeArgs(&spec, &offset)
	if err != nil {
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	var columnInfo *model.ColumnInfo
	columnInfo = findCol(tblInfo.Columns, spec.Column.Name)
	if columnInfo != nil {
		if columnInfo.State == model.StatePublic {
			// we already have a column with same column name
			job.State = model.JobCancelled
			return errors.Errorf("ADD COLUMN: column already exist %s", spec.Column.Name)
		}
	} else {
		columnInfo, offset, err = d.addColumn(tblInfo, spec)
		if err != nil {
			job.State = model.JobCancelled
			return errors.Trace(err)
		}

		// Set offset arg to job.
		if offset != 0 {
			job.Args = []interface{}{spec, offset}
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
		job.SchemaState = model.StateReorgnization
		columnInfo.State = model.StateReorgnization
		// initialize SnapshotVer to 0 for later reorgnization check.
		job.SnapshotVer = 0
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateReorgnization:
		// reorganization -> public
		// get the current version for reorgnization if we don't have
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
			return d.backfillColumn(tbl, columnInfo, job.SnapshotVer)
		})
		if errors2.ErrorEqual(err, errWaitReorgTimeout) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		// Adjust column offset.
		d.adjustColumnOffset(tblInfo.Columns, tblInfo.Indices, offset)

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

func (d *ddl) onColumnDrop(t *meta.Meta, job *model.Job) error {
	// TODO: complete it.
	return nil
}

func (d *ddl) backfillColumn(t table.Table, columnInfo *model.ColumnInfo, version uint64) error {
	seekHandle := int64(0)
	for {
		handles, err := d.getSnapshotRows(t, version, seekHandle)
		if err != nil {
			return errors.Trace(err)
		} else if len(handles) == 0 {
			return nil
		}

		seekHandle = handles[len(handles)-1] + 1
		// TODO: save seekHandle in reorgnization job, so we can resume this job later from this handle.

		err = d.backfillColumnData(t, columnInfo, handles)
		if err != nil {
			return errors.Trace(err)
		}
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

			// If row column doesn't exist, we need to backfill column.
			// Lock row first.
			err = txn.LockKeys(t.RecordKey(handle, nil))
			if err != nil {
				return errors.Trace(err)
			}

			value, _, err := tables.GetColDefaultValue(nil, columnInfo)
			if err != nil {
				return errors.Trace(err)
			}

			err = t.SetColValue(txn, backfillKey, value)
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
