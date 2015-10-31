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
	"bytes"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/errors2"
)

func (d *ddl) addColumn(tblInfo *model.TableInfo, spec *AlterSpecification) (*model.ColumnInfo, error) {
	// Check column name duplicate.
	cols := tblInfo.Columns
	position := len(cols)

	// Get column position.
	if spec.Position.Type == ColumnPositionFirst {
		position = 0
	} else if spec.Position.Type == ColumnPositionAfter {
		c := findCol(tblInfo.Columns, spec.Position.RelativeColumn)
		if c == nil {
			return nil, errors.Errorf("No such column: %v", spec.Column.Name)
		}

		// Insert position is after the mentioned column.
		position = c.Offset + 1
	}

	// TODO: set constraint
	col, _, err := d.buildColumnAndConstraint(position, spec.Column)
	if err != nil {
		return nil, errors.Trace(err)
	}

	colInfo := &col.ColumnInfo

	// Insert col into the right place of the column list.
	newCols := make([]*model.ColumnInfo, 0, len(cols)+1)
	newCols = append(newCols, cols[:position]...)
	newCols = append(newCols, colInfo)
	newCols = append(newCols, cols[position:]...)

	// Adjust position.
	if position != len(cols) {
		offsetChanged := make(map[int]int)
		for i := position + 1; i < len(newCols); i++ {
			offsetChanged[newCols[i].Offset] = i
			newCols[i].Offset = i
		}

		// Update index column offset info.
		for _, idx := range tblInfo.Indices {
			for _, c := range idx.Columns {
				newOffset, ok := offsetChanged[c.Offset]
				if ok {
					c.Offset = newOffset
				}
			}
		}
	}

	tblInfo.Columns = newCols
	return colInfo, nil
}

func (d *ddl) onColumnAdd(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tblInfo, err := d.getTableInfo(t, job)
	if err != nil {
		return errors.Trace(err)
	}

	spec := &AlterSpecification{}
	err = job.DecodeArgs(&spec)
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
		columnInfo, err = d.addColumn(tblInfo, spec)
		if err != nil {
			job.State = model.JobCancelled
			return errors.Trace(err)
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
		// write only -> public
		job.SchemaState = model.StateReorgnization
		columnInfo.State = model.StateReorgnization

		// get the current version for later Reorgnization.
		var ver kv.Version
		ver, err = d.store.CurrentVersion()
		if err != nil {
			return errors.Trace(err)
		}

		job.SnapshotVer = ver.Ver

		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateReorgnization:
		// reorganization -> public
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

	return nil
}

func (d *ddl) onColumnDrop(t *meta.Meta, job *model.Job) error {
	// TODO: complete it.
	return nil
}

func (d *ddl) needBackfillColumnForRow(txn kv.Transaction, t table.Table, handle int64, key []byte) (bool, error) {
	if ok, err := checkRowExist(txn, t, handle); err != nil {
		return false, errors.Trace(err)
	} else if !ok {
		// If row doesn't exist, we don't need to backfill column.
		return false, nil
	}

	_, err := txn.Get([]byte(key))
	if kv.IsErrNotFound(err) {
		// If row column doesn't exist, we need to backfill column.
		return true, nil
	}
	if err != nil {
		return false, errors.Trace(err)
	}

	return false, nil
}

func (d *ddl) backfillColumn(t table.Table, columnInfo *model.ColumnInfo, version uint64) error {
	ver := kv.Version{Ver: version}

	snap, err := d.store.GetSnapshot(ver)
	if err != nil {
		return errors.Trace(err)
	}

	defer snap.MvccRelease()

	firstKey := t.FirstKey()
	prefix := []byte(t.KeyPrefix())

	ctx := d.newReorgContext()
	txn, err := ctx.GetTxn(true)
	if err != nil {
		return errors.Trace(err)
	}
	defer txn.Rollback()

	it := snap.NewMvccIterator(kv.EncodeKey([]byte(firstKey)), ver)
	defer it.Close()

	for it.Valid() {
		key := kv.DecodeKey([]byte(it.Key()))
		if !bytes.HasPrefix(key, prefix) {
			break
		}

		var handle int64
		handle, err = util.DecodeHandleFromRowKey(string(key))
		if err != nil {
			return errors.Trace(err)
		}

		log.Info("backfill column...", handle)

		// Check if need backfill column data.
		backfillKey := t.RecordKey(handle, &column.Col{*columnInfo})
		need, err := d.needBackfillColumnForRow(txn, t, handle, backfillKey)
		if err != nil {
			return errors.Trace(err)
		}

		if need {
			// TODO: check and get timestamp/datetime default value.
			// refer to getDefaultValue in stmt/stmts/stmt_helper.go.
			err = t.(*tables.Table).SetColValue(txn, backfillKey, columnInfo.DefaultValue)
			if err != nil {
				return errors.Trace(err)
			}
		}

		rk := kv.EncodeKey([]byte(t.RecordKey(handle, nil)))
		it, err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if errors2.ErrorEqual(err, kv.ErrNotExist) {
			break
		} else if err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(txn.Commit())
}
