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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
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
			return d.fillColumnData(tbl, columnInfo, job.SnapshotVer)
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

func (d *ddl) fillColumnData(t table.Table, columnInfo *model.ColumnInfo, version uint64) error {
	// TODO: complete it.
	return nil
}
