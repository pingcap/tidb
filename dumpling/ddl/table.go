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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
)

func (d *ddl) onCreateTable(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tbInfo := &model.TableInfo{}
	if err := job.DecodeArgs(tbInfo); err != nil {
		// arg error, cancel this job.
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	tbInfo.State = model.StateNone

	tables, err := t.ListTables(schemaID)
	if terror.ErrorEqual(err, meta.ErrDBNotExists) {
		job.State = model.JobCancelled
		return errors.Trace(infoschema.DatabaseNotExists)
	} else if err != nil {
		return errors.Trace(err)
	}

	for _, tbl := range tables {
		if tbl.Name.L == tbInfo.Name.L {
			if tbl.ID != tbInfo.ID {
				// table exists, can't create, we should cancel this job now.
				job.State = model.JobCancelled
				return errors.Trace(infoschema.TableExists)
			}

			tbInfo = tbl
		}
	}

	_, err = t.GenSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	switch tbInfo.State {
	case model.StateNone:
		// none -> public
		job.SchemaState = model.StatePublic
		tbInfo.State = model.StatePublic
		err = t.CreateTable(schemaID, tbInfo)
		if err != nil {
			return errors.Trace(err)
		}
		// finish this job
		job.State = model.JobDone
		return nil
	default:
		return errors.Errorf("invalid table state %v", tbInfo.State)
	}
}

func (d *ddl) delReorgTable(t *meta.Meta, task *model.Job) error {
	tblInfo, err := t.GetTable(task.SchemaID, task.TableID)
	if terror.ErrorEqual(err, meta.ErrDBNotExists) {
		task.State = model.JobCancelled
		return errors.Trace(err)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if tblInfo == nil {
		task.State = model.JobCancelled
		return errors.Trace(err)
	}

	_, err = t.GenSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	// reorganization -> absent
	var tbl table.Table
	tbl, err = d.getTable(t, task.SchemaID, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.dropTableData(tbl)
	if err != nil {
		return errors.Trace(err)
	}

	// all reorganization jobs done, drop this table.
	if err = t.DropTable(task.SchemaID, task.TableID); err != nil {
		return errors.Trace(err)
	}

	// finish this job
	task.SchemaState = model.StateNone
	task.State = model.JobDone

	return nil
}

func (d *ddl) onDropTable(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tableID := job.TableID

	tblInfo, err := t.GetTable(schemaID, tableID)
	if terror.ErrorEqual(err, meta.ErrDBNotExists) {
		job.State = model.JobCancelled
		return errors.Trace(infoschema.DatabaseNotExists)
	} else if err != nil {
		return errors.Trace(err)
	}

	if tblInfo == nil {
		job.State = model.JobCancelled
		return errors.Trace(infoschema.TableNotExists)
	}

	_, err = t.GenSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	switch tblInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		tblInfo.State = model.StateWriteOnly
		err = t.UpdateTable(schemaID, tblInfo)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		tblInfo.State = model.StateDeleteOnly
		err = t.UpdateTable(schemaID, tblInfo)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		tblInfo.State = model.StateDeleteReorganization
		err = t.UpdateTable(schemaID, tblInfo)
		// finish this job
		job.State = model.JobDone
		job.SchemaState = model.StateNone
	default:
		err = errors.Errorf("invalid table state %v", tblInfo.State)
	}

	return errors.Trace(err)
}

func (d *ddl) getTable(t *meta.Meta, schemaID int64, tblInfo *model.TableInfo) (table.Table, error) {
	alloc := autoid.NewAllocator(d.store, schemaID)
	tbl, err := table.TableFromMeta(alloc, tblInfo)
	return tbl, errors.Trace(err)
}

func (d *ddl) getTableInfo(t *meta.Meta, job *model.Job) (*model.TableInfo, error) {
	schemaID := job.SchemaID
	tableID := job.TableID
	tblInfo, err := t.GetTable(schemaID, tableID)
	if terror.ErrorEqual(err, meta.ErrDBNotExists) {
		job.State = model.JobCancelled
		return nil, errors.Trace(infoschema.DatabaseNotExists)
	} else if err != nil {
		return nil, errors.Trace(err)
	} else if tblInfo == nil {
		job.State = model.JobCancelled
		return nil, errors.Trace(infoschema.TableNotExists)
	}

	if tblInfo.State != model.StatePublic {
		job.State = model.JobCancelled
		return nil, errors.Errorf("table %s is not in public, but %s", tblInfo.Name.L, tblInfo.State)
	}

	return tblInfo, nil
}

func (d *ddl) dropTableData(t table.Table) error {
	// delete table data
	err := d.delKeysWithPrefix(t.RecordPrefix())
	if err != nil {
		return errors.Trace(err)
	}

	// delete table index
	err = d.delKeysWithPrefix(t.IndexPrefix())

	return errors.Trace(err)
}
