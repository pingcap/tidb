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
		return errors.Trace(terror.DatabaseNotExists)
	} else if err != nil {
		return errors.Trace(err)
	}

	for _, tbl := range tables {
		if tbl.Name.L == tbInfo.Name.L {
			if tbl.ID != tbInfo.ID {
				// table exists, can't create, we should cancel this job now.
				job.State = model.JobCancelled
				return errors.Trace(ErrExists)
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

func (d *ddl) onDropTable(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tableID := job.TableID

	tblInfo, err := t.GetTable(schemaID, tableID)
	if terror.ErrorEqual(err, meta.ErrDBNotExists) {
		job.State = model.JobCancelled
		return errors.Trace(terror.DatabaseNotExists)
	} else if err != nil {
		return errors.Trace(err)
	}

	if tblInfo == nil {
		job.State = model.JobCancelled
		return errors.Trace(ErrNotExists)
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
		return errors.Trace(err)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		tblInfo.State = model.StateDeleteOnly
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		tblInfo.State = model.StateDeleteReorganization
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateDeleteReorganization:
		// reorganization -> absent
		var tbl table.Table
		tbl, err = d.getTable(t, schemaID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}

		err = d.runReorgJob(func() error {
			return d.dropTableData(tbl)
		})

		if terror.ErrorEqual(err, errWaitReorgTimeout) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		// all reorganization jobs done, drop this database
		if err = t.DropTable(schemaID, tableID); err != nil {
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

func (d *ddl) getTable(t *meta.Meta, schemaID int64, tblInfo *model.TableInfo) (table.Table, error) {
	alloc := autoid.NewAllocator(d.store, schemaID)
	tbl := table.TableFromMeta(alloc, tblInfo)
	return tbl, nil
}

func (d *ddl) getTableInfo(t *meta.Meta, job *model.Job) (*model.TableInfo, error) {
	schemaID := job.SchemaID
	tableID := job.TableID
	tblInfo, err := t.GetTable(schemaID, tableID)
	if terror.ErrorEqual(err, meta.ErrDBNotExists) {
		job.State = model.JobCancelled
		return nil, errors.Trace(ErrNotExists)
	} else if err != nil {
		return nil, errors.Trace(err)
	} else if tblInfo == nil {
		job.State = model.JobCancelled
		return nil, errors.Trace(ErrNotExists)
	}

	if tblInfo.State != model.StatePublic {
		job.State = model.JobCancelled
		return nil, errors.Errorf("table %s is not in public, but %s", tblInfo.Name.L, tblInfo.State)
	}

	return tblInfo, nil
}

func (d *ddl) dropTableData(t table.Table) error {
	// delete table data
	err := d.delKeysWithPrefix(t.KeyPrefix())
	if err != nil {
		return errors.Trace(err)
	}

	// delete table index
	err = d.delKeysWithPrefix(t.IndexPrefix())

	return errors.Trace(err)
}
