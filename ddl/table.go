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
	qerror "github.com/pingcap/tidb/util/errors"
	"github.com/pingcap/tidb/util/errors2"
)

func (d *ddl) onTableCreate(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tbInfo := &model.TableInfo{}
	if err := job.DecodeArgs(tbInfo); err != nil {
		// arg error, cancel this job.
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	tbInfo.State = model.StateNone

	tables, err := t.ListTables(schemaID)
	if errors2.ErrorEqual(err, meta.ErrDBNotExists) {
		job.State = model.JobCancelled
		return errors.Trace(qerror.ErrDatabaseNotExist)
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
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		tbInfo.State = model.StateDeleteOnly
		err = t.CreateTable(schemaID, tbInfo)
		return errors.Trace(err)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		tbInfo.State = model.StateWriteOnly
		err = t.UpdateTable(schemaID, tbInfo)
		return errors.Trace(err)
	case model.StateWriteOnly:
		// write only -> public
		job.SchemaState = model.StatePublic
		tbInfo.State = model.StatePublic
		err = t.UpdateTable(schemaID, tbInfo)
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

func (d *ddl) onTableDrop(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tableID := job.TableID

	tblInfo, err := t.GetTable(schemaID, tableID)
	if errors2.ErrorEqual(err, meta.ErrDBNotExists) {
		job.State = model.JobCancelled
		return errors.Trace(qerror.ErrDatabaseNotExist)
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
		job.SchemaState = model.StateReorgnization
		tblInfo.State = model.StateReorgnization
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateReorgnization:
		// reorganization -> absent
		var tbl table.Table
		tbl, err = d.getTable(t, schemaID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}

		err = d.runReorgJob(func() error {
			return d.dropTableData(tbl)
		})

		if errors2.ErrorEqual(err, errWaitReorgTimeout) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		// all reorgnization jobs done, drop this database
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
	dbInfo, err := t.GetDatabase(schemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	alloc := autoid.NewAllocator(d.store, dbInfo.ID)
	tbl := table.TableFromMeta(dbInfo.Name.L, alloc, tblInfo)
	return tbl, nil
}

func (d *ddl) dropTableData(t table.Table) error {
	ctx := d.newReorgContext()
	txn, err := ctx.GetTxn(true)

	if err != nil {
		return errors.Trace(err)
	}

	// Remove indices.
	for _, v := range t.Indices() {
		if v != nil && v.X != nil {
			if err = v.X.Drop(txn); err != nil {
				ctx.FinishTxn(true)
				return errors.Trace(err)
			}
		}
	}

	return errors.Trace(ctx.FinishTxn(false))
}
