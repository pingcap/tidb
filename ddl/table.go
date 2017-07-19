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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
)

func (d *ddl) onCreateTable(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tbInfo := &model.TableInfo{}
	if err := job.DecodeArgs(tbInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	tbInfo.State = model.StateNone
	err := checkTableNotExists(t, job, schemaID, tbInfo.Name.L)
	if err != nil {
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch tbInfo.State {
	case model.StateNone:
		// none -> public
		job.SchemaState = model.StatePublic
		tbInfo.State = model.StatePublic
		err = t.CreateTable(schemaID, tbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.State = model.JobDone
		job.BinlogInfo.AddTableInfo(ver, tbInfo)
		d.asyncNotifyEvent(&Event{Tp: model.ActionCreateTable, TableInfo: tbInfo})
		return ver, nil
	default:
		return ver, ErrInvalidTableState.Gen("invalid table state %v", tbInfo.State)
	}
}

func (d *ddl) onDropTable(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tableID := job.TableID

	// Check this table's database.
	tblInfo, err := t.GetTable(schemaID, tableID)
	if err != nil {
		if terror.ErrorEqual(err, meta.ErrDBNotExists) {
			job.State = model.JobCancelled
			return ver, errors.Trace(infoschema.ErrDatabaseNotExists)
		}
		return ver, errors.Trace(err)
	}

	// Check the table.
	if tblInfo == nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(infoschema.ErrTableNotExists)
	}

	originalState := job.SchemaState
	switch tblInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		tblInfo.State = model.StateWriteOnly
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		tblInfo.State = model.StateDeleteOnly
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
	case model.StateDeleteOnly:
		tblInfo.State = model.StateNone
		job.SchemaState = model.StateNone
		ver, err = updateTableInfo(t, job, tblInfo, originalState)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if err = t.DropTable(job.SchemaID, job.TableID, true); err != nil {
			break
		}
		// Finish this job.
		job.State = model.JobDone
		job.BinlogInfo.AddTableInfo(ver, tblInfo)
		startKey := tablecodec.EncodeTablePrefix(tableID)
		job.Args = append(job.Args, startKey)
		d.asyncNotifyEvent(&Event{Tp: model.ActionDropTable, TableInfo: tblInfo})
	default:
		err = ErrInvalidTableState.Gen("invalid table state %v", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

// Maximum number of keys to delete for each reorg table job run.
var reorgTableDeleteLimit = 65536

func (d *ddl) delReorgTable(t *meta.Meta, job *model.Job) error {
	var startKey kv.Key
	if err := job.DecodeArgs(&startKey); err != nil {
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	limit := reorgTableDeleteLimit
	delCount, err := d.dropTableData(startKey, job, limit)
	if err != nil {
		return errors.Trace(err)
	}
	// Finish this background job.
	if delCount < limit {
		job.SchemaState = model.StateNone
		job.State = model.JobDone
	}
	return nil
}

func (d *ddl) getTable(schemaID int64, tblInfo *model.TableInfo) (table.Table, error) {
	if tblInfo.OldSchemaID != 0 {
		schemaID = tblInfo.OldSchemaID
	}
	alloc := autoid.NewAllocator(d.store, schemaID)
	tbl, err := table.TableFromMeta(alloc, tblInfo)
	return tbl, errors.Trace(err)
}

func getTableInfo(t *meta.Meta, job *model.Job, schemaID int64) (*model.TableInfo, error) {
	tableID := job.TableID
	tblInfo, err := t.GetTable(schemaID, tableID)
	if err != nil {
		if terror.ErrorEqual(err, meta.ErrDBNotExists) {
			job.State = model.JobCancelled
			return nil, errors.Trace(infoschema.ErrDatabaseNotExists)
		}
		return nil, errors.Trace(err)
	} else if tblInfo == nil {
		job.State = model.JobCancelled
		return nil, errors.Trace(infoschema.ErrTableNotExists)
	}

	if tblInfo.State != model.StatePublic {
		job.State = model.JobCancelled
		return nil, ErrInvalidTableState.Gen("table %s is not in public, but %s", tblInfo.Name, tblInfo.State)
	}

	return tblInfo, nil
}

// dropTableData deletes data in a limited number. If limit < 0, deletes all data.
func (d *ddl) dropTableData(startKey kv.Key, job *model.Job, limit int) (int, error) {
	prefix := tablecodec.EncodeTablePrefix(job.TableID)
	delCount, nextStartKey, err := d.delKeysWithStartKey(prefix, startKey, bgJobFlag, job, limit)
	job.Args = []interface{}{nextStartKey}
	return delCount, errors.Trace(err)
}

// onTruncateTable delete old table meta, and creates a new table identical to old table except for table ID.
// As all the old data is encoded with old table ID, it can not be accessed any more.
// A background job will be created to delete old data.
func (d *ddl) onTruncateTable(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tableID := job.TableID
	var newTableID int64
	err := job.DecodeArgs(&newTableID)
	if err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = t.DropTable(schemaID, tableID, true)
	if err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}
	tblInfo.ID = newTableID
	err = t.CreateTable(schemaID, tblInfo)
	if err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.State = model.JobDone
	job.BinlogInfo.AddTableInfo(ver, tblInfo)
	startKey := tablecodec.EncodeTablePrefix(tableID)
	job.Args = []interface{}{startKey}
	return ver, nil
}

func (d *ddl) onRenameTable(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var oldSchemaID int64
	var tableName model.CIStr
	if err := job.DecodeArgs(&oldSchemaID, &tableName); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfo(t, job, oldSchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	newSchemaID := job.SchemaID
	if newSchemaID != oldSchemaID {
		err = checkTableNotExists(t, job, newSchemaID, tblInfo.Name.L)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if tblInfo.OldSchemaID == 0 {
			tblInfo.OldSchemaID = oldSchemaID
		}
	}

	err = t.DropTable(oldSchemaID, tblInfo.ID, false)
	if err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}
	tblInfo.Name = tableName
	err = t.CreateTable(newSchemaID, tblInfo)
	if err != nil {
		job.State = model.JobCancelled
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.State = model.JobDone
	job.SchemaState = model.StatePublic
	job.BinlogInfo.AddTableInfo(ver, tblInfo)
	return ver, nil
}

func checkTableNotExists(t *meta.Meta, job *model.Job, schemaID int64, tableName string) error {
	// Check this table's database.
	tables, err := t.ListTables(schemaID)
	if err != nil {
		if terror.ErrorEqual(err, meta.ErrDBNotExists) {
			job.State = model.JobCancelled
			return infoschema.ErrDatabaseNotExists.GenByArgs("")
		}
		return errors.Trace(err)
	}

	// Check the table.
	for _, tbl := range tables {
		if tbl.Name.L == tableName {
			// This table already exists and can't be created, we should cancel this job now.
			job.State = model.JobCancelled
			return infoschema.ErrTableExists.GenByArgs(tbl.Name)
		}
	}

	return nil
}

func updateTableInfo(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, originalState model.SchemaState) (
	ver int64, err error) {
	if originalState != job.SchemaState {
		ver, err = updateSchemaVersion(t, job)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	return ver, t.UpdateTable(job.SchemaID, tblInfo)
}
