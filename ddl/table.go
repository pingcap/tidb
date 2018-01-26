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
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	log "github.com/sirupsen/logrus"
)

func (d *ddl) onCreateTable(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tbInfo := &model.TableInfo{}
	if err := job.DecodeArgs(tbInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
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
		tbInfo.State = model.StatePublic
		tbInfo.UpdateTS = t.StartTS
		err = t.CreateTable(schemaID, tbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if EnableSplitTableRegion {
			err = d.splitTableRegion(tbInfo.ID)
			// It will be automatically splitting by TiKV later.
			if err != nil {
				log.Warnf("[ddl] split table region failed %v", err)
			}
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		d.asyncNotifyEvent(&util.Event{Tp: model.ActionCreateTable, TableInfo: tbInfo})
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
		if meta.ErrDBNotExists.Equal(err) {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(infoschema.ErrDatabaseNotExists.GenByArgs(
				fmt.Sprintf("(Schema ID %d)", schemaID),
			))
		}
		return ver, errors.Trace(err)
	}

	// Check the table.
	if tblInfo == nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(infoschema.ErrTableNotExists.GenByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
			fmt.Sprintf("(Table ID %d)", tableID),
		))
	}

	originalState := job.SchemaState
	switch tblInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		tblInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != tblInfo.State)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		tblInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != tblInfo.State)
	case model.StateDeleteOnly:
		tblInfo.State = model.StateNone
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != tblInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if err = t.DropTable(job.SchemaID, tableID, true); err != nil {
			break
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		startKey := tablecodec.EncodeTablePrefix(tableID)
		job.Args = append(job.Args, startKey)
	default:
		err = ErrInvalidTableState.Gen("invalid table state %v", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

func (d *ddl) splitTableRegion(tableID int64) error {
	type splitableStore interface {
		SplitRegion(splitKey kv.Key) error
	}
	store, ok := d.store.(splitableStore)
	if !ok {
		return nil
	}
	tableStartKey := tablecodec.GenTablePrefix(tableID)
	if err := store.SplitRegion(tableStartKey); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (d *ddl) getTable(schemaID int64, tblInfo *model.TableInfo) (table.Table, error) {
	alloc := autoid.NewAllocator(d.store, tblInfo.GetDBID(schemaID))
	tbl, err := table.TableFromMeta(alloc, tblInfo)
	return tbl, errors.Trace(err)
}

func getTableInfo(t *meta.Meta, job *model.Job, schemaID int64) (*model.TableInfo, error) {
	tableID := job.TableID
	tblInfo, err := t.GetTable(schemaID, tableID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			job.State = model.JobStateCancelled
			return nil, errors.Trace(infoschema.ErrDatabaseNotExists.GenByArgs(
				fmt.Sprintf("(Schema ID %d)", schemaID),
			))
		}
		return nil, errors.Trace(err)
	} else if tblInfo == nil {
		job.State = model.JobStateCancelled
		return nil, errors.Trace(infoschema.ErrTableNotExists.GenByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
			fmt.Sprintf("(Table ID %d)", tableID),
		))
	}

	if tblInfo.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return nil, ErrInvalidTableState.Gen("table %s is not in public, but %s", tblInfo.Name, tblInfo.State)
	}

	return tblInfo, nil
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
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = t.DropTable(schemaID, tblInfo.ID, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo.ID = newTableID
	err = t.CreateTable(schemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	startKey := tablecodec.EncodeTablePrefix(tableID)
	job.Args = []interface{}{startKey}
	return ver, nil
}

func (d *ddl) onRebaseAutoID(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	var newBase int64
	err := job.DecodeArgs(&newBase)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo.AutoIncID = newBase
	tbl, err := d.getTable(schemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// The operation of the minus 1 to make sure that the current value doesn't be used,
	// the next Alloc operation will get this value.
	// Its behavior is consistent with MySQL.
	err = tbl.RebaseAutoID(nil, tblInfo.AutoIncID-1, false)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (d *ddl) onShardRowID(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var shardRowIDBits uint64
	err := job.DecodeArgs(&shardRowIDBits)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo.ShardRowIDBits = shardRowIDBits
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (d *ddl) onRenameTable(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var oldSchemaID int64
	var tableName model.CIStr
	if err := job.DecodeArgs(&oldSchemaID, &tableName); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfo(t, job, oldSchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	var baseID int64
	shouldDelAutoID := false
	newSchemaID := job.SchemaID
	if newSchemaID != oldSchemaID {
		shouldDelAutoID = true
		err = checkTableNotExists(t, job, newSchemaID, tblInfo.Name.L)
		if err != nil {
			return ver, errors.Trace(err)
		}
		baseID, err = t.GetAutoTableID(tblInfo.GetDBID(oldSchemaID), tblInfo.ID)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// It's compatible with old version.
		// TODO: Remove it.
		tblInfo.OldSchemaID = 0
	}

	err = t.DropTable(oldSchemaID, tblInfo.ID, shouldDelAutoID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo.Name = tableName
	err = t.CreateTable(newSchemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// Update the table's auto-increment ID.
	if newSchemaID != oldSchemaID {
		_, err = t.GenAutoTableID(newSchemaID, tblInfo.ID, baseID)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func checkTableNotExists(t *meta.Meta, job *model.Job, schemaID int64, tableName string) error {
	// Check this table's database.
	tables, err := t.ListTables(schemaID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			job.State = model.JobStateCancelled
			return infoschema.ErrDatabaseNotExists.GenByArgs("")
		}
		return errors.Trace(err)
	}

	// Check the table.
	for _, tbl := range tables {
		if tbl.Name.L == tableName {
			// This table already exists and can't be created, we should cancel this job now.
			job.State = model.JobStateCancelled
			return infoschema.ErrTableExists.GenByArgs(tbl.Name)
		}
	}

	return nil
}

// updateVersionAndTableInfo updates the schema version and the table information.
func updateVersionAndTableInfo(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, shouldUpdateVer bool) (
	ver int64, err error) {
	if shouldUpdateVer {
		ver, err = updateSchemaVersion(t, job)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	if tblInfo.State == model.StatePublic {
		tblInfo.UpdateTS = t.StartTS
	}
	return ver, t.UpdateTable(job.SchemaID, tblInfo)
}
