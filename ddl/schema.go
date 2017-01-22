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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/tablecodec"
)

func (d *ddl) onCreateSchema(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	dbInfo := &model.DBInfo{}
	if err := job.DecodeArgs(dbInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	dbInfo.ID = schemaID
	dbInfo.State = model.StateNone

	dbs, err := t.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}

	for _, db := range dbs {
		if db.Name.L == dbInfo.Name.L {
			if db.ID != schemaID {
				// The database already exists, can't create it, we should cancel this job now.
				job.State = model.JobCancelled
				return infoschema.ErrDatabaseExists.GenByArgs(db.Name)
			}
			dbInfo = db
		}
	}

	ver, err := updateSchemaVersion(t, job)
	if err != nil {
		return errors.Trace(err)
	}

	switch dbInfo.State {
	case model.StateNone:
		// none -> public
		job.SchemaState = model.StatePublic
		dbInfo.State = model.StatePublic
		err = t.CreateDatabase(dbInfo)
		if err != nil {
			return errors.Trace(err)
		}
		// Finish this job.
		job.State = model.JobDone
		job.BinlogInfo.AddDBInfo(ver, dbInfo)
		return nil
	default:
		// We can't enter here.
		return errors.Errorf("invalid db state %v", dbInfo.State)
	}
}

func (d *ddl) onDropSchema(t *meta.Meta, job *model.Job) error {
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if dbInfo == nil {
		job.State = model.JobCancelled
		return infoschema.ErrDatabaseDropExists.GenByArgs("")
	}

	ver, err := updateSchemaVersion(t, job)
	if err != nil {
		return errors.Trace(err)
	}

	switch dbInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		dbInfo.State = model.StateWriteOnly
		err = t.UpdateDatabase(dbInfo)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		dbInfo.State = model.StateDeleteOnly
		err = t.UpdateDatabase(dbInfo)
	case model.StateDeleteOnly:
		dbInfo.State = model.StateNone
		tables, err := t.ListTables(job.SchemaID)
		if err != nil {
			return errors.Trace(err)
		}

		err = t.UpdateDatabase(dbInfo)
		if err = t.DropDatabase(dbInfo.ID); err != nil {
			break
		}

		// Finish this job.
		job.BinlogInfo.AddDBInfo(ver, dbInfo)
		if len(tables) > 0 {
			job.Args = append(job.Args, getIDs(tables))
		}
		job.State = model.JobDone
		job.SchemaState = model.StateNone
	default:
		// We can't enter here.
		err = errors.Errorf("invalid db state %v", dbInfo.State)
	}

	return errors.Trace(err)
}

func getIDs(tables []*model.TableInfo) []int64 {
	ids := make([]int64, 0, len(tables))
	for _, t := range tables {
		ids = append(ids, t.ID)
	}

	return ids
}

func (d *ddl) delReorgSchema(t *meta.Meta, job *model.Job) error {
	var startKey kv.Key
	var tableIDs []int64
	if err := job.DecodeArgs(&tableIDs, &startKey); err != nil {
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	isFinished, err := d.dropSchemaData(tableIDs, startKey, job, t)
	if err != nil {
		return errors.Trace(err)
	}
	if !isFinished {
		return nil
	}

	// Finish this background job.
	job.SchemaState = model.StateNone
	job.State = model.JobDone

	return nil
}

func (d *ddl) dropSchemaData(tIDs []int64, startKey kv.Key, job *model.Job, m *meta.Meta) (bool, error) {
	if len(tIDs) == 0 {
		return true, nil
	}

	isFinished := false
	var nextStartKey kv.Key
	for i, id := range tIDs {
		job.TableID = id
		if startKey == nil {
			startKey = tablecodec.EncodeTablePrefix(id)
		}
		delCount, err := d.dropTableData(startKey, job, defaultBatchCnt)
		if err != nil {
			return false, errors.Trace(err)
		}

		if delCount == defaultBatchCnt {
			isFinished = false
			nextStartKey = job.Args[len(job.Args)-1].(kv.Key)
			break
		}

		if i < len(tIDs)-1 {
			tIDs = tIDs[i+1:]
		} else {
			tIDs = nil
		}
		startKey = nil
		isFinished = true
	}
	job.TableID = 0
	job.Args = []interface{}{tIDs, nextStartKey}

	return isFinished, nil
}
