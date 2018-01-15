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
	"github.com/pingcap/tidb/model"
)

func (d *ddl) onCreateSchema(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	dbInfo := &model.DBInfo{}
	if err := job.DecodeArgs(dbInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbInfo.ID = schemaID
	dbInfo.State = model.StateNone

	dbs, err := t.ListDatabases()
	if err != nil {
		return ver, errors.Trace(err)
	}

	for _, db := range dbs {
		if db.Name.L == dbInfo.Name.L {
			if db.ID != schemaID {
				// The database already exists, can't create it, we should cancel this job now.
				job.State = model.JobStateCancelled
				return ver, infoschema.ErrDatabaseExists.GenByArgs(db.Name)
			}
			dbInfo = db
		}
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch dbInfo.State {
	case model.StateNone:
		// none -> public
		dbInfo.State = model.StatePublic
		err = t.CreateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
		return ver, nil
	default:
		// We can't enter here.
		return ver, errors.Errorf("invalid db state %v", dbInfo.State)
	}
}

func (d *ddl) onDropSchema(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if dbInfo == nil {
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrDatabaseDropExists.GenByArgs("")
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
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
		var tables []*model.TableInfo
		tables, err = t.ListTables(job.SchemaID)
		if err != nil {
			return ver, errors.Trace(err)
		}

		err = t.UpdateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if err = t.DropDatabase(dbInfo.ID); err != nil {
			break
		}

		// Finish this job.
		if len(tables) > 0 {
			job.Args = append(job.Args, getIDs(tables))
		}
		job.FinishDBJob(model.JobStateDone, model.StateNone, ver, dbInfo)
	default:
		// We can't enter here.
		err = errors.Errorf("invalid db state %v", dbInfo.State)
	}

	return ver, errors.Trace(err)
}

func getIDs(tables []*model.TableInfo) []int64 {
	ids := make([]int64, 0, len(tables))
	for _, t := range tables {
		ids = append(ids, t.ID)
	}

	return ids
}
