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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
)

func onCreateSchema(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	dbInfo := &model.DBInfo{}
	if err := job.DecodeArgs(dbInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbInfo.ID = schemaID
	dbInfo.State = model.StateNone

	err := checkSchemaNotExists(d, t, schemaID, dbInfo)
	if err != nil {
		if infoschema.ErrDatabaseExists.Equal(err) {
			// The database already exists, can't create it, we should cancel this job now.
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
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

func checkSchemaNotExists(d *ddlCtx, t *meta.Meta, schemaID int64, dbInfo *model.DBInfo) error {
	// d.infoHandle maybe nil in some test.
	if d.infoHandle == nil {
		return checkSchemaNotExistsFromStore(t, schemaID, dbInfo)
	}
	// Try to use memory schema info to check first.
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := d.infoHandle.Get()
	if is.SchemaMetaVersion() == currVer {
		return checkSchemaNotExistsFromInfoSchema(is, schemaID, dbInfo)
	}
	return checkSchemaNotExistsFromStore(t, schemaID, dbInfo)
}

func checkSchemaNotExistsFromInfoSchema(is infoschema.InfoSchema, schemaID int64, dbInfo *model.DBInfo) error {
	// Check database exists by name.
	if is.SchemaExists(dbInfo.Name) {
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	// Check database exists by ID.
	if _, ok := is.SchemaByID(schemaID); ok {
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	return nil
}

func checkSchemaNotExistsFromStore(t *meta.Meta, schemaID int64, dbInfo *model.DBInfo) error {
	dbs, err := t.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}

	for _, db := range dbs {
		if db.Name.L == dbInfo.Name.L {
			if db.ID != schemaID {
				return infoschema.ErrDatabaseExists.GenWithStackByArgs(db.Name)
			}
			dbInfo = db
		}
	}
	return nil
}

func onModifySchemaCharsetAndCollate(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var toCharset, toCollate string
	if err := job.DecodeArgs(&toCharset, &toCollate); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if dbInfo.Charset == toCharset && dbInfo.Collate == toCollate {
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
		return ver, nil
	}

	dbInfo.Charset = toCharset
	dbInfo.Collate = toCollate

	if err = t.UpdateDatabase(dbInfo); err != nil {
		return ver, errors.Trace(err)
	}
	if ver, err = updateSchemaVersion(t, job); err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
	return ver, nil
}

func onDropSchema(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return ver, errors.Trace(err)
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

func checkSchemaExistAndCancelNotExistJob(t *meta.Meta, job *model.Job) (*model.DBInfo, error) {
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if dbInfo == nil {
		job.State = model.JobStateCancelled
		return nil, infoschema.ErrDatabaseDropExists.GenWithStackByArgs("")
	}
	return dbInfo, nil
}

func getIDs(tables []*model.TableInfo) []int64 {
	ids := make([]int64, 0, len(tables))
	for _, t := range tables {
		ids = append(ids, t.ID)
		if t.GetPartitionInfo() != nil {
			ids = append(ids, getPartitionIDs(t)...)
		}
	}

	return ids
}
