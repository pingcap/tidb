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

func (d *ddl) onCreateSchema(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	var name model.CIStr
	if err := job.DecodeArgs(&name); err != nil {
		// arg error, cancel this job.
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	dbInfo := &model.DBInfo{
		ID:    schemaID,
		Name:  name,
		State: model.StateNone,
	}

	dbs, err := t.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}

	for _, db := range dbs {
		if db.Name.L == name.L {
			if db.ID != schemaID {
				// database exists, can't create, we should cancel this job now.
				job.State = model.JobCancelled
				return errors.Trace(ErrExists)
			}

			dbInfo = db
		}
	}

	_, err = t.GenSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	switch dbInfo.State {
	case model.StateNone:
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		dbInfo.State = model.StateDeleteOnly
		err = t.CreateDatabase(dbInfo)
		return errors.Trace(err)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		dbInfo.State = model.StateWriteOnly
		err = t.UpdateDatabase(dbInfo)
		return errors.Trace(err)
	case model.StateWriteOnly:
		// write only -> public
		job.SchemaState = model.StatePublic
		dbInfo.State = model.StatePublic
		err = t.UpdateDatabase(dbInfo)
		if err != nil {
			return errors.Trace(err)
		}

		// finish this job.
		job.State = model.JobDone
		return nil
	default:
		// we can't enter here.
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
		return errors.Trace(ErrNotExists)
	}

	_, err = t.GenSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	switch dbInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		dbInfo.State = model.StateWriteOnly
		err = t.UpdateDatabase(dbInfo)
		return errors.Trace(err)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		dbInfo.State = model.StateDeleteOnly
		err = t.UpdateDatabase(dbInfo)
		return errors.Trace(err)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		dbInfo.State = model.StateDeleteReorganization
		err = t.UpdateDatabase(dbInfo)
		return errors.Trace(err)
	case model.StateDeleteReorganization:
		// wait reorganization jobs done and drop meta.
		var tables []*model.TableInfo
		tables, err = t.ListTables(dbInfo.ID)
		if err != nil {
			return errors.Trace(err)
		}

		err = d.runReorgJob(func() error {
			return d.dropSchemaData(dbInfo, tables)
		})

		if terror.ErrorEqual(err, errWaitReorgTimeout) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		// all reorganization jobs done, drop this database
		if err = t.DropDatabase(dbInfo.ID); err != nil {
			return errors.Trace(err)
		}

		// finish this job
		job.State = model.JobDone
		job.SchemaState = model.StateNone
		return nil
	default:
		// we can't enter here.
		return errors.Errorf("invalid db state %v", dbInfo.State)
	}
}

func (d *ddl) dropSchemaData(dbInfo *model.DBInfo, tables []*model.TableInfo) error {
	for _, tblInfo := range tables {
		alloc := autoid.NewAllocator(d.store, dbInfo.ID)
		t := table.TableFromMeta(alloc, tblInfo)
		err := d.dropTableData(t)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
