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
	"github.com/pingcap/tidb/model"
)

func (d *ddl) onSchemaCreate(t *meta.TMeta, job *model.Job) error {
	schemaID := job.SchemaID

	name := model.CIStr{}
	if err := job.DecodeArgs(&name); err != nil {
		return errors.Trace(err)
	}

	var dbInfo *model.DBInfo
	dbs, err := t.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}

	for _, db := range dbs {
		if db.Name.L == name.L {
			if db.ID != schemaID {
				// database exists, can't create, we can cancel this job now.
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

	if dbInfo == nil {
		// first create, enter delete only state
		dbInfo = &model.DBInfo{
			ID:    schemaID,
			Name:  name,
			State: model.StateDeleteOnly,
		}

		err = t.CreateDatabase(dbInfo)
		return errors.Trace(err)
	}

	if dbInfo.State == model.StateDeleteOnly {
		// delete only -> write only
		dbInfo.State = model.StateWriteOnly
		err = t.UpdateDatabase(dbInfo)
		return errors.Trace(err)
	}

	if dbInfo.State == model.StateWriteOnly {
		// write only -> public
		dbInfo.State = model.StatePublic
		err = t.UpdateDatabase(dbInfo)
		if err != nil {
			return errors.Trace(err)
		}

		// finish this job.
		job.State = model.JobDone
		return nil
	}

	// we can't enter here.
	return errors.Errorf("invalid db state %v", dbInfo.State)
}
