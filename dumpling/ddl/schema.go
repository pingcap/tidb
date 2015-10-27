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
	"github.com/reborndb/go/errors2"
)

func (d *ddl) onSchemaCreate(t *meta.TMeta, job *model.Job) error {
	schemaID := job.SchemaID

	name := model.CIStr{}
	if err := job.DecodeArgs(&name); err != nil {
		// arg error, cancel this job.
		job.State = model.JobCancelled
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

	switch dbInfo.State {
	case model.StateDeleteOnly:
		// delete only -> write only
		dbInfo.State = model.StateWriteOnly
		err = t.UpdateDatabase(dbInfo)
		return errors.Trace(err)
	case model.StateWriteOnly:
		// write only -> public
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

func (d *ddl) onSchemaDrop(t *meta.TMeta, job *model.Job) error {
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return errors.Trace(err)
	} else if dbInfo == nil {
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
		dbInfo.State = model.StateWriteOnly
		err = t.UpdateDatabase(dbInfo)
		return errors.Trace(err)
	case model.StateWriteOnly:
		// write only -> delete only
		dbInfo.State = model.StateDeleteOnly
		err = t.UpdateDatabase(dbInfo)
		return errors.Trace(err)
	case model.StateDeleteOnly:
		// delete only -> re orgnization
		dbInfo.State = model.StateReOrgnization
		err = t.UpdateDatabase(dbInfo)
		return errors.Trace(err)
	case model.StateReOrgnization:
		// wait re-orgnization jobs done and drop meta.
		var tables []*model.TableInfo
		tables, err = t.ListTables(dbInfo.ID)
		if err != nil {
			return errors.Trace(err)
		}

		err = d.runReOrgJob(func() error {
			return d.dropSchemaData(dbInfo, tables)
		})

		if errors2.ErrorEqual(err, errWaitReOrgTimeout) {
			// if timeout, we will return, check the owner and retry wait job done again.
			return nil
		} else if err != nil {
			return errors.Trace(err)
		}

		// all re-orgnization jobs done, drop this database
		if err = t.DropDatabase(dbInfo.ID); err != nil {
			return errors.Trace(err)
		}

		// finish this job
		job.State = model.JobDone
		return nil
	default:
		// we can't enter here.
		return errors.Errorf("invalid db state %v", dbInfo.State)
	}
}

func (d *ddl) dropSchemaData(dbInfo *model.DBInfo, tables []*model.TableInfo) error {
	ctx := d.newReOrgContext()
	defer ctx.FinishTxn(true)

	txn, err := ctx.GetTxn(true)

	for _, tblInfo := range tables {

		alloc := autoid.NewAllocator(d.meta, dbInfo.ID)
		t := table.TableFromMeta(dbInfo.Name.L, alloc, tblInfo)

		err = t.Truncate(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		// Remove indices.
		for _, v := range t.Indices() {
			if v != nil && v.X != nil {
				if err = v.X.Drop(txn); err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
	return errors.Trace(ctx.FinishTxn(false))
}
