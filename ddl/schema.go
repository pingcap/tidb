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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/label"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
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

	ver, err = updateSchemaVersion(d, t, job)
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
	// Try to use memory schema info to check first.
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := d.infoCache.GetLatest()
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

func onModifySchemaCharsetAndCollate(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
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
	if ver, err = updateSchemaVersion(d, t, job); err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
	return ver, nil
}

func onModifySchemaDefaultPlacement(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var placementPolicyRef *model.PolicyRefInfo
	if err := job.DecodeArgs(&placementPolicyRef); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Double Check if policy exits while ddl executing
	if _, err = checkPlacementPolicyRefValidAndCanNonValidJob(t, job, placementPolicyRef); err != nil {
		return ver, errors.Trace(err)
	}

	// Notice: dbInfo.DirectPlacementOpts and dbInfo.PlacementPolicyRef can not be both not nil, which checked before constructing ddl job.
	// So that we can just check the two situation that do not need ddl: 1. DB.DP == DDL.DP && nil == nil 2. nil == nil && DB.PP == DDL.PP
	if placementPolicyRef != nil && dbInfo.PlacementPolicyRef != nil && *dbInfo.PlacementPolicyRef == *placementPolicyRef {
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
		return ver, nil
	}

	// If placementPolicyRef and directPlacementOpts are both nil, And placement of dbInfo is not nil, it will remove all placement options.
	dbInfo.PlacementPolicyRef = placementPolicyRef

	if err = t.UpdateDatabase(dbInfo); err != nil {
		return ver, errors.Trace(err)
	}
	if ver, err = updateSchemaVersion(d, t, job); err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
	return ver, nil
}

func onDropSchema(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(d, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	switch dbInfo.State {
	case model.StatePublic:
		// public -> write only
		dbInfo.State = model.StateWriteOnly
		err = t.UpdateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		var tables []*model.TableInfo
		tables, err = t.ListTables(job.SchemaID)
		if err != nil {
			return ver, errors.Trace(err)
		}
		var ruleIDs []string
		for _, tblInfo := range tables {
			rules := append(getPartitionRuleIDs(job.SchemaName, tblInfo), fmt.Sprintf(label.TableIDFormat, label.IDPrefix, job.SchemaName, tblInfo.Name.L))
			ruleIDs = append(ruleIDs, rules...)
		}
		patch := label.NewRulePatch([]*label.Rule{}, ruleIDs)
		err = infosync.UpdateLabelRules(context.TODO(), patch)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		dbInfo.State = model.StateDeleteOnly
		err = t.UpdateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
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
		return ver, errors.Trace(errors.Errorf("invalid db state %v", dbInfo.State))
	}
	job.SchemaState = dbInfo.State
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
