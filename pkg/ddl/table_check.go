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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// checking using cached info schema should be enough, as:
//   - we will reload schema until success when become the owner
//   - existing tables are correctly checked in the first place
//   - we calculate job dependencies before running jobs, so there will not be 2
//     jobs creating same table running concurrently.
//
// if there are 2 owners A and B, we have 2 consecutive jobs J1 and J2 which
// are creating the same table T. those 2 jobs might be running concurrently when
// A sees J1 first and B sees J2 first. But for B sees J2 first, J1 must already
// be done and synced, and been deleted from tidb_ddl_job table, as we are querying
// jobs in the order of job id. During syncing J1, B should have synced the schema
// with the latest schema version, so when B runs J2, below check will see the table
// T already exists, and J2 will fail.
func checkTableNotExists(infoCache *infoschema.InfoCache, schemaID int64, tableName string) error {
	is := infoCache.GetLatest()
	return checkTableNotExistsFromInfoSchema(is, schemaID, tableName)
}

func checkConstraintNamesNotExists(t *meta.Mutator, schemaID int64, constraints []*model.ConstraintInfo) error {
	if len(constraints) == 0 {
		return nil
	}
	tbInfos, err := t.ListTables(context.Background(), schemaID)
	if err != nil {
		return err
	}

	for _, tb := range tbInfos {
		for _, constraint := range constraints {
			if constraint.State != model.StateWriteOnly {
				if constraintInfo := tb.FindConstraintInfoByName(constraint.Name.L); constraintInfo != nil {
					return infoschema.ErrCheckConstraintDupName.GenWithStackByArgs(constraint.Name.L)
				}
			}
		}
	}

	return nil
}

func checkTableIDNotExists(t *meta.Mutator, schemaID, tableID int64) error {
	tbl, err := t.GetTable(schemaID, tableID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
		}
		return errors.Trace(err)
	}
	if tbl != nil {
		return infoschema.ErrTableExists.GenWithStackByArgs(tbl.Name)
	}
	return nil
}

func checkTableNotExistsFromInfoSchema(is infoschema.InfoSchema, schemaID int64, tableName string) error {
	// Check this table's database.
	schema, ok := is.SchemaByID(schemaID)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
	}
	if is.TableExists(schema.Name, ast.NewCIStr(tableName)) {
		return infoschema.ErrTableExists.GenWithStackByArgs(tableName)
	}
	return nil
}

// updateVersionAndTableInfoWithCheck checks table info validate and updates the schema version and the table information
func updateVersionAndTableInfoWithCheck(jobCtx *jobContext, job *model.Job, tblInfo *model.TableInfo, shouldUpdateVer bool, multiInfos ...schemaIDAndTableInfo) (
	ver int64, err error) {
	err = checkTableInfoValid(tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	for _, info := range multiInfos {
		err = checkTableInfoValid(info.tblInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}
	return updateVersionAndTableInfo(jobCtx, job, tblInfo, shouldUpdateVer, multiInfos...)
}

// updateVersionAndTableInfo updates the schema version and the table information.
func updateVersionAndTableInfo(jobCtx *jobContext, job *model.Job, tblInfo *model.TableInfo, shouldUpdateVer bool, multiInfos ...schemaIDAndTableInfo) (
	ver int64, err error) {
	failpoint.Inject("mockUpdateVersionAndTableInfoErr", func(val failpoint.Value) {
		switch val.(int) {
		case 1:
			failpoint.Return(ver, errors.New("mock update version and tableInfo error"))
		case 2:
			// We change it cancelled directly here, because we want to get the original error with the job id appended.
			// The job ID will be used to get the job from history queue and we will assert it's args.
			job.State = model.JobStateCancelled
			failpoint.Return(ver, errors.New("mock update version and tableInfo error, jobID="+strconv.Itoa(int(job.ID))))
		default:
		}
	})
	if shouldUpdateVer && (job.MultiSchemaInfo == nil || !job.MultiSchemaInfo.SkipVersion) {
		ver, err = updateSchemaVersion(jobCtx, job, multiInfos...)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	needUpdateTs := tblInfo.State == model.StatePublic &&
		job.Type != model.ActionTruncateTable &&
		job.Type != model.ActionTruncateTablePartition &&
		job.Type != model.ActionRenameTable &&
		job.Type != model.ActionRenameTables &&
		job.Type != model.ActionExchangeTablePartition

	err = updateTable(jobCtx.metaMut, job.SchemaID, tblInfo, needUpdateTs)
	if err != nil {
		return 0, errors.Trace(err)
	}
	for _, info := range multiInfos {
		err = updateTable(jobCtx.metaMut, info.schemaID, info.tblInfo, needUpdateTs)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
	return ver, nil
}

func updateTable(t *meta.Mutator, schemaID int64, tblInfo *model.TableInfo, needUpdateTs bool) error {
	if needUpdateTs {
		tblInfo.UpdateTS = t.StartTS
	}
	return t.UpdateTable(schemaID, tblInfo)
}

type schemaIDAndTableInfo struct {
	schemaID int64
	tblInfo  *model.TableInfo
}

func onRepairTable(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	metaMut := jobCtx.metaMut
	args, err := model.GetRepairTableArgs(job)
	if err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo := args.TableInfo
	tblInfo.State = model.StateNone

	// Check the old DB and old table exist.
	_, err = GetTableInfoAndCancelFaultJob(metaMut, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// When in repair mode, the repaired table in a server is not access to user,
	// the table after repairing will be removed from repair list. Other server left
	// behind alive may need to restart to get the latest schema version.
	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	switch tblInfo.State {
	case model.StateNone:
		// none -> public
		tblInfo.State = model.StatePublic
		tblInfo.UpdateTS = metaMut.StartTS
		err = repairTableOrViewWithCheck(metaMut, job, schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return ver, nil
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("table", tblInfo.State)
	}
}


