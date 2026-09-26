// Copyright 2025 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// onAlterTableMode should only be called by alterTableMode, will call updateVersionAndTableInfo
func onAlterTableMode(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetAlterTableModeArgs(job)
	if err != nil {
		return ver, err
	}

	var tbInfo *model.TableInfo
	metaMut := jobCtx.metaMut
	tbInfo, err = GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, err
	}

	// tbInfo is read fresh from the job's own metadata mutator, so this
	// read is atomic with respect to any other concurrent DDL job on the
	// same table (the job scheduler serializes them). Rejecting here, rather
	// than relying only on a caller-side pre-check, closes the race where a
	// concurrent schema change lands after the caller's own check but before
	// this job actually runs.
	if args.ExpectedRevision != nil && tbInfo.Revision != *args.ExpectedRevision {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrInfoSchemaChanged.GenWithStackByArgs()
	}

	switch tbInfo.Mode {
	case model.TableModeNormal, model.TableModeImport, model.TableModeRestore:
		if tbInfo.Mode == args.TableMode {
			job.State = model.JobStateDone
			return ver, err
		}
		// directly change table mode to target mode
		err = alterTableMode(tbInfo, args)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		// update table info and schema version
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
	default:
		job.State = model.JobStateCancelled
		err = infoschema.ErrInvalidTableModeSet.GenWithStackByArgs(tbInfo.Mode, args.TableMode, tbInfo.Name.O)
	}

	return ver, err
}

// alterTableMode first checks if the change is valid and changes table mode to target mode
// Currently we can assume args.TableMode will NEVER be model.TableModeRestore.
// Because BR will NOT use this function to set a table into ModeRestore,
// instead BR will use (batch)CreateTableWithInfo.
func alterTableMode(tbInfo *model.TableInfo, args *model.AlterTableModeArgs) error {
	ok := tbInfo.Mode.CanTransitionTo(args.TableMode)
	if !ok {
		return infoschema.ErrInvalidTableModeSet.GenWithStackByArgs(tbInfo.Mode, args.TableMode, tbInfo.Name.O)
	}

	tbInfo.Mode = args.TableMode
	return nil
}

// AlterTableMode creates a DDL job for alter table mode.
//
// expectedRevision is optional (pass none, or a single value): when given,
// the job is rejected with dbterror.ErrInfoSchemaChanged unless the table's
// Revision, read atomically at job-execution time, still matches. Callers
// that captured a table schema snapshot earlier and only later request the
// mode switch should pass the snapshot's Revision to detect and reject a
// schema change that raced ahead of them.
func AlterTableMode(de Executor, sctx sessionctx.Context, mode model.TableMode, schemaID, tableID int64, expectedRevision ...uint64) error {
	if len(expectedRevision) > 1 {
		return errors.Errorf("AlterTableMode: at most one expectedRevision is allowed, got %d", len(expectedRevision))
	}
	args := &model.AlterTableModeArgs{
		TableMode: mode,
		SchemaID:  schemaID,
		TableID:   tableID,
	}
	if len(expectedRevision) == 1 {
		args.ExpectedRevision = &expectedRevision[0]
	}
	return de.AlterTableMode(sctx, args)
}
