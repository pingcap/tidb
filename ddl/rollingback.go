// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/schemautil"
	"go.uber.org/zap"
)

func convertAddIdxJob2RollbackJob(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, err error) (int64, error) {
	job.State = model.JobStateRollingback
	// the second args will be used in onDropIndex.
	job.Args = []interface{}{indexInfo.Name, getPartitionIDs(tblInfo)}
	// If add index job rollbacks in write reorganization state, its need to delete all keys which has been added.
	// Its work is the same as drop index job do.
	// The write reorganization state in add index job that likes write only state in drop index job.
	// So the next state is delete only state.
	originalState := indexInfo.State
	indexInfo.State = model.StateDeleteOnly
	job.SchemaState = model.StateDeleteOnly
	ver, err1 := updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	if err1 != nil {
		return ver, errors.Trace(err1)
	}

	if kv.ErrKeyExists.Equal(err) {
		return ver, kv.ErrKeyExists.GenWithStack("Duplicate for key %s", indexInfo.Name.O)
	}

	return ver, errors.Trace(err)
}

// convertNotStartAddIdxJob2RollbackJob converts the add index job that are not started workers to rollingbackJob,
// to rollback add index operations. job.SnapshotVer == 0 indicates the workers are not started.
func convertNotStartAddIdxJob2RollbackJob(t *meta.Meta, job *model.Job, occuredErr error) (ver int64, err error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var (
		unique      bool
		indexName   model.CIStr
		idxColNames []*ast.IndexColName
		indexOption *ast.IndexOption
	)
	err = job.DecodeArgs(&unique, &indexName, &idxColNames, &indexOption)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := schemautil.FindIndexByName(indexName.L, tblInfo.Indices)
	if indexInfo == nil {
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}
	return convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, occuredErr)
}

func cancelOnlyNotHandledJob(job *model.Job) (ver int64, err error) {
	// We can only cancel the not handled job.
	if job.SchemaState == model.StateNone {
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}

	job.State = model.JobStateRunning

	return ver, nil
}

func rollingbackAddColumn(t *meta.Meta, job *model.Job) (ver int64, err error) {
	job.State = model.JobStateRollingback
	col := &model.ColumnInfo{}
	pos := &ast.ColumnPosition{}
	offset := 0
	err = job.DecodeArgs(col, pos, &offset)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	columnInfo := model.FindColumnInfo(tblInfo.Columns, col.Name.L)
	if columnInfo == nil {
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}

	if columnInfo.State == model.StatePublic {
		// We already have a column with the same column name.
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrColumnExists.GenWithStackByArgs(col.Name)
	}

	originalState := columnInfo.State
	columnInfo.State = model.StateDeleteOnly
	job.SchemaState = model.StateDeleteOnly

	job.Args = []interface{}{col.Name}
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return ver, errCancelledDDLJob
}

func rollingbackDropIndex(t *meta.Meta, job *model.Job) (ver int64, err error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var indexName model.CIStr
	err = job.DecodeArgs(&indexName)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := schemautil.FindIndexByName(indexName.L, tblInfo.Indices)
	if indexInfo == nil {
		job.State = model.JobStateCancelled
		return ver, ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StateDeleteOnly, model.StateDeleteReorganization, model.StateNone:
		// We can not rollback now, so just continue to drop index.
		// Normally won't fetch here, because there is check when cancel ddl jobs. see function: isJobRollbackable.
		job.State = model.JobStateRunning
		return ver, nil
	case model.StatePublic, model.StateWriteOnly:
		job.State = model.JobStateRollbackDone
		indexInfo.State = model.StatePublic
	default:
		return ver, ErrInvalidIndexState.GenWithStack("invalid index state %v", indexInfo.State)
	}

	job.SchemaState = indexInfo.State
	job.Args = []interface{}{indexInfo.Name}
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StatePublic, ver, tblInfo)
	return ver, errCancelledDDLJob
}

func rollingbackAddindex(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	// If the value of SnapshotVer isn't zero, it means the work is backfilling the indexes.
	if job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0 {
		// add index workers are started. need to ask them to exit.
		logutil.Logger(w.logCtx).Info("[ddl] run the cancelling DDL job", zap.String("job", job.String()))
		w.reorgCtx.notifyReorgCancel()
		ver, err = w.onCreateIndex(d, t, job)
	} else {
		// add index workers are not started, remove the indexInfo in tableInfo.
		ver, err = convertNotStartAddIdxJob2RollbackJob(t, job, errCancelledDDLJob)
	}
	return
}

func rollingbackAddTablePartition(t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, err = getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return cancelOnlyNotHandledJob(job)
}

func rollingbackDropColumn(t *meta.Meta, job *model.Job) (ver int64, err error) {
	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var colName model.CIStr
	err = job.DecodeArgs(&colName)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	colInfo := model.FindColumnInfo(tblInfo.Columns, colName.L)
	if colInfo == nil {
		job.State = model.JobStateCancelled
		return ver, ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
	}

	// StatePublic means when the job is not running yet.
	if colInfo.State == model.StatePublic {
		job.State = model.JobStateCancelled
	} else {
		// In the state of drop column `write only -> delete only -> reorganization`,
		// We can not rollback now, so just continue to drop column.
		job.State = model.JobStateRunning
		return ver, errors.Trace(nil)
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StatePublic, ver, tblInfo)
	return ver, errors.Trace(errCancelledDDLJob)
}

func rollingbackDropTablePartition(t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, err = getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return cancelOnlyNotHandledJob(job)
}

func rollingbackRenameIndex(job *model.Job) (ver int64, err error) {
	return cancelOnlyNotHandledJob(job)
}

func rollingbackTruncateTable(t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, err = getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return cancelOnlyNotHandledJob(job)
}

func convertJob2RollbackJob(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	switch job.Type {
	case model.ActionAddColumn:
		ver, err = rollingbackAddColumn(t, job)
	case model.ActionAddIndex:
		ver, err = rollingbackAddindex(w, d, t, job)
	case model.ActionAddTablePartition:
		ver, err = rollingbackAddTablePartition(t, job)
	case model.ActionDropColumn:
		ver, err = rollingbackDropColumn(t, job)
	case model.ActionDropTablePartition:
		ver, err = rollingbackDropTablePartition(t, job)
	case model.ActionRenameIndex:
		ver, err = rollingbackRenameIndex(job)
	case model.ActionTruncateTable:
		ver, err = rollingbackTruncateTable(t, job)
	case model.ActionDropIndex:
		ver, err = rollingbackDropIndex(t, job)
	case model.ActionRebaseAutoID, model.ActionShardRowID,
		model.ActionModifyColumn, model.ActionAddForeignKey,
		model.ActionDropForeignKey, model.ActionRenameTable,
		model.ActionModifyTableCharsetAndCollate, model.ActionTruncateTablePartition,
<<<<<<< HEAD
		model.ActionModifySchemaCharsetAndCollate:
=======
		model.ActionModifySchemaCharsetAndCollate, model.ActionRepairTable, model.ActionModifyTableAutoIdCache:
>>>>>>> 1c73dec... ddl: add syntax for setting the cache step of auto id explicitly. (#15409)
		ver, err = cancelOnlyNotHandledJob(job)
	case model.ActionDropTable, model.ActionDropSchema:
		job.State = model.JobStateRollingback
	default:
		job.State = model.JobStateCancelled
		err = errCancelledDDLJob
	}

	if err != nil {
		if job.State != model.JobStateRollingback && job.State != model.JobStateCancelled {
			logutil.Logger(w.logCtx).Error("[ddl] run DDL job failed", zap.String("job", job.String()), zap.Error(err))
		} else {
			logutil.Logger(w.logCtx).Info("[ddl] the DDL job is cancelled normally", zap.String("job", job.String()), zap.Error(err))
		}

		job.Error = toTError(err)
		job.ErrorCount++
	}
	return
}
