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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func updateColsNull2NotNull(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) error {
	nullCols, err := getNullColInfos(tblInfo, indexInfo)
	if err != nil {
		return errors.Trace(err)
	}

	for _, col := range nullCols {
		col.Flag |= mysql.NotNullFlag
		col.Flag &^= mysql.PreventNullInsertFlag
	}
	return nil
}

func convertAddIdxJob2RollbackJob(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, err error) (int64, error) {
	failpoint.Inject("mockConvertAddIdxJob2RollbackJobError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(0, errors.New("mock convert add index job to rollback job error"))
		}
	})
	if indexInfo.Primary {
		nullCols, err := getNullColInfos(tblInfo, indexInfo)
		if err != nil {
			return 0, errors.Trace(err)
		}
		for _, col := range nullCols {
			// Field PreventNullInsertFlag flag reset.
			col.Flag &^= mysql.PreventNullInsertFlag
		}
	}

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
	job.State = model.JobStateRollingback
	return ver, errors.Trace(err)
}

// convertNotStartAddIdxJob2RollbackJob converts the add index job that are not started workers to rollingbackJob,
// to rollback add index operations. job.SnapshotVer == 0 indicates the workers are not started.
func convertNotStartAddIdxJob2RollbackJob(t *meta.Meta, job *model.Job, occuredErr error) (ver int64, err error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var (
		unique                  bool
		indexName               model.CIStr
		indexPartSpecifications []*ast.IndexPartSpecification
		indexOption             *ast.IndexOption
	)
	err = job.DecodeArgs(&unique, &indexName, &indexPartSpecifications, &indexOption)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo == nil {
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}
	return convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, occuredErr)
}

// rollingbackModifyColumn change the modifying-column job into rolling back state.
// Since modifying column job has two types: normal-type and reorg-type, we should handle it respectively.
// normal-type has only two states:    None -> Public
// reorg-type has five states:         None -> Delete-only -> Write-only -> Write-org -> Public
func rollingbackModifyColumn(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	// If the value of SnapshotVer isn't zero, it means the reorg workers have been started.
	if job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0 {
		// column type change workers are started. we have to ask them to exit.
		logutil.Logger(w.logCtx).Info("[ddl] run the cancelling DDL job", zap.String("job", job.String()))
		w.reorgCtx.notifyReorgCancel()
		// Give the this kind of ddl one more round to run, the errCancelledDDLJob should be fetched from the bottom up.
		return w.onModifyColumn(d, t, job)
	}
	_, tblInfo, oldCol, jp, err := getModifyColumnInfo(t, job)
	if err != nil {
		return ver, err
	}
	if !needChangeColumnData(oldCol, jp.newCol) {
		// Normal-type rolling back
		if job.SchemaState == model.StateNone {
			// When change null to not null, although state is unchanged with none, the oldCol flag's has been changed to preNullInsertFlag.
			// To roll back this kind of normal job, it is necessary to mark the state as JobStateRollingback to restore the old col's flag.
			if jp.modifyColumnTp == mysql.TypeNull && tblInfo.Columns[oldCol.Offset].Flag|mysql.PreventNullInsertFlag != 0 {
				job.State = model.JobStateRollingback
				return ver, errCancelledDDLJob
			}
			// Normal job with stateNone can be cancelled directly.
			job.State = model.JobStateCancelled
			return ver, errCancelledDDLJob
		}
		// StatePublic couldn't be cancelled.
		job.State = model.JobStateRunning
		return ver, nil
	}
	// reorg-type rolling back
	if jp.changingCol == nil {
		// The job hasn't been handled and we cancel it directly.
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}
	// The job has been in its middle state (but the reorg worker hasn't started) and we roll it back here.
	job.State = model.JobStateRollingback
	return ver, errCancelledDDLJob
}

func rollingbackAddColumn(t *meta.Meta, job *model.Job) (ver int64, err error) {
	tblInfo, columnInfo, col, _, _, err := checkAddColumn(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if columnInfo == nil {
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}

	originalState := columnInfo.State
	columnInfo.State = model.StateDeleteOnly
	job.SchemaState = model.StateDeleteOnly

	job.Args = []interface{}{col.Name}
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.State = model.JobStateRollingback
	return ver, errCancelledDDLJob
}

func rollingbackAddColumns(t *meta.Meta, job *model.Job) (ver int64, err error) {
	tblInfo, columnInfos, _, _, _, _, err := checkAddColumns(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if len(columnInfos) == 0 {
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}

	colNames := make([]model.CIStr, len(columnInfos))
	originalState := columnInfos[0].State
	for i, columnInfo := range columnInfos {
		columnInfos[i].State = model.StateDeleteOnly
		colNames[i] = columnInfo.Name
	}
	ifExists := make([]bool, len(columnInfos))

	job.SchemaState = model.StateDeleteOnly
	job.Args = []interface{}{colNames, ifExists}
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfos[0].State)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.State = model.JobStateRollingback
	return ver, errCancelledDDLJob
}

func rollingbackDropColumn(t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, colInfo, idxInfos, err := checkDropColumn(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	for _, indexInfo := range idxInfos {
		switch indexInfo.State {
		case model.StateWriteOnly, model.StateDeleteOnly, model.StateDeleteReorganization, model.StateNone:
			// We can not rollback now, so just continue to drop index.
			// In function isJobRollbackable will let job rollback when state is StateNone.
			// When there is no index related to the drop column job it is OK, but when there has indices, we should
			// make sure the job is not rollback.
			job.State = model.JobStateRunning
			return ver, nil
		case model.StatePublic:
		default:
			return ver, ErrInvalidDDLState.GenWithStackByArgs("index", indexInfo.State)
		}
	}

	// StatePublic means when the job is not running yet.
	if colInfo.State == model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}
	// In the state of drop column `write only -> delete only -> reorganization`,
	// We can not rollback now, so just continue to drop column.
	job.State = model.JobStateRunning
	return ver, nil
}

func rollingbackDropColumns(t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, colInfos, _, idxInfos, err := checkDropColumns(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	for _, indexInfo := range idxInfos {
		switch indexInfo.State {
		case model.StateWriteOnly, model.StateDeleteOnly, model.StateDeleteReorganization, model.StateNone:
			// We can not rollback now, so just continue to drop index.
			// In function isJobRollbackable will let job rollback when state is StateNone.
			// When there is no index related to the drop columns job it is OK, but when there has indices, we should
			// make sure the job is not rollback.
			job.State = model.JobStateRunning
			return ver, nil
		case model.StatePublic:
		default:
			return ver, ErrInvalidDDLState.GenWithStackByArgs("index", indexInfo.State)
		}
	}

	// StatePublic means when the job is not running yet.
	if colInfos[0].State == model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}
	// In the state of drop columns `write only -> delete only -> reorganization`,
	// We can not rollback now, so just continue to drop columns.
	job.State = model.JobStateRunning
	return ver, nil
}

func rollingbackDropIndex(t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, indexInfo, err := checkDropIndex(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch indexInfo.State {
	case model.StateWriteOnly, model.StateDeleteOnly, model.StateDeleteReorganization, model.StateNone:
		// We can not rollback now, so just continue to drop index.
		// Normally won't fetch here, because there is check when cancel ddl jobs. see function: isJobRollbackable.
		job.State = model.JobStateRunning
		return ver, nil
	case model.StatePublic:
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	default:
		return ver, ErrInvalidDDLState.GenWithStackByArgs("index", indexInfo.State)
	}
}

func rollingbackAddIndex(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job, isPK bool) (ver int64, err error) {
	// If the value of SnapshotVer isn't zero, it means the work is backfilling the indexes.
	if job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0 {
		// add index workers are started. need to ask them to exit.
		logutil.Logger(w.logCtx).Info("[ddl] run the cancelling DDL job", zap.String("job", job.String()))
		w.reorgCtx.notifyReorgCancel()
		ver, err = w.onCreateIndex(d, t, job, isPK)
	} else {
		// add index workers are not started, remove the indexInfo in tableInfo.
		ver, err = convertNotStartAddIdxJob2RollbackJob(t, job, errCancelledDDLJob)
	}
	return
}

func convertAddTablePartitionJob2RollbackJob(t *meta.Meta, job *model.Job, otherwiseErr error, tblInfo *model.TableInfo) (ver int64, err error) {
	addingDefinitions := tblInfo.Partition.AddingDefinitions
	partNames := make([]string, 0, len(addingDefinitions))
	for _, pd := range addingDefinitions {
		partNames = append(partNames, pd.Name.L)
	}
	job.Args = []interface{}{partNames}
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.State = model.JobStateRollingback
	return ver, errors.Trace(otherwiseErr)
}

func rollingbackAddTablePartition(t *meta.Meta, job *model.Job) (ver int64, err error) {
	tblInfo, _, addingDefinitions, err := checkAddPartition(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// addingDefinitions' len = 0 means the job hasn't reached the replica-only state.
	if len(addingDefinitions) == 0 {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(errCancelledDDLJob)
	}
	// addingDefinitions is also in tblInfo, here pass the tblInfo as parameter directly.
	return convertAddTablePartitionJob2RollbackJob(t, job, errCancelledDDLJob, tblInfo)
}

func rollingbackDropTableOrView(t *meta.Meta, job *model.Job) error {
	tblInfo, err := checkTableExistAndCancelNonExistJob(t, job, job.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	// To simplify the rollback logic, cannot be canceled after job start to run.
	// Normally won't fetch here, because there is check when cancel ddl jobs. see function: isJobRollbackable.
	if tblInfo.State == model.StatePublic {
		job.State = model.JobStateCancelled
		return errCancelledDDLJob
	}
	job.State = model.JobStateRunning
	return nil
}

func rollingbackDropTablePartition(t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, err = getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return cancelOnlyNotHandledJob(job)
}

func rollingbackDropSchema(t *meta.Meta, job *model.Job) error {
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return errors.Trace(err)
	}
	// To simplify the rollback logic, cannot be canceled after job start to run.
	// Normally won't fetch here, because there is check when cancel ddl jobs. see function: isJobRollbackable.
	if dbInfo.State == model.StatePublic {
		job.State = model.JobStateCancelled
		return errCancelledDDLJob
	}
	job.State = model.JobStateRunning
	return nil
}

func rollingbackRenameIndex(t *meta.Meta, job *model.Job) (ver int64, err error) {
	tblInfo, from, _, err := checkRenameIndex(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Here rename index is done in a transaction, if the job is not completed, it can be canceled.
	idx := tblInfo.FindIndexByName(from.L)
	if idx.State == model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, errCancelledDDLJob
	}
	job.State = model.JobStateRunning
	return ver, errors.Trace(err)
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
	case model.ActionAddColumns:
		ver, err = rollingbackAddColumns(t, job)
	case model.ActionAddIndex:
		ver, err = rollingbackAddIndex(w, d, t, job, false)
	case model.ActionAddPrimaryKey:
		ver, err = rollingbackAddIndex(w, d, t, job, true)
	case model.ActionAddTablePartition:
		ver, err = rollingbackAddTablePartition(t, job)
	case model.ActionDropColumn:
		ver, err = rollingbackDropColumn(t, job)
	case model.ActionDropColumns:
		ver, err = rollingbackDropColumns(t, job)
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		ver, err = rollingbackDropIndex(t, job)
	case model.ActionDropTable, model.ActionDropView, model.ActionDropSequence:
		err = rollingbackDropTableOrView(t, job)
	case model.ActionDropTablePartition:
		ver, err = rollingbackDropTablePartition(t, job)
	case model.ActionDropSchema:
		err = rollingbackDropSchema(t, job)
	case model.ActionRenameIndex:
		ver, err = rollingbackRenameIndex(t, job)
	case model.ActionTruncateTable:
		ver, err = rollingbackTruncateTable(t, job)
	case model.ActionModifyColumn:
		ver, err = rollingbackModifyColumn(w, d, t, job)
	case model.ActionRebaseAutoID, model.ActionShardRowID, model.ActionAddForeignKey,
		model.ActionDropForeignKey, model.ActionRenameTable, model.ActionRenameTables,
		model.ActionModifyTableCharsetAndCollate, model.ActionTruncateTablePartition,
		model.ActionModifySchemaCharsetAndCollate, model.ActionRepairTable,
		model.ActionModifyTableAutoIdCache, model.ActionAlterIndexVisibility,
		model.ActionExchangeTablePartition:
		ver, err = cancelOnlyNotHandledJob(job)
	default:
		job.State = model.JobStateCancelled
		err = errCancelledDDLJob
	}

	if err != nil {
		if job.Error == nil {
			job.Error = toTError(err)
		}
		job.ErrorCount++

		if errCancelledDDLJob.Equal(err) {
			// The job is normally cancelled.
			if !job.Error.Equal(errCancelledDDLJob) {
				job.Error = terror.GetErrClass(job.Error).Synthesize(terror.ErrCode(job.Error.Code()),
					fmt.Sprintf("DDL job rollback, error msg: %s", terror.ToSQLError(job.Error).Message))
			}
		} else {
			// A job canceling meet other error.
			//
			// Once `convertJob2RollbackJob` meets an error, the job state can't be set as `JobStateRollingback` since
			// job state and args may not be correctly overwritten. The job will be fetched to run with the cancelling
			// state again. So we should check the error count here.
			if err1 := loadDDLVars(w); err1 != nil {
				logutil.Logger(w.logCtx).Error("[ddl] load DDL global variable failed", zap.Error(err1))
			}
			errorCount := variable.GetDDLErrorCountLimit()
			if job.ErrorCount > errorCount {
				logutil.Logger(w.logCtx).Warn("[ddl] rollback DDL job error count exceed the limit, cancelled it now", zap.Int64("jobID", job.ID), zap.Int64("errorCountLimit", errorCount))
				job.Error = toTError(errors.Errorf("rollback DDL job error count exceed the limit %d, cancelled it now", errorCount))
				job.State = model.JobStateCancelled
			}
		}

		if job.State != model.JobStateRollingback && job.State != model.JobStateCancelled {
			logutil.Logger(w.logCtx).Error("[ddl] run DDL job failed", zap.String("job", job.String()), zap.Error(err))
		} else {
			logutil.Logger(w.logCtx).Info("[ddl] the DDL job is cancelled normally", zap.String("job", job.String()), zap.Error(err))
			// If job is cancelled, we shouldn't return an error.
			return ver, nil
		}
	}

	return
}
