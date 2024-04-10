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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

// UpdateColsNull2NotNull changes the null option of columns of an index.
func UpdateColsNull2NotNull(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) error {
	nullCols, err := getNullColInfos(tblInfo, indexInfo)
	if err != nil {
		return errors.Trace(err)
	}

	for _, col := range nullCols {
		col.AddFlag(mysql.NotNullFlag)
		col.DelFlag(mysql.PreventNullInsertFlag)
	}
	return nil
}

func convertAddIdxJob2RollbackJob(
	d *ddlCtx,
	t *meta.Meta,
	job *model.Job,
	tblInfo *model.TableInfo,
	allIndexInfos []*model.IndexInfo,
	err error,
) (int64, error) {
	failpoint.Inject("mockConvertAddIdxJob2RollbackJobError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(0, errors.New("mock convert add index job to rollback job error"))
		}
	})

	originalState := allIndexInfos[0].State
	idxNames := make([]model.CIStr, 0, len(allIndexInfos))
	ifExists := make([]bool, 0, len(allIndexInfos))
	for _, indexInfo := range allIndexInfos {
		if indexInfo.Primary {
			nullCols, err := getNullColInfos(tblInfo, indexInfo)
			if err != nil {
				return 0, errors.Trace(err)
			}
			for _, col := range nullCols {
				// Field PreventNullInsertFlag flag reset.
				col.DelFlag(mysql.PreventNullInsertFlag)
			}
		}
		// If add index job rollbacks in write reorganization state, its need to delete all keys which has been added.
		// Its work is the same as drop index job do.
		// The write reorganization state in add index job that likes write only state in drop index job.
		// So the next state is delete only state.
		indexInfo.State = model.StateDeleteOnly
		idxNames = append(idxNames, indexInfo.Name)
		ifExists = append(ifExists, false)
	}

	// the second and the third args will be used in onDropIndex.
	job.Args = []any{idxNames, ifExists, getPartitionIDs(tblInfo)}
	job.SchemaState = model.StateDeleteOnly
	ver, err1 := updateVersionAndTableInfo(d, t, job, tblInfo, originalState != model.StateDeleteOnly)
	if err1 != nil {
		return ver, errors.Trace(err1)
	}
	job.State = model.JobStateRollingback
	// TODO(tangenta): get duplicate column and match index.
	err = completeErr(err, allIndexInfos[0])
	if ingest.LitBackCtxMgr != nil {
		ingest.LitBackCtxMgr.Unregister(job.ID)
	}
	return ver, errors.Trace(err)
}

// convertNotReorgAddIdxJob2RollbackJob converts the add index job that are not started workers to rollingbackJob,
// to rollback add index operations. job.SnapshotVer == 0 indicates the workers are not started.
func convertNotReorgAddIdxJob2RollbackJob(d *ddlCtx, t *meta.Meta, job *model.Job, occuredErr error) (ver int64, err error) {
	defer func() {
		if ingest.LitBackCtxMgr != nil {
			ingest.LitBackCtxMgr.Unregister(job.ID)
		}
	}()
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	unique := make([]bool, 1)
	indexName := make([]model.CIStr, 1)
	indexPartSpecifications := make([][]*ast.IndexPartSpecification, 1)
	indexOption := make([]*ast.IndexOption, 1)

	err = job.DecodeArgs(&unique[0], &indexName[0], &indexPartSpecifications[0], &indexOption[0])
	if err != nil {
		err = job.DecodeArgs(&unique, &indexName, &indexPartSpecifications, &indexOption)
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	var indexesInfo []*model.IndexInfo
	for _, idxName := range indexName {
		indexInfo := tblInfo.FindIndexByName(idxName.L)
		if indexInfo != nil {
			indexesInfo = append(indexesInfo, indexInfo)
		}
	}
	if len(indexesInfo) == 0 {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}
	return convertAddIdxJob2RollbackJob(d, t, job, tblInfo, indexesInfo, occuredErr)
}

// rollingbackModifyColumn change the modifying-column job into rolling back state.
// Since modifying column job has two types: normal-type and reorg-type, we should handle it respectively.
// normal-type has only two states:    None -> Public
// reorg-type has five states:         None -> Delete-only -> Write-only -> Write-org -> Public
func rollingbackModifyColumn(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	if needNotifyAndStopReorgWorker(job) {
		// column type change workers are started. we have to ask them to exit.
		w.jobLogger(job).Info("run the cancelling DDL job", zap.String("job", job.String()))
		d.notifyReorgWorkerJobStateChange(job)
		// Give the this kind of ddl one more round to run, the dbterror.ErrCancelledDDLJob should be fetched from the bottom up.
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
			if jp.modifyColumnTp == mysql.TypeNull && tblInfo.Columns[oldCol.Offset].GetFlag()|mysql.PreventNullInsertFlag != 0 {
				job.State = model.JobStateRollingback
				return ver, dbterror.ErrCancelledDDLJob
			}
			// Normal job with stateNone can be cancelled directly.
			job.State = model.JobStateCancelled
			return ver, dbterror.ErrCancelledDDLJob
		}
		// StatePublic couldn't be cancelled.
		job.State = model.JobStateRunning
		return ver, nil
	}
	// reorg-type rolling back
	if jp.changingCol == nil {
		// The job hasn't been handled and we cancel it directly.
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}
	// The job has been in its middle state (but the reorg worker hasn't started) and we roll it back here.
	job.State = model.JobStateRollingback
	return ver, dbterror.ErrCancelledDDLJob
}

func rollingbackAddColumn(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	tblInfo, columnInfo, col, _, _, err := checkAddColumn(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if columnInfo == nil {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}

	originalState := columnInfo.State
	columnInfo.State = model.StateDeleteOnly
	job.SchemaState = model.StateDeleteOnly

	job.Args = []any{col.Name}
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != columnInfo.State)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.State = model.JobStateRollingback
	return ver, dbterror.ErrCancelledDDLJob
}

func rollingbackDropColumn(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, colInfo, idxInfos, _, err := checkDropColumn(d, t, job)
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
			return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", indexInfo.State)
		}
	}

	// StatePublic means when the job is not running yet.
	if colInfo.State == model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}
	// In the state of drop column `write only -> delete only -> reorganization`,
	// We can not rollback now, so just continue to drop column.
	job.State = model.JobStateRunning
	return ver, nil
}

func rollingbackDropIndex(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, indexInfo, _, err := checkDropIndex(d, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch indexInfo[0].State {
	case model.StateWriteOnly, model.StateDeleteOnly, model.StateDeleteReorganization, model.StateNone:
		// We can not rollback now, so just continue to drop index.
		// Normally won't fetch here, because there is check when cancel ddl jobs. see function: isJobRollbackable.
		job.State = model.JobStateRunning
		return ver, nil
	case model.StatePublic:
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", indexInfo[0].State)
	}
}

func rollingbackAddIndex(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job, isPK bool) (ver int64, err error) {
	if needNotifyAndStopReorgWorker(job) {
		// add index workers are started. need to ask them to exit.
		w.jobLogger(job).Info("run the cancelling DDL job", zap.String("job", job.String()))
		d.notifyReorgWorkerJobStateChange(job)
		ver, err = w.onCreateIndex(d, t, job, isPK)
	} else {
		// add index's reorg workers are not running, remove the indexInfo in tableInfo.
		ver, err = convertNotReorgAddIdxJob2RollbackJob(d, t, job, dbterror.ErrCancelledDDLJob)
	}
	return
}

func needNotifyAndStopReorgWorker(job *model.Job) bool {
	if job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0 {
		// If the value of SnapshotVer isn't zero, it means the reorg workers have been started.
		if job.MultiSchemaInfo != nil {
			// However, if the sub-job is non-revertible, it means the reorg process is finished.
			// We don't need to start another round to notify reorg workers to exit.
			return job.MultiSchemaInfo.Revertible
		}
		return true
	}
	return false
}

// rollbackExchangeTablePartition will clear the non-partitioned
// table's ExchangePartitionInfo state.
func rollbackExchangeTablePartition(d *ddlCtx, t *meta.Meta, job *model.Job, tblInfo *model.TableInfo) (ver int64, err error) {
	tblInfo.ExchangePartitionInfo = nil
	job.State = model.JobStateRollbackDone
	job.SchemaState = model.StatePublic
	if len(tblInfo.Constraints) == 0 {
		return updateVersionAndTableInfo(d, t, job, tblInfo, true)
	}
	var (
		defID          int64
		ptSchemaID     int64
		ptID           int64
		partName       string
		withValidation bool
	)
	if err = job.DecodeArgs(&defID, &ptSchemaID, &ptID, &partName, &withValidation); err != nil {
		return ver, errors.Trace(err)
	}
	pt, err := getTableInfo(t, ptID, ptSchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	pt.ExchangePartitionInfo = nil
	var ptInfo []schemaIDAndTableInfo
	ptInfo = append(ptInfo, schemaIDAndTableInfo{
		schemaID: ptSchemaID,
		tblInfo:  pt,
	})
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true, ptInfo...)
	return ver, errors.Trace(err)
}

func rollingbackExchangeTablePartition(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	if job.SchemaState == model.StateNone {
		// Nothing is changed
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}
	var nt *model.TableInfo
	nt, err = GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	ver, err = rollbackExchangeTablePartition(d, t, job, nt)
	return ver, errors.Trace(err)
}

func convertAddTablePartitionJob2RollbackJob(d *ddlCtx, t *meta.Meta, job *model.Job, otherwiseErr error, tblInfo *model.TableInfo) (ver int64, err error) {
	addingDefinitions := tblInfo.Partition.AddingDefinitions
	partNames := make([]string, 0, len(addingDefinitions))
	for _, pd := range addingDefinitions {
		partNames = append(partNames, pd.Name.L)
	}
	if job.Type == model.ActionReorganizePartition ||
		job.Type == model.ActionAlterTablePartitioning ||
		job.Type == model.ActionRemovePartitioning {
		partInfo := &model.PartitionInfo{}
		var pNames []string
		err = job.DecodeArgs(&pNames, &partInfo)
		if err != nil {
			return ver, err
		}
		job.Args = []any{partNames, partInfo}
	} else {
		job.Args = []any{partNames}
	}
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.State = model.JobStateRollingback
	return ver, errors.Trace(otherwiseErr)
}

func rollingbackAddTablePartition(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	tblInfo, _, addingDefinitions, err := checkAddPartition(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// addingDefinitions' len = 0 means the job hasn't reached the replica-only state.
	if len(addingDefinitions) == 0 {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(dbterror.ErrCancelledDDLJob)
	}
	// addingDefinitions is also in tblInfo, here pass the tblInfo as parameter directly.
	return convertAddTablePartitionJob2RollbackJob(d, t, job, dbterror.ErrCancelledDDLJob, tblInfo)
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
		return dbterror.ErrCancelledDDLJob
	}
	job.State = model.JobStateRunning
	return nil
}

func rollingbackDropTablePartition(t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, err = GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return cancelOnlyNotHandledJob(job, model.StatePublic)
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
		return dbterror.ErrCancelledDDLJob
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
		return ver, dbterror.ErrCancelledDDLJob
	}
	job.State = model.JobStateRunning
	return ver, errors.Trace(err)
}

func cancelOnlyNotHandledJob(job *model.Job, initialState model.SchemaState) (ver int64, err error) {
	// We can only cancel the not handled job.
	if job.SchemaState == initialState {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}

	job.State = model.JobStateRunning

	return ver, nil
}

func rollingbackTruncateTable(t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, err = GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return cancelOnlyNotHandledJob(job, model.StateNone)
}

func rollingbackReorganizePartition(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	if job.SchemaState == model.StateNone {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// addingDefinitions is also in tblInfo, here pass the tblInfo as parameter directly.
	// TODO: Test this with reorganize partition p1 into (partition p1 ...)!
	return convertAddTablePartitionJob2RollbackJob(d, t, job, dbterror.ErrCancelledDDLJob, tblInfo)
}

func pauseReorgWorkers(w *worker, d *ddlCtx, job *model.Job) (err error) {
	if needNotifyAndStopReorgWorker(job) {
		w.jobLogger(job).Info("pausing the DDL job", zap.String("job", job.String()))
		d.notifyReorgWorkerJobStateChange(job)
	}

	return dbterror.ErrPausedDDLJob.GenWithStackByArgs(job.ID)
}

func convertJob2RollbackJob(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	switch job.Type {
	case model.ActionAddColumn:
		ver, err = rollingbackAddColumn(d, t, job)
	case model.ActionAddIndex:
		ver, err = rollingbackAddIndex(w, d, t, job, false)
	case model.ActionAddPrimaryKey:
		ver, err = rollingbackAddIndex(w, d, t, job, true)
	case model.ActionAddTablePartition:
		ver, err = rollingbackAddTablePartition(d, t, job)
	case model.ActionReorganizePartition, model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		ver, err = rollingbackReorganizePartition(d, t, job)
	case model.ActionDropColumn:
		ver, err = rollingbackDropColumn(d, t, job)
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		ver, err = rollingbackDropIndex(d, t, job)
	case model.ActionDropTable, model.ActionDropView, model.ActionDropSequence:
		err = rollingbackDropTableOrView(t, job)
	case model.ActionDropTablePartition:
		ver, err = rollingbackDropTablePartition(t, job)
	case model.ActionExchangeTablePartition:
		ver, err = rollingbackExchangeTablePartition(d, t, job)
	case model.ActionDropSchema:
		err = rollingbackDropSchema(t, job)
	case model.ActionRenameIndex:
		ver, err = rollingbackRenameIndex(t, job)
	case model.ActionTruncateTable:
		ver, err = rollingbackTruncateTable(t, job)
	case model.ActionModifyColumn:
		ver, err = rollingbackModifyColumn(w, d, t, job)
	case model.ActionDropForeignKey, model.ActionTruncateTablePartition:
		ver, err = cancelOnlyNotHandledJob(job, model.StatePublic)
	case model.ActionRebaseAutoID, model.ActionShardRowID, model.ActionAddForeignKey,
		model.ActionRenameTable, model.ActionRenameTables,
		model.ActionModifyTableCharsetAndCollate,
		model.ActionModifySchemaCharsetAndCollate, model.ActionRepairTable,
		model.ActionModifyTableAutoIdCache, model.ActionAlterIndexVisibility,
		model.ActionModifySchemaDefaultPlacement, model.ActionRecoverSchema:
		ver, err = cancelOnlyNotHandledJob(job, model.StateNone)
	case model.ActionMultiSchemaChange:
		err = rollingBackMultiSchemaChange(job)
	case model.ActionAddCheckConstraint:
		ver, err = rollingBackAddConstraint(d, t, job)
	case model.ActionDropCheckConstraint:
		ver, err = rollingBackDropConstraint(t, job)
	case model.ActionAlterCheckConstraint:
		ver, err = rollingBackAlterConstraint(d, t, job)
	default:
		job.State = model.JobStateCancelled
		err = dbterror.ErrCancelledDDLJob
	}

	logger := w.jobLogger(job)
	if err != nil {
		if job.Error == nil {
			job.Error = toTError(err)
		}
		job.ErrorCount++

		if dbterror.ErrCancelledDDLJob.Equal(err) {
			// The job is normally cancelled.
			if !job.Error.Equal(dbterror.ErrCancelledDDLJob) {
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
				logger.Error("load DDL global variable failed", zap.Error(err1))
			}
			errorCount := variable.GetDDLErrorCountLimit()
			if job.ErrorCount > errorCount {
				logger.Warn("rollback DDL job error count exceed the limit, cancelled it now", zap.Int64("errorCountLimit", errorCount))
				job.Error = toTError(errors.Errorf("rollback DDL job error count exceed the limit %d, cancelled it now", errorCount))
				job.State = model.JobStateCancelled
			}
		}

		if !(job.State != model.JobStateRollingback && job.State != model.JobStateCancelled) {
			logger.Info("the DDL job is cancelled normally", zap.String("job", job.String()), zap.Error(err))
			// If job is cancelled, we shouldn't return an error.
			return ver, nil
		}
		logger.Error("run DDL job failed", zap.String("job", job.String()), zap.Error(err))
	}

	return
}

func rollingBackAddConstraint(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, tblInfo, constrInfoInMeta, _, err := checkAddCheckConstraint(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if constrInfoInMeta == nil {
		// Add constraint hasn't stored constraint info into meta, so we can cancel the job
		// directly without further rollback action.
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}
	for i, constr := range tblInfo.Constraints {
		if constr.Name.L == constrInfoInMeta.Name.L {
			tblInfo.Constraints = append(tblInfo.Constraints[0:i], tblInfo.Constraints[i+1:]...)
			break
		}
	}
	if job.IsRollingback() {
		job.State = model.JobStateRollbackDone
	}
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	return ver, errors.Trace(err)
}

func rollingBackDropConstraint(t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, constrInfoInMeta, err := checkDropCheckConstraint(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// StatePublic means when the job is not running yet.
	if constrInfoInMeta.State == model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}
	// Can not rollback like drop other element, so just continue to drop constraint.
	job.State = model.JobStateRunning
	return ver, nil
}

func rollingBackAlterConstraint(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	_, tblInfo, constraintInfo, enforced, err := checkAlterCheckConstraint(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// StatePublic means when the job is not running yet.
	if constraintInfo.State == model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}

	// Only alter check constraints ENFORCED can get here.
	constraintInfo.Enforced = !enforced
	constraintInfo.State = model.StatePublic
	if job.IsRollingback() {
		job.State = model.JobStateRollbackDone
	}
	ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
	return ver, errors.Trace(err)
}
