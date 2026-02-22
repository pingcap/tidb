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
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

// UpdateColsNull2NotNull changes the null option of columns of an index.
func UpdateColsNull2NotNull(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) error {
	nullCols := getNullColInfos(tblInfo, indexInfo.Columns)

	for _, col := range nullCols {
		col.AddFlag(mysql.NotNullFlag)
		col.DelFlag(mysql.PreventNullInsertFlag)
	}
	return nil
}

func convertAddIdxJob2RollbackJob(
	jobCtx *jobContext,
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
	if job.Type == model.ActionModifyColumn {
		job.State = model.JobStateRollingback
		return 0, err
	}

	dropArgs := &model.ModifyIndexArgs{
		PartitionIDs: getPartitionIDs(tblInfo),
		OpType:       model.OpRollbackAddIndex,
	}

	originalState := allIndexInfos[0].State
	for _, indexInfo := range allIndexInfos {
		if indexInfo.Primary {
			nullCols := getNullColInfos(tblInfo, indexInfo.Columns)
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
		dropArgs.IndexArgs = append(dropArgs.IndexArgs, &model.IndexArg{
			IndexName: indexInfo.Name,
			IfExist:   false,
		})
	}

	// Convert to ModifyIndexArgs
	job.FillFinishedArgs(dropArgs)

	job.SchemaState = model.StateDeleteOnly
	ver, err1 := updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateDeleteOnly)
	if err1 != nil {
		return ver, errors.Trace(err1)
	}
	job.State = model.JobStateRollingback
	// TODO(tangenta): get duplicate column and match index.
	err = completeErr(err, allIndexInfos[0])
	return ver, errors.Trace(err)
}

// convertNotReorgAddIdxJob2RollbackJob converts the add index job that are not started workers to rollingbackJob,
// to rollback add index operations. job.SnapshotVer == 0 indicates the workers are not started.
func convertNotReorgAddIdxJob2RollbackJob(jobCtx *jobContext, job *model.Job, occuredErr error) (ver int64, err error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	var indexesInfo []*model.IndexInfo
	for _, a := range args.IndexArgs {
		indexInfo := tblInfo.FindIndexByName(a.IndexName.L)
		if indexInfo != nil {
			indexesInfo = append(indexesInfo, indexInfo)
		}
	}
	if len(indexesInfo) == 0 {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}
	return convertAddIdxJob2RollbackJob(jobCtx, job, tblInfo, indexesInfo, occuredErr)
}

// rollingbackModifyColumn change the modifying-column job into rolling back state.
// Since modifying column job has two types: normal-type and reorg-type, we should handle it respectively.
// normal-type has only two states:    None -> Public
// reorg-type has five states:         None -> Delete-only -> Write-only -> Write-org -> Public
func rollingbackModifyColumn(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	if !job.IsRollbackable() {
		job.State = model.JobStateRunning
		return ver, nil
	}

	args, err := model.GetModifyColumnArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	_, tblInfo, oldCol, err := getModifyColumnInfo(jobCtx.metaMut, job, args)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch args.ModifyColumnType {
	case model.ModifyTypeNone:
		// The job hasn't been handled and we cancel it directly.
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	case model.ModifyTypeNoReorg, model.ModifyTypeNoReorgWithCheck, model.ModifyTypePrecheck:
		if job.SchemaState == model.StateNone {
			// When change null to not null, although state is unchanged with none, the oldCol flag's has been changed to preNullInsertFlag.
			// To roll back this kind of normal job, it is necessary to mark the state as JobStateRollingback to restore the old col's flag.
			if hasModifyFlag(tblInfo.Columns[oldCol.Offset]) {
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
	case model.ModifyTypeReorg, model.ModifyTypeIndexReorg:
		// reorg-type rolling back
		if args.ChangingColumn == nil {
			// The job hasn't been handled and we cancel it directly.
			job.State = model.JobStateCancelled
			return ver, dbterror.ErrCancelledDDLJob
		}
		// The job has been in its middle state (but the reorg worker hasn't started) and we roll it back here.
		job.State = model.JobStateRollingback
		return ver, dbterror.ErrCancelledDDLJob
	}
	return ver, dbterror.ErrCancelledDDLJob
}

func rollingbackAddColumn(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	tblInfo, columnInfo, col, _, _, err := checkAddColumn(jobCtx.metaMut, job)
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

	// rollback the AddColumn ddl. fill the DropColumn args into job.
	args := &model.TableColumnArgs{
		Col: &model.ColumnInfo{Name: col.Name},
	}
	model.FillRollBackArgsForAddColumn(job, args)
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != columnInfo.State)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.State = model.JobStateRollingback
	return ver, dbterror.ErrCancelledDDLJob
}

func rollingbackDropColumn(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	_, colInfo, idxInfos, _, err := checkDropColumn(jobCtx, job)
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

func rollingbackDropIndex(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	_, indexInfo, _, err := checkDropIndex(jobCtx.infoCache, jobCtx.metaMut, job)
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

func rollingbackAddColumanrIndex(w *worker, jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	if job.SchemaState == model.StateWriteReorganization {
		// Add columnar index workers are started. need to ask them to exit.
		jobCtx.logger.Info("run the cancelling DDL job", zap.String("job", job.String()))
		ver, err = w.onCreateColumnarIndex(jobCtx, job)
	} else {
		// add index's reorg workers are not running, remove the indexInfo in tableInfo.
		ver, err = convertNotReorgAddIdxJob2RollbackJob(jobCtx, job, dbterror.ErrCancelledDDLJob)
	}
	return
}

func rollingbackAddIndex(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	// add index's reorg workers are not running, remove the indexInfo in tableInfo.
	return convertNotReorgAddIdxJob2RollbackJob(jobCtx, job, dbterror.ErrCancelledDDLJob)
}

// rollbackExchangeTablePartition will clear the non-partitioned
// table's ExchangePartitionInfo state.
func rollbackExchangeTablePartition(jobCtx *jobContext, job *model.Job, tblInfo *model.TableInfo) (ver int64, err error) {
	tblInfo.ExchangePartitionInfo = nil
	job.State = model.JobStateRollbackDone
	job.SchemaState = model.StatePublic
	if len(tblInfo.Constraints) == 0 {
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	}
	args, err := model.GetExchangeTablePartitionArgs(job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	pt, err := getTableInfo(jobCtx.metaMut, args.PTTableID, args.PTSchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	pt.ExchangePartitionInfo = nil
	var ptInfo []schemaIDAndTableInfo
	ptInfo = append(ptInfo, schemaIDAndTableInfo{
		schemaID: args.PTSchemaID,
		tblInfo:  pt,
	})
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true, ptInfo...)
	return ver, errors.Trace(err)
}

func rollingbackExchangeTablePartition(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	if job.SchemaState == model.StateNone {
		// Nothing is changed
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}
	var nt *model.TableInfo
	nt, err = GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	ver, err = rollbackExchangeTablePartition(jobCtx, job, nt)
	return ver, errors.Trace(err)
}

func rollingbackTruncateTablePartition(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	return convertTruncateTablePartitionJob2RollbackJob(jobCtx, job, dbterror.ErrCancelledDDLJob, tblInfo)
}

func convertTruncateTablePartitionJob2RollbackJob(jobCtx *jobContext, job *model.Job, otherwiseErr error, tblInfo *model.TableInfo) (ver int64, err error) {
	if !job.IsRollbackable() {
		// Only Original state and StateWrite can be rolled back, otherwise new partitions
		// may have been used and new data would get lost.
		// So we must continue to roll forward!
		job.State = model.JobStateRunning
		return ver, nil
	}
	pi := tblInfo.Partition
	if len(pi.NewPartitionIDs) != 0 || pi.DDLAction != model.ActionNone || pi.DDLState != model.StateNone {
		// Rollback the changes, note that no new partitions has been used yet!
		// so only metadata rollback and we can cancel the DDL
		tblInfo.Partition.NewPartitionIDs = nil
		tblInfo.Partition.DDLAction = model.ActionNone
		tblInfo.Partition.DDLState = model.StateNone
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}
	// No change yet, just cancel the job.
	job.State = model.JobStateCancelled
	return ver, errors.Trace(otherwiseErr)
}

func convertAddTablePartitionJob2RollbackJob(jobCtx *jobContext, job *model.Job, otherwiseErr error, tblInfo *model.TableInfo) (ver int64, err error) {
	addingDefinitions := tblInfo.Partition.AddingDefinitions
	partNames := make([]string, 0, len(addingDefinitions))
	for _, pd := range addingDefinitions {
		partNames = append(partNames, pd.Name.L)
	}

	args := &model.TablePartitionArgs{
		PartNames: partNames,
	}
	model.FillRollbackArgsForAddPartition(job, args)
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.State = model.JobStateRollingback
	return ver, errors.Trace(otherwiseErr)
}

func onRollbackReorganizePartition(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if job.SchemaState == model.StatePublic {
		// We started to destroy the old indexes, so we can no longer rollback!
		job.State = model.JobStateRunning
		return ver, nil
	}
	jobCtx.jobArgs = args

	return rollbackReorganizePartitionWithErr(jobCtx, job, dbterror.ErrCancelledDDLJob)
}

func rollbackReorganizePartitionWithErr(jobCtx *jobContext, job *model.Job, otherwiseErr error) (ver int64, err error) {
	if job.SchemaState == model.StateNone {
		job.State = model.JobStateCancelled
		return ver, otherwiseErr
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// addingDefinitions is also in tblInfo, here pass the tblInfo as parameter directly.
	return convertReorgPartitionJob2RollbackJob(jobCtx, job, otherwiseErr, tblInfo)
}

