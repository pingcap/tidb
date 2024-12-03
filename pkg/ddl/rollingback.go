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
	"github.com/pingcap/tidb/pkg/meta/model"
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

	dropArgs := &model.ModifyIndexArgs{
		PartitionIDs: getPartitionIDs(tblInfo),
		OpType:       model.OpRollbackAddIndex,
	}

	originalState := allIndexInfos[0].State
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
	if ingest.LitBackCtxMgr != nil {
		ingest.LitBackCtxMgr.Unregister(job.ID)
	}
	return ver, errors.Trace(err)
}

// convertNotReorgAddIdxJob2RollbackJob converts the add index job that are not started workers to rollingbackJob,
// to rollback add index operations. job.SnapshotVer == 0 indicates the workers are not started.
func convertNotReorgAddIdxJob2RollbackJob(jobCtx *jobContext, job *model.Job, occuredErr error) (ver int64, err error) {
	defer func() {
		if ingest.LitBackCtxMgr != nil {
			ingest.LitBackCtxMgr.Unregister(job.ID)
		}
	}()
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
	args, err := model.GetModifyColumnArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	_, tblInfo, oldCol, err := getModifyColumnInfo(jobCtx.metaMut, job, args.OldColumnName)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if !needChangeColumnData(oldCol, args.Column) {
		// Normal-type rolling back
		if job.SchemaState == model.StateNone {
			// When change null to not null, although state is unchanged with none, the oldCol flag's has been changed to preNullInsertFlag.
			// To roll back this kind of normal job, it is necessary to mark the state as JobStateRollingback to restore the old col's flag.
			if args.ModifyColumnType == mysql.TypeNull && tblInfo.Columns[oldCol.Offset].GetFlag()|mysql.PreventNullInsertFlag != 0 {
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
	if args.ChangingColumn == nil {
		// The job hasn't been handled and we cancel it directly.
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrCancelledDDLJob
	}
	// The job has been in its middle state (but the reorg worker hasn't started) and we roll it back here.
	job.State = model.JobStateRollingback
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

func rollingbackAddVectorIndex(w *worker, jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	if job.SchemaState == model.StateWriteReorganization {
		// Add vector index workers are started. need to ask them to exit.
		jobCtx.logger.Info("run the cancelling DDL job", zap.String("job", job.String()))
		ver, err = w.onCreateVectorIndex(jobCtx, job)
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

func convertReorgPartitionJob2RollbackJob(jobCtx *jobContext, job *model.Job, otherwiseErr error, tblInfo *model.TableInfo) (ver int64, err error) {
	pi := tblInfo.Partition
	addingDefinitions := pi.AddingDefinitions
	partNames := make([]string, 0, len(addingDefinitions))
	for _, pd := range addingDefinitions {
		partNames = append(partNames, pd.Name.L)
	}
	var dropIndices []*model.IndexInfo
	for _, indexInfo := range tblInfo.Indices {
		if !indexInfo.Unique {
			continue
		}
		isNew, ok := pi.DDLChangedIndex[indexInfo.ID]
		if !ok {
			// non-changed index
			continue
		}
		if !isNew {
			if pi.DDLState == model.StateDeleteReorganization {
				// Revert the non-public state
				indexInfo.State = model.StatePublic
			}
		} else {
			if pi.DDLState == model.StateDeleteReorganization {
				indexInfo.State = model.StateWriteOnly
			} else {
				dropIndices = append(dropIndices, indexInfo)
			}
		}
	}
	for _, indexInfo := range dropIndices {
		DropIndexColumnFlag(tblInfo, indexInfo)
		RemoveDependentHiddenColumns(tblInfo, indexInfo)
		removeIndexInfo(tblInfo, indexInfo)
	}
	if pi.DDLState == model.StateDeleteReorganization {
		// New partitions are public,
		// but old is still double written.
		// OK to revert.
		// Remove the AddingDefinitions
		// Add back the DroppingDefinitions
		if job.Type == model.ActionReorganizePartition {
			// Reassemble the list of partitions in the OriginalPartitionIDsOrder
			// Special handling, since for LIST partitioning,
			// only pi.OriginalPartitionIDsOrder shows how to merge back the DroppingDefinitions.
			// Implicitly it will also filter away AddingPartitions.
			// pi.Definitions and pi.DroppingDefinitions contain the original partitions
			// in the original order, but where the DroppingDefinitions should be placed,
			// can only be known through pi.OriginalPartitionIDsOrder.
			// RANGE/HASH/KEY would have consecutive added/dropped partitions, but use
			// the same code to avoid confusion.
			defPos := 0
			dropPos := 0
			newDefs := make([]model.PartitionDefinition, 0, len(pi.OriginalPartitionIDsOrder))
			for _, id := range pi.OriginalPartitionIDsOrder {
				if defPos < len(pi.Definitions) && pi.Definitions[defPos].ID == id {
					newDefs = append(newDefs, pi.Definitions[defPos])
					defPos++
					continue
				}
				if dropPos < len(pi.DroppingDefinitions) && id == pi.DroppingDefinitions[dropPos].ID {
					newDefs = append(newDefs, pi.DroppingDefinitions[dropPos])
					dropPos++
					continue
				}
				for {
					defPos++
					if defPos < len(pi.Definitions) && pi.Definitions[defPos].ID == id {
						newDefs = append(newDefs, pi.Definitions[defPos])
						break
					}
				}
			}
			if len(newDefs) != len(pi.OriginalPartitionIDsOrder) {
				return ver, errors.Trace(errors.New("Internal error, failed to find original partition definitions"))
			}
			pi.Definitions = newDefs
		} else {
			// Move back to StateWriteReorganization, i.e. use the original table
			// (non-partitioned or differently partitioned) as the main table to use.
			// Otherwise, the Type does not match the expression.
			pi.Type, pi.DDLType = pi.DDLType, pi.Type
			pi.Expr, pi.DDLExpr = pi.DDLExpr, pi.Expr
			pi.Columns, pi.DDLColumns = pi.DDLColumns, pi.Columns
			pi.Definitions = pi.DroppingDefinitions
		}
		pi.Num = uint64(len(pi.Definitions))
		// We should move back one state, since there might be other sessions seeing the new partitions.
		job.SchemaState = model.StateWriteReorganization
		pi.DDLState = job.SchemaState
	}

	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	args.PartNames = partNames
	job.FillArgs(args)
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.State = model.JobStateRollingback
	return ver, errors.Trace(otherwiseErr)
}

func rollingbackAddTablePartition(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	tblInfo, _, addingDefinitions, err := checkAddPartition(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// addingDefinitions' len = 0 means the job hasn't reached the replica-only state.
	if len(addingDefinitions) == 0 {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(dbterror.ErrCancelledDDLJob)
	}
	// addingDefinitions is also in tblInfo, here pass the tblInfo as parameter directly.
	return convertAddTablePartitionJob2RollbackJob(jobCtx, job, dbterror.ErrCancelledDDLJob, tblInfo)
}

func rollingbackDropTableOrView(jobCtx *jobContext, job *model.Job) error {
	tblInfo, err := checkTableExistAndCancelNonExistJob(jobCtx.metaMut, job, job.SchemaID)
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

func rollingbackDropTablePartition(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	_, err = GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return cancelOnlyNotHandledJob(job, model.StatePublic)
}

func rollingbackDropSchema(jobCtx *jobContext, job *model.Job) error {
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(jobCtx.metaMut, job)
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

func rollingbackRenameIndex(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	tblInfo, from, _, err := checkRenameIndex(jobCtx.metaMut, job)
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

func rollingbackTruncateTable(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	_, err = GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return cancelOnlyNotHandledJob(job, model.StateNone)
}

func convertJob2RollbackJob(w *worker, jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	switch job.Type {
	case model.ActionAddColumn:
		ver, err = rollingbackAddColumn(jobCtx, job)
	case model.ActionAddIndex:
		ver, err = rollingbackAddIndex(jobCtx, job)
	case model.ActionAddPrimaryKey:
		ver, err = rollingbackAddIndex(jobCtx, job)
	case model.ActionAddVectorIndex:
		ver, err = rollingbackAddVectorIndex(w, jobCtx, job)
	case model.ActionAddTablePartition:
		ver, err = rollingbackAddTablePartition(jobCtx, job)
	case model.ActionReorganizePartition, model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		ver, err = onRollbackReorganizePartition(jobCtx, job)
	case model.ActionDropColumn:
		ver, err = rollingbackDropColumn(jobCtx, job)
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		ver, err = rollingbackDropIndex(jobCtx, job)
	case model.ActionDropTable, model.ActionDropView, model.ActionDropSequence:
		err = rollingbackDropTableOrView(jobCtx, job)
	case model.ActionDropTablePartition:
		ver, err = rollingbackDropTablePartition(jobCtx, job)
	case model.ActionExchangeTablePartition:
		ver, err = rollingbackExchangeTablePartition(jobCtx, job)
	case model.ActionDropSchema:
		err = rollingbackDropSchema(jobCtx, job)
	case model.ActionRenameIndex:
		ver, err = rollingbackRenameIndex(jobCtx, job)
	case model.ActionTruncateTable:
		ver, err = rollingbackTruncateTable(jobCtx, job)
	case model.ActionModifyColumn:
		ver, err = rollingbackModifyColumn(jobCtx, job)
	case model.ActionDropForeignKey:
		ver, err = cancelOnlyNotHandledJob(job, model.StatePublic)
	case model.ActionTruncateTablePartition:
		ver, err = rollingbackTruncateTablePartition(jobCtx, job)
	case model.ActionRebaseAutoID, model.ActionShardRowID, model.ActionAddForeignKey,
		model.ActionRenameTable, model.ActionRenameTables,
		model.ActionModifyTableCharsetAndCollate,
		model.ActionModifySchemaCharsetAndCollate, model.ActionRepairTable,
		model.ActionModifyTableAutoIDCache, model.ActionAlterIndexVisibility,
		model.ActionModifySchemaDefaultPlacement, model.ActionRecoverSchema:
		ver, err = cancelOnlyNotHandledJob(job, model.StateNone)
	case model.ActionMultiSchemaChange:
		err = rollingBackMultiSchemaChange(job)
	case model.ActionAddCheckConstraint:
		ver, err = rollingBackAddConstraint(jobCtx, job)
	case model.ActionDropCheckConstraint:
		ver, err = rollingBackDropConstraint(jobCtx, job)
	case model.ActionAlterCheckConstraint:
		ver, err = rollingBackAlterConstraint(jobCtx, job)
	default:
		job.State = model.JobStateCancelled
		err = dbterror.ErrCancelledDDLJob
	}

	logger := jobCtx.logger
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

func rollingBackAddConstraint(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	_, tblInfo, constrInfoInMeta, _, err := checkAddCheckConstraint(jobCtx.metaMut, job)
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
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	return ver, errors.Trace(err)
}

func rollingBackDropConstraint(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	_, constrInfoInMeta, err := checkDropCheckConstraint(jobCtx.metaMut, job)
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

func rollingBackAlterConstraint(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	_, tblInfo, constraintInfo, enforced, err := checkAlterCheckConstraint(jobCtx.metaMut, job)
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
	ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
	return ver, errors.Trace(err)
}
