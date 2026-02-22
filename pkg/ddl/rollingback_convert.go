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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)
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
	case model.ActionAddColumnarIndex:
		ver, err = rollingbackAddColumanrIndex(w, jobCtx, job)
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
			if err1 := w.loadGlobalVars(vardef.TiDBDDLErrorCountLimit); err1 != nil {
				logger.Error("load DDL global variable failed", zap.Error(err1))
			}
			errorCount := vardef.GetDDLErrorCountLimit()
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
