// Copyright 2024 PingCAP, Inc.
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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

func (w *worker) doModifyColumnTypeWithData(
	jobCtx *jobContext,
	job *model.Job,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	changingCol, oldCol *model.ColumnInfo,
	args *model.ModifyColumnArgs,
) (ver int64, _ error) {
	colName, pos := args.Column.Name, args.Position

	var err error
	originalState := changingCol.State
	targetCol := changingCol.Clone()
	targetCol.Name = colName
	changingIdxs := buildRelatedIndexInfos(tblInfo, changingCol.ID)
	switch changingCol.State {
	case model.StateNone:
		err = validatePosition(tblInfo, oldCol, pos)
		if err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
		// Column from null to not null.
		if isNullToNotNullChange(oldCol, changingCol) {
			oldCol.AddFlag(mysql.PreventNullInsertFlag)
		}
		// none -> delete only
		updateObjectState(changingCol, changingIdxs, model.StateDeleteOnly)
		job.ReorgMeta.Stage = model.ReorgStageModifyColumnUpdateColumn
		err = initForReorgIndexes(w, job, changingIdxs)
		if err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
		failpoint.Inject("mockInsertValueAfterCheckNull", func(val failpoint.Value) {
			if valStr, ok := val.(string); ok {
				var sctx sessionctx.Context
				sctx, err := w.sessPool.Get()
				if err != nil {
					failpoint.Return(ver, err)
				}
				defer w.sessPool.Put(sctx)

				ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
				//nolint:forcetypeassert
				_, _, err = sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, valStr)
				if err != nil {
					job.State = model.JobStateCancelled
					failpoint.Return(ver, err)
				}
			}
		})
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Make sure job args change after `updateVersionAndTableInfoWithCheck`, otherwise, the job args will
		// be updated in `updateDDLJob` even if it meets an error in `updateVersionAndTableInfoWithCheck`.
		job.SchemaState = model.StateDeleteOnly
		metrics.GetBackfillProgressByLabel(metrics.LblModifyColumn, job.SchemaName, tblInfo.Name.String(), args.OldColumnName.O).Set(0)
		args.ChangingColumn = changingCol
		args.ChangingIdxs = changingIdxs
		failpoint.InjectCall("modifyColumnTypeWithData", job, args)
		job.FillArgs(args)
	case model.StateDeleteOnly:
		// Column from null to not null.
		if isNullToNotNullChange(oldCol, changingCol) {
			checked, err := checkModifyColumnData(
				jobCtx.stepCtx, w,
				dbInfo.Name, tblInfo.Name,
				oldCol, args.Column, false)
			if err != nil {
				if checked {
					job.State = model.JobStateRollingback
				}
				return ver, errors.Trace(err)
			}
		}
		// delete only -> write only
		updateObjectState(changingCol, changingIdxs, model.StateWriteOnly)
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
		failpoint.InjectCall("afterModifyColumnStateDeleteOnly", job.ID)
	case model.StateWriteOnly:
		// write only -> reorganization
		updateObjectState(changingCol, changingIdxs, model.StateWriteReorganization)
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), dbInfo.ID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		switch job.ReorgMeta.AnalyzeState {
		case model.AnalyzeStateNone:
			switch job.ReorgMeta.Stage {
			case model.ReorgStageModifyColumnUpdateColumn:
				var done bool
				reorgElements := BuildElements(changingCol, changingIdxs)
				done, ver, err = doReorgWorkForModifyColumn(jobCtx, w, job, tbl, oldCol, reorgElements)
				if !done {
					return ver, err
				}
				if len(changingIdxs) > 0 {
					job.SnapshotVer = 0
					job.ReorgMeta.Stage = model.ReorgStageModifyColumnRecreateIndex
				} else {
					job.ReorgMeta.Stage = model.ReorgStageModifyColumnCompleted
				}
			case model.ReorgStageModifyColumnRecreateIndex:
				var done bool
				done, ver, err = doReorgWorkForCreateIndex(w, jobCtx, job, tbl, changingIdxs)
				if !done {
					return ver, err
				}
				job.ReorgMeta.Stage = model.ReorgStageModifyColumnCompleted
			case model.ReorgStageModifyColumnCompleted:
				// For multi-schema change, analyze is done by parent job.
				if job.MultiSchemaInfo == nil && checkNeedAnalyze(job, tblInfo) {
					job.ReorgMeta.AnalyzeState = model.AnalyzeStateRunning
				} else {
					job.ReorgMeta.AnalyzeState = model.AnalyzeStateSkipped
					checkAndMarkNonRevertible(job)
				}
			}
		case model.AnalyzeStateRunning:
			intest.Assert(job.MultiSchemaInfo == nil, "multi schema change shouldn't reach here")
			w.startAnalyzeAndWait(job, tblInfo)
		case model.AnalyzeStateDone, model.AnalyzeStateSkipped, model.AnalyzeStateTimeout, model.AnalyzeStateFailed:
			failpoint.InjectCall("afterReorgWorkForModifyColumn")
			oldIdxInfos := buildRelatedIndexInfos(tblInfo, oldCol.ID)
			if tblInfo.TTLInfo != nil {
				updateTTLInfoWhenModifyColumn(tblInfo, oldCol.Name, colName)
			}
			changingIdxInfos := buildRelatedIndexInfos(tblInfo, changingCol.ID)
			intest.Assert(len(oldIdxInfos) == len(changingIdxInfos))

			// In multi-schema change, the order of changingIdxInfos may not be the same as oldIdxInfos,
			// because we will allocate new indexID for previous temp index.
			reorderChangingIdx(oldIdxInfos, changingIdxInfos)

			updateObjectState(oldCol, oldIdxInfos, model.StateWriteOnly)
			updateObjectState(changingCol, changingIdxInfos, model.StatePublic)
			markOldObjectRemoving(oldCol, changingCol, oldIdxInfos, changingIdxInfos, colName)
			moveChangingColumnToDest(tblInfo, oldCol, changingCol, pos)
			moveOldColumnToBack(tblInfo, oldCol)
			moveIndexInfoToDest(tblInfo, changingCol, oldIdxInfos, changingIdxInfos)
			updateModifyingCols(oldCol, changingCol)

			job.SchemaState = model.StatePublic
			ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
	case model.StatePublic:
		oldIdxInfos := buildRelatedIndexInfos(tblInfo, oldCol.ID)
		switch oldCol.State {
		case model.StateWriteOnly:
			updateObjectState(oldCol, oldIdxInfos, model.StateDeleteOnly)
			moveOldColumnToBack(tblInfo, oldCol)
			ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
		case model.StateDeleteOnly:
			removedIdxIDs := removeOldObjects(tblInfo, oldCol, oldIdxInfos)
			removedIdxIDs = append(removedIdxIDs, getIngestTempIndexIDs(job, changingIdxs)...)
			analyzed := job.ReorgMeta.AnalyzeState == model.AnalyzeStateDone
			modifyColumnEvent := notifier.NewModifyColumnEvent(tblInfo, []*model.ColumnInfo{changingCol}, analyzed)
			err = asyncNotifyEvent(jobCtx, modifyColumnEvent, job, noSubJob, w.sess)
			if err != nil {
				return ver, errors.Trace(err)
			}

			ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
			// Finish this job.
			job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
			// Refactor the job args to add the old index ids into delete range table.
			rmIdxs := append(removedIdxIDs, args.RedundantIdxs...)
			args.IndexIDs = rmIdxs
			newIdxIDs := make([]int64, 0, len(changingIdxs))
			for _, idx := range changingIdxs {
				newIdxIDs = append(newIdxIDs, idx.ID)
			}
			args.NewIndexIDs = newIdxIDs
			args.PartitionIDs = getPartitionIDs(tblInfo)
			job.FillFinishedArgs(args)
		default:
			errMsg := fmt.Sprintf("unexpected column state %s in modify column job", oldCol.State)
			intest.Assert(false, errMsg)
			return ver, errors.Errorf("%s", errMsg)
		}
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("column", changingCol.State)
	}
	return ver, errors.Trace(err)
}

func (w *worker) doModifyColumnIndexReorg(
	jobCtx *jobContext,
	job *model.Job,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	oldCol *model.ColumnInfo,
	args *model.ModifyColumnArgs,
) (ver int64, err error) {
	colName, pos := args.Column.Name, args.Position

	allIdxs := buildRelatedIndexInfos(tblInfo, oldCol.ID)
	oldIdxInfos := make([]*model.IndexInfo, 0, len(allIdxs)/2)
	changingIdxInfos := make([]*model.IndexInfo, 0, len(allIdxs)/2)

	if job.SchemaState == model.StatePublic {
		// The constraint that the first half of allIdxs is oldIdxInfos and
		// the second half is changingIdxInfos isn't true now, so we need to
		// find the oldIdxInfos again.
		for _, idx := range allIdxs {
			if idx.IsRemoving() {
				oldIdxInfos = append(oldIdxInfos, idx)
			}
		}
	} else {
		changingIdxInfos = allIdxs[len(allIdxs)/2:]
		oldIdxInfos = allIdxs[:len(allIdxs)/2]
	}

	finishFunc := func() (ver int64, err error) {
		removedIdxIDs := make([]int64, 0, len(oldIdxInfos))
		for _, idx := range oldIdxInfos {
			removedIdxIDs = append(removedIdxIDs, idx.ID)
		}
		removedIdxIDs = append(removedIdxIDs, getIngestTempIndexIDs(job, changingIdxInfos)...)
		removeOldIndexes(tblInfo, oldIdxInfos)
		oldCol.ChangingFieldType = nil
		oldCol.DelFlag(mysql.PreventNullInsertFlag)

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		// Refactor the job args to add the old index ids into delete range table.
		rmIdxs := append(removedIdxIDs, args.RedundantIdxs...)
		args.IndexIDs = rmIdxs
		args.PartitionIDs = getPartitionIDs(tblInfo)
		job.FillFinishedArgs(args)
		return ver, nil
	}

	switch job.SchemaState {
	case model.StateNone:
		oldCol.AddFlag(mysql.PreventNullInsertFlag)
		oldCol.ChangingFieldType = &args.Column.FieldType
		// none -> delete only
		updateObjectState(nil, changingIdxInfos, model.StateDeleteOnly)
		job.ReorgMeta.Stage = model.ReorgStageModifyColumnUpdateColumn
		err := initForReorgIndexes(w, job, changingIdxInfos)
		if err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteOnly
		metrics.GetBackfillProgressByLabel(metrics.LblModifyColumn, job.SchemaName, tblInfo.Name.String(), args.OldColumnName.O).Set(0)
		args.ChangingIdxs = changingIdxInfos
		failpoint.InjectCall("modifyColumnTypeWithData", job, args)
		job.FillArgs(args)
	case model.StateDeleteOnly:
		checked, err := checkModifyColumnData(
			jobCtx.stepCtx, w,
			dbInfo.Name, tblInfo.Name,
			oldCol, args.Column, true)
		if err != nil {
			if checked {
				job.State = model.JobStateRollingback
			}
			return ver, errors.Trace(err)
		}

		// delete only -> write only
		updateObjectState(nil, changingIdxInfos, model.StateWriteOnly)
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
		failpoint.InjectCall("afterModifyColumnStateDeleteOnly", job.ID)
	case model.StateWriteOnly:
		// write only -> reorganization
		updateObjectState(nil, changingIdxInfos, model.StateWriteReorganization)
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), dbInfo.ID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		switch job.ReorgMeta.AnalyzeState {
		case model.AnalyzeStateNone:
			switch job.ReorgMeta.Stage {
			case model.ReorgStageModifyColumnUpdateColumn:
				// Now row reorg
				job.SnapshotVer = 0
				job.ReorgMeta.Stage = model.ReorgStageModifyColumnRecreateIndex
			case model.ReorgStageModifyColumnRecreateIndex:
				var done bool
				done, ver, err = doReorgWorkForCreateIndex(w, jobCtx, job, tbl, changingIdxInfos)
				if !done {
					return ver, err
				}
				job.ReorgMeta.Stage = model.ReorgStageModifyColumnCompleted
			case model.ReorgStageModifyColumnCompleted:
				// For multi-schema change, analyze is done by parent job.
				if job.MultiSchemaInfo == nil && checkNeedAnalyze(job, tblInfo) {
					job.ReorgMeta.AnalyzeState = model.AnalyzeStateRunning
				} else {
					job.ReorgMeta.AnalyzeState = model.AnalyzeStateSkipped
					checkAndMarkNonRevertible(job)
				}
			}
		case model.AnalyzeStateRunning:
			intest.Assert(job.MultiSchemaInfo == nil, "multi schema change shouldn't reach here")
			w.startAnalyzeAndWait(job, tblInfo)
		case model.AnalyzeStateDone, model.AnalyzeStateSkipped, model.AnalyzeStateTimeout, model.AnalyzeStateFailed:
			failpoint.InjectCall("afterReorgWorkForModifyColumn")
			reorderChangingIdx(oldIdxInfos, changingIdxInfos)
			oldTp := oldCol.FieldType
			oldName := oldCol.Name
			oldID := oldCol.ID
			tblInfo.Columns[oldCol.Offset] = args.Column.Clone()
			tblInfo.Columns[oldCol.Offset].ChangingFieldType = &oldTp
			tblInfo.Columns[oldCol.Offset].Offset = oldCol.Offset
			tblInfo.Columns[oldCol.Offset].ID = oldID
			tblInfo.Columns[oldCol.Offset].State = model.StatePublic
			oldCol = tblInfo.Columns[oldCol.Offset]

			updateObjectState(nil, oldIdxInfos, model.StateWriteOnly)
			updateObjectState(nil, changingIdxInfos, model.StatePublic)
			moveChangingColumnToDest(tblInfo, oldCol, oldCol, pos)
			moveIndexInfoToDest(tblInfo, oldCol, oldIdxInfos, changingIdxInfos)
			markOldIndexesRemoving(oldIdxInfos, changingIdxInfos)
			for i, idx := range changingIdxInfos {
				for j, idxCol := range idx.Columns {
					if idxCol.Name.L == oldName.L {
						oldIdxInfos[i].Columns[j].Name = colName
						oldIdxInfos[i].Columns[j].Offset = oldCol.Offset
						oldIdxInfos[i].Columns[j].UseChangingType = true
						changingIdxInfos[i].Columns[j].Name = colName
						changingIdxInfos[i].Columns[j].Offset = oldCol.Offset
						changingIdxInfos[i].Columns[j].UseChangingType = false
					}
				}
			}
			job.SchemaState = model.StatePublic
			ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
	case model.StatePublic:
		if len(oldIdxInfos) == 0 {
			// All the old indexes has been deleted by previous modify column,
			// we can just finish the job.
			return finishFunc()
		}

		switch oldIdxInfos[0].State {
		case model.StateWriteOnly:
			updateObjectState(nil, oldIdxInfos, model.StateDeleteOnly)
			return updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		case model.StateDeleteOnly:
			return finishFunc()
		default:
			errMsg := fmt.Sprintf("unexpected column state %s in modify column job", oldCol.State)
			intest.Assert(false, errMsg)
			return ver, errors.Errorf("%s", errMsg)
		}
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("column", oldIdxInfos[0].State)
	}
	return ver, errors.Trace(err)
}

// checkAndMarkNonRevertible should be called when the job is in the final revertible state before public.
func checkAndMarkNonRevertible(job *model.Job) {
	// previously, when reorg is done, and before we set the state to public, we need all other sub-task to
	// be ready as well which is via setting this job as NornRevertible, then we can continue skip current
	// sub and process the others.
	// And when all sub-task are ready, which means each of them could be public in one more ddl round. And
	// in onMultiSchemaChange, we just give all sub-jobs each one more round to public the schema, and only
	// use the schema version generated once.
	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
	}
}

func doReorgWorkForModifyColumn(
	jobCtx *jobContext,
	w *worker, job *model.Job, tbl table.Table,
	oldCol *model.ColumnInfo, elements []*meta.Element,
) (done bool, ver int64, err error) {
	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		err = errors.Trace(err1)
		return
	}
	defer w.sessPool.Put(sctx)
	rh := newReorgHandler(sess.NewSession(sctx))
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfo(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta),
		jobCtx, rh, job, dbInfo, tbl, elements, false)
	if err != nil || reorgInfo == nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		return false, ver, errors.Trace(err)
	}

	// Inject a failpoint so that we can pause here and do verification on other components.
	// With a failpoint-enabled version of TiDB, you can trigger this failpoint by the following command:
	// enable: curl -X PUT -d "pause" "http://127.0.0.1:10080/fail/github.com/pingcap/tidb/pkg/ddl/mockDelayInModifyColumnTypeWithData".
	// disable: curl -X DELETE "http://127.0.0.1:10080/fail/github.com/pingcap/tidb/pkg/ddl/mockDelayInModifyColumnTypeWithData"
	failpoint.Inject("mockDelayInModifyColumnTypeWithData", func() {})
	err = w.runReorgJob(jobCtx, reorgInfo, tbl.Meta(), func() (addIndexErr error) {
		defer util.Recover(metrics.LabelDDL, "onModifyColumn",
			func() {
				addIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("modify table `%v` column `%v` panic", tbl.Meta().Name, oldCol.Name)
			}, false)
		return w.modifyTableColumn(jobCtx, tbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			return false, ver, nil
		}

		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// If timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		if kv.IsTxnRetryableError(err) || dbterror.ErrNotOwner.Equal(err) {
			return false, ver, errors.Trace(err)
		}
		if err1 := rh.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
			logutil.DDLLogger().Warn("run modify column job failed, RemoveDDLReorgHandle failed, can't convert job to rollback",
				zap.String("job", job.String()), zap.Error(err1))
		}
		logutil.DDLLogger().Warn("run modify column job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
		job.State = model.JobStateRollingback
		return false, ver, errors.Trace(err)
	}
	return true, ver, nil
}
