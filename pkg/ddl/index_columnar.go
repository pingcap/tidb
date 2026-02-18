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
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
	"go.uber.org/zap"
)

func (w *worker) onCreateColumnarIndex(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropIndex(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if err := checkTableTypeForColumnarIndex(tblInfo); err != nil {
		return ver, errors.Trace(err)
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	a := args.IndexArgs[0]
	columnarIndexType := a.GetColumnarIndexType()
	if columnarIndexType == model.ColumnarIndexTypeVector {
		a.IndexPartSpecifications[0].Expr, err = generatedexpr.ParseExpression(a.FuncExpr)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		defer func() {
			a.IndexPartSpecifications[0].Expr = nil
		}()
	}

	indexInfo, err := checkAndBuildIndexInfo(job, tblInfo, columnarIndexType, false, a)
	if err != nil {
		return ver, errors.Trace(err)
	}
	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StateNone:
		// none -> delete only
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		indexInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		indexInfo.State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		if job.IsCancelling() {
			return convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), []*model.IndexInfo{indexInfo}, dbterror.ErrCancelledDDLJob)
		}

		// Send sync schema notification to TiFlash.
		if job.SnapshotVer == 0 {
			currVer, err := getValidCurrentVersion(jobCtx.store)
			if err != nil {
				return ver, errors.Trace(err)
			}
			err = infosync.SyncTiFlashTableSchema(jobCtx.stepCtx, tbl.Meta().ID)
			if err != nil {
				return ver, errors.Trace(err)
			}
			job.SnapshotVer = currVer.Ver
			return ver, nil
		}

		// Check the progress of the TiFlash backfill index.
		var done bool
		done, ver, err = w.checkColumnarIndexProcessOnTiFlash(jobCtx, job, tbl, indexInfo)
		if err != nil || !done {
			return ver, err
		}

		indexInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		finishedArgs := &model.ModifyIndexArgs{
			IndexArgs:    []*model.IndexArg{{IndexID: indexInfo.ID}},
			PartitionIDs: getPartitionIDs(tblInfo),
			OpType:       model.OpAddIndex,
		}
		job.FillFinishedArgs(finishedArgs)

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		logutil.DDLLogger().Info("[ddl] run add columnar index job done",
			zap.Int64("ver", ver),
			zap.String("charset", job.Charset),
			zap.String("collation", job.Collate))
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", indexInfo.State)
	}

	return ver, errors.Trace(err)
}

func (w *worker) checkColumnarIndexProcessOnTiFlash(jobCtx *jobContext, job *model.Job, tbl table.Table, indexInfo *model.IndexInfo,
) (done bool, ver int64, err error) {
	err = w.checkColumnarIndexProcess(jobCtx, tbl, job, indexInfo)
	if err != nil {
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			return false, ver, nil
		}
		if !isRetryableJobError(err, job.ErrorCount) {
			logutil.DDLLogger().Warn("run add columnar index job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), []*model.IndexInfo{indexInfo}, err)
		}
		return false, ver, errors.Trace(err)
	}

	return true, ver, nil
}

func (w *worker) checkColumnarIndexProcess(jobCtx *jobContext, tbl table.Table, job *model.Job, index *model.IndexInfo) error {
	waitTimeout := 500 * time.Millisecond
	ticker := time.NewTicker(waitTimeout)
	defer ticker.Stop()
	notAddedRowCnt := int64(-1)
	for {
		select {
		case <-w.ddlCtx.ctx.Done():
			return dbterror.ErrInvalidWorker.GenWithStack("worker is closed")
		case <-ticker.C:
			logutil.DDLLogger().Info(
				"index backfill state running, check columnar index process",
				zap.Stringer("job", job),
				zap.Stringer("index name", index.Name),
				zap.Int64("index ID", index.ID),
				zap.Duration("wait time", waitTimeout),
				zap.Int64("total added row count", job.RowCount),
				zap.Int64("not added row count", notAddedRowCnt))
			return dbterror.ErrWaitReorgTimeout
		default:
		}

		if !w.ddlCtx.isOwner() {
			// If it's not the owner, we will try later, so here just returns an error.
			logutil.DDLLogger().Info("DDL is not the DDL owner", zap.String("ID", w.ddlCtx.uuid))
			return errors.Trace(dbterror.ErrNotOwner)
		}

		isDone, notAddedIndexCnt, addedIndexCnt, err := w.checkColumnarIndexProcessOnce(jobCtx, tbl, index.ID)
		if err != nil {
			return errors.Trace(err)
		}
		notAddedRowCnt = notAddedIndexCnt
		job.RowCount = addedIndexCnt

		if isDone {
			break
		}
	}
	return nil
}

// checkColumnarIndexProcessOnce checks the backfill process of a columnar index from TiFlash once.
func (w *worker) checkColumnarIndexProcessOnce(jobCtx *jobContext, tbl table.Table, indexID int64) (
	isDone bool, notAddedIndexCnt, addedIndexCnt int64, err error) {
	failpoint.Inject("MockCheckColumnarIndexProcess", func(val failpoint.Value) {
		if valInt, ok := val.(int); ok {
			logutil.DDLLogger().Info("MockCheckColumnarIndexProcess", zap.Int("val", valInt))
			if valInt < 0 {
				failpoint.Return(false, 0, 0, dbterror.ErrTiFlashBackfillIndex.FastGenByArgs("mock a check error"))
			} else if valInt == 0 {
				failpoint.Return(false, 0, 0, nil)
			} else {
				failpoint.Return(true, 0, int64(valInt), nil)
			}
		}
	})

	sql := fmt.Sprintf("select rows_stable_not_indexed, rows_stable_indexed, error_message from information_schema.tiflash_indexes where table_id = %d and index_id = %d;",
		tbl.Meta().ID, indexID)
	rows, err := w.sess.Execute(jobCtx.stepCtx, sql, "add_vector_index_check_result")
	if err != nil || len(rows) == 0 {
		return false, 0, 0, errors.Trace(err)
	}

	// Get and process info from multiple TiFlash nodes.
	errMsg := ""
	for _, row := range rows {
		notAddedIndexCnt += row.GetInt64(0)
		addedIndexCnt += row.GetInt64(1)
		errMsg = row.GetString(2)
		if len(errMsg) != 0 {
			err = dbterror.ErrTiFlashBackfillIndex.FastGenByArgs(errMsg)
			break
		}
	}
	if err != nil {
		return false, 0, 0, errors.Trace(err)
	}
	if notAddedIndexCnt != 0 {
		return false, 0, 0, nil
	}

	return true, notAddedIndexCnt, addedIndexCnt, nil
}
