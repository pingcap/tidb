// Copyright 2023 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var checkBackfillJobFinishInterval = 300 * time.Millisecond

func (dc *ddlCtx) controlWritePhysicalTableRecord(sess *session, t table.PhysicalTable, bfWorkerType backfillerType, reorgInfo *reorgInfo) error {
	startKey, endKey := reorgInfo.StartKey, reorgInfo.EndKey
	if startKey == nil && endKey == nil {
		return nil
	}

	ddlJobID := reorgInfo.Job.ID
	if err := dc.isReorgRunnable(ddlJobID, true); err != nil {
		return errors.Trace(err)
	}

	currEle := reorgInfo.currElement
	defaultSQLMode := sess.GetSessionVars().SQLMode
	defer func() {
		sess.GetSessionVars().SQLMode = defaultSQLMode
	}()
	// Make timestamp type can be inserted ZeroTimestamp.
	sess.GetSessionVars().SQLMode = mysql.ModeNone
	currBackfillJobID := int64(1)
	err := checkAndHandleInterruptedBackfillJobs(sess, ddlJobID, currEle.ID, currEle.TypeKey)
	if err != nil {
		return errors.Trace(err)
	}
	maxBfJob, err := GetMaxBackfillJob(sess, ddlJobID, currEle.ID, currEle.TypeKey)
	if err != nil {
		return errors.Trace(err)
	}
	if maxBfJob != nil {
		startKey = maxBfJob.EndKey
		currBackfillJobID = maxBfJob.ID + 1
	}

	var isUnique bool
	if bfWorkerType == typeAddIndexWorker {
		idxInfo := model.FindIndexInfoByID(t.Meta().Indices, currEle.ID)
		isUnique = idxInfo.Unique
	}
	err = dc.splitTableToBackfillJobs(sess, reorgInfo, t, isUnique, bfWorkerType, startKey, currBackfillJobID)
	if err != nil {
		return errors.Trace(err)
	}
	dc.ctx.Done()
	return checkReorgJobFinished(dc.ctx, sess, &dc.reorgCtx, ddlJobID, currEle)
}

func checkReorgJobFinished(ctx context.Context, sess *session, reorgCtxs *reorgContexts, ddlJobID int64, currEle *meta.Element) error {
	var times int64
	var bfJob *BackfillJob
	var backfillJobFinished bool
	ticker := time.NewTicker(checkBackfillJobFinishInterval)
	defer ticker.Stop()
	for {
		if getReorgCtx(reorgCtxs, ddlJobID).isReorgCanceled() {
			// Job is cancelled. So it can't be done.
			return dbterror.ErrCancelledDDLJob
		}

		select {
		case <-ticker.C:
			times++
			// Print this log every 5 min.
			if times%1000 == 0 {
				logutil.BgLogger().Info("[ddl] check all backfill jobs is finished",
					zap.Int64("job ID", ddlJobID), zap.Bool("isFinished", backfillJobFinished), zap.Reflect("bfJob", bfJob))
			}
			if !backfillJobFinished {
				err := checkAndHandleInterruptedBackfillJobs(sess, ddlJobID, currEle.ID, currEle.TypeKey)
				if err != nil {
					logutil.BgLogger().Warn("[ddl] finish interrupted backfill jobs", zap.Int64("job ID", ddlJobID), zap.Error(err))
					return errors.Trace(err)
				}

				bfJob, err = getBackfillJobWithRetry(sess, BackfillTable, ddlJobID, currEle.ID, currEle.TypeKey, false)
				if err != nil {
					logutil.BgLogger().Info("[ddl] getBackfillJobWithRetry failed", zap.Int64("job ID", ddlJobID), zap.Error(err))
					return errors.Trace(err)
				}
				if bfJob == nil {
					backfillJobFinished = true
					logutil.BgLogger().Info("[ddl] finish all backfill jobs", zap.Int64("job ID", ddlJobID))
				}
			}
			if backfillJobFinished {
				// TODO: Consider whether these backfill jobs are always out of sync.
				isSynced, err := checkJobIsFinished(sess, ddlJobID)
				if err != nil {
					logutil.BgLogger().Warn("[ddl] checkJobIsFinished failed", zap.Int64("job ID", ddlJobID), zap.Error(err))
					return errors.Trace(err)
				}
				if isSynced {
					logutil.BgLogger().Info("[ddl] finish all backfill jobs and put them to history", zap.Int64("job ID", ddlJobID))
					return GetBackfillErr(sess, ddlJobID, currEle.ID, currEle.TypeKey)
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func checkJobIsFinished(sess *session, ddlJobID int64) (bool, error) {
	var err error
	var unsyncedInstanceIDs []string
	for i := 0; i < retrySQLTimes; i++ {
		unsyncedInstanceIDs, err = getUnsyncedInstanceIDs(sess, ddlJobID, "check_backfill_history_job_sync")
		if err == nil && len(unsyncedInstanceIDs) == 0 {
			return true, nil
		}

		logutil.BgLogger().Info("[ddl] checkJobIsSynced failed",
			zap.Strings("unsyncedInstanceIDs", unsyncedInstanceIDs), zap.Int("tryTimes", i), zap.Error(err))
		time.Sleep(retrySQLInterval)
	}

	return false, errors.Trace(err)
}

// GetBackfillErr gets the error in backfill job.
func GetBackfillErr(sess *session, ddlJobID, currEleID int64, currEleKey []byte) error {
	var err error
	var metas []*model.BackfillMeta
	for i := 0; i < retrySQLTimes; i++ {
		metas, err = GetBackfillMetas(sess, BackfillHistoryTable, fmt.Sprintf("ddl_job_id = %d and ele_id = %d and ele_key = %s",
			ddlJobID, currEleID, wrapKey2String(currEleKey)), "get_backfill_job_metas")
		if err == nil {
			for _, m := range metas {
				if m.Error != nil {
					return m.Error
				}
			}
			return nil
		}

		logutil.BgLogger().Info("[ddl] GetBackfillMetas failed in checkJobIsSynced", zap.Int("tryTimes", i), zap.Error(err))
		time.Sleep(retrySQLInterval)
	}

	return errors.Trace(err)
}

func checkAndHandleInterruptedBackfillJobs(sess *session, ddlJobID, currEleID int64, currEleKey []byte) (err error) {
	var bJobs []*BackfillJob
	for i := 0; i < retrySQLTimes; i++ {
		bJobs, err = GetInterruptedBackfillJobForOneEle(sess, ddlJobID, currEleID, currEleKey)
		if err == nil {
			break
		}
		logutil.BgLogger().Info("[ddl] getInterruptedBackfillJobsForOneEle failed", zap.Error(err))
		time.Sleep(retrySQLInterval)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if len(bJobs) == 0 {
		return nil
	}

	for i := 0; i < retrySQLTimes; i++ {
		err = MoveBackfillJobsToHistoryTable(sess, bJobs[0])
		if err == nil {
			return bJobs[0].Meta.Error
		}
		logutil.BgLogger().Info("[ddl] MoveBackfillJobsToHistoryTable failed", zap.Error(err))
		time.Sleep(retrySQLInterval)
	}
	return errors.Trace(err)
}

func checkBackfillJobCount(sess *session, ddlJobID, currEleID int64, currEleKey []byte) (backfillJobCnt int, err error) {
	err = checkAndHandleInterruptedBackfillJobs(sess, ddlJobID, currEleID, currEleKey)
	if err != nil {
		return 0, errors.Trace(err)
	}

	backfillJobCnt, err = GetBackfillJobCount(sess, BackfillTable, fmt.Sprintf("ddl_job_id = %d and ele_id = %d and ele_key = %s",
		ddlJobID, currEleID, wrapKey2String(currEleKey)), "check_backfill_job_count")
	if err != nil {
		return 0, errors.Trace(err)
	}

	return backfillJobCnt, nil
}

func getBackfillJobWithRetry(sess *session, tableName string, ddlJobID, currEleID int64, currEleKey []byte, isDesc bool) (*BackfillJob, error) {
	var err error
	var bJobs []*BackfillJob
	descStr := ""
	if isDesc {
		descStr = "order by id desc"
	}
	for i := 0; i < retrySQLTimes; i++ {
		bJobs, err = GetBackfillJobs(sess, tableName, fmt.Sprintf("ddl_job_id = %d and ele_id = %d and ele_key = %s %s limit 1",
			ddlJobID, currEleID, wrapKey2String(currEleKey), descStr), "check_backfill_job_state")
		if err != nil {
			logutil.BgLogger().Warn("[ddl] GetBackfillJobs failed", zap.Error(err))
			continue
		}

		if len(bJobs) != 0 {
			return bJobs[0], nil
		}
		break
	}
	return nil, errors.Trace(err)
}

// GetMaxBackfillJob gets the max backfill job in BackfillTable and BackfillHistoryTable.
func GetMaxBackfillJob(sess *session, ddlJobID, currEleID int64, currEleKey []byte) (*BackfillJob, error) {
	bfJob, err := getBackfillJobWithRetry(sess, BackfillTable, ddlJobID, currEleID, currEleKey, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	hJob, err := getBackfillJobWithRetry(sess, BackfillHistoryTable, ddlJobID, currEleID, currEleKey, true)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if bfJob == nil {
		return hJob, nil
	}
	if hJob == nil {
		return bfJob, nil
	}
	if bfJob.ID > hJob.ID {
		return bfJob, nil
	}
	return hJob, nil
}

// MoveBackfillJobsToHistoryTable moves backfill table jobs to the backfill history table.
func MoveBackfillJobsToHistoryTable(sctx sessionctx.Context, bfJob *BackfillJob) error {
	s, ok := sctx.(*session)
	if !ok {
		return errors.Errorf("sess ctx:%#v convert session failed", sctx)
	}

	return s.runInTxn(func(se *session) error {
		// TODO: Consider batch by batch update backfill jobs and insert backfill history jobs.
		bJobs, err := GetBackfillJobs(se, BackfillTable, fmt.Sprintf("ddl_job_id = %d and ele_id = %d and ele_key = %s",
			bfJob.JobID, bfJob.EleID, wrapKey2String(bfJob.EleKey)), "update_backfill_job")
		if err != nil {
			return errors.Trace(err)
		}
		if len(bJobs) == 0 {
			return nil
		}

		txn, err := se.txn()
		if err != nil {
			return errors.Trace(err)
		}
		startTS := txn.StartTS()
		err = RemoveBackfillJob(se, true, bJobs[0])
		if err == nil {
			for _, bj := range bJobs {
				bj.State = model.JobStateCancelled
				bj.FinishTS = startTS
			}
			err = AddBackfillHistoryJob(se, bJobs)
		}
		logutil.BgLogger().Info("[ddl] move backfill jobs to history table", zap.Int("job count", len(bJobs)))
		return errors.Trace(err)
	})
}
