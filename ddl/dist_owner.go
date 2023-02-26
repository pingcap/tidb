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
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// CheckBackfillJobFinishInterval is export for test.
var (
	CheckBackfillJobFinishInterval = 300 * time.Millisecond
	telemetryDistReorgUsage        = metrics.TelemetryDistReorgCnt
)

const (
	distPhysicalTableConcurrency = 16
)

func initDistReorg(reorgMeta *model.DDLReorgMeta) {
	isDistReorg := variable.DDLEnableDistributeReorg.Load()
	reorgMeta.IsDistReorg = isDistReorg
	if isDistReorg {
		metrics.TelemetryDistReorgCnt.Inc()
	}
}

// BackfillJobRangeMeta is export for test.
type BackfillJobRangeMeta struct {
	ID       int64
	PhyTblID int64
	PhyTbl   table.PhysicalTable
	StartKey []byte
	EndKey   []byte
}

func (m *BackfillJobRangeMeta) String() string {
	physicalID := strconv.FormatInt(m.PhyTblID, 10)
	startKey := hex.EncodeToString(m.StartKey)
	endKey := hex.EncodeToString(m.EndKey)
	rangeStr := "taskID_" + strconv.Itoa(int(m.ID)) + "_physicalTableID_" + physicalID + "_" + "[" + startKey + "," + endKey + ")"
	return rangeStr
}

type splitJobContext struct {
	ctx               context.Context
	cancel            context.CancelFunc
	isMultiPhyTbl     bool
	bfWorkerType      backfillerType
	isUnique          bool
	batchSize         int
	minBatchSize      int
	currBackfillJobID *atomicutil.Int64
	currPhysicalID    int64
	phyTblMetaCh      chan *BackfillJobRangeMeta
	resultCh          chan error
}

func getRunningPhysicalTableMetas(sess *session, sJobCtx *splitJobContext, reorgInfo *reorgInfo) ([]*BackfillJobRangeMeta, error) {
	ddlJobID, eleID, eleKey, currPID := reorgInfo.Job.ID, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey, reorgInfo.PhysicalTableID
	pTblMetas, err := GetPhysicalTableMetas(sess, ddlJobID, eleID, eleKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	currBfJobID := int64(1)
	physicalTIDs := make([]int64, 0, len(pTblMetas))
	phyTblMetas := make([]*BackfillJobRangeMeta, 0, len(pTblMetas))
	if len(pTblMetas) == 0 {
		bfJM := &BackfillJobRangeMeta{PhyTblID: currPID, StartKey: reorgInfo.StartKey, EndKey: reorgInfo.EndKey}
		phyTblMetas = append(phyTblMetas, bfJM)
		physicalTIDs = append(physicalTIDs, bfJM.PhyTblID)
	} else {
		for _, pMeta := range pTblMetas {
			phyTblMetas = append(phyTblMetas, pMeta)
			currPID = mathutil.Max(pMeta.PhyTblID, currPID)
			currBfJobID = mathutil.Max(pMeta.ID, currBfJobID)
			physicalTIDs = append(physicalTIDs, pMeta.PhyTblID)
		}
	}
	sJobCtx.currPhysicalID = currPID
	sJobCtx.currBackfillJobID = atomicutil.NewInt64(currBfJobID)
	logutil.BgLogger().Info("[ddl] unprocessed physical table ranges get from table", zap.Int64("jobID", ddlJobID),
		zap.Int64("eleID", eleID), zap.ByteString("eleKey", eleKey),
		zap.Int64("currPID", sJobCtx.currPhysicalID), zap.Int64s("phyTblIDs", physicalTIDs))
	return phyTblMetas, nil
}

func (dc *ddlCtx) sendPhysicalTableMetas(reorgInfo *reorgInfo, t table.Table, sJobCtx *splitJobContext, runningPTblMetas []*BackfillJobRangeMeta) {
	var err error
	physicalTIDs := make([]int64, 0, distPhysicalTableConcurrency)
	defer func() {
		logutil.BgLogger().Info("[ddl] send physical table ranges to split finished", zap.Int64("jobID", reorgInfo.Job.ID),
			zap.Stringer("ele", reorgInfo.currElement), zap.Int64s("phyTblIDs", physicalTIDs), zap.Error(err))
		if err != nil {
			sJobCtx.cancel()
		} else {
			close(sJobCtx.phyTblMetaCh)
		}
	}()

	for _, pTblM := range runningPTblMetas {
		err = dc.isReorgRunnable(reorgInfo.Job.ID, false)
		if err != nil {
			return
		}

		if tbl, ok := t.(table.PartitionedTable); ok {
			pTblM.PhyTbl = tbl.GetPartition(pTblM.PhyTblID)
			sJobCtx.phyTblMetaCh <- pTblM
		} else {
			//nolint:forcetypeassert
			phyTbl := t.(table.PhysicalTable)
			pTblM.PhyTbl = phyTbl
			sJobCtx.phyTblMetaCh <- pTblM
		}
		physicalTIDs = append(physicalTIDs, pTblM.PhyTblID)
	}

	if tbl, ok := t.(table.PartitionedTable); ok {
		currPhysicalID := sJobCtx.currPhysicalID
		for {
			err = dc.isReorgRunnable(reorgInfo.Job.ID, false)
			if err != nil {
				return
			}
			select {
			case <-sJobCtx.ctx.Done():
				err = sJobCtx.ctx.Err()
				return
			default:
			}

			pID, startKey, endKey, err1 := getNextPartitionInfo(reorgInfo, tbl, currPhysicalID)
			if err1 != nil {
				err = err1
				return
			}
			if pID == 0 {
				// Next partition does not exist, all the job done.
				return
			}
			pTbl := tbl.GetPartition(pID)
			if pTbl == nil {
				err = dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", pID, t.Meta().ID)
				return
			}
			bfJM := &BackfillJobRangeMeta{PhyTblID: pID, PhyTbl: pTbl, StartKey: startKey, EndKey: endKey}
			sJobCtx.phyTblMetaCh <- bfJM
			currPhysicalID = pID

			physicalTIDs = append(physicalTIDs, pID)
		}
	}
}

func (dc *ddlCtx) controlWriteTableRecord(sessPool *sessionPool, t table.Table, bfWorkerType backfillerType, reorgInfo *reorgInfo) error {
	startKey, endKey := reorgInfo.StartKey, reorgInfo.EndKey
	if startKey == nil && endKey == nil {
		return nil
	}

	ddlJobID := reorgInfo.Job.ID
	currEle := reorgInfo.currElement
	logutil.BgLogger().Info("[ddl] control write table record start",
		zap.Int64("jobID", ddlJobID), zap.Stringer("ele", currEle),
		zap.Int64("tblID", t.Meta().ID), zap.Int64("currPID", reorgInfo.PhysicalTableID))
	sCtx, err := sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer sessPool.put(sCtx)
	sess := newSession(sCtx)

	if err := dc.isReorgRunnable(ddlJobID, true); err != nil {
		return errors.Trace(err)
	}
	var isUnique bool
	if bfWorkerType == typeAddIndexWorker {
		idxInfo := model.FindIndexInfoByID(t.Meta().Indices, currEle.ID)
		isUnique = idxInfo.Unique
	}

	wg := tidbutil.WaitGroupWrapper{}
	sJobCtx := &splitJobContext{
		bfWorkerType: bfWorkerType,
		isUnique:     isUnique,
		batchSize:    genTaskBatch,
		minBatchSize: minGenTaskBatch,
		phyTblMetaCh: make(chan *BackfillJobRangeMeta, 1),
		resultCh:     make(chan error, distPhysicalTableConcurrency),
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	sJobCtx.ctx, sJobCtx.cancel = context.WithCancel(ctx)
	concurrency := 1
	if tbl, ok := t.(table.PartitionedTable); ok {
		ids := len(tbl.GetAllPartitionIDs())
		if ids > 1 {
			sJobCtx.isMultiPhyTbl = true
			concurrency = ids
		}
		if ids > distPhysicalTableConcurrency {
			concurrency = distPhysicalTableConcurrency
		}
		sJobCtx.batchSize = genPhysicalTableTaskBatch
		sJobCtx.minBatchSize = minGenPhysicalTableTaskBatch
	}

	err = checkAndHandleInterruptedBackfillJobs(sess, ddlJobID, currEle.ID, currEle.TypeKey)
	if err != nil {
		return errors.Trace(err)
	}
	phyTblMetas, err := getRunningPhysicalTableMetas(sess, sJobCtx, reorgInfo)
	if err != nil {
		return err
	}

	sCtxs := make([]sessionctx.Context, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		sCtx, err := sessPool.get()
		if err != nil {
			return err
		}
		sCtxs = append(sCtxs, sCtx)
	}

	wg.Run(func() {
		defer tidbutil.Recover(metrics.LabelDistReorg, "sendPhysicalTableMeta", nil, false)
		dc.sendPhysicalTableMetas(reorgInfo, t, sJobCtx, phyTblMetas)
	})
	for _, sCtx := range sCtxs {
		func(ctx sessionctx.Context) {
			wg.Run(func() {
				defer func() {
					tidbutil.Recover(metrics.LabelDistReorg, "splitTableToBackfillJobs", nil, false)
				}()
				se := newSession(ctx)
				dc.splitPhysicalTableToBackfillJobs(se, reorgInfo, sJobCtx)
			})
		}(sCtx)
	}
	wg.Wait()
	for _, sCtx := range sCtxs {
		sessPool.put(sCtx)
	}
	return checkReorgJobFinished(dc.ctx, sess, &dc.reorgCtx, ddlJobID, currEle)
}

func addBatchBackfillJobs(sess *session, reorgInfo *reorgInfo, sJobCtx *splitJobContext, phyTblID int64, notDistTask bool,
	batchTasks []*reorgBackfillTask, bJobs []*BackfillJob) error {
	bJobs = bJobs[:0]
	instanceID := ""
	if notDistTask {
		instanceID = reorgInfo.d.uuid
	}

	// TODO: Adjust the number of ranges(region) for each task.
	for _, task := range batchTasks {
		bm := &model.BackfillMeta{
			IsUnique:   sJobCtx.isUnique,
			EndInclude: task.endInclude,
			ReorgTp:    reorgInfo.Job.ReorgMeta.ReorgTp,
			SQLMode:    reorgInfo.ReorgMeta.SQLMode,
			Location:   reorgInfo.ReorgMeta.Location,
			JobMeta: &model.JobMeta{
				SchemaID: reorgInfo.Job.SchemaID,
				TableID:  reorgInfo.Job.TableID,
				Type:     reorgInfo.Job.Type,
				Query:    reorgInfo.Job.Query,
			},
			StartKey: task.startKey,
			EndKey:   task.endKey,
		}
		bj := &BackfillJob{
			ID:              sJobCtx.currBackfillJobID.Add(1),
			JobID:           reorgInfo.Job.ID,
			EleID:           reorgInfo.currElement.ID,
			EleKey:          reorgInfo.currElement.TypeKey,
			PhysicalTableID: phyTblID,
			Tp:              sJobCtx.bfWorkerType,
			State:           model.JobStateNone,
			InstanceID:      instanceID,
			Meta:            bm,
		}
		bj.Meta.CurrKey = task.startKey
		bJobs = append(bJobs, bj)
	}
	if err := AddBackfillJobs(sess, bJobs); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (dc *ddlCtx) splitTableToBackfillJobs(sess *session, reorgInfo *reorgInfo, sJobCtx *splitJobContext, pTblMeta *BackfillJobRangeMeta) error {
	isFirstOps := !sJobCtx.isMultiPhyTbl
	batchSize := sJobCtx.batchSize
	startKey, endKey := pTblMeta.StartKey, pTblMeta.EndKey
	bJobs := make([]*BackfillJob, 0, batchSize)
	for {
		kvRanges, err := splitTableRanges(pTblMeta.PhyTbl, reorgInfo.d.store, startKey, endKey, batchSize)
		if err != nil {
			return errors.Trace(err)
		}
		batchTasks := getBatchTasks(pTblMeta.PhyTbl, reorgInfo, kvRanges, batchSize)
		if len(batchTasks) == 0 {
			break
		}
		notNeedDistProcess := isFirstOps && (len(kvRanges) < minDistTaskCnt)
		if err = addBatchBackfillJobs(sess, reorgInfo, sJobCtx, pTblMeta.PhyTblID, notNeedDistProcess, batchTasks, bJobs); err != nil {
			return errors.Trace(err)
		}
		isFirstOps = false

		remains := kvRanges[len(batchTasks):]
		dc.asyncNotifyWorker(dc.backfillJobCh, addingBackfillJob, reorgInfo.Job.ID, "backfill_job")
		logutil.BgLogger().Info("[ddl] split backfill jobs to the backfill table",
			zap.Int64("physicalID", pTblMeta.PhyTblID),
			zap.Int("batchTasksCnt", len(batchTasks)),
			zap.Int("totalRegionCnt", len(kvRanges)),
			zap.Int("remainRegionCnt", len(remains)),
			zap.String("startHandle", hex.EncodeToString(startKey)),
			zap.String("endHandle", hex.EncodeToString(endKey)))

		if len(remains) == 0 {
			break
		}

		for {
			bJobCnt, err := checkBackfillJobCount(sess, reorgInfo.Job.ID, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey)
			if err != nil {
				return errors.Trace(err)
			}
			if bJobCnt < sJobCtx.minBatchSize {
				break
			}
			time.Sleep(RetrySQLInterval)
		}
		startKey = remains[0].StartKey
	}
	return nil
}

func (dc *ddlCtx) splitPhysicalTableToBackfillJobs(sess *session, reorgInfo *reorgInfo, sJobCtx *splitJobContext) {
	defaultSQLMode := sess.GetSessionVars().SQLMode
	defer func() { sess.GetSessionVars().SQLMode = defaultSQLMode }()
	// Make timestamp type can be inserted ZeroTimestamp.
	sess.GetSessionVars().SQLMode = mysql.ModeNone

	var err error
	var pTblMetaCnt int
	var pTblMeta *BackfillJobRangeMeta
	defer func() {
		if err != nil {
			sJobCtx.cancel()
		}
		logutil.BgLogger().Info("[ddl] split backfill jobs to table finish", zap.Int64("jobID", reorgInfo.Job.ID),
			zap.Stringer("ele", reorgInfo.currElement), zap.Int("donePTbls", pTblMetaCnt), zap.Stringer("physical_tbl", pTblMeta), zap.Error(err))
	}()

	var ok bool
	for {
		select {
		case <-sJobCtx.ctx.Done():
			err = sJobCtx.ctx.Err()
		case pTblMeta, ok = <-sJobCtx.phyTblMetaCh:
			if !ok {
				return
			}
			if err = dc.isReorgRunnable(reorgInfo.Job.ID, false); err != nil {
				return
			}

			err = dc.splitTableToBackfillJobs(sess, reorgInfo, sJobCtx, pTblMeta)
			if err != nil {
				return
			}
			pTblMetaCnt++
		}
	}
}

func checkReorgJobFinished(ctx context.Context, sess *session, reorgCtxs *reorgContexts, ddlJobID int64, currEle *meta.Element) error {
	var times int64
	var bfJob *BackfillJob
	var backfillJobFinished bool
	ticker := time.NewTicker(CheckBackfillJobFinishInterval)
	defer ticker.Stop()
	bjPrefixKey := backfillJobPrefixKeyString(ddlJobID, currEle.TypeKey, currEle.ID)
	for {
		failpoint.Inject("MockCanceledErr", func() {
			getReorgCtx(reorgCtxs, ddlJobID).notifyReorgCancel()
		})
		if getReorgCtx(reorgCtxs, ddlJobID).isReorgCanceled() {
			err := cleanupBackfillJobs(sess, bjPrefixKey)
			if err != nil {
				return err
			}
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
					logutil.BgLogger().Warn("[ddl] finish interrupted backfill jobs", zap.Int64("job ID", ddlJobID), zap.Stringer("ele", currEle), zap.Error(err))
					return errors.Trace(err)
				}

				bfJobs, err := getBackfillJobWithRetry(sess, BackgroundSubtaskTable, bjPrefixKey)
				if err != nil {
					logutil.BgLogger().Info("[ddl] getBackfillJobWithRetry failed", zap.Int64("job ID", ddlJobID), zap.Stringer("ele", currEle), zap.Error(err))
					return errors.Trace(err)
				}
				if len(bfJobs) == 0 {
					backfillJobFinished = true
					logutil.BgLogger().Info("[ddl] finish all backfill jobs", zap.Int64("job ID", ddlJobID), zap.Stringer("ele", currEle))
				}
			}
			if backfillJobFinished {
				// TODO: Consider whether these backfill jobs are always out of sync.
				isSynced, err := checkJobIsFinished(sess, ddlJobID)
				if err != nil {
					logutil.BgLogger().Warn("[ddl] checkJobIsFinished failed", zap.Int64("job ID", ddlJobID), zap.Stringer("ele", currEle), zap.Error(err))
					return errors.Trace(err)
				}
				if isSynced {
					logutil.BgLogger().Info("[ddl] finish all backfill jobs and put them to history", zap.Int64("job ID", ddlJobID), zap.Stringer("ele", currEle))
					return GetBackfillErr(sess, bjPrefixKey)
				}
			}
		case <-ctx.Done():
			err := cleanupBackfillJobs(sess, bjPrefixKey)
			if err != nil {
				return err
			}
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

		time.Sleep(RetrySQLInterval)
	}
	logutil.BgLogger().Info("[ddl] checkJobIsSynced failed",
		zap.Strings("unsyncedInstanceIDs", unsyncedInstanceIDs), zap.Int("tryTimes", retrySQLTimes), zap.Error(err))

	return false, errors.Trace(err)
}

// GetBackfillErr gets the error in backfill job.
func GetBackfillErr(sess *session, bjPrefixKey string) error {
	var err error
	var metas []*model.BackfillMeta
	for i := 0; i < retrySQLTimes; i++ {
		metas, err = GetBackfillMetas(sess, BackgroundSubtaskHistoryTable, fmt.Sprintf("task_key like '%s'", bjPrefixKey), "get_backfill_job_metas")
		if err == nil {
			for _, m := range metas {
				if m.Error != nil {
					return m.Error
				}
			}
			return nil
		}

		logutil.BgLogger().Info("[ddl] GetBackfillMetas failed in checkJobIsSynced", zap.Int("tryTimes", i), zap.Error(err))
		time.Sleep(RetrySQLInterval)
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
		time.Sleep(RetrySQLInterval)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if len(bJobs) == 0 {
		return nil
	}

	return cleanupBackfillJobs(sess, bJobs[0].PrefixKeyString())
}

func cleanupBackfillJobs(sess *session, prefixKey string) error {
	var err error
	for i := 0; i < retrySQLTimes; i++ {
		err = MoveBackfillJobsToHistoryTable(sess, prefixKey)
		if err == nil {
			return nil
		}
		logutil.BgLogger().Info("[ddl] MoveBackfillJobsToHistoryTable failed", zap.Error(err))
		time.Sleep(RetrySQLInterval)
	}
	return err
}

func checkBackfillJobCount(sess *session, ddlJobID, currEleID int64, currEleKey []byte) (backfillJobCnt int, err error) {
	err = checkAndHandleInterruptedBackfillJobs(sess, ddlJobID, currEleID, currEleKey)
	if err != nil {
		return 0, errors.Trace(err)
	}

	backfillJobCnt, err = GetBackfillJobCount(sess, BackgroundSubtaskTable,
		fmt.Sprintf("task_key like '%s'", backfillJobPrefixKeyString(ddlJobID, currEleKey, currEleID)), "check_backfill_job_count")
	if err != nil {
		return 0, errors.Trace(err)
	}

	return backfillJobCnt, nil
}

func getBackfillJobWithRetry(sess *session, tableName, bjPrefixKey string) ([]*BackfillJob, error) {
	var err error
	var bJobs []*BackfillJob
	for i := 0; i < retrySQLTimes; i++ {
		bJobs, err = GetBackfillJobs(sess, tableName, fmt.Sprintf("task_key like '%s' limit 1", bjPrefixKey), "check_backfill_job_state")
		if err != nil {
			logutil.BgLogger().Warn("[ddl] GetBackfillJobs failed", zap.Error(err))
			time.Sleep(RetrySQLInterval)
			continue
		}
		return bJobs, nil
	}
	return nil, errors.Trace(err)
}

// GetPhysicalTableMetas gets the max backfill metas per physical table in BackgroundSubtaskTable and BackgroundSubtaskHistoryTable.
func GetPhysicalTableMetas(sess *session, ddlJobID, currEleID int64, currEleKey []byte) (map[int64]*BackfillJobRangeMeta, error) {
	condition := fmt.Sprintf("task_key like '%s'", backfillJobPrefixKeyString(ddlJobID, currEleKey, currEleID))
	pTblMs, err := GetBackfillIDAndMetas(sess, BackgroundSubtaskTable, condition, "get_ptbl_metas")
	if err != nil {
		return nil, errors.Trace(err)
	}
	hPTblMs, err := GetBackfillIDAndMetas(sess, BackgroundSubtaskHistoryTable, condition, "get_ptbl_metas")
	if err != nil {
		return nil, errors.Trace(err)
	}
	metaMap := make(map[int64]*BackfillJobRangeMeta, len(pTblMs)+len(hPTblMs))
	for _, m := range pTblMs {
		metaMap[m.PhyTblID] = m
	}
	for _, m := range hPTblMs {
		val, ok := metaMap[m.PhyTblID]
		if !ok || (ok && m.ID > val.ID) {
			metaMap[m.PhyTblID] = m
		}
	}
	return metaMap, nil
}

// MoveBackfillJobsToHistoryTable moves backfill table jobs to the backfill history table.
func MoveBackfillJobsToHistoryTable(sctx sessionctx.Context, prefixKey string) error {
	s, ok := sctx.(*session)
	if !ok {
		return errors.Errorf("sess ctx:%#v convert session failed", sctx)
	}

	return s.runInTxn(func(se *session) error {
		// TODO: Consider batch by batch update backfill jobs and insert backfill history jobs.
		bJobs, err := GetBackfillJobs(se, BackgroundSubtaskTable, fmt.Sprintf("task_key like '%s'", prefixKey), "update_backfill_job")
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
				bj.StateUpdateTS = startTS
			}
			err = AddBackfillHistoryJob(se, bJobs)
		}
		logutil.BgLogger().Info("[ddl] move backfill jobs to history table", zap.Int("job count", len(bJobs)))
		return errors.Trace(err)
	})
}
