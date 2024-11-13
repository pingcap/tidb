// Copyright 2022 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/syncer"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	tidb_util "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	addingDDLJobConcurrent      = "/tidb/ddl/add_ddl_job_general"
	dispatchLoopWaitingDuration = 1 * time.Second
	localWorkerWaitingDuration  = 10 * time.Millisecond
)

func init() {
	// In test the wait duration can be reduced to make test case run faster
	if intest.InTest {
		dispatchLoopWaitingDuration = 50 * time.Millisecond
	}
}

type jobType int

func (t jobType) String() string {
	switch t {
	case jobTypeGeneral:
		return "general"
	case jobTypeReorg:
		return "reorg"
	case jobTypeLocal:
		return "local"
	}
	return "unknown job type: " + strconv.Itoa(int(t))
}

const (
	jobTypeGeneral jobType = iota
	jobTypeReorg
	jobTypeLocal
)

func (d *ddl) getJob(se *sess.Session, tp jobType, filter func(*model.Job) (bool, error)) (*model.Job, error) {
	not := "not"
	label := "get_job_general"
	if tp == jobTypeReorg {
		not = ""
		label = "get_job_reorg"
	}
	const getJobSQL = `select job_meta, processing from mysql.tidb_ddl_job where job_id in
		(select min(job_id) from mysql.tidb_ddl_job group by schema_ids, table_ids, processing)
		and %s reorg %s order by processing desc, job_id`
	var excludedJobIDs string
	if ids := d.runningJobs.allIDs(); len(ids) > 0 {
		excludedJobIDs = fmt.Sprintf("and job_id not in (%s)", ids)
	}
	sql := fmt.Sprintf(getJobSQL, not, excludedJobIDs)
	rows, err := se.Execute(context.Background(), sql, label)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		isJobProcessing := row.GetInt64(1) == 1

		job := model.Job{}
		err = job.Decode(jobBinary)
		if err != nil {
			return nil, errors.Trace(err)
		}

		isRunnable, err := d.processJobDuringUpgrade(se, &job)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !isRunnable {
			continue
		}

		// The job has already been picked up, just return to continue it.
		if isJobProcessing {
			return &job, nil
		}

		b, err := filter(&job)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if b {
			if err = d.markJobProcessing(se, &job); err != nil {
				logutil.DDLLogger().Warn(
					"[ddl] handle ddl job failed: mark job is processing meet error",
					zap.Error(err),
					zap.Stringer("job", &job))
				return nil, errors.Trace(err)
			}
			return &job, nil
		}
	}
	return nil, nil
}

func hasSysDB(job *model.Job) bool {
	for _, info := range job.GetInvolvingSchemaInfo() {
		if tidb_util.IsSysDB(info.Database) {
			return true
		}
	}
	return false
}

func (d *ddl) processJobDuringUpgrade(sess *sess.Session, job *model.Job) (isRunnable bool, err error) {
	if d.stateSyncer.IsUpgradingState() {
		if job.IsPaused() {
			return false, nil
		}
		// We need to turn the 'pausing' job to be 'paused' in ddl worker,
		// and stop the reorganization workers
		if job.IsPausing() || hasSysDB(job) {
			return true, nil
		}
		var errs []error
		// During binary upgrade, pause all running DDL jobs
		errs, err = PauseJobsBySystem(sess.Session(), []int64{job.ID})
		if len(errs) > 0 && errs[0] != nil {
			err = errs[0]
		}

		if err != nil {
			isCannotPauseDDLJobErr := dbterror.ErrCannotPauseDDLJob.Equal(err)
			logutil.DDLUpgradingLogger().Warn("pause the job failed", zap.Stringer("job", job),
				zap.Bool("isRunnable", isCannotPauseDDLJobErr), zap.Error(err))
			if isCannotPauseDDLJobErr {
				return true, nil
			}
		} else {
			logutil.DDLUpgradingLogger().Warn("pause the job successfully", zap.Stringer("job", job))
		}

		return false, nil
	}

	if job.IsPausedBySystem() {
		var errs []error
		errs, err = ResumeJobsBySystem(sess.Session(), []int64{job.ID})
		if len(errs) > 0 && errs[0] != nil {
			logutil.DDLUpgradingLogger().Warn("normal cluster state, resume the job failed", zap.Stringer("job", job), zap.Error(errs[0]))
			return false, errs[0]
		}
		if err != nil {
			logutil.DDLUpgradingLogger().Warn("normal cluster state, resume the job failed", zap.Stringer("job", job), zap.Error(err))
			return false, err
		}
		logutil.DDLUpgradingLogger().Warn("normal cluster state, resume the job successfully", zap.Stringer("job", job))
		return false, errors.Errorf("system paused job:%d need to be resumed", job.ID)
	}

	if job.IsPaused() {
		return false, nil
	}

	return true, nil
}

func (d *ddl) getGeneralJob(sess *sess.Session) (*model.Job, error) {
	return d.getJob(sess, jobTypeGeneral, func(job *model.Job) (bool, error) {
		if !d.runningJobs.checkRunnable(job) {
			return false, nil
		}
		if job.Type == model.ActionDropSchema {
			// Check if there is any reorg job on this schema.
			sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where CONCAT(',', schema_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and processing limit 1", strconv.Quote(strconv.FormatInt(job.SchemaID, 10)))
			rows, err := sess.Execute(d.ctx, sql, "check conflict jobs")
			return len(rows) == 0, err
		}
		// Check if there is any running job works on the same table.
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job t1, (select table_ids from mysql.tidb_ddl_job where job_id = %d) t2 where "+
			"(processing and CONCAT(',', t2.table_ids, ',') REGEXP CONCAT(',(', REPLACE(t1.table_ids, ',', '|'), '),') != 0)"+
			"or (type = %d and processing)", job.ID, model.ActionFlashbackCluster)
		rows, err := sess.Execute(d.ctx, sql, "check conflict jobs")
		return len(rows) == 0, err
	})
}

func (d *ddl) getReorgJob(sess *sess.Session) (*model.Job, error) {
	return d.getJob(sess, jobTypeReorg, func(job *model.Job) (bool, error) {
		if !d.runningJobs.checkRunnable(job) {
			return false, nil
		}
		// Check if there is any block ddl running, like drop schema and flashback cluster.
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where "+
			"(CONCAT(',', schema_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and type = %d and processing) "+
			"or (CONCAT(',', table_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and processing) "+
			"or (type = %d and processing) limit 1",
			strconv.Quote(strconv.FormatInt(job.SchemaID, 10)), model.ActionDropSchema, strconv.Quote(strconv.FormatInt(job.TableID, 10)), model.ActionFlashbackCluster)
		rows, err := sess.Execute(d.ctx, sql, "check conflict jobs")
		return len(rows) == 0, err
	})
}

// startLocalWorkerLoop starts the local worker loop to run the DDL job of v2.
func (d *ddl) startLocalWorkerLoop() {
	for {
		select {
		case <-d.ctx.Done():
			return
		case task, ok := <-d.localJobCh:
			if !ok {
				return
			}
			d.delivery2LocalWorker(d.localWorkerPool, task)
		}
	}
}

func (d *ddl) startDispatchLoop() {
	sessCtx, err := d.sessPool.Get()
	if err != nil {
		logutil.DDLLogger().Fatal("dispatch loop get session failed, it should not happen, please try restart TiDB", zap.Error(err))
	}
	defer d.sessPool.Put(sessCtx)
	se := sess.NewSession(sessCtx)
	var notifyDDLJobByEtcdCh clientv3.WatchChan
	if d.etcdCli != nil {
		notifyDDLJobByEtcdCh = d.etcdCli.Watch(d.ctx, addingDDLJobConcurrent)
	}
	if err := d.checkAndUpdateClusterState(true); err != nil {
		logutil.DDLLogger().Fatal("dispatch loop get cluster state failed, it should not happen, please try restart TiDB", zap.Error(err))
	}
	ticker := time.NewTicker(dispatchLoopWaitingDuration)
	defer ticker.Stop()
	isOnce := false
	for {
		if d.ctx.Err() != nil {
			return
		}
		if !d.isOwner() {
			isOnce = true
			d.onceMap = make(map[int64]struct{}, jobOnceCapacity)
			time.Sleep(dispatchLoopWaitingDuration)
			continue
		}
		failpoint.Inject("ownerResignAfterDispatchLoopCheck", func() {
			if ingest.ResignOwnerForTest.Load() {
				err2 := d.ownerManager.ResignOwner(context.Background())
				if err2 != nil {
					logutil.DDLLogger().Info("resign meet error", zap.Error(err2))
				}
				ingest.ResignOwnerForTest.Store(false)
			}
		})
		select {
		case <-d.ddlJobCh:
		case <-ticker.C:
		case _, ok := <-notifyDDLJobByEtcdCh:
			if !ok {
				logutil.DDLLogger().Warn("start worker watch channel closed", zap.String("watch key", addingDDLJobConcurrent))
				notifyDDLJobByEtcdCh = d.etcdCli.Watch(d.ctx, addingDDLJobConcurrent)
				time.Sleep(time.Second)
				continue
			}
		case <-d.ctx.Done():
			return
		}
		if err := d.checkAndUpdateClusterState(isOnce); err != nil {
			continue
		}
		isOnce = false
		d.loadDDLJobAndRun(se, d.generalDDLWorkerPool, d.getGeneralJob)
		d.loadDDLJobAndRun(se, d.reorgWorkerPool, d.getReorgJob)
	}
}

func (d *ddl) checkAndUpdateClusterState(needUpdate bool) error {
	select {
	case _, ok := <-d.stateSyncer.WatchChan():
		if !ok {
			d.stateSyncer.Rewatch(d.ctx)
		}
	default:
		if !needUpdate {
			return nil
		}
	}

	oldState := d.stateSyncer.IsUpgradingState()
	stateInfo, err := d.stateSyncer.GetGlobalState(d.ctx)
	if err != nil {
		logutil.DDLLogger().Warn("get global state failed", zap.Error(err))
		return errors.Trace(err)
	}
	logutil.DDLLogger().Info("get global state and global state change",
		zap.Bool("oldState", oldState), zap.Bool("currState", d.stateSyncer.IsUpgradingState()))
	if !d.isOwner() {
		return nil
	}

	ownerOp := owner.OpNone
	if stateInfo.State == syncer.StateUpgrading {
		ownerOp = owner.OpSyncUpgradingState
	}
	err = d.ownerManager.SetOwnerOpValue(d.ctx, ownerOp)
	if err != nil {
		logutil.DDLLogger().Warn("the owner sets global state to owner operator value failed", zap.Error(err))
		return errors.Trace(err)
	}
	logutil.DDLLogger().Info("the owner sets owner operator value", zap.Stringer("ownerOp", ownerOp))
	return nil
}

func (d *ddl) loadDDLJobAndRun(se *sess.Session, pool *workerPool, getJob func(*sess.Session) (*model.Job, error)) {
	wk, err := pool.get()
	if err != nil || wk == nil {
		logutil.DDLLogger().Debug(fmt.Sprintf("[ddl] no %v worker available now", pool.tp()), zap.Error(err))
		return
	}

	d.mu.RLock()
	d.mu.hook.OnGetJobBefore(pool.tp().String())
	d.mu.RUnlock()

	startTime := time.Now()
	job, err := getJob(se)
	if job == nil || err != nil {
		if err != nil {
			wk.jobLogger(job).Warn("get job met error", zap.Duration("take time", time.Since(startTime)), zap.Error(err))
		}
		pool.put(wk)
		return
	}
	d.mu.RLock()
	d.mu.hook.OnGetJobAfter(pool.tp().String(), job)
	d.mu.RUnlock()

	d.delivery2Worker(wk, pool, job)
}

// delivery2LocalWorker runs the DDL job of v2 in local.
// send the result to the error channels in the task.
// delivery2Localworker owns the worker, need to put it back to the pool in this function.
func (d *ddl) delivery2LocalWorker(pool *workerPool, task *limitJobTask) {
	job := task.job
	wk, err := pool.get()
	if err != nil {
		task.NotifyError(err)
		return
	}
	for wk == nil {
		select {
		case <-d.ctx.Done():
			return
		case <-time.After(localWorkerWaitingDuration):
		}
		wk, err = pool.get()
		if err != nil {
			task.NotifyError(err)
			return
		}
	}
	d.wg.Run(func() {
		metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Inc()
		defer func() {
			metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Dec()
		}()

		err := wk.HandleLocalDDLJob(d.ddlCtx, job)
		pool.put(wk)
		if err != nil {
			logutil.DDLLogger().Info("handle ddl job failed", zap.Error(err), zap.Stringer("job", job))
		}
		task.NotifyError(err)
	})
}

// delivery2Worker owns the worker, need to put it back to the pool in this function.
func (d *ddl) delivery2Worker(wk *worker, pool *workerPool, job *model.Job) {
	injectFailPointForGetJob(job)
	d.runningJobs.add(job)
	d.wg.Run(func() {
		metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Inc()
		defer func() {
			failpoint.InjectCall("afterDelivery2Worker", job)
			d.runningJobs.remove(job)
			asyncNotify(d.ddlJobCh)
			metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Dec()
		}()
		ownerID := d.ownerManager.ID()
		// check if this ddl job is synced to all servers.
		if !job.NotStarted() && (!d.isSynced(job) || !d.maybeAlreadyRunOnce(job.ID)) {
			if variable.EnableMDL.Load() {
				exist, version, err := checkMDLInfo(job.ID, d.sessPool)
				if err != nil {
					wk.jobLogger(job).Warn("check MDL info failed", zap.Error(err))
					// Release the worker resource.
					pool.put(wk)
					return
				} else if exist {
					// Release the worker resource.
					pool.put(wk)
					err = waitSchemaSyncedForMDL(d.ddlCtx, job, version)
					if err != nil {
						return
					}
					d.setAlreadyRunOnce(job.ID)
					cleanMDLInfo(d.sessPool, job, d.etcdCli, ownerID, job.State == model.JobStateSynced)
					// Don't have a worker now.
					return
				}
			} else {
				err := waitSchemaSynced(d.ddlCtx, job, 2*d.lease)
				if err != nil {
					time.Sleep(time.Second)
					// Release the worker resource.
					pool.put(wk)
					return
				}
				d.setAlreadyRunOnce(job.ID)
			}
		}

		schemaVer, err := wk.HandleDDLJobTable(d.ddlCtx, job)
		logCtx := wk.logCtx
		pool.put(wk)
		if err != nil {
			tidblogutil.Logger(logCtx).Info("handle ddl job failed", zap.Error(err), zap.Stringer("job", job))
		} else {
			failpoint.Inject("mockDownBeforeUpdateGlobalVersion", func(val failpoint.Value) {
				if val.(bool) {
					if mockDDLErrOnce == 0 {
						mockDDLErrOnce = schemaVer
						failpoint.Return()
					}
				}
			})

			// Here means the job enters another state (delete only, write only, public, etc...) or is cancelled.
			// If the job is done or still running or rolling back, we will wait 2 * lease time or util MDL synced to guarantee other servers to update
			// the newest schema.
			err := waitSchemaChanged(d.ddlCtx, d.lease*2, schemaVer, job)
			if err != nil {
				return
			}
			cleanMDLInfo(d.sessPool, job, d.etcdCli, ownerID, job.State == model.JobStateSynced)
			d.synced(job)

			if RunInGoTest {
				// d.mu.hook is initialed from domain / test callback, which will force the owner host update schema diff synchronously.
				d.mu.RLock()
				d.mu.hook.OnSchemaStateChanged(schemaVer)
				d.mu.RUnlock()
			}

			d.mu.RLock()
			d.mu.hook.OnJobUpdated(job)
			d.mu.RUnlock()
		}
	})
}

func (*ddl) markJobProcessing(se *sess.Session, job *model.Job) error {
	se.GetSessionVars().SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	_, err := se.Execute(context.Background(), fmt.Sprintf(
		"update mysql.tidb_ddl_job set processing = 1 where job_id = %d", job.ID),
		"mark_job_processing")
	return errors.Trace(err)
}

func (d *ddl) getTableByTxn(r autoid.Requirement, schemaID, tableID int64) (*model.DBInfo, table.Table, error) {
	var tbl table.Table
	var dbInfo *model.DBInfo
	err := kv.RunInNewTxn(d.ctx, r.Store(), false, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		dbInfo, err1 = t.GetDatabase(schemaID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		tblInfo, err1 := getTableInfo(t, tableID, schemaID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		tbl, err1 = getTable(r, schemaID, tblInfo)
		return errors.Trace(err1)
	})
	return dbInfo, tbl, err
}

const (
	addDDLJobSQL    = "insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing) values"
	updateDDLJobSQL = "update mysql.tidb_ddl_job set job_meta = %s where job_id = %d"
)

func insertDDLJobs2Table(se *sess.Session, updateRawArgs bool, jobs ...*model.Job) error {
	failpoint.Inject("mockAddBatchDDLJobsErr", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.Errorf("mockAddBatchDDLJobsErr"))
		}
	})
	if len(jobs) == 0 {
		return nil
	}
	var sql bytes.Buffer
	sql.WriteString(addDDLJobSQL)
	for i, job := range jobs {
		b, err := job.Encode(updateRawArgs)
		if err != nil {
			return err
		}
		if i != 0 {
			sql.WriteString(",")
		}
		fmt.Fprintf(&sql, "(%d, %t, %s, %s, %s, %d, %t)", job.ID, job.MayNeedReorg(), strconv.Quote(job2SchemaIDs(job)), strconv.Quote(job2TableIDs(job)), util.WrapKey2String(b), job.Type, !job.NotStarted())
	}
	se.GetSessionVars().SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	_, err := se.Execute(ctx, sql.String(), "insert_job")
	logutil.DDLLogger().Debug("add job to mysql.tidb_ddl_job table", zap.String("sql", sql.String()))
	return errors.Trace(err)
}

func job2SchemaIDs(job *model.Job) string {
	return job2UniqueIDs(job, true)
}

func job2TableIDs(job *model.Job) string {
	return job2UniqueIDs(job, false)
}

func job2UniqueIDs(job *model.Job, schema bool) string {
	switch job.Type {
	case model.ActionExchangeTablePartition, model.ActionRenameTables, model.ActionRenameTable:
		var ids []int64
		if schema {
			ids = job.CtxVars[0].([]int64)
		} else {
			ids = job.CtxVars[1].([]int64)
		}
		set := make(map[int64]struct{}, len(ids))
		for _, id := range ids {
			set[id] = struct{}{}
		}

		s := make([]string, 0, len(set))
		for id := range set {
			s = append(s, strconv.FormatInt(id, 10))
		}
		slices.Sort(s)
		return strings.Join(s, ",")
	case model.ActionTruncateTable:
		return strconv.FormatInt(job.TableID, 10) + "," + strconv.FormatInt(job.Args[0].(int64), 10)
	}
	if schema {
		return strconv.FormatInt(job.SchemaID, 10)
	}
	return strconv.FormatInt(job.TableID, 10)
}

func (w *worker) deleteDDLJob(job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_job where job_id = %d", job.ID)
	_, err := w.sess.Execute(context.Background(), sql, "delete_job")
	return errors.Trace(err)
}

func updateDDLJob2Table(se *sess.Session, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(updateDDLJobSQL, util.WrapKey2String(b), job.ID)
	_, err = se.Execute(context.Background(), sql, "update_job")
	return errors.Trace(err)
}

// getDDLReorgHandle gets DDL reorg handle.
func getDDLReorgHandle(se *sess.Session, job *model.Job) (element *meta.Element,
	startKey, endKey kv.Key, physicalTableID int64, err error) {
	sql := fmt.Sprintf("select ele_id, ele_type, start_key, end_key, physical_id, reorg_meta from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	ctx := kv.WithInternalSourceType(context.Background(), getDDLRequestSource(job.Type))
	rows, err := se.Execute(ctx, sql, "get_handle")
	if err != nil {
		return nil, nil, nil, 0, err
	}
	if len(rows) == 0 {
		return nil, nil, nil, 0, meta.ErrDDLReorgElementNotExist
	}
	id := rows[0].GetInt64(0)
	tp := rows[0].GetBytes(1)
	element = &meta.Element{
		ID:      id,
		TypeKey: tp,
	}
	startKey = rows[0].GetBytes(2)
	endKey = rows[0].GetBytes(3)
	physicalTableID = rows[0].GetInt64(4)
	return
}

func getCheckpointReorgHandle(se *sess.Session, job *model.Job) (startKey, endKey kv.Key, physicalTableID int64, err error) {
	startKey, endKey = kv.Key{}, kv.Key{}
	sql := fmt.Sprintf("select reorg_meta from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	ctx := kv.WithInternalSourceType(context.Background(), getDDLRequestSource(job.Type))
	rows, err := se.Execute(ctx, sql, "get_handle")
	if err != nil {
		return nil, nil, 0, err
	}
	if len(rows) == 0 {
		return nil, nil, 0, meta.ErrDDLReorgElementNotExist
	}
	if !rows[0].IsNull(0) {
		rawReorgMeta := rows[0].GetBytes(0)
		var reorgMeta ingest.JobReorgMeta
		err = json.Unmarshal(rawReorgMeta, &reorgMeta)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		if cp := reorgMeta.Checkpoint; cp != nil {
			logutil.DDLIngestLogger().Info("resume physical table ID from checkpoint",
				zap.Int64("jobID", job.ID),
				zap.String("start", hex.EncodeToString(cp.StartKey)),
				zap.String("end", hex.EncodeToString(cp.EndKey)),
				zap.Int64("checkpoint physical ID", cp.PhysicalID))
			physicalTableID = cp.PhysicalID
			if len(cp.StartKey) > 0 {
				startKey = cp.StartKey
			}
			if len(cp.EndKey) > 0 {
				endKey = cp.EndKey
				endKey = adjustEndKeyAcrossVersion(job, endKey)
			}
		}
	}
	return
}

// updateDDLReorgHandle update startKey, endKey physicalTableID and element of the handle.
// Caller should wrap this in a separate transaction, to avoid conflicts.
func updateDDLReorgHandle(se *sess.Session, jobID int64, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	sql := fmt.Sprintf("update mysql.tidb_ddl_reorg set ele_id = %d, ele_type = %s, start_key = %s, end_key = %s, physical_id = %d where job_id = %d",
		element.ID, util.WrapKey2String(element.TypeKey), util.WrapKey2String(startKey), util.WrapKey2String(endKey), physicalTableID, jobID)
	_, err := se.Execute(context.Background(), sql, "update_handle")
	return err
}

// initDDLReorgHandle initializes the handle for ddl reorg.
func initDDLReorgHandle(s *sess.Session, jobID int64, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	rawReorgMeta, err := json.Marshal(ingest.JobReorgMeta{
		Checkpoint: &ingest.ReorgCheckpoint{
			PhysicalID: physicalTableID,
			StartKey:   startKey,
			EndKey:     endKey,
			Version:    ingest.JobCheckpointVersionCurrent,
		}})
	if err != nil {
		return errors.Trace(err)
	}
	del := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", jobID)
	ins := fmt.Sprintf("insert into mysql.tidb_ddl_reorg(job_id, ele_id, ele_type, start_key, end_key, physical_id, reorg_meta) values (%d, %d, %s, %s, %s, %d, %s)",
		jobID, element.ID, util.WrapKey2String(element.TypeKey), util.WrapKey2String(startKey), util.WrapKey2String(endKey), physicalTableID, util.WrapKey2String(rawReorgMeta))
	return s.RunInTxn(func(se *sess.Session) error {
		_, err := se.Execute(context.Background(), del, "init_handle")
		if err != nil {
			logutil.DDLLogger().Info("initDDLReorgHandle failed to delete", zap.Int64("jobID", jobID), zap.Error(err))
		}
		_, err = se.Execute(context.Background(), ins, "init_handle")
		return err
	})
}

// deleteDDLReorgHandle deletes the handle for ddl reorg.
func removeDDLReorgHandle(se *sess.Session, job *model.Job, elements []*meta.Element) error {
	if len(elements) == 0 {
		return nil
	}
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	return se.RunInTxn(func(se *sess.Session) error {
		_, err := se.Execute(context.Background(), sql, "remove_handle")
		return err
	})
}

// removeReorgElement removes the element from ddl reorg, it is the same with removeDDLReorgHandle, only used in failpoint
func removeReorgElement(se *sess.Session, job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	return se.RunInTxn(func(se *sess.Session) error {
		_, err := se.Execute(context.Background(), sql, "remove_handle")
		return err
	})
}

// cleanDDLReorgHandles removes handles that are no longer needed.
func cleanDDLReorgHandles(se *sess.Session, job *model.Job) error {
	sql := "delete from mysql.tidb_ddl_reorg where job_id = " + strconv.FormatInt(job.ID, 10)
	return se.RunInTxn(func(se *sess.Session) error {
		_, err := se.Execute(context.Background(), sql, "clean_handle")
		return err
	})
}

func getJobsBySQL(se *sess.Session, tbl, condition string) ([]*model.Job, error) {
	rows, err := se.Execute(context.Background(), fmt.Sprintf("select job_meta from mysql.%s where %s", tbl, condition), "get_job")
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := make([]*model.Job, 0, 16)
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		job := model.Job{}
		err := job.Decode(jobBinary)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

func filterProcessingJobIDs(se *sess.Session, jobIDs []int64) ([]int64, error) {
	if len(jobIDs) == 0 {
		return nil, nil
	}

	var sb strings.Builder
	for i, id := range jobIDs {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(strconv.FormatInt(id, 10))
	}
	sql := fmt.Sprintf(
		"SELECT job_id FROM mysql.tidb_ddl_job WHERE job_id IN (%s) AND processing",
		sb.String())
	rows, err := se.Execute(context.Background(), sql, "filter_processing_job_ids")
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret := make([]int64, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, row.GetInt64(0))
	}
	return ret, nil
}
