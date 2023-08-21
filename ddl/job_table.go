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
	"github.com/pingcap/tidb/ddl/ingest"
	sess "github.com/pingcap/tidb/ddl/internal/session"
	"github.com/pingcap/tidb/ddl/syncer"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	tidb_util "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	addingDDLJobConcurrent      = "/tidb/ddl/add_ddl_job_general"
	dispatchLoopWaitingDuration = 1 * time.Second
)

func init() {
	// In test the wait duration can be reduced to make test case run faster
	if intest.InTest {
		dispatchLoopWaitingDuration = 50 * time.Millisecond
	}
}

func (dc *ddlCtx) insertRunningDDLJobMap(id int64) {
	dc.runningJobs.Lock()
	defer dc.runningJobs.Unlock()
	dc.runningJobs.ids[id] = struct{}{}
}

func (dc *ddlCtx) deleteRunningDDLJobMap(id int64) {
	dc.runningJobs.Lock()
	defer dc.runningJobs.Unlock()
	delete(dc.runningJobs.ids, id)
}

func (dc *ddlCtx) excludeJobIDs() string {
	dc.runningJobs.RLock()
	defer dc.runningJobs.RUnlock()
	if len(dc.runningJobs.ids) == 0 {
		return ""
	}
	dc.runningJobIDs = dc.runningJobIDs[:0]
	for id := range dc.runningJobs.ids {
		dc.runningJobIDs = append(dc.runningJobIDs, strconv.Itoa(int(id)))
	}
	return fmt.Sprintf("and job_id not in (%s)", strings.Join(dc.runningJobIDs, ","))
}

const (
	getJobSQL = "select job_meta, processing from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_ids, table_ids, processing) and %s reorg %s order by processing desc, job_id"
)

type jobType int

func (t jobType) String() string {
	switch t {
	case general:
		return "general"
	case reorg:
		return "reorg"
	}
	return "unknown job type: " + strconv.Itoa(int(t))
}

const (
	general jobType = iota
	reorg
)

func (d *ddl) getJob(se *sess.Session, tp jobType, filter func(*model.Job) (bool, error)) (*model.Job, error) {
	not := "not"
	label := "get_job_general"
	if tp == reorg {
		not = ""
		label = "get_job_reorg"
	}
	sql := fmt.Sprintf(getJobSQL, not, d.excludeJobIDs())
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
				logutil.BgLogger().Warn(
					"[ddl] handle ddl job failed: mark job is processing meet error",
					zap.Error(err),
					zap.String("job", job.String()))
				return nil, errors.Trace(err)
			}
			return &job, nil
		}
	}
	return nil, nil
}

func hasSysDB(job *model.Job) bool {
	sNames := job2SchemaNames(job)
	// TODO: Handle for the name is empty, like ActionCreatePlacementPolicy.
	for _, name := range sNames {
		if tidb_util.IsSysDB(name) {
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
			logutil.BgLogger().Warn("pause the job failed", zap.String("category", "ddl-upgrading"), zap.Stringer("job", job),
				zap.Bool("isRunnable", isCannotPauseDDLJobErr), zap.Error(err))
			if isCannotPauseDDLJobErr {
				return true, nil
			}
		} else {
			logutil.BgLogger().Warn("pause the job successfully", zap.String("category", "ddl-upgrading"), zap.Stringer("job", job))
		}

		return false, nil
	}

	if job.IsPausedBySystem() {
		var errs []error
		errs, err = ResumeJobsBySystem(sess.Session(), []int64{job.ID})
		if len(errs) > 0 && errs[0] != nil {
			logutil.BgLogger().Warn("normal cluster state, resume the job failed", zap.String("category", "ddl-upgrading"), zap.Stringer("job", job), zap.Error(errs[0]))
			return false, errs[0]
		}
		if err != nil {
			logutil.BgLogger().Warn("normal cluster state, resume the job failed", zap.String("category", "ddl-upgrading"), zap.Stringer("job", job), zap.Error(err))
			return false, err
		}
		logutil.BgLogger().Warn("normal cluster state, resume the job successfully", zap.String("category", "ddl-upgrading"), zap.Stringer("job", job))
		return false, errors.Errorf("system paused job:%d need to be resumed", job.ID)
	}

	if job.IsPaused() {
		return false, nil
	}

	return true, nil
}

func (d *ddl) getGeneralJob(sess *sess.Session) (*model.Job, error) {
	return d.getJob(sess, general, func(job *model.Job) (bool, error) {
		if job.Type == model.ActionDropSchema {
			// Check if there is any reorg job on this schema.
			sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where CONCAT(',', schema_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and processing limit 1", strconv.Quote(strconv.FormatInt(job.SchemaID, 10)))
			return d.NoConflictJob(sess, sql)
		}
		// Check if there is any running job works on the same table.
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job t1, (select table_ids from mysql.tidb_ddl_job where job_id = %d) t2 where "+
			"(processing and CONCAT(',', t2.table_ids, ',') REGEXP CONCAT(',', REPLACE(t1.table_ids, ',', '|'), ',') != 0)"+
			"or (type = %d and processing)", job.ID, model.ActionFlashbackCluster)
		return d.NoConflictJob(sess, sql)
	})
}

func (*ddl) NoConflictJob(se *sess.Session, sql string) (bool, error) {
	rows, err := se.Execute(context.Background(), sql, "check conflict jobs")
	return len(rows) == 0, err
}

func (d *ddl) getReorgJob(sess *sess.Session) (*model.Job, error) {
	return d.getJob(sess, reorg, func(job *model.Job) (bool, error) {
		// Check if there is any block ddl running, like drop schema and flashback cluster.
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where "+
			"(CONCAT(',', schema_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and type = %d and processing) "+
			"or (CONCAT(',', table_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and processing) "+
			"or (type = %d and processing) limit 1",
			strconv.Quote(strconv.FormatInt(job.SchemaID, 10)), model.ActionDropSchema, strconv.Quote(strconv.FormatInt(job.TableID, 10)), model.ActionFlashbackCluster)
		return d.NoConflictJob(sess, sql)
	})
}

func (d *ddl) startDispatchLoop() {
	sessCtx, err := d.sessPool.Get()
	if err != nil {
		logutil.BgLogger().Fatal("dispatch loop get session failed, it should not happen, please try restart TiDB", zap.Error(err))
	}
	defer d.sessPool.Put(sessCtx)
	se := sess.NewSession(sessCtx)
	var notifyDDLJobByEtcdCh clientv3.WatchChan
	if d.etcdCli != nil {
		notifyDDLJobByEtcdCh = d.etcdCli.Watch(d.ctx, addingDDLJobConcurrent)
	}
	ticker := time.NewTicker(dispatchLoopWaitingDuration)
	if err := d.checkAndUpdateClusterState(true); err != nil {
		logutil.BgLogger().Fatal("dispatch loop get cluster state failed, it should not happen, please try restart TiDB", zap.Error(err))
	}
	defer ticker.Stop()
	isOnce := false
	for {
		if isChanClosed(d.ctx.Done()) {
			return
		}
		if !d.isOwner() {
			isOnce = true
			d.once.Store(true)
			time.Sleep(dispatchLoopWaitingDuration)
			continue
		}
		select {
		case <-d.ddlJobCh:
		case <-ticker.C:
		case _, ok := <-notifyDDLJobByEtcdCh:
			if !ok {
				logutil.BgLogger().Warn("start worker watch channel closed", zap.String("category", "ddl"), zap.String("watch key", addingDDLJobConcurrent))
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
		logutil.BgLogger().Warn("get global state failed", zap.String("category", "ddl"), zap.Error(err))
		return errors.Trace(err)
	}
	logutil.BgLogger().Info("get global state and global state change", zap.String("category", "ddl"),
		zap.Bool("oldState", oldState), zap.Bool("currState", d.stateSyncer.IsUpgradingState()))
	if !d.isOwner() {
		return nil
	}

	ownerOp := owner.OpNone
	if stateInfo.State == syncer.StateUpgrading {
		ownerOp = owner.OpGetUpgradingState
	}
	err = d.ownerManager.SetOwnerOpValue(d.ctx, ownerOp)
	if err != nil {
		logutil.BgLogger().Warn("the owner sets global state to owner operator value failed", zap.String("category", "ddl"), zap.Error(err))
		return errors.Trace(err)
	}
	logutil.BgLogger().Info("the owner sets owner operator value", zap.String("category", "ddl"), zap.Stringer("ownerOp", ownerOp))
	return nil
}

func (d *ddl) loadDDLJobAndRun(se *sess.Session, pool *workerPool, getJob func(*sess.Session) (*model.Job, error)) {
	wk, err := pool.get()
	if err != nil || wk == nil {
		logutil.BgLogger().Debug(fmt.Sprintf("[ddl] no %v worker available now", pool.tp()), zap.Error(err))
		return
	}

	d.mu.RLock()
	d.mu.hook.OnGetJobBefore(pool.tp().String())
	d.mu.RUnlock()

	job, err := getJob(se)
	if job == nil || err != nil {
		if err != nil {
			logutil.BgLogger().Warn("get job met error", zap.String("category", "ddl"), zap.Error(err))
		}
		pool.put(wk)
		return
	}
	d.mu.RLock()
	d.mu.hook.OnGetJobAfter(pool.tp().String(), job)
	d.mu.RUnlock()

	d.delivery2worker(wk, pool, job)
}

// delivery2worker owns the worker, need to put it back to the pool in this function.
func (d *ddl) delivery2worker(wk *worker, pool *workerPool, job *model.Job) {
	injectFailPointForGetJob(job)
	d.insertRunningDDLJobMap(job.ID)
	d.wg.Run(func() {
		metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Inc()
		defer func() {
			d.deleteRunningDDLJobMap(job.ID)
			asyncNotify(d.ddlJobCh)
			metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Dec()
		}()
		// check if this ddl job is synced to all servers.
		if !d.isSynced(job) || d.once.Load() {
			if variable.EnableMDL.Load() {
				exist, version, err := checkMDLInfo(job.ID, d.sessPool)
				if err != nil {
					logutil.BgLogger().Warn("check MDL info failed", zap.String("category", "ddl"), zap.Error(err), zap.String("job", job.String()))
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
					d.once.Store(false)
					cleanMDLInfo(d.sessPool, job.ID, d.etcdCli)
					// Don't have a worker now.
					return
				}
			} else {
				err := waitSchemaSynced(d.ddlCtx, job, 2*d.lease)
				if err != nil {
					logutil.BgLogger().Warn("wait ddl job sync failed", zap.String("category", "ddl"), zap.Error(err), zap.String("job", job.String()))
					time.Sleep(time.Second)
					// Release the worker resource.
					pool.put(wk)
					return
				}
				d.once.Store(false)
			}
		}

		schemaVer, err := wk.HandleDDLJobTable(d.ddlCtx, job)
		pool.put(wk)
		if err != nil {
			logutil.BgLogger().Info("handle ddl job failed", zap.String("category", "ddl"), zap.Error(err), zap.String("job", job.String()))
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
			// If the job is done or still running or rolling back, we will wait 2 * lease time to guarantee other servers to update
			// the newest schema.
			waitSchemaChanged(d.ddlCtx, d.lease*2, schemaVer, job)
			cleanMDLInfo(d.sessPool, job.ID, d.etcdCli)
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
	se.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	_, err := se.Execute(context.Background(), fmt.Sprintf(
		"update mysql.tidb_ddl_job set processing = 1 where job_id = %d", job.ID),
		"mark_job_processing")
	return errors.Trace(err)
}

func (d *ddl) getTableByTxn(store kv.Storage, schemaID, tableID int64) (*model.DBInfo, table.Table, error) {
	var tbl table.Table
	var dbInfo *model.DBInfo
	err := kv.RunInNewTxn(d.ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
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
		tbl, err1 = getTable(store, schemaID, tblInfo)
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
	se.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	_, err := se.Execute(ctx, sql.String(), "insert_job")
	logutil.BgLogger().Debug("add job to mysql.tidb_ddl_job table", zap.String("category", "ddl"), zap.String("sql", sql.String()))
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

func job2SchemaNames(job *model.Job) []string {
	if job.Type == model.ActionRenameTable {
		var oldSchemaID int64
		var oldSchemaName model.CIStr
		var tableName model.CIStr
		// TODO: Handle this error
		_ = job.DecodeArgs(&oldSchemaID, &tableName, &oldSchemaName)
		names := make([]string, 0, 2)
		names = append(names, strings.ToLower(job.SchemaName))
		names = append(names, oldSchemaName.O)
		return names
	}
	// TODO: consider about model.ActionRenameTables and model.ActionExchangeTablePartition, which need to get the schema names.

	return []string{job.SchemaName}
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
			logutil.BgLogger().Info("resume physical table ID from checkpoint", zap.String("category", "ddl-ingest"),
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
			logutil.BgLogger().Info("initDDLReorgHandle failed to delete", zap.Int64("jobID", jobID), zap.Error(err))
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
