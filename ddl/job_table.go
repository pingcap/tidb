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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var (
	addingDDLJobConcurrent = "/tidb/ddl/add_ddl_job_general"
	addingBackfillJob      = "/tidb/ddl/add_backfill_job"
)

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

func (d *ddl) getJob(sess *session, tp jobType, filter func(*model.Job) (bool, error)) (*model.Job, error) {
	not := "not"
	label := "get_job_general"
	if tp == reorg {
		not = ""
		label = "get_job_reorg"
	}
	sql := fmt.Sprintf(getJobSQL, not, d.excludeJobIDs())
	rows, err := sess.execute(context.Background(), sql, label)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		runJob := model.Job{}
		err := runJob.Decode(jobBinary)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row.GetInt64(1) == 1 {
			return &runJob, nil
		}
		b, err := filter(&runJob)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if b {
			if err := d.markJobProcessing(sess, &runJob); err != nil {
				logutil.BgLogger().Warn("[ddl] handle ddl job failed: mark job is processing meet error", zap.Error(err), zap.String("job", runJob.String()))
				return nil, errors.Trace(err)
			}
			return &runJob, nil
		}
	}
	return nil, nil
}

func (d *ddl) getGeneralJob(sess *session) (*model.Job, error) {
	return d.getJob(sess, general, func(job *model.Job) (bool, error) {
		if job.Type == model.ActionDropSchema {
			// Check if there is any reorg job on this schema.
			sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where CONCAT(',', schema_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and processing limit 1", strconv.Quote(strconv.FormatInt(job.SchemaID, 10)))
			return d.checkJobIsRunnable(sess, sql)
		}
		// Check if there is any running job works on the same table.
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job t1, (select table_ids from mysql.tidb_ddl_job where job_id = %d) t2 where "+
			"(processing and CONCAT(',', t2.table_ids, ',') REGEXP CONCAT(',', REPLACE(t1.table_ids, ',', '|'), ',') != 0)"+
			"or (type = %d and processing)", job.ID, model.ActionFlashbackCluster)
		return d.checkJobIsRunnable(sess, sql)
	})
}

func (d *ddl) checkJobIsRunnable(sess *session, sql string) (bool, error) {
	rows, err := sess.execute(context.Background(), sql, "check_runnable")
	return len(rows) == 0, err
}

func (d *ddl) getReorgJob(sess *session) (*model.Job, error) {
	return d.getJob(sess, reorg, func(job *model.Job) (bool, error) {
		// Check if there is any block ddl running, like drop schema and flashback cluster.
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where "+
			"(CONCAT(',', schema_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and type = %d and processing) "+
			"or (CONCAT(',', table_ids, ',') REGEXP CONCAT(',', %s, ',') != 0 and processing) "+
			"or (type = %d and processing) limit 1",
			strconv.Quote(strconv.FormatInt(job.SchemaID, 10)), model.ActionDropSchema, strconv.Quote(strconv.FormatInt(job.TableID, 10)), model.ActionFlashbackCluster)
		return d.checkJobIsRunnable(sess, sql)
	})
}

func (d *ddl) startDispatchLoop() {
	se, err := d.sessPool.get()
	if err != nil {
		logutil.BgLogger().Fatal("dispatch loop get session failed, it should not happen, please try restart TiDB", zap.Error(err))
	}
	defer d.sessPool.put(se)
	sess := newSession(se)
	var notifyDDLJobByEtcdCh clientv3.WatchChan
	if d.etcdCli != nil {
		notifyDDLJobByEtcdCh = d.etcdCli.Watch(d.ctx, addingDDLJobConcurrent)
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		if isChanClosed(d.ctx.Done()) {
			return
		}
		if !d.isOwner() || d.waiting.Load() {
			d.once.Store(true)
			time.Sleep(time.Second)
			continue
		}
		select {
		case <-d.ddlJobCh:
		case <-ticker.C:
		case _, ok := <-notifyDDLJobByEtcdCh:
			if !ok {
				logutil.BgLogger().Warn("[ddl] start worker watch channel closed", zap.String("watch key", addingDDLJobConcurrent))
				notifyDDLJobByEtcdCh = d.etcdCli.Watch(d.ctx, addingDDLJobConcurrent)
				time.Sleep(time.Second)
				continue
			}
		case <-d.ctx.Done():
			return
		}
		d.loadDDLJobAndRun(sess, d.generalDDLWorkerPool, d.getGeneralJob)
		d.loadDDLJobAndRun(sess, d.reorgWorkerPool, d.getReorgJob)
	}
}

func (d *ddl) loadDDLJobAndRun(sess *session, pool *workerPool, getJob func(*session) (*model.Job, error)) {
	wk, err := pool.get()
	if err != nil || wk == nil {
		logutil.BgLogger().Debug(fmt.Sprintf("[ddl] no %v worker available now", pool.tp()), zap.Error(err))
		return
	}

	d.mu.RLock()
	d.mu.hook.OnGetJobBefore(pool.tp().String())
	d.mu.RUnlock()

	job, err := getJob(sess)
	if job == nil || err != nil {
		if err != nil {
			logutil.BgLogger().Warn("[ddl] get job met error", zap.Error(err))
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
					logutil.BgLogger().Warn("[ddl] check MDL info failed", zap.Error(err), zap.String("job", job.String()))
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
					logutil.BgLogger().Warn("[ddl] wait ddl job sync failed", zap.Error(err), zap.String("job", job.String()))
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
			logutil.BgLogger().Info("[ddl] handle ddl job failed", zap.Error(err), zap.String("job", job.String()))
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

func (d *ddl) markJobProcessing(sess *session, job *model.Job) error {
	sess.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	_, err := sess.execute(context.Background(), fmt.Sprintf("update mysql.tidb_ddl_job set processing = 1 where job_id = %d", job.ID), "mark_job_processing")
	return errors.Trace(err)
}

func (d *ddl) startDispatchBackfillJobsLoop() {
	d.backfillCtx.jobCtxMap = make(map[int64]*JobContext)

	var notifyBackfillJobByEtcdCh clientv3.WatchChan
	if d.etcdCli != nil {
		notifyBackfillJobByEtcdCh = d.etcdCli.Watch(d.ctx, addingBackfillJob)
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if isChanClosed(d.ctx.Done()) {
			return
		}
		select {
		case <-d.backfillJobCh:
		case <-ticker.C:
		case _, ok := <-notifyBackfillJobByEtcdCh:
			if !ok {
				logutil.BgLogger().Warn("[ddl] start backfill worker watch channel closed", zap.String("watch key", addingBackfillJob))
				notifyBackfillJobByEtcdCh = d.etcdCli.Watch(d.ctx, addingBackfillJob)
				time.Sleep(time.Second)
				continue
			}
		case <-d.ctx.Done():
			return
		}
		d.loadBackfillJobAndRun()
	}
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

func (d *ddl) loadBackfillJobAndRun() {
	isDistReorg := variable.DDLEnableDistributeReorg.Load()
	if !isDistReorg {
		return
	}
	se, err := d.sessPool.get()
	if err != nil {
		logutil.BgLogger().Fatal("dispatch backfill jobs loop get session failed, it should not happen, please try restart TiDB", zap.Error(err))
	}
	sess := newSession(se)

	runningJobIDs := d.backfillCtxJobIDs()
	if len(runningJobIDs) >= reorgWorkerCnt {
		d.sessPool.put(se)
		return
	}

	// TODO: Add ele info to distinguish backfill jobs.
	// Get a Backfill job to get the reorg info like element info, schema ID and so on.
	bfJob, err := GetBackfillJobForOneEle(sess, runningJobIDs, InstanceLease)
	if err != nil || bfJob == nil {
		if err != nil {
			logutil.BgLogger().Warn("[ddl] get backfill jobs failed in this instance", zap.Error(err))
		} else {
			logutil.BgLogger().Debug("[ddl] get no backfill job in this instance")
		}
		d.sessPool.put(se)
		return
	}

	jobCtx, existent := d.setBackfillCtxJobContext(bfJob.JobID, bfJob.Meta.Query, bfJob.Meta.Type)
	if existent {
		logutil.BgLogger().Warn("[ddl] get the type of backfill job is running in this instance", zap.String("backfill job", bfJob.AbbrStr()))
		d.sessPool.put(se)
		return
	}
	// TODO: Adjust how the non-owner uses ReorgCtx.
	d.newReorgCtx(bfJob.JobID, bfJob.Meta.StartKey, &meta.Element{ID: bfJob.EleID, TypeKey: bfJob.EleKey}, bfJob.Meta.RowCount)
	d.wg.Run(func() {
		defer func() {
			tidbutil.Recover(metrics.LabelDistReorg, "runBackfillJobs", nil, false)
			d.removeBackfillCtxJobCtx(bfJob.JobID)
			d.removeReorgCtx(bfJob.JobID)
			d.sessPool.put(se)
		}()

		if bfJob.Meta.ReorgTp == model.ReorgTypeLitMerge {
			if !ingest.LitInitialized {
				logutil.BgLogger().Warn("[ddl] we can't do ingest in this instance",
					zap.Bool("LitInitialized", ingest.LitInitialized), zap.String("bfJob", bfJob.AbbrStr()))
				return
			}
			logutil.BgLogger().Info("[ddl] run backfill jobs with ingest in this instance", zap.String("bfJob", bfJob.AbbrStr()))
			err = runBackfillJobsWithLightning(d, sess, bfJob, jobCtx)
		} else {
			logutil.BgLogger().Info("[ddl] run backfill jobs with txn-merge in this instance", zap.String("bfJob", bfJob.AbbrStr()))
			_, err = runBackfillJobs(d, sess, nil, bfJob, jobCtx)
		}

		if err == nil {
			err = syncBackfillHistoryJobs(sess, d.uuid, bfJob)
		}
		logutil.BgLogger().Info("[ddl] run backfill jobs finished in this instance", zap.Stringer("reorg type", bfJob.Meta.ReorgTp), zap.Error(err))
	})
}

const (
	addDDLJobSQL    = "insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing) values"
	updateDDLJobSQL = "update mysql.tidb_ddl_job set job_meta = %s where job_id = %d"
)

func insertDDLJobs2Table(sess *session, updateRawArgs bool, jobs ...*model.Job) error {
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
		sql.WriteString(fmt.Sprintf("(%d, %t, %s, %s, %s, %d, %t)", job.ID, job.MayNeedReorg(), strconv.Quote(job2SchemaIDs(job)), strconv.Quote(job2TableIDs(job)), wrapKey2String(b), job.Type, !job.NotStarted()))
	}
	sess.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	_, err := sess.execute(ctx, sql.String(), "insert_job")
	logutil.BgLogger().Debug("[ddl] add job to mysql.tidb_ddl_job table", zap.String("sql", sql.String()))
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
	_, err := w.sess.execute(context.Background(), sql, "delete_job")
	return errors.Trace(err)
}

func updateDDLJob2Table(sctx *session, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(updateDDLJobSQL, wrapKey2String(b), job.ID)
	_, err = sctx.execute(context.Background(), sql, "update_job")
	return errors.Trace(err)
}

// getDDLReorgHandle gets DDL reorg handle.
func getDDLReorgHandle(sess *session, job *model.Job) (element *meta.Element, startKey, endKey kv.Key, physicalTableID int64, err error) {
	sql := fmt.Sprintf("select ele_id, ele_type, start_key, end_key, physical_id from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	ctx := kv.WithInternalSourceType(context.Background(), getDDLRequestSource(job.Type))
	rows, err := sess.execute(ctx, sql, "get_handle")
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

// updateDDLReorgHandle update startKey, endKey physicalTableID and element of the handle.
// Caller should wrap this in a separate transaction, to avoid conflicts.
func updateDDLReorgHandle(sess *session, jobID int64, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	sql := fmt.Sprintf("update mysql.tidb_ddl_reorg set ele_id = %d, ele_type = %s, start_key = %s, end_key = %s, physical_id = %d where job_id = %d",
		element.ID, wrapKey2String(element.TypeKey), wrapKey2String(startKey), wrapKey2String(endKey), physicalTableID, jobID)
	_, err := sess.execute(context.Background(), sql, "update_handle")
	return err
}

// initDDLReorgHandle initializes the handle for ddl reorg.
func initDDLReorgHandle(s *session, jobID int64, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	del := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", jobID)
	ins := fmt.Sprintf("insert into mysql.tidb_ddl_reorg(job_id, ele_id, ele_type, start_key, end_key, physical_id) values (%d, %d, %s, %s, %s, %d)",
		jobID, element.ID, wrapKey2String(element.TypeKey), wrapKey2String(startKey), wrapKey2String(endKey), physicalTableID)
	return s.runInTxn(func(se *session) error {
		_, err := se.execute(context.Background(), del, "init_handle")
		if err != nil {
			logutil.BgLogger().Info("initDDLReorgHandle failed to delete", zap.Int64("jobID", jobID), zap.Error(err))
		}
		_, err = se.execute(context.Background(), ins, "init_handle")
		return err
	})
}

// deleteDDLReorgHandle deletes the handle for ddl reorg.
func removeDDLReorgHandle(s *session, job *model.Job, elements []*meta.Element) error {
	if len(elements) == 0 {
		return nil
	}
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	return s.runInTxn(func(se *session) error {
		_, err := se.execute(context.Background(), sql, "remove_handle")
		return err
	})
}

// removeReorgElement removes the element from ddl reorg, it is the same with removeDDLReorgHandle, only used in failpoint
func removeReorgElement(s *session, job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	return s.runInTxn(func(se *session) error {
		_, err := se.execute(context.Background(), sql, "remove_handle")
		return err
	})
}

// cleanDDLReorgHandles removes handles that are no longer needed.
func cleanDDLReorgHandles(s *session, job *model.Job) error {
	sql := "delete from mysql.tidb_ddl_reorg where job_id = " + strconv.FormatInt(job.ID, 10)
	return s.runInTxn(func(se *session) error {
		_, err := se.execute(context.Background(), sql, "clean_handle")
		return err
	})
}

func wrapKey2String(key []byte) string {
	if len(key) == 0 {
		return "''"
	}
	return fmt.Sprintf("0x%x", key)
}

func getJobsBySQL(sess *session, tbl, condition string) ([]*model.Job, error) {
	rows, err := sess.execute(context.Background(), fmt.Sprintf("select job_meta from mysql.%s where %s", tbl, condition), "get_job")
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

func syncBackfillHistoryJobs(sess *session, uuid string, backfillJob *BackfillJob) error {
	sql := fmt.Sprintf("update mysql.%s set state = '%s' where task_key like '%s' and exec_id = '%s' limit 1;",
		BackgroundSubtaskHistoryTable, model.JobStateSynced.String(), backfillJob.PrefixKeyString(), uuid)
	_, err := sess.execute(context.Background(), sql, "sync_backfill_history_job")
	return err
}

func generateInsertBackfillJobSQL(tableName string, backfillJobs []*BackfillJob) (string, error) {
	sqlBuilder := strings.Builder{}
	sqlBuilder.WriteString("insert into mysql.")
	sqlBuilder.WriteString(tableName)
	sqlBuilder.WriteString("(task_key, ddl_physical_tid, type, exec_id, exec_expired, state, checkpoint, start_time, state_update_time, meta) values")
	for i, bj := range backfillJobs {
		mateByte, err := bj.Meta.Encode()
		if err != nil {
			return "", errors.Trace(err)
		}

		if i != 0 {
			sqlBuilder.WriteString(", ")
		}
		sqlBuilder.WriteString(fmt.Sprintf("('%s', %d, %d, '%s', '%s', '%s', %s, %d, %d, %s)",
			bj.keyString(), bj.PhysicalTableID, bj.Tp, bj.InstanceID, bj.InstanceLease, bj.State.String(), wrapKey2String(bj.Meta.CurrKey),
			bj.StartTS, bj.StateUpdateTS, wrapKey2String(mateByte)))
	}
	return sqlBuilder.String(), nil
}

// AddBackfillHistoryJob adds the backfill jobs to the tidb_background_subtask_history table.
func AddBackfillHistoryJob(sess *session, backfillJobs []*BackfillJob) error {
	label := fmt.Sprintf("add_%s_job", BackgroundSubtaskHistoryTable)
	sql, err := generateInsertBackfillJobSQL(BackgroundSubtaskHistoryTable, backfillJobs)
	if err != nil {
		return err
	}
	_, err = sess.execute(context.Background(), sql, label)
	return errors.Trace(err)
}

// AddBackfillJobs adds the backfill jobs to the tidb_background_subtask table.
func AddBackfillJobs(s *session, backfillJobs []*BackfillJob) error {
	label := fmt.Sprintf("add_%s_job", BackgroundSubtaskTable)
	// Do runInTxn to get StartTS.
	return s.runInTxn(func(se *session) error {
		txn, err := se.txn()
		if err != nil {
			return errors.Trace(err)
		}
		startTS := txn.StartTS()
		for _, bj := range backfillJobs {
			bj.StartTS = startTS
		}

		sql, err := generateInsertBackfillJobSQL(BackgroundSubtaskTable, backfillJobs)
		if err != nil {
			return err
		}
		_, err = se.execute(context.Background(), sql, label)
		return errors.Trace(err)
	})
}

// GetBackfillJobForOneEle gets the backfill jobs in the tblName table that contains only one element.
func GetBackfillJobForOneEle(s *session, excludedJobIDs []int64, lease time.Duration) (*BackfillJob, error) {
	eJobIDsBuilder := strings.Builder{}
	for _, id := range excludedJobIDs {
		eJobIDsBuilder.WriteString(fmt.Sprintf(" and task_key not like \"%d_%%\"", id))
	}

	var err error
	var bJobs []*BackfillJob
	err = s.runInTxn(func(se *session) error {
		currTime, err := GetOracleTimeWithStartTS(se)
		if err != nil {
			return err
		}
		leaseStr := currTime.Add(-lease).Format(types.TimeFormat)
		bJobs, err = GetBackfillJobs(se, BackgroundSubtaskTable,
			fmt.Sprintf("(exec_id = '' or exec_expired < '%v') %s order by task_key limit 1",
				leaseStr, eJobIDsBuilder.String()), "get_backfill_job")
		return err
	})
	if err != nil || len(bJobs) == 0 {
		return nil, err
	}

	return bJobs[0], nil
}

// GetAndMarkBackfillJobsForOneEle batch gets the backfill jobs in the tblName table that contains only one element,
// and update these jobs with instance ID and lease.
func GetAndMarkBackfillJobsForOneEle(s *session, batch int, jobID int64, uuid string, pTblID int64, lease time.Duration) ([]*BackfillJob, error) {
	var validLen int
	var bJobs []*BackfillJob
	err := s.runInTxn(func(se *session) error {
		currTime, err := GetOracleTimeWithStartTS(se)
		if err != nil {
			return err
		}
		leaseStr := currTime.Add(-lease).Format(types.TimeFormat)

		getJobsSQL := fmt.Sprintf("(exec_id = '' or exec_expired < '%v') and task_key like \"%d_%%\" order by task_key limit %d",
			leaseStr, jobID, batch)
		if pTblID != getJobWithoutPartition {
			if pTblID == 0 {
				rows, err := s.execute(context.Background(),
					fmt.Sprintf("select ddl_physical_tid from mysql.%s group by substring_index(task_key,\"_\",3), ddl_physical_tid having max(length(exec_id)) = 0 or max(exec_expired) < '%s' order by substring_index(task_key,\"_\",3), ddl_physical_tid limit 1",
						BackgroundSubtaskTable, leaseStr), "get_mark_backfill_job")
				if err != nil {
					return errors.Trace(err)
				}

				if len(rows) == 0 {
					return dbterror.ErrDDLJobNotFound.FastGen("get zero backfill job")
				}

				pTblID = rows[0].GetInt64(0)
			}
			getJobsSQL = fmt.Sprintf("(exec_id = '' or exec_expired < '%s') and task_key like \"%d_%%\" and ddl_physical_tid = %d order by task_key limit %d",
				leaseStr, jobID, pTblID, batch)
		}

		bJobs, err = GetBackfillJobs(se, BackgroundSubtaskTable, getJobsSQL, "get_mark_backfill_job")
		if err != nil {
			return err
		}
		if len(bJobs) == 0 {
			return dbterror.ErrDDLJobNotFound.FastGen("get zero backfill job")
		}

		validLen = 0
		firstJobID, firstEleID, firstEleKey := bJobs[0].JobID, bJobs[0].EleID, bJobs[0].EleKey
		for i := 0; i < len(bJobs); i++ {
			if bJobs[i].JobID != firstJobID || bJobs[i].EleID != firstEleID || !bytes.Equal(bJobs[i].EleKey, firstEleKey) {
				break
			}
			validLen++

			bJobs[i].InstanceID = uuid
			bJobs[i].InstanceLease = GetLeaseGoTime(currTime, lease)
			// TODO: batch update
			if err = updateBackfillJob(se, BackgroundSubtaskTable, bJobs[i], "get_mark_backfill_job"); err != nil {
				return err
			}
		}
		return nil
	})
	if validLen == 0 {
		return nil, err
	}

	return bJobs[:validLen], err
}

// GetInterruptedBackfillJobForOneEle gets an interrupted backfill job that contains only one element.
func GetInterruptedBackfillJobForOneEle(sess *session, jobID, eleID int64, eleKey []byte) ([]*BackfillJob, error) {
	bJobs, err := GetBackfillJobs(sess, BackgroundSubtaskHistoryTable, fmt.Sprintf("task_key like '%s' and state = \"%s\" limit 1",
		backfillJobPrefixKeyString(jobID, eleKey, eleID), model.JobStateCancelled.String()), "get_interrupt_backfill_job")
	if err != nil || len(bJobs) == 0 {
		return nil, err
	}
	return bJobs, nil
}

// GetBackfillJobCount gets the number of rows in the tblName table according to condition.
func GetBackfillJobCount(sess *session, tblName, condition string, label string) (int, error) {
	rows, err := sess.execute(context.Background(), fmt.Sprintf("select count(1) from mysql.%s where %s", tblName, condition), label)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) == 0 {
		return 0, dbterror.ErrDDLJobNotFound.FastGenByArgs(fmt.Sprintf("get wrong result cnt:%d", len(rows)))
	}

	return int(rows[0].GetInt64(0)), nil
}

// GetBackfillMetas gets the backfill metas in the tblName table according to condition.
func GetBackfillMetas(sess *session, tblName, condition string, label string) ([]*model.BackfillMeta, error) {
	rows, err := sess.execute(context.Background(), fmt.Sprintf("select meta from mysql.%s where %s", tblName, condition), label)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, dbterror.ErrDDLJobNotFound.FastGenByArgs(fmt.Sprintf("get wrong result cnt:%d", len(rows)))
	}

	metas := make([]*model.BackfillMeta, 0, len(rows))
	for _, r := range rows {
		meta := &model.BackfillMeta{}
		err = meta.Decode(r.GetBytes(0))
		if err != nil {
			return nil, errors.Trace(err)
		}
		metas = append(metas, meta)
	}

	return metas, nil
}

// GetBackfillIDAndMetas gets the backfill IDs and metas in the tblName table according to condition.
func GetBackfillIDAndMetas(sess *session, tblName, condition string, label string) ([]*BackfillJobRangeMeta, error) {
	sql := "select tbl.task_key, tbl.meta, tbl.ddl_physical_tid from (select max(task_key) max_id, ddl_physical_tid " +
		fmt.Sprintf(" from mysql.%s tbl where %s group by ddl_physical_tid) tmp join mysql.%s tbl",
			tblName, condition, tblName) + " on tbl.task_key=tmp.max_id and tbl.ddl_physical_tid=tmp.ddl_physical_tid;"
	rows, err := sess.execute(context.Background(), sql, label)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	pTblMetas := make([]*BackfillJobRangeMeta, 0, len(rows))
	for _, r := range rows {
		key := r.GetString(0)
		keySlice := strings.Split(key, "_")
		id, err := strconv.ParseInt(keySlice[3], 10, 64)
		if err != nil {
			return nil, err
		}
		meta := &model.BackfillMeta{}
		err = meta.Decode(r.GetBytes(1))
		if err != nil {
			return nil, err
		}
		pTblMeta := BackfillJobRangeMeta{
			ID:       id,
			StartKey: meta.StartKey,
			EndKey:   meta.EndKey,
			PhyTblID: r.GetInt64(2),
		}
		pTblMetas = append(pTblMetas, &pTblMeta)
	}
	return pTblMetas, nil
}

func getUnsyncedInstanceIDs(sess *session, jobID int64, label string) ([]string, error) {
	sql := fmt.Sprintf("select sum((state='%s') + (state='%s')) as tmp, exec_id from mysql.tidb_background_subtask_history where task_key like \"%d_%%\" group by exec_id having tmp = 0;",
		model.JobStateSynced.String(), model.JobStateCancelled.String(), jobID)
	rows, err := sess.execute(context.Background(), sql, label)
	if err != nil {
		return nil, errors.Trace(err)
	}
	InstanceIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		InstanceID := row.GetString(1)
		InstanceIDs = append(InstanceIDs, InstanceID)
	}
	return InstanceIDs, nil
}

// GetBackfillJobs gets the backfill jobs in the tblName table according to condition.
func GetBackfillJobs(sess *session, tblName, condition string, label string) ([]*BackfillJob, error) {
	rows, err := sess.execute(context.Background(), fmt.Sprintf("select * from mysql.%s where %s", tblName, condition), label)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bJobs := make([]*BackfillJob, 0, len(rows))
	for _, row := range rows {
		key := row.GetString(2)
		keySlice := strings.Split(key, "_")
		jobID, err := strconv.ParseInt(keySlice[0], 10, 64)
		if err != nil {
			return nil, err
		}
		eleKey, err := hex.DecodeString(keySlice[1])
		if err != nil {
			return nil, err
		}
		eleID, err := strconv.ParseInt(keySlice[2], 10, 64)
		if err != nil {
			return nil, err
		}
		id, err := strconv.ParseInt(keySlice[3], 10, 64)
		if err != nil {
			return nil, err
		}
		bfJob := BackfillJob{
			ID:              id,
			JobID:           jobID,
			EleID:           eleID,
			EleKey:          eleKey,
			PhysicalTableID: row.GetInt64(3),
			Tp:              backfillerType(row.GetInt64(4)),
			InstanceID:      row.GetString(5),
			InstanceLease:   row.GetTime(6),
			State:           model.StrToJobState(row.GetString(7)),
			StartTS:         row.GetUint64(9),
			StateUpdateTS:   row.GetUint64(10),
		}
		bfJob.Meta = &model.BackfillMeta{}
		err = bfJob.Meta.Decode(row.GetBytes(11))
		if err != nil {
			return nil, errors.Trace(err)
		}
		bfJob.Meta.CurrKey = row.GetBytes(8)
		bJobs = append(bJobs, &bfJob)
	}
	return bJobs, nil
}

// RemoveBackfillJob removes the backfill jobs from the tidb_background_subtask table.
// If isOneEle is true, removes all jobs with backfillJob's ddl_job_id, ele_id and ele_key. Otherwise, removes the backfillJob.
func RemoveBackfillJob(sess *session, isOneEle bool, backfillJob *BackfillJob) error {
	sql := "delete from mysql.tidb_background_subtask"
	if !isOneEle {
		sql += fmt.Sprintf(" where task_key like '%s'", backfillJob.keyString())
	} else {
		sql += fmt.Sprintf(" where task_key like '%s'", backfillJob.PrefixKeyString())
	}
	_, err := sess.execute(context.Background(), sql, "remove_backfill_job")
	return err
}

func updateBackfillJob(sess *session, tableName string, backfillJob *BackfillJob, label string) error {
	mate, err := backfillJob.Meta.Encode()
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("update mysql.%s set exec_id = '%s', exec_expired = '%s', state = '%s', checkpoint = %s, meta = %s where task_key = '%s'",
		tableName, backfillJob.InstanceID, backfillJob.InstanceLease, backfillJob.State.String(), wrapKey2String(backfillJob.Meta.CurrKey), wrapKey2String(mate), backfillJob.keyString())
	_, err = sess.execute(context.Background(), sql, label)
	return err
}
