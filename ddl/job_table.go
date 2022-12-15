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
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var (
	addingDDLJobConcurrent = "/tidb/ddl/add_ddl_job_general"
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
		if !variable.EnableConcurrentDDL.Load() || !d.isOwner() || d.waiting.Load() {
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
				exist, err := checkMDLInfo(job.ID, d.sessPool)
				if err != nil {
					logutil.BgLogger().Warn("[ddl] check MDL info failed", zap.Error(err), zap.String("job", job.String()))
					// Release the worker resource.
					pool.put(wk)
					return
				} else if exist {
					// Release the worker resource.
					pool.put(wk)
					err = waitSchemaSynced(d.ddlCtx, job, 2*d.lease)
					if err != nil {
						logutil.BgLogger().Warn("[ddl] wait ddl job sync failed", zap.Error(err), zap.String("job", job.String()))
						time.Sleep(time.Second)
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
			waitSchemaChanged(context.Background(), d.ddlCtx, d.lease*2, schemaVer, job)
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
	rows, err := sess.execute(context.Background(), sql, "get_handle")
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
	// physicalTableID may be 0, because older version TiDB (without table partition) doesn't store them.
	// update them to table's in this case.
	if physicalTableID == 0 {
		if job.ReorgMeta != nil {
			endKey = kv.IntHandle(job.ReorgMeta.EndHandle).Encoded()
		} else {
			endKey = kv.IntHandle(math.MaxInt64).Encoded()
		}
		physicalTableID = job.TableID
		logutil.BgLogger().Warn("new TiDB binary running on old TiDB DDL reorg data",
			zap.Int64("partition ID", physicalTableID),
			zap.Stringer("startHandle", startKey),
			zap.Stringer("endHandle", endKey))
	}
	return
}

// updateDDLReorgStartHandle update the startKey of the handle.
func updateDDLReorgStartHandle(sess *session, job *model.Job, element *meta.Element, startKey kv.Key) error {
	sql := fmt.Sprintf("update mysql.tidb_ddl_reorg set ele_id = %d, ele_type = %s, start_key = %s where job_id = %d",
		element.ID, wrapKey2String(element.TypeKey), wrapKey2String(startKey), job.ID)
	_, err := sess.execute(context.Background(), sql, "update_start_handle")
	return err
}

// updateDDLReorgHandle update startKey, endKey physicalTableID and element of the handle.
func updateDDLReorgHandle(sess *session, jobID int64, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	sql := fmt.Sprintf("update mysql.tidb_ddl_reorg set ele_id = %d, ele_type = %s, start_key = %s, end_key = %s, physical_id = %d where job_id = %d",
		element.ID, wrapKey2String(element.TypeKey), wrapKey2String(startKey), wrapKey2String(endKey), physicalTableID, jobID)
	_, err := sess.execute(context.Background(), sql, "update_handle")
	return err
}

// initDDLReorgHandle initializes the handle for ddl reorg.
func initDDLReorgHandle(sess *session, jobID int64, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	sql := fmt.Sprintf("insert into mysql.tidb_ddl_reorg(job_id, ele_id, ele_type, start_key, end_key, physical_id) values (%d, %d, %s, %s, %s, %d)",
		jobID, element.ID, wrapKey2String(element.TypeKey), wrapKey2String(startKey), wrapKey2String(endKey), physicalTableID)
	_, err := sess.execute(context.Background(), sql, "update_handle")
	return err
}

// deleteDDLReorgHandle deletes the handle for ddl reorg.
func removeDDLReorgHandle(sess *session, job *model.Job, elements []*meta.Element) error {
	if len(elements) == 0 {
		return nil
	}
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	_, err := sess.execute(context.Background(), sql, "remove_handle")
	return err
}

// removeReorgElement removes the element from ddl reorg, it is the same with removeDDLReorgHandle, only used in failpoint
func removeReorgElement(sess *session, job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	_, err := sess.execute(context.Background(), sql, "remove_handle")
	return err
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

// AddBackfillJobs adds the backfill jobs to the tidb_ddl_backfill table.
func AddBackfillJobs(sess *session, backfillJobs []*BackfillJob) error {
	return addBackfillJobs(sess, BackfillTable, backfillJobs)
}

// AddBackfillHistoryJob adds the backfill jobs to the tidb_ddl_backfill_history table.
func AddBackfillHistoryJob(sess *session, backfillJobs []*BackfillJob) error {
	return addBackfillJobs(sess, BackfillHistoryTable, backfillJobs)
}

// addBackfillJobs adds the backfill jobs to the tidb_ddl_backfill table.
func addBackfillJobs(sess *session, tableName string, backfillJobs []*BackfillJob) error {
	sqlPrefix := fmt.Sprintf(
		"insert into mysql.%s(id, ddl_job_id, ele_id, ele_key, store_id, type, exec_id, exec_lease, state, curr_key, start_key, end_key, start_ts, finish_ts, row_count, backfill_meta) values", tableName)
	var sql string
	label := fmt.Sprintf("add_%s_job", tableName)
	// Do runInTxn to get StartTS.
	return runInTxn(newSession(sess), func(se *session) error {
		txn, err := se.txn()
		if err != nil {
			return errors.Trace(err)
		}

		startTS := txn.StartTS()
		for i, bj := range backfillJobs {
			if tableName == BackfillTable {
				bj.StartTS = startTS
			}
			if tableName == BackfillHistoryTable {
				bj.FinishTS = startTS
			}
			mateByte, err := bj.Meta.Encode()
			if err != nil {
				return errors.Trace(err)
			}

			if i == 0 {
				sql = sqlPrefix + fmt.Sprintf("(%d, %d, %d, '%s', %d, %d, '%s', '%s', %d, '%s', '%s', '%s', %d, %d, %d, '%s')",
					bj.ID, bj.JobID, bj.EleID, bj.EleKey, bj.StoreID, bj.Tp, bj.InstanceID, bj.InstanceLease, bj.State,
					bj.CurrKey, bj.StartKey, bj.EndKey, bj.StartTS, bj.FinishTS, bj.RowCount, mateByte)
				continue
			}
			sql += fmt.Sprintf(", (%d, %d, %d, '%s', %d, %d, '%s', '%s', %d, '%s', '%s', '%s', %d, %d, %d, '%s')",
				bj.ID, bj.JobID, bj.EleID, bj.EleKey, bj.StoreID, bj.Tp, bj.InstanceID, bj.InstanceLease, bj.State,
				bj.CurrKey, bj.StartKey, bj.EndKey, bj.StartTS, bj.FinishTS, bj.RowCount, mateByte)
		}
		_, err = sess.execute(context.Background(), sql, label)
		return errors.Trace(err)
	})
}

// GetBackfillJobsForOneEle batch gets the backfill jobs in the tblName table that contains only one element.
func GetBackfillJobsForOneEle(sess *session, batch int, excludedJobIDs []int64, lease time.Duration) ([]*BackfillJob, error) {
	eJobIDsBuilder := strings.Builder{}
	for i, id := range excludedJobIDs {
		if i == 0 {
			eJobIDsBuilder.WriteString(" and ddl_job_id not in (")
		}
		eJobIDsBuilder.WriteString(strconv.Itoa(int(id)))
		if i == len(excludedJobIDs)-1 {
			eJobIDsBuilder.WriteString(")")
		} else {
			eJobIDsBuilder.WriteString(", ")
		}
	}

	var err error
	var bJobs []*BackfillJob
	s := newSession(sess)
	err = runInTxn(s, func(se *session) error {
		currTime, err := GetOracleTime(s)
		if err != nil {
			return err
		}

		bJobs, err = GetBackfillJobs(sess, BackfillTable,
			fmt.Sprintf("(exec_ID = '' or exec_lease < '%v') %s order by ddl_job_id, ele_key, ele_id limit %d",
				currTime.Add(-lease), eJobIDsBuilder.String(), batch), "get_backfill_job")
		return err
	})
	if err != nil || len(bJobs) == 0 {
		return nil, err
	}

	validLen := 1
	firstJobID, firstEleID, firstEleKey := bJobs[0].JobID, bJobs[0].EleID, bJobs[0].EleKey
	for i := 1; i < len(bJobs); i++ {
		if bJobs[i].JobID != firstJobID || bJobs[i].EleID != firstEleID || !bytes.Equal(bJobs[i].EleKey, firstEleKey) {
			break
		}
		validLen++
	}

	return bJobs[:validLen], nil
}

// GetAndMarkBackfillJobsForOneEle batch gets the backfill jobs in the tblName table that contains only one element,
// and update these jobs with instance ID and lease.
func GetAndMarkBackfillJobsForOneEle(sess *session, batch int, jobID int64, uuid string, lease time.Duration) ([]*BackfillJob, error) {
	var validLen int
	var bJobs []*BackfillJob
	s := newSession(sess)
	err := runInTxn(s, func(se *session) error {
		currTime, err := GetOracleTime(se)
		if err != nil {
			return err
		}

		bJobs, err = GetBackfillJobs(sess, BackfillTable,
			fmt.Sprintf("(exec_ID = '' or exec_lease < '%v') and ddl_job_id = %d order by ddl_job_id, ele_key, ele_id limit %d",
				currTime.Add(-lease), jobID, batch), "get_mark_backfill_job")
		if err != nil {
			return err
		}
		if len(bJobs) == 0 {
			return dbterror.ErrDDLJobNotFound.FastGen("get zero backfill job, lease is timeout")
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
			if err = updateBackfillJob(sess, BackfillTable, bJobs[i], "get_mark_backfill_job"); err != nil {
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

// GetInterruptedBackfillJobsForOneEle gets the interrupted backfill jobs in the tblName table that contains only one element.
func GetInterruptedBackfillJobsForOneEle(sess *session, jobID, eleID int64, eleKey []byte) ([]*BackfillJob, error) {
	bJobs, err := GetBackfillJobs(sess, BackfillTable, fmt.Sprintf("ddl_job_id = %d and ele_id = %d and ele_key = '%s' and (state = %d or state = %d)",
		jobID, eleID, eleKey, model.JobStateRollingback, model.JobStateCancelling), "get_interrupt_backfill_job")
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

// GetBackfillJobs gets the backfill jobs in the tblName table according to condition.
func GetBackfillJobs(sess *session, tblName, condition string, label string) ([]*BackfillJob, error) {
	rows, err := sess.execute(context.Background(), fmt.Sprintf("select * from mysql.%s where %s", tblName, condition), label)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bJobs := make([]*BackfillJob, 0, len(rows))
	for _, row := range rows {
		bJob := BackfillJob{
			ID:            row.GetInt64(0),
			JobID:         row.GetInt64(1),
			EleID:         row.GetInt64(2),
			EleKey:        row.GetBytes(3),
			StoreID:       row.GetInt64(4),
			Tp:            backfillerType(row.GetInt64(5)),
			InstanceID:    row.GetString(6),
			InstanceLease: row.GetTime(7),
			State:         model.JobState(row.GetInt64(8)),
			CurrKey:       row.GetBytes(9),
			StartKey:      row.GetBytes(10),
			EndKey:        row.GetBytes(11),
			StartTS:       row.GetUint64(12),
			FinishTS:      row.GetUint64(13),
			RowCount:      row.GetInt64(14),
		}
		bJob.Meta = &model.BackfillMeta{}
		err = bJob.Meta.Decode(row.GetBytes(15))
		if err != nil {
			return nil, errors.Trace(err)
		}
		bJobs = append(bJobs, &bJob)
	}
	return bJobs, nil
}

// RemoveBackfillJob removes the backfill jobs from the tidb_ddl_backfill table.
// If isOneEle is true, removes all jobs with backfillJob's ddl_job_id, ele_id and ele_key. Otherwise, removes the backfillJob.
func RemoveBackfillJob(sess *session, isOneEle bool, backfillJob *BackfillJob) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_backfill where ddl_job_id = %d and ele_id = %d and ele_key = '%s'",
		backfillJob.JobID, backfillJob.EleID, backfillJob.EleKey)
	if !isOneEle {
		sql += fmt.Sprintf(" and id = %d", backfillJob.ID)
	}
	_, err := sess.execute(context.Background(), sql, "remove_backfill_job")
	return err
}

func updateBackfillJob(sess *session, tableName string, backfillJob *BackfillJob, label string) error {
	mate, err := backfillJob.Meta.Encode()
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("update mysql.%s set exec_id = '%s', exec_lease = '%s', state = %d, backfill_meta = '%s' where ddl_job_id = %d and ele_id = %d and ele_key = '%s' and id = %d",
		tableName, backfillJob.InstanceID, backfillJob.InstanceLease, backfillJob.State, mate, backfillJob.JobID, backfillJob.EleID, backfillJob.EleKey, backfillJob.ID)
	_, err = sess.execute(context.Background(), sql, label)
	return err
}

// MoveJobFromQueue2Table move existing DDLs in queue to table.
func (d *ddl) MoveJobFromQueue2Table(inBootstrap bool) error {
	sess, err := d.sessPool.get()
	if err != nil {
		return err
	}
	defer d.sessPool.put(sess)
	return runInTxn(newSession(sess), func(se *session) error {
		txn, err := se.txn()
		if err != nil {
			return errors.Trace(err)
		}
		t := meta.NewMeta(txn)
		isConcurrentDDL, err := t.IsConcurrentDDL()
		if !inBootstrap && (isConcurrentDDL || err != nil) {
			return errors.Trace(err)
		}
		systemDBID, err := t.GetSystemDBID()
		if err != nil {
			return errors.Trace(err)
		}
		for _, tp := range []workerType{addIdxWorker, generalWorker} {
			t := newMetaWithQueueTp(txn, tp)
			jobs, err := t.GetAllDDLJobsInQueue()
			if err != nil {
				return errors.Trace(err)
			}
			for _, job := range jobs {
				// In bootstrap, we can ignore the internal DDL.
				if inBootstrap && job.SchemaID == systemDBID {
					continue
				}
				err = insertDDLJobs2Table(se, false, job)
				if err != nil {
					return errors.Trace(err)
				}
				if tp == generalWorker {
					// General job do not have reorg info.
					continue
				}
				element, start, end, pid, err := t.GetDDLReorgHandle(job)
				if meta.ErrDDLReorgElementNotExist.Equal(err) {
					continue
				}
				if err != nil {
					return errors.Trace(err)
				}
				err = initDDLReorgHandle(se, job.ID, start, end, pid, element)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}

		if err = t.ClearALLDDLJob(); err != nil {
			return errors.Trace(err)
		}
		if err = t.ClearAllDDLReorgHandle(); err != nil {
			return errors.Trace(err)
		}
		return t.SetConcurrentDDL(true)
	})
}

// MoveJobFromTable2Queue move existing DDLs in table to queue.
func (d *ddl) MoveJobFromTable2Queue() error {
	sess, err := d.sessPool.get()
	if err != nil {
		return err
	}
	defer d.sessPool.put(sess)
	return runInTxn(newSession(sess), func(se *session) error {
		txn, err := se.txn()
		if err != nil {
			return errors.Trace(err)
		}
		t := meta.NewMeta(txn)
		isConcurrentDDL, err := t.IsConcurrentDDL()
		if !isConcurrentDDL || err != nil {
			return errors.Trace(err)
		}
		jobs, err := getJobsBySQL(se, "tidb_ddl_job", "1 order by job_id")
		if err != nil {
			return errors.Trace(err)
		}

		for _, job := range jobs {
			jobListKey := meta.DefaultJobListKey
			if job.MayNeedReorg() {
				jobListKey = meta.AddIndexJobListKey
			}
			if err := t.EnQueueDDLJobNoUpdate(job, jobListKey); err != nil {
				return errors.Trace(err)
			}
		}

		reorgHandle, err := se.execute(context.Background(), "select job_id, start_key, end_key, physical_id, ele_id, ele_type from mysql.tidb_ddl_reorg", "get_handle")
		if err != nil {
			return errors.Trace(err)
		}
		for _, row := range reorgHandle {
			if err := t.UpdateDDLReorgHandle(row.GetInt64(0), row.GetBytes(1), row.GetBytes(2), row.GetInt64(3), &meta.Element{ID: row.GetInt64(4), TypeKey: row.GetBytes(5)}); err != nil {
				return errors.Trace(err)
			}
		}

		// clean up these 2 tables.
		_, err = se.execute(context.Background(), "delete from mysql.tidb_ddl_job", "delete_old_ddl")
		if err != nil {
			return errors.Trace(err)
		}
		_, err = se.execute(context.Background(), "delete from mysql.tidb_ddl_reorg", "delete_old_reorg")
		if err != nil {
			return errors.Trace(err)
		}
		return t.SetConcurrentDDL(false)
	})
}

func runInTxn(se *session, f func(*session) error) (err error) {
	err = se.begin()
	if err != nil {
		return err
	}
	err = f(se)
	if err != nil {
		se.rollback()
		return
	}
	return errors.Trace(se.commit())
}
