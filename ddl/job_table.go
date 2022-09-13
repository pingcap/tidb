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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
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
	getJobSQL = "select job_meta, processing from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_ids, table_ids) and %s reorg %s order by processing desc, job_id"
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
			sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where find_in_set(%s, schema_ids) != 0 and processing limit 1", strconv.Quote(strconv.FormatInt(job.SchemaID, 10)))
			return d.checkJobIsRunnable(sess, sql)
		}
		// For general job, there is only 1 general worker to handle it, so at this moment the processing job must be reorg job and the reorg job must only contain one table id.
		// So it's not possible the find_in_set("1,2", "1,2,3") occurs.
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job t1, (select table_ids from mysql.tidb_ddl_job where job_id = %d) t2 where processing and find_in_set(t1.table_ids, t2.table_ids) != 0", job.ID)
		return d.checkJobIsRunnable(sess, sql)
	})
}

func (d *ddl) checkJobIsRunnable(sess *session, sql string) (bool, error) {
	rows, err := sess.execute(context.Background(), sql, "check_runnable")
	return len(rows) == 0, err
}

func (d *ddl) getReorgJob(sess *session) (*model.Job, error) {
	return d.getJob(sess, reorg, func(job *model.Job) (bool, error) {
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where (find_in_set(%s, schema_ids) != 0 and type = %d and processing) or (find_in_set(%s, table_ids) != 0 and processing) limit 1",
			strconv.Quote(strconv.FormatInt(job.SchemaID, 10)), model.ActionDropSchema, strconv.Quote(strconv.FormatInt(job.TableID, 10)))
		return d.checkJobIsRunnable(sess, sql)
	})
}

func (d *ddl) startDispatchBackfillJobsLoop() {
	if !enableDistReorg {
		return
	}
	d.backfillCtx.jobCtxMap = make(map[int64]*JobContext)
	d.backfillCtx.backfillCtxMap = make(map[int64]struct{})

	logutil.BgLogger().Warn("------------------------------- start backfill jobs loop")
	se, err := d.sessPool.get()
	if err != nil {
		logutil.BgLogger().Fatal("dispatch backfill jobs loop get session failed, it should not happen, please try restart TiDB", zap.Error(err))
	}
	defer d.sessPool.put(se)
	sess := newSession(se)
	var notifyBackfillJobByEtcdCh clientv3.WatchChan
	if d.etcdCli != nil {
		notifyBackfillJobByEtcdCh = d.etcdCli.Watch(d.ctx, addingBackfillJob)
	}
	// TODO: set the tick time
	ticker := time.NewTicker(100 * time.Millisecond)
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
		d.loadBackfillJobAndRun(sess, d.backfillWorkerPool)
	}
}

func (d *ddl) getTableByTxn(store kv.Storage, schemaID, tableID int64) (table.Table, error) {
	var tbl table.Table
	err := kv.RunInNewTxn(d.ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		// TODO: update type?
		t := newMetaWithQueueTp(txn, addIdxWorker)
		tblInfo, err := getTableInfo(t, tableID, schemaID)
		if err != nil {
			return err
		}
		tbl, err = getTable(store, schemaID, tblInfo)
		return err
	})
	return tbl, err
}

func (d *ddl) loadBackfillJobAndRun(sess *session, pool *backfillWorkerPool) {
	if err := ddlutil.LoadDDLReorgVars(context.Background(), sess); err != nil {
		logutil.BgLogger().Error("[ddl] load DDL reorganization variable failed", zap.Error(err))
	}
	// TODO:
	// workerCnt := int(variable.GetDDLReorgWorkerCounter())
	workerCnt := 6

	isIncluded := false
	d.backfillCtx.Lock()
	jobCtxMapLen := len(d.backfillCtx.jobCtxMap)
	runningJobIDs := make([]int64, 0, jobCtxMapLen)
	if jobCtxMapLen >= reorgWorkerCnt {
		isIncluded = true
		for id, jobCtx := range d.backfillCtx.jobCtxMap {
			if jobCtx.backfillJobCnt < workerCnt {
				runningJobIDs = append(runningJobIDs, id)
				break
			}
		}
	} else {
		for id, _ := range d.backfillCtx.jobCtxMap {
			runningJobIDs = append(runningJobIDs, id)
		}
	}
	d.backfillCtx.Unlock()

	// TODO: add exclude job IDs.
	// TODO: add ele info
	bJobs, err := getAndMarkBackfillJobsForOneEle(sess, workerCnt, isIncluded, runningJobIDs, d.uuid, instanceLease*time.Second)
	bJobCnt := len(bJobs)
	logutil.BgLogger().Warn("00 ******** load backfill job and run " +
		fmt.Sprintf("is included:%v, bf job cnt:%d, worker cnt:%d, map len:%d, default:%d", isIncluded, bJobCnt, workerCnt, jobCtxMapLen, reorgWorkerCnt))
	if bJobCnt == 0 || err != nil {
		if err != nil {
			logutil.BgLogger().Warn("[ddl] get backfill jobs met error", zap.Error(err))
		}
		return
	}
	if bJobCnt < workerCnt {
		workerCnt = bJobCnt
	}

	jobs, err := getJobsBySQL(sess, JobTable, fmt.Sprintf("job_id = %d", bJobs[0].JobID))
	if len(jobs) == 0 || err != nil {
		logutil.BgLogger().Warn("[ddl] get job met error", zap.Reflect("jobs", jobs), zap.Error(err))
		return
	}
	job := jobs[0]

	d.backfillCtx.Lock()
	jobCtx, ok := d.backfillCtx.jobCtxMap[job.ID]
	if !ok {
		jobCtx = NewJobContextWithArgs(job.Query)
		d.backfillCtx.jobCtxMap[job.ID] = jobCtx
	}
	d.backfillCtx.Unlock()
	defer func() {
		d.backfillCtx.Lock()
		delete(d.backfillCtx.jobCtxMap, job.ID)
		d.backfillCtx.Unlock()
	}()

	bws, err := pool.batchGet(workerCnt)
	if err != nil || len(bws) == 0 {
		logutil.BgLogger().Debug(fmt.Sprintf("[ddl] no backfill worker available now"), zap.Error(err))
		return
	}

	// TODO: lease
	var bf backfiller
	// TODO: optimize
	tbl, err := d.getTableByTxn(d.store, job.SchemaID, job.TableID)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] backfill job get table failed", zap.String("job", job.String()), zap.Error(err))
		return
	}
	decodeColMap, err := makeupDecodeColMap(sess, tbl.(table.PhysicalTable))
	if err != nil {
		logutil.BgLogger().Debug(fmt.Sprintf("[ddl] make up decode col map failed"), zap.Error(err))
		return
	}

	wg := sync.WaitGroup{}
	for i, bw := range bws {
		se, err := d.sessPool.get()
		if err != nil {
			logutil.BgLogger().Fatal("dispatch backfill jobs loop get session failed, it should not happen, please try restart TiDB", zap.Error(err))
		}
		defer d.sessPool.put(se)
		sess := newSession(se)

		switch bJobs[i].Tp {
		case model.TypeAddIndexBackfill:
			// TODO: set batch
			batch := 128
			bf = newAddIndexWorker(decodeColMap, newBackfillCtx(d.ddlCtx, sess, tbl, batch), jobCtx, bJobs[i].EleID, bJobs[i].EleKey, job.Query)
			// case model.TypeUpdateColumnBackfill:
			// 	// Setting InCreateOrAlterStmt tells the difference between SELECT casting and ALTER COLUMN casting.
			// 	sess.GetSessionVars().StmtCtx.InCreateOrAlterStmt = true
			// 	bf = newUpdateColumnWorker(jobCtx.tbl.(table.PhysicalTable), decodeColMap, nil, jobCtx)
			// case model.TypeCleanUpIndexBackfill:
			// 	bf = newCleanUpIndexWorker(jobCtx.tbl.(table.PhysicalTable), decodeColMap, nil, jobCtx)
		default:
			logutil.BgLogger().Warn("[ddl] unknow backfill type", zap.Reflect("tp", bJobs[i].Tp))
			pool.put(bw)
			return
		}

		wg.Add(1)
		d.backfillCtx.Lock()
		d.backfillCtx.backfillCtxMap[bJobs[i].ID] = struct{}{}
		d.backfillCtx.Unlock()

		go func(num int, bwTemp *backfillWorker, bfTemp backfiller) {
			defer func() {
				wg.Done()
				pool.put(bwTemp)

				d.backfillCtx.Lock()
				delete(d.backfillCtx.backfillCtxMap, bJobs[num].ID)
				d.backfillCtx.Unlock()
			}()
			bwTemp.run(d.ddlCtx, bfTemp, bJobs[num].JobID, job.Query)
		}(i, bw, bf)
		d.sendBackfillJob(bw, bJobs[i], tbl, job.Priority)
	}
	for i, bw := range bws {
		d.waitBackfillJob(bw, bJobs[i])
		bw.Close()
	}
	wg.Wait()
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

func (d *ddl) delivery2worker(wk *worker, pool *workerPool, job *model.Job) {
	injectFailPointForGetJob(job)
	d.insertRunningDDLJobMap(job.ID)
	d.wg.Run(func() {
		metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Inc()
		defer func() {
			pool.put(wk)
			d.deleteRunningDDLJobMap(job.ID)
			asyncNotify(d.ddlJobCh)
			metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Dec()
		}()
		// we should wait 2 * d.lease time to guarantee all TiDB server have finished the schema change.
		// see waitSchemaSynced for more details.
		if !d.isSynced(job) || d.once.Load() {
			err := wk.waitSchemaSynced(d.ddlCtx, job, 2*d.lease)
			if err == nil {
				d.once.Store(false)
			} else {
				logutil.BgLogger().Warn("[ddl] wait ddl job sync failed", zap.Error(err), zap.String("job", job.String()))
				time.Sleep(time.Second)
				return
			}
		}
		if err := wk.HandleDDLJobTable(d.ddlCtx, job); err != nil {
			logutil.BgLogger().Info("[ddl] handle ddl job failed", zap.Error(err), zap.String("job", job.String()))
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

func addBackfillJobs(sess *session, backfillJobs []*model.BackfillJob) error {
	sqlPrefix := "insert into mysql.tidb_ddl_backfill(section_id, job_id, ele_id, ele_key, physical_id, type, exec_id, exec_lease, state, backfill_meta) values"
	var sql string
	emptyInstance := "null"
	// TODO: If this the length of backfillJobs is very big.
	for i, bj := range backfillJobs {
		mateByte, err := bj.Mate.Encode()
		if err != nil {
			return err
		}
		instanceID := bj.Instance_ID
		if len(bj.Instance_ID) == 0 {
			instanceID = emptyInstance
		}

		if i == 0 {
			sql = sqlPrefix + fmt.Sprintf("(%d, %d, %d, '%s', %d, %d, '%s', '%s', %d, '%s')",
				bj.ID, bj.JobID, bj.EleID, bj.EleKey, bj.PhysicalID, bj.Tp, instanceID, bj.Instance_Lease, bj.State, mateByte)
			continue
		}
		sql += fmt.Sprintf(", (%d, %d, %d, '%s', %d, %d, '%s', '%s', %d, '%s')",
			bj.ID, bj.JobID, bj.EleID, bj.EleKey, bj.PhysicalID, bj.Tp, instanceID, bj.Instance_Lease, bj.State, mateByte)
	}
	logutil.BgLogger().Warn("insert *****************************   ", zap.String("sql", sql))
	_, err := sess.execute(context.Background(), sql, "add_backfill_jobs")
	return err
}

func getBackfillJobsForOneEle(sess *session, batch int, isInclude bool, jobIDs []int64, lease time.Duration) ([]*model.BackfillJob, error) {
	currTime, err := getOracleTime(sess.GetStore())
	if err != nil {
		return nil, err
	}
	jobInfo := ""
	symbol := "="
	if !isInclude {
		symbol = "!="
	}
	for _, id := range jobIDs {
		jobInfo += fmt.Sprintf("and job_id %s %d", symbol, id)
	}

	jobs, err := getBackfillJobs(sess, BackfillTable, fmt.Sprintf("exec_ID = '' or exec_lease < '%v' %s order by job_id limit %d", currTime.Add(-lease), jobInfo, batch))
	if err != nil || len(jobs) == 0 {
		return nil, err
	}
	validLen := 1
	firstJobID, firstEleID := jobs[0].JobID, jobs[0].EleID
	for i := 1; i < len(jobs); i++ {
		if jobs[i].JobID != firstJobID || jobs[i].EleID != firstEleID {
			break
		}
		validLen++
	}

	return jobs[:validLen], nil
}

func getAndMarkBackfillJobsForOneEle(sess *session, batch int, isInclude bool, jobIDs []int64, uuid string, lease time.Duration) ([]*model.BackfillJob, error) {
	currTime, err := getOracleTime(sess.GetStore())
	if err != nil {
		return nil, err
	}
	jobInfo := ""
	symbol := "="
	if !isInclude {
		symbol = "!="
	}
	for _, id := range jobIDs {
		jobInfo += fmt.Sprintf("and job_id %s %d", symbol, id)
	}

	_, err = sess.execute(context.Background(), "begin", "get_mark_backfill_job")
	if err != nil {
		return nil, err
	}

	jobs, err := getBackfillJobs(sess, BackfillTable, fmt.Sprintf("exec_ID = '' or exec_lease < '%v' %s order by job_id limit %d", currTime.Add(-lease), jobInfo, batch))
	if err != nil || len(jobs) == 0 {
		return nil, err
	}
	validLen := 1
	firstJobID, firstEleID := jobs[0].JobID, jobs[0].EleID
	for i := 1; i < len(jobs); i++ {
		if jobs[i].JobID != firstJobID || jobs[i].EleID != firstEleID {
			break
		}
		validLen++

		jobs[i].Instance_ID = uuid
		// TODO: w.backfillJob.Instance_Lease = types.NewTime(types.FromGoTime(lease), mysql.TypeTimestamp, types.DefaultFsp)
		jobs[i].Instance_Lease = currTime.Add(-lease)
		if err = updateBackfillJob(sess, jobs[i]); err != nil {
			_, err = sess.execute(context.Background(), "rollback", "get_mark_backfill_job")
			return nil, err
		}
	}

	_, err = sess.execute(context.Background(), "commit", "update_backfill_job")
	return jobs[:validLen], err
}

func getInterruptBackfillJobsForOneEle(sess *session, jobID, eleID int64) ([]*model.BackfillJob, error) {
	jobs, err := getBackfillJobs(sess, BackfillTable, fmt.Sprintf("job_id = %d and ele_id = %d and (state = %d or state = %d)",
		jobID, eleID, model.JobStateRollingback, model.JobStateCancelling))
	if err != nil || len(jobs) == 0 {
		return nil, err
	}
	validLen := 1
	firstJobID, firstEleID := jobs[0].JobID, jobs[0].EleID
	for i := 1; i < len(jobs); i++ {
		if jobs[i].JobID != firstJobID || jobs[i].EleID != firstEleID {
			break
		}
		validLen++
	}

	return jobs[:validLen], nil
}

func getBackfillJobs(sess *session, tblName, condition string) ([]*model.BackfillJob, error) {
	rows, err := sess.execute(context.Background(), fmt.Sprintf("select * from mysql.%s where %s", tblName, condition), "get_backfill_job")
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := make([]*model.BackfillJob, 0, len(rows))
	for _, row := range rows {
		job := model.BackfillJob{
			ID:          row.GetInt64(0),
			JobID:       row.GetInt64(1),
			EleID:       row.GetInt64(2),
			EleKey:      row.GetBytes(3),
			PhysicalID:  row.GetInt64(4),
			Tp:          model.BackfillType(row.GetInt64(5)),
			Instance_ID: row.GetString(6),
			// TODO:
			// Instance_Lease: row.GetTime(7),
			State: model.JobState(row.GetInt64(8)),
		}
		job.Mate = &model.BackfillMeta{}
		err = job.Mate.Decode(row.GetBytes(9))
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, &job)
		// logutil.BgLogger().Info(fmt.Sprintf("get *****************************  no.%d, tp:%v, bf job:%#v", i, row.GetInt64(4), job))
	}
	return jobs, nil
}

func removeBackfillJob(sess *session, backfillJob *model.BackfillJob) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_backfill where section_id = %d and job_id = %d and ele_id = %d",
		backfillJob.ID, backfillJob.JobID, backfillJob.EleID)
	logutil.BgLogger().Warn("remove *****************************   " + fmt.Sprintf("sql:%v", sql))
	_, err := sess.execute(context.Background(), sql, "remove_backfill_job")
	return err
}

func updateBackfillJob(sess *session, backfillJob *model.BackfillJob) error {
	mate, err := backfillJob.Mate.Encode()
	if err != nil {
		return err
	}
	// TODO: add check for "lease is null" or "lease > limit"
	sql := fmt.Sprintf("update mysql.tidb_ddl_backfill set exec_id = '%s', exec_lease = '%s', state = %d, backfill_meta = '%s' where section_id = %d",
		backfillJob.Instance_ID, backfillJob.Instance_Lease, backfillJob.State, mate, backfillJob.ID)
	logutil.BgLogger().Warn("update *****************************   " + fmt.Sprintf("sql:%v, sess:%#v", sql, sess))
	_, err = sess.execute(context.Background(), sql, "update_backfill_job")
	return err
}

func addBackfillHistoryJob(sess *session, backfillJob *model.BackfillJob) error {
	mate, err := backfillJob.Mate.Encode()
	if err != nil {
		return err
	}
	emptyInstance := "null"
	instanceID := backfillJob.Instance_ID
	if len(backfillJob.Instance_ID) == 0 {
		instanceID = emptyInstance
	}

	sql := fmt.Sprintf("insert into mysql.tidb_ddl_backfill_history(section_id, job_id, ele_id, ele_key, physical_id, type, exec_id, exec_lease, state, backfill_meta) values (%d, %d, %d, '%s', %d, %d, '%s', '%s', %d, '%s')",
		backfillJob.ID, backfillJob.JobID, backfillJob.EleID, backfillJob.EleKey, backfillJob.PhysicalID, backfillJob.Tp, instanceID, backfillJob.Instance_Lease.String(), backfillJob.State, mate)
	logutil.BgLogger().Warn("add history *****************************   " + fmt.Sprintf("sql:%v", sql))
	_, err = sess.execute(context.Background(), sql, "add_backfill_history_job")
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
