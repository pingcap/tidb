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
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	addingDDLJobGeneral = "/tidb/ddl/add_ddl_job_general"
	addingDDLJobReorg   = "/tidb/ddl/add_ddl_job_reorg"
)

func (d *ddl) insertRunningDDLJobMap(id int64) {
	d.runningJobs.Lock()
	defer d.runningJobs.Unlock()
	d.runningJobs.runningJobMap[id] = struct{}{}
}

func (d *ddl) deleteRunningDDLJobMap(id int64) {
	d.runningJobs.Lock()
	defer d.runningJobs.Unlock()
	delete(d.runningJobs.runningJobMap, id)
}

func (d *ddl) getRunningDDLJobIDs() string {
	d.resetRunningIDs()
	d.runningJobs.RLock()
	defer d.runningJobs.RUnlock()
	for id := range d.runningJobs.runningJobMap {
		d.runningJobIDs = append(d.runningJobIDs, strconv.Itoa(int(id)))
	}
	return strings.Join(d.runningJobIDs, ",")
}

func (d *ddl) resetRunningIDs() {
	d.runningJobIDs = d.runningJobIDs[:1]
}

const (
	getJob = "select job_meta from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id) and %s reorg and job_id not in (%s)"
)

type jobType int

func (t jobType) String() string {
	switch t {
	case general:
		return "general"
	case reorg:
		return "reorg"
	}
	return ""
}

const (
	general jobType = iota
	reorg           = iota
)

func (d *ddl) getJob(sess *session, tp jobType, filter func(*model.Job) (bool, error)) (*model.Job, error) {
	not := "not"
	label := "get_job_general"
	if tp == reorg {
		not = ""
		label = "get_job_reorg"
	}
	sql := fmt.Sprintf(getJob, not, d.getRunningDDLJobIDs())
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
		b, err := filter(&runJob)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if b {
			return &runJob, nil
		}
	}
	return nil, nil
}

func (d *ddl) getGeneralJob(sess *session) (*model.Job, error) {
	return d.getJob(sess, general, func(job *model.Job) (bool, error) {
		if job.Type == model.ActionDropSchema {
			sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where schema_id = %d and job_id < %d limit 1", job.SchemaID, job.ID)
			return d.checkJobIsRunnable(sess, sql)
		}
		return true, nil
	})
}

func (d *ddl) checkJobIsRunnable(sess *session, sql string) (bool, error) {
	rows, err := sess.execute(context.Background(), sql, "check_runnable")
	return len(rows) == 0, err
}

func (d *ddl) getReorgJob(sess *session) (*model.Job, error) {
	return d.getJob(sess, reorg, func(job *model.Job) (bool, error) {
		sql := fmt.Sprintf("select job_id from mysql.tidb_ddl_job where schema_id = %d and is_drop_schema and job_id < %d limit 1", job.SchemaID, job.ID)
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
	var notifyDDLJobByEtcdChGeneral clientv3.WatchChan
	var notifyDDLJobByEtcdChReorg clientv3.WatchChan
	if d.etcdCli != nil {
		notifyDDLJobByEtcdChGeneral = d.etcdCli.Watch(context.Background(), addingDDLJobGeneral)
		notifyDDLJobByEtcdChReorg = d.etcdCli.Watch(context.Background(), addingDDLJobReorg)
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		d.concurrentDDL.Wait()
		if isChanClosed(d.ctx.Done()) {
			return
		}
		if !d.isOwner() {
			d.once.Store(true)
			time.Sleep(time.Second)
			continue
		}
		select {
		case <-d.ddlJobCh:
		case <-ticker.C:
		case _, ok := <-notifyDDLJobByEtcdChGeneral:
			if !ok {
				logutil.BgLogger().Warn("[ddl] start worker watch channel closed", zap.String("watch key", addingDDLJobGeneral))
				notifyDDLJobByEtcdChGeneral = d.etcdCli.Watch(context.Background(), addingDDLJobGeneral)
				time.Sleep(time.Duration(1) * time.Second)
				continue
			}
		case _, ok := <-notifyDDLJobByEtcdChReorg:
			if !ok {
				logutil.BgLogger().Warn("[ddl] start worker watch channel closed", zap.String("watch key", addingDDLJobReorg))
				notifyDDLJobByEtcdChReorg = d.etcdCli.Watch(context.Background(), addingDDLJobReorg)
				time.Sleep(time.Duration(1) * time.Second)
				continue
			}
		case <-d.ctx.Done():
			return
		}
		d.runDDLJob(sess, d.generalDDLWorkerPool, d.getGeneralJob)
		d.runDDLJob(sess, d.reorgWorkerPool, d.getReorgJob)
	}
}

func (d *ddl) runDDLJob(sess *session, pool *workerPool, getJob func(*session) (*model.Job, error)) {
	wk, err := pool.get()
	if err != nil || wk == nil {
		logutil.BgLogger().Debug(fmt.Sprintf("[ddl] no %v worker available now", pool.tp()), zap.Error(err))
		return
	}

	job, err := getJob(sess)
	if job == nil || err != nil {
		if err != nil {
			logutil.BgLogger().Warn("[ddl] get job met error", zap.Error(err))
		}
		pool.put(wk)
		return
	}

	d.doDDLJob(wk, pool, job)
}

func (d *ddl) doDDLJob(wk *worker, pool *workerPool, job *model.Job) {
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
			wk.waitSchemaSynced(d.ddlCtx, job, 2*d.lease)
			d.once.Store(false)
		}
		if err := wk.HandleDDLJob(d.ddlCtx, job); err != nil {
			logutil.BgLogger().Info("[ddl] handle ddl job failed", zap.Error(err), zap.String("job", job.String()))
		}
	})
}

const (
	addDDLJobSQL               = "insert into mysql.tidb_ddl_job values"
	updateConcurrencyDDLJobSQL = "update mysql.tidb_ddl_job set job_meta = %s where job_id = %d"
)

func (d *ddl) addDDLJobs(jobs []*model.Job) error {
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
		b, err := job.Encode(true)
		if err != nil {
			return err
		}
		if i != 0 {
			sql.WriteString(",")
		}
		sql.WriteString(fmt.Sprintf("(%d, %t, %d, %d, %s, %t)", job.ID, job.MayNeedReorg(), job.SchemaID, job.TableID, wrapKey2String(b), job.Type == model.ActionDropSchema))
	}
	sess, err := d.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	sess.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	defer d.sessPool.put(sess)
	_, err = newSession(sess).execute(context.Background(), sql.String(), "insert_job")
	logutil.BgLogger().Debug("[ddl] add job to mysql.tidb_ddl_job table", zap.String("sql", sql.String()))
	return errors.Trace(err)
}

func (w *worker) deleteDDLJob(job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_job where job_id = %d", job.ID)
	_, err := w.sess.execute(context.Background(), sql, "delete_job")
	return errors.Trace(err)
}

func updateConcurrencyDDLJob(sctx *session, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(updateConcurrencyDDLJobSQL, wrapKey2String(b), job.ID)
	_, err = sctx.execute(context.Background(), sql, "update_job")
	return errors.Trace(err)
}

// getDDLReorgHandle get ddl reorg handle.
func getDDLReorgHandle(sess *session, job *model.Job) (element *meta.Element, startKey, endKey kv.Key, physicalTableID int64, err error) {
	var id int64
	var tp []byte
	sql := fmt.Sprintf("select curr_ele_id, curr_ele_type from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	rows, err := sess.execute(context.Background(), sql, "get_handle")
	if err != nil {
		return nil, nil, nil, 0, err
	}
	if len(rows) == 0 {
		return nil, nil, nil, 0, meta.ErrDDLReorgElementNotExist
	}
	id = rows[0].GetInt64(0)
	tp = rows[0].GetBytes(1)
	element = &meta.Element{
		ID:      id,
		TypeKey: tp,
	}
	sql = fmt.Sprintf("select start_key, end_key, physical_id from mysql.tidb_ddl_reorg where job_id = %d and ele_id = %d", job.ID, id)
	rows, err = sess.execute(context.Background(), sql, "get_handle")
	if err != nil {
		return nil, nil, nil, 0, err
	}

	startKey = rows[0].GetBytes(0)
	endKey = rows[0].GetBytes(1)
	physicalTableID = rows[0].GetInt64(2)
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

func updateDDLReorgStartHandle(sess *session, job *model.Job, element *meta.Element, startKey kv.Key) error {
	sql := fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, curr_ele_id, curr_ele_type) values (%d, %d, %s)", job.ID, element.ID, wrapKey2String(element.TypeKey))
	_, err := sess.execute(context.Background(), sql, "update_handle")
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, ele_id, start_key) values (%d, %d, %s)", job.ID, element.ID, wrapKey2String(startKey))
	_, err = sess.execute(context.Background(), sql, "update_handle")
	return err
}

func updateDDLReorgHandle(sess *session, job *model.Job, startKey kv.Key, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	sql := fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, curr_ele_id, curr_ele_type) values (%d, %d, %s)", job.ID, element.ID, wrapKey2String(element.TypeKey))
	_, err := sess.execute(context.Background(), sql, "update_handle")
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, ele_id, start_key, end_key, physical_id) values (%d, %d, %s, %s, %d)", job.ID, element.ID, wrapKey2String(startKey), wrapKey2String(endKey), physicalTableID)
	_, err = sess.execute(context.Background(), sql, "update_handle")
	return err
}

func removeDDLReorgHandle(sess *session, job *model.Job, elements []*meta.Element) error {
	if len(elements) == 0 {
		return nil
	}
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	_, err := sess.execute(context.Background(), sql, "remove_handle")
	return err
}

func removeReorgElement(sess *session, job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d and curr_ele_id is null", job.ID)
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

func (d *ddl) MigrateExistingDDLs() (err error) {
	sess, err := d.sessPool.get()
	if err != nil {
		return err
	}
	defer d.sessPool.put(sess)
	se := newSession(sess)
	// clean up these 3 tables.
	_, err = se.execute(context.Background(), "delete from mysql.tidb_ddl_job", "delete_old_ddl")
	if err != nil {
		return err
	}
	_, err = se.execute(context.Background(), "delete from mysql.tidb_ddl_reorg", "delete_old_reorg")
	if err != nil {
		return err
	}
	_, err = se.execute(context.Background(), "delete from mysql.tidb_ddl_history", "delete_old_history")
	if err != nil {
		return err
	}

	err = se.begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			se.rollback()
		} else {
			err = se.commit()
		}
	}()
	txn, err := se.txn()
	if err != nil {
		return err
	}

	for _, tp := range []workerType{addIdxWorker, generalWorker} {
		t := newMetaWithQueueTp(txn, tp)
		jobs, err := t.GetAllDDLJobsInQueue()
		if err != nil {
			return err
		}
		err = d.addDDLJobs(jobs)
		if err != nil {
			return err
		}
		if tp == addIdxWorker {
			for _, job := range jobs {
				element, start, end, pid, err := t.GetDDLReorgHandle(job)
				if err != nil {
					return err
				}
				err = updateDDLReorgHandle(se, job, start, end, pid, element)
				if err != nil {
					return err
				}
			}
		}
	}

	t := meta.NewMeta(txn)
	iterator, err := t.GetLastHistoryDDLJobsIterator()
	if err != nil {
		return err
	}
	jobs := make([]*model.Job, 0, 128)
	for {
		jobs, err = iterator.GetLastJobs(128, jobs)
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			break
		}
		for _, job := range jobs {
			err := addHistoryDDLJob(se, job, false)
			if err != nil {
				return err
			}
		}
	}

	if err = t.ClearALLDDLJob(); err != nil {
		return
	}
	if err = t.ClearALLDDLReorgHandle(); err != nil {
		return
	}
	return t.ClearALLHistoryJob()
}

func (d *ddl) BackOffDDLs() (err error) {
	sess, err := d.sessPool.get()
	if err != nil {
		return err
	}
	defer d.sessPool.put(sess)

	err = kv.RunInNewTxn(context.Background(), d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		if err := t.ClearALLDDLReorgHandle(); err != nil {
			return err
		}
		if err := t.ClearALLDDLJob(); err != nil {
			return err
		}
		return t.ClearALLHistoryJob()
	})
	if err != nil {
		return err
	}

	se := newSession(sess)
	err = se.begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			se.rollback()
		} else {
			err = se.commit()
		}
	}()
	jobs, err := getJobsBySQL(se, "tidb_ddl_job", "1 order by job_id")
	if err != nil {
		return err
	}

	reorgHandle, err := se.execute(context.Background(), "select start_key, end_key, physical_id, curr_ele_id, curr_ele_type from mysql.tidb_ddl_reorg", "get_handle")
	if err != nil {
		return err
	}
	txn, err := se.txn()
	if err != nil {
		return err
	}
	t := meta.NewMeta(txn)
	for _, job := range jobs {
		jobListKey := meta.DefaultJobListKey
		if job.MayNeedReorg() {
			jobListKey = meta.AddIndexJobListKey
		}
		if err := t.EnQueueDDLJob(job, jobListKey); err != nil {
			return err
		}
	}
	for _, row := range reorgHandle {
		jobID := row.GetInt64(0)
		if err := t.UpdateDDLReorgHandle(jobs[jobID], row.GetBytes(0), row.GetBytes(1), row.GetInt64(2), &meta.Element{ID: row.GetInt64(3), TypeKey: row.GetBytes(4)}); err != nil {
			return err
		}
	}

	// clean up these 3 tables.
	_, err = se.execute(context.Background(), "delete from mysql.tidb_ddl_job", "delete_old_ddl")
	if err != nil {
		return err
	}
	_, err = se.execute(context.Background(), "delete from mysql.tidb_ddl_reorg", "delete_old_reorg")
	if err != nil {
		return err
	}
	_, err = se.execute(context.Background(), "delete from mysql.tidb_ddl_history", "delete_old_history")
	if err != nil {
		return err
	}
	return
}
