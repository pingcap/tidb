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
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	addingDDLJobGeneral = "/tidb/ddl/add_ddl_job_general"
	addingDDLJobReorg   = "/tidb/ddl/add_ddl_job_reorg"
)

func (d *ddl) insertRunningDDLJobMap(id int) {
	d.runningDDLMapMu.Lock()
	defer d.runningDDLMapMu.Unlock()
	d.runningJobMap[id] = struct{}{}
}

func (d *ddl) deleteRunningDDLJobMap(id int) {
	d.runningDDLMapMu.Lock()
	defer d.runningDDLMapMu.Unlock()
	delete(d.runningJobMap, id)
}

const (
	getJob = "select job_meta from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id) and %s reorg and job_id not in (%s)"
)

type jobType int

const (
	general jobType = iota
	reorg           = iota
)

func (d *ddl) getJob(sess *session, tp jobType, filter func(*model.Job) (bool, error)) (*model.Job, error) {
	d.resetRunningIDs()
	d.runningDDLMapMu.RLock()
	for id := range d.runningJobMap {
		d.runningOrBlockedIDs = append(d.runningOrBlockedIDs, strconv.Itoa(id))
	}
	d.runningDDLMapMu.RUnlock()
	not := "not"
	label := "get_job_general"
	if tp == reorg {
		not = ""
		label = "get_job_reorg"
	}
	sql := fmt.Sprintf(getJob, not, strings.Join(d.runningOrBlockedIDs, ","))
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

func (d *ddl) resetRunningIDs() {
	d.runningOrBlockedIDs = d.runningOrBlockedIDs[:1]
}

func (d *ddl) getGeneralJob(sess *session) (*model.Job, error) {
	return d.getJob(sess, general, func(job *model.Job) (bool, error) {
		if job.Type == model.ActionDropSchema {
			sql := fmt.Sprintf("select * from mysql.tidb_ddl_job where schema_id = %d and job_id < %d limit 1", job.SchemaID, job.ID)
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
		sql := fmt.Sprintf("select * from mysql.tidb_ddl_job where schema_id = %d and is_drop_schema and job_id < %d limit 1", job.SchemaID, job.ID)
		return d.checkJobIsRunnable(sess, sql)
	})
}

func (d *ddl) startDispatchLoop() {
	se, err := d.sessPool.get()
	if err != nil {
		log.Fatal("fail to get sessPool", zap.Error(err))
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
	var ok bool
	for {
		if isChanClosed(d.ctx.Done()) {
			return
		}
		if !d.isOwner() {
			time.Sleep(time.Second)
			continue
		}
		var enableGeneralDDLJob, enableReorgDDLJob bool
		select {
		case <-d.ddlJobCh:
			enableGeneralDDLJob = true
			enableReorgDDLJob = true
		case <-ticker.C:
			enableGeneralDDLJob = true
			enableReorgDDLJob = true
		case _, ok = <-notifyDDLJobByEtcdChGeneral:
			if !ok {
				logutil.BgLogger().Error("[ddl] notifyDDLJobByEtcdChGeneral channel closed")
				notifyDDLJobByEtcdChGeneral = d.etcdCli.Watch(context.Background(), addingDDLJobGeneral)
				time.Sleep(time.Duration(1) * time.Second)
				continue
			}
			enableGeneralDDLJob = true
		case _, ok = <-notifyDDLJobByEtcdChReorg:
			if !ok {
				logutil.BgLogger().Error("[ddl] notifyDDLJobByEtcdChGeneral channel closed")
				notifyDDLJobByEtcdChReorg = d.etcdCli.Watch(context.Background(), addingDDLJobReorg)
				time.Sleep(time.Duration(1) * time.Second)
				continue
			}
			enableReorgDDLJob = true
		case <-d.ctx.Done():
			return
		}
		for enableGeneralDDLJob || enableReorgDDLJob {
			if enableGeneralDDLJob {
				enableGeneralDDLJob = d.generalDDLJob(sess)
			}
			if enableReorgDDLJob {
				enableReorgDDLJob = d.reorgDDLJob(sess)
			}
		}
	}
}

func (d *ddl) generalDDLJob(sess *session) bool {
	wk, err := d.generalDDLWorkerPool.get()
	if err != nil {
		logutil.BgLogger().Warn("[ddl] get general worker fail", zap.Error(err))
	}
	if wk == nil {
		logutil.BgLogger().Debug("[ddl] no general worker available now")
		return false
	}
	job, err := d.getGeneralJob(sess)
	if job == nil || err != nil {
		if err != nil {
			logutil.BgLogger().Warn("[ddl] get job met error", zap.Error(err))
		}
		d.generalDDLWorkerPool.put(wk)
		return false
	}
	d.doGeneralDDLJobWorker(wk, job)
	return true
}

func (d *ddl) doGeneralDDLJobWorker(wk *worker, job *model.Job) {
	injectFailPointForGetJob(job)
	d.insertRunningDDLJobMap(int(job.ID))
	d.wg.Run(func() {
		metrics.DDLRunningJobCount.WithLabelValues("general").Inc()
		defer func() {
			d.generalDDLWorkerPool.put(wk)
			d.deleteRunningDDLJobMap(int(job.ID))
			if job.IsSynced() || job.IsCancelled() || job.IsRollbackDone() {
				asyncNotify(d.ddlJobDoneCh)
			}
			asyncNotify(d.ddlJobCh)
			metrics.DDLRunningJobCount.WithLabelValues("general").Dec()
		}()
		wk.handleDDLJobWaitSchemaSynced(d.ddlCtx, job)
		if err := wk.HandleDDLJob(d.ddlCtx, job); err != nil {
			log.Error("[ddl] handle General DDL job failed", zap.Error(err))
		}
	})
}

func (d *ddl) reorgDDLJob(sess *session) bool {
	wk, err := d.reorgWorkerPool.get()
	if err != nil {
		logutil.BgLogger().Warn("[ddl] get reorg worker fail", zap.Error(err))
	}
	if wk == nil {
		return false
	}
	job, err := d.getReorgJob(sess)
	if job == nil || err != nil {
		if err != nil {
			logutil.BgLogger().Warn("[ddl] get job met error", zap.Error(err))
		}
		d.reorgWorkerPool.put(wk)
		return false
	}
	d.doReorgDDLJobWorker(wk, job)
	return true
}

func (d *ddl) doReorgDDLJobWorker(wk *worker, job *model.Job) {
	injectFailPointForGetJob(job)
	d.insertRunningDDLJobMap(int(job.ID))
	d.wg.Run(func() {
		metrics.DDLRunningJobCount.WithLabelValues("reorg").Inc()
		defer func() {
			d.reorgWorkerPool.put(wk)
			d.deleteRunningDDLJobMap(int(job.ID))
			if job.IsSynced() || job.IsCancelled() || job.IsRollbackDone() {
				asyncNotify(d.ddlJobDoneCh)
			}
			asyncNotify(d.ddlJobCh)
			metrics.DDLRunningJobCount.WithLabelValues("reorg").Dec()
		}()
		wk.handleDDLJobWaitSchemaSynced(d.ddlCtx, job)
		if err := wk.HandleDDLJob(d.ddlCtx, job); err != nil {
			log.Error("[ddl] handle Reorg DDL job failed", zap.Error(err))
		}
	})
}

const addDDLJobSQL = "insert into mysql.tidb_ddl_job values"

func (d *ddl) addDDLJobs(jobs []*model.Job) error {
	if len(jobs) == 0 {
		return nil
	}
	var sql string
	for i, job := range jobs {
		b, err := job.Encode(true)
		if err != nil {
			return err
		}
		if i != 0 {
			sql += ","
		}
		sql += fmt.Sprintf("(%d, %t, %d, %d, 0x%x, %d, %t)", job.ID, job.MayNeedReorg(), job.SchemaID, job.TableID, b, 0, job.Type == model.ActionDropSchema)
	}
	logutil.BgLogger().Debug("add ddl job to table", zap.String("sql", sql))
	sess, err := d.sessPool.get()
	if err != nil {
		logutil.BgLogger().Error("[ddl] get session from sessPool", zap.Error(err))
		return err
	}
	sess.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	defer d.sessPool.put(sess)
	_, err = newSession(sess).execute(context.Background(), addDDLJobSQL+sql, "insert_job")
	if err != nil {
		logutil.BgLogger().Error("[ddl] add job to mysql.tidb_ddl_job table", zap.Error(err))
	}
	return err
}

func (w *worker) deleteDDLJob(job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_job where job_id = %d", job.ID)
	_, err := w.sess.execute(context.Background(), sql, "delete_job")
	return err
}

const updateConcurrencyDDLJobSQL = "update mysql.tidb_ddl_job set job_meta = 0x%x where job_id = %d"

func (w *worker) updateConcurrencyDDLJob(job *model.Job, updateRawArgs bool) error {
	return updateConcurrencyDDLJob(w.sess, job, updateRawArgs)
}

func updateConcurrencyDDLJob(sctx *session, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(updateConcurrencyDDLJobSQL, b, job.ID)
	_, err = sctx.execute(context.Background(), sql, "update_job")
	if err != nil {
		logutil.BgLogger().Error("update meet error", zap.Error(err))
		return err
	}
	return nil
}

// GetDDLReorgHandleForTest gets the latest processed DDL reorganize position. It is only used for test.
func GetDDLReorgHandleForTest(job *model.Job, t *meta.Meta, sess sessionctx.Context) (*meta.Element, kv.Key, kv.Key, int64, error) {
	return getDDLReorgHandle(job, t, newSession(sess))
}

// getDDLReorgHandle gets the latest processed DDL reorganize position.
func getDDLReorgHandle(job *model.Job, t *meta.Meta, sess *session) (*meta.Element, kv.Key, kv.Key, int64, error) {
	if variable.AllowConcurrencyDDL.Load() {
		return GetConcurrentDDLReorgHandle(job, sess)
	}
	return t.GetDDLReorgHandle(job)
}

// UpdateDDLReorgStartHandle saves the job reorganization latest processed element and start handle for later resuming.
func (w *worker) UpdateDDLReorgStartHandle(t *meta.Meta, job *model.Job, element *meta.Element, startKey kv.Key) error {
	if variable.AllowConcurrencyDDL.Load() {
		sql := fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, curr_ele_id, curr_ele_type) values (%d, %d, 0x%x)", job.ID, element.ID, element.TypeKey)
		_, err := w.sess.execute(context.Background(), sql, "update_handle")
		if err != nil {
			return err
		}
		sql = fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, ele_id, start_key) values (%d, %d, %s)", job.ID, element.ID, wrapKey2String(startKey))
		_, err = w.sess.execute(context.Background(), sql, "update_handle")
		return err
	}
	return t.UpdateDDLReorgStartHandle(job, element, startKey)
}

// UpdateDDLReorgHandle saves the job reorganization latest processed information for later resuming.
func UpdateDDLReorgHandle(t *meta.Meta, sess *session, job *model.Job, startKey, endKey kv.Key, physicalTableID int64, element *meta.Element) error {
	if variable.AllowConcurrencyDDL.Load() {
		sql := fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, curr_ele_id, curr_ele_type) values (%d, %d, 0x%x)", job.ID, element.ID, element.TypeKey)
		_, err := sess.execute(context.Background(), sql, "update_handle")
		if err != nil {
			return err
		}
		sql = fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, ele_id, start_key, end_key, physical_id) values (%d, %d, %s, %s, %d)", job.ID, element.ID, wrapKey2String(startKey), wrapKey2String(endKey), physicalTableID)
		_, err = sess.execute(context.Background(), sql, "update_handle")
		return err
	}
	return t.UpdateDDLReorgHandle(job, startKey, endKey, physicalTableID, element)
}

func (w *worker) RemoveDDLReorgHandle(t *meta.Meta, job *model.Job, elements []*meta.Element) error {
	if variable.AllowConcurrencyDDL.Load() {
		if len(elements) == 0 {
			return nil
		}
		sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
		_, err := w.sess.execute(context.Background(), sql, "remove_handle")
		if err != nil {
			return err
		}
		return nil
	}
	return t.RemoveDDLReorgHandle(job, elements)
}

func wrapKey2String(key []byte) string {
	if len(key) == 0 {
		return "''"
	}
	return fmt.Sprintf("0x%x", key)
}

// GetDDLJobID is a sql that can get job id
const GetDDLJobID = "select job_meta from mysql.tidb_ddl_job order by job_id"

// getConcurrencyDDLJobs get all DDL jobs and sort by job.ID
func getConcurrencyDDLJobs(sess sessionctx.Context) ([]*model.Job, error) {
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), GetDDLJobID)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = rs.Close()
		if err != nil {
			logutil.BgLogger().Error("close result set error", zap.Error(err))
		}
	}()
	var rows []chunk.Row
	rows, err = sqlexec.DrainRecordSet(context.TODO(), rs, 8)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	jobs := make([]*model.Job, 0, 10)
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		job := &model.Job{}
		err = job.Decode(jobBinary)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// getDDLJobs get all DDL jobs and sorts jobs by job.ID.
func getDDLJobs(t *meta.Meta) ([]*model.Job, error) {
	generalJobs, err := getDDLJobsInQueue(t, meta.DefaultJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	addIdxJobs, err := getDDLJobsInQueue(t, meta.AddIndexJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := append(generalJobs, addIdxJobs...)
	sort.Sort(jobArray(jobs))
	return jobs, nil
}

// GetAllDDLJobs get all DDL jobs and sorts jobs by job.ID.
func GetAllDDLJobs(sess sessionctx.Context, t *meta.Meta) ([]*model.Job, error) {
	if variable.AllowConcurrencyDDL.Load() {
		return getConcurrencyDDLJobs(sess)
	}

	return getDDLJobs(t)
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
