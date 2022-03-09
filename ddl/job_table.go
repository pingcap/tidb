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
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
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
	getGeneralJobSQL                    = "select job_meta from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id order by min(job_id)) and not reorg"
	getGeneralJobWithoutRunningSQL      = "select job_meta from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id order by min(job_id)) and not reorg and job_id not in (%s)"
	getGeneralJobWithoutRunningAgainSQL = "select job_meta from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id order by min(job_id) limit 20) and not reorg and job_id not in (%s)"
)

func (d *ddl) getGeneralJob(sess sessionctx.Context) (*model.Job, error) {
	runningOrBlockedIDs := make([]string, 0, 10)
	d.runningDDLMapMu.RLock()
	for id := range d.runningJobMap {
		runningOrBlockedIDs = append(runningOrBlockedIDs, strconv.Itoa(id))
	}
	d.runningDDLMapMu.RUnlock()
	var sql string
	if len(runningOrBlockedIDs) == 0 {
		sql = getGeneralJobSQL
	} else {
		sql = fmt.Sprintf(getGeneralJobWithoutRunningSQL, strings.Join(runningOrBlockedIDs, ", "))
	}
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var rows []chunk.Row
	if rows, err = sqlexec.DrainRecordSet(d.ctx, rs, 8); err != nil {
		return nil, errors.Trace(err)
	}
	if err = rs.Close(); err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		logutil.BgLogger().Error("No job")
		return nil, nil
	}
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		job := model.Job{}
		err = job.Decode(jobBinary)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if job.Type == model.ActionDropSchema {
			for {
				canRun, err := d.checkDropSchemaJobIsRunnable(sess, &job)
				if err != nil {
					return nil, errors.Trace(err)
				}
				if canRun {
					log.Info("get ddl job from table", zap.String("job", job.String()))
					return &job, nil
				}
				runningOrBlockedIDs = append(runningOrBlockedIDs, strconv.FormatInt(job.ID, 10))
				sql = fmt.Sprintf(getGeneralJobWithoutRunningAgainSQL, strings.Join(runningOrBlockedIDs, ", "))
				if rs, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql); err != nil {
					return nil, errors.Trace(err)
				}
				if rows, err = sqlexec.DrainRecordSet(d.ctx, rs, 8); err != nil {
					return nil, errors.Trace(err)
				}
				if err = rs.Close(); err != nil {
					return nil, errors.Trace(err)
				}
				if len(rows) == 0 {
					continue
				}
				jobBinary = rows[0].GetBytes(0)
				var job model.Job
				if err = job.Decode(jobBinary); err != nil {
					return nil, errors.Trace(err)
				}
			}
		} else {
			log.Info("get ddl job from table", zap.String("job", job.String()))
			return &job, nil
		}
	}
	return nil, nil
}

func (d *ddl) checkDropSchemaJobIsRunnable(sess sessionctx.Context, job *model.Job) (bool, error) {
	sql := fmt.Sprintf("select * from mysql.tidb_ddl_job where schema_id = %d and reorg and job_id < %d limit 1", job.SchemaID, job.ID)
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return false, err
	}
	rows, err := sqlexec.DrainRecordSet(d.ctx, rs, 8)
	if err != nil {
		return false, err
	}
	err = rs.Close()
	if err != nil {
		return false, err
	}
	return len(rows) == 0, nil
}

func (d *ddl) checkReorgJobIsRunnable(sess sessionctx.Context, job *model.Job) (bool, error) {
	sql := fmt.Sprintf("select * from mysql.tidb_ddl_job where schema_id = %d and is_drop_schema and job_id < %d limit 1", job.SchemaID, job.ID)
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return false, err
	}
	rows, err := sqlexec.DrainRecordSet(d.ctx, rs, 8)
	if err != nil {
		return false, err
	}
	err = rs.Close()
	if err != nil {
		return false, err
	}
	return len(rows) == 0, nil
}

func (d *ddl) getReorgJob(sess sessionctx.Context) (*model.Job, error) {
	const (
		getReorgJobSQL            = "select job_meta from mysql.tidb_ddl_job where job_id = (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id having reorg and job_id order by min(job_id) limit 1)"
		getReorgJobWithRunningSQL = "select job_meta from mysql.tidb_ddl_job where job_id = (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id having reorg and job_id not in (%s) order by min(job_id) limit 1)"
	)
	runningOrBlockedIDs := make([]string, 0, 10)
	d.runningDDLMapMu.RLock()
	for id := range d.runningJobMap {
		runningOrBlockedIDs = append(runningOrBlockedIDs, strconv.Itoa(id))
	}
	d.runningDDLMapMu.RUnlock()
	var sql string
	if len(runningOrBlockedIDs) == 0 {
		sql = getReorgJobSQL
	} else {
		sql = fmt.Sprintf(getReorgJobWithRunningSQL, strings.Join(runningOrBlockedIDs, ", "))
	}
	rs, err := sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return nil, err
	}
	var rows []chunk.Row
	rows, err = sqlexec.DrainRecordSet(d.ctx, rs, 8)
	if err != nil {
		err = rs.Close()
		if err != nil {
			log.Error("close RecordSet", zap.Error(err))
		}
		return nil, err
	}
	err = rs.Close()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	jobBinary := rows[0].GetBytes(0)
	job := &model.Job{}
	err = job.Decode(jobBinary)
	if err != nil {
		return nil, err
	}
	cnt := 0
	for {
		if cnt > 5 {
			return nil, err
		}
		canRun, err := d.checkReorgJobIsRunnable(sess, job)
		if err != nil {
			return nil, err
		}
		if canRun {
			return job, nil
		}
		runningOrBlockedIDs = append(runningOrBlockedIDs, strconv.Itoa(int(job.ID)))
		sql = fmt.Sprintf("select job_meta from mysql.tidb_ddl_job where job_id = (select min(job_id) and not processing from mysql.tidb_ddl_job group by schema_id, table_id having reorg and job_id not in (%s) order by min(job_id) limit 1)", strings.Join(runningOrBlockedIDs, ", "))
		rs, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
		if err != nil {
			return nil, err
		}
		rows, err = sqlexec.DrainRecordSet(d.ctx, rs, 8)
		if err != nil {
			return nil, err
		}
		err = rs.Close()
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, nil
		}
		jobBinary = rows[0].GetBytes(0)
		job = &model.Job{}
		err = job.Decode(jobBinary)
		if err != nil {
			return nil, err
		}
		cnt = cnt + 1
	}
}

func (d *ddl) startDispatchLoop() {
	for !d.isOwner() {
		time.Sleep(time.Second)
		if isChanClosed(d.ctx.Done()) {
			return
		}
	}
	sess, err := d.sessPool.get()
	if err != nil {
		log.Fatal("fail to get sessPool", zap.Error(err))
	}
	defer d.sessPool.put(sess)
	var notifyDDLJobByEtcdChGeneral clientv3.WatchChan
	var notifyDDLJobByEtcdChReorg clientv3.WatchChan
	if d.etcdCli != nil {
		notifyDDLJobByEtcdChGeneral = d.etcdCli.Watch(context.Background(), addingDDLJobGeneral)
		notifyDDLJobByEtcdChReorg = d.etcdCli.Watch(context.Background(), addingDDLJobReorg)
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var ok bool
	var isTicker bool
	for {
		if isChanClosed(d.ctx.Done()) {
			return
		}
		if !d.isOwner() {
			time.Sleep(time.Second)
			continue
		}
		isTicker = false
		var enableGeneralDDLJob, enableReorgDDLJob bool
		select {
		case <-d.ddlJobCh:
			enableGeneralDDLJob = true
		case <-ticker.C:
			enableGeneralDDLJob = true
			enableReorgDDLJob = true
			isTicker = true
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
		}
		var retryGeneral, retryReorgDDLJob bool
		var cnt = 0
		for {
			if enableGeneralDDLJob {
				retryGeneral = d.generalDDLJob(sess)
			}
			if enableReorgDDLJob {
				retryReorgDDLJob = d.reorgDDLJob(sess)
			}
			if (retryGeneral || retryReorgDDLJob) && cnt < 2 && !isTicker {
				cnt = cnt + 1
				continue
			}
			break
		}
	}
}

func (d *ddl) generalDDLJob(sctx sessionctx.Context) bool {
	job, err := d.getGeneralJob(sctx)
	if err != nil {
		logutil.BgLogger().Error("[ddl] getGeneralJob", zap.Error(err))
		return true
	}
	if job == nil {
		return false
	}
	d.doGeneralDDLJobWorker(job)
	return false
}

func (d *ddl) doGeneralDDLJobWorker(job *model.Job) {
	d.insertRunningDDLJobMap(int(job.ID))
	d.wg.Run(func() {
		defer d.deleteRunningDDLJobMap(int(job.ID))
		log.Info("ready to get generalDDLWorkerPool")
		wk, err := d.generalDDLWorkerPool.get()
		log.Info("complete to get generalDDLWorkerPool")
		if err != nil {
			log.Error("fail to get generalDDLWorkerPool", zap.Error(err))
			return
		}
		defer func() {
			if r := recover(); r != nil {
				logutil.BgLogger().Error("doGeneralDDLJobWorker panic", zap.Any("recover()", r), zap.Stack("stack"))
			}
			d.generalDDLWorkerPool.put(wk)
		}()
		wk.handleDDLJobWaitSchemaSynced(d.ddlCtx, job)
		if err := wk.HandleDDLJob(d.ddlCtx, job, d.ddlJobCh); err != nil {
			log.Error("[ddl] handle General DDL job failed", zap.Error(err))
		}
	})
}

func (d *ddl) reorgDDLJob(sctx sessionctx.Context) bool {
	job, err := d.getReorgJob(sctx)
	if err != nil {
		logutil.BgLogger().Error("[ddl] getReorgJob", zap.Error(err))
		return true
	}
	if job == nil {
		return false
	}
	d.doReorgDDLJobWorker(job)
	return false
}

func (d *ddl) doReorgDDLJobWorker(job *model.Job) {
	d.insertRunningDDLJobMap(int(job.ID))
	d.wg.Run(func() {
		defer d.deleteRunningDDLJobMap(int(job.ID))
		log.Info("ready to get reorgWorkerPool")
		wk, err := d.reorgWorkerPool.get()
		log.Info("complete to get reorgWorkerPool")
		if err != nil {
			log.Error("fail to get reorgWorkerPool", zap.Error(err))
			return
		}
		defer func() {
			if r := recover(); r != nil {
				logutil.BgLogger().Error("doReorgDDLJobWorker panic", zap.Any("recover()", r), zap.Stack("stack"))
			}
			d.reorgWorkerPool.put(wk)
		}()
		wk.handleDDLJobWaitSchemaSynced(d.ddlCtx, job)
		if err := wk.HandleDDLJob(d.ddlCtx, job, d.ddlJobCh); err != nil {
			log.Error("[ddl] handle Reorg DDL job failed", zap.Error(err))
		}
	})
}

const addDDLJobSQL = "insert into mysql.tidb_ddl_job values"

func (d *ddl) addDDLJobs(job []*model.Job) error {
	var sql string
	for i, job := range job {
		b, err := job.Encode(true)
		if err != nil {
			return err
		}
		if i != 0 {
			sql += ","
		}
		sql += fmt.Sprintf("(%d, %t, %d, %d, 0x%x, %d, %t)", job.ID, mayNeedReorg(job), job.SchemaID, job.TableID, b, 0, job.Type == model.ActionDropSchema)
	}
	logutil.BgLogger().Debug("add ddl job to table", zap.String("sql", sql))
	sess, err := d.sessPool.get()
	if err != nil {
		logutil.BgLogger().Error("[ddl] get session from sessPool", zap.Error(err))
		return err
	}
	defer d.sessPool.put(sess)
	_, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), addDDLJobSQL+sql)
	if err != nil {
		logutil.BgLogger().Error("[ddl] add job to mysql.tidb_ddl_job table", zap.Error(err))
	}
	return err
}

func (w *worker) deleteDDLJob(job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_job where job_id = %d", job.ID)
	_, err := w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	return err
}

const updateConcurrencyDDLJobSQL = "update mysql.tidb_ddl_job set job_meta = 0x%x where job_id = %d"

func (w *worker) updateConcurrencyDDLJob(job *model.Job, updateRawArgs bool) error {
	return updateConcurrencyDDLJob(w.sessForJob, job, updateRawArgs)
}

func updateConcurrencyDDLJob(sctx sessionctx.Context, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return err
	}
	log.Warn("update ddl job", zap.String("job", job.String()))
	sql := fmt.Sprintf(updateConcurrencyDDLJobSQL, b, job.ID)
	_, err = sctx.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) UpdateDDLReorgStartHandleNew(job *model.Job, element *meta.Element, startKey kv.Key) error {
	sql := fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, curr_ele_id, curr_ele_type) values (%d, %d, 0x%x)", job.ID, element.ID, element.TypeKey)
	_, err := w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, ele_id, start_key) values (%d, %d, 0x%s)", job.ID, element.ID, wrapKey2String(startKey))
	_, err = w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) UpdateDDLReorgHandle(job *model.Job, startKey, endKey kv.Key, physicalTableID int64, element *meta.Element, newSess bool) error {
	var err error
	sess := w.sessForJob
	if newSess {
		sess, err = w.sessPool.get()
		if err != nil {
			logutil.BgLogger().Error("[ddl] fail to get sessPool", zap.Error(err))
			return err
		}
		defer w.sessPool.put(sess)
		if _, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "begin"); err != nil {
			logutil.BgLogger().Error("[ddl] fail to begin", zap.Error(err))
		}
		defer func() {
			if _, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "commit"); err != nil {
				logutil.BgLogger().Error("[ddl] fail to begin", zap.Error(err))
			}
		}()
	}
	sql := fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, curr_ele_id, curr_ele_type) values (%d, %d, 0x%x)", job.ID, element.ID, element.TypeKey)
	_, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("replace into mysql.tidb_ddl_reorg(job_id, ele_id, start_key, end_key, physical_id) values (%d, %d, %s, %s, %d)", job.ID, element.ID, wrapKey2String(startKey), wrapKey2String(endKey), physicalTableID)
	_, err = sess.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) RemoveDDLReorgHandle(job *model.Job, elements []*meta.Element) error {
	if len(elements) == 0 {
		return nil
	}
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_reorg where job_id = %d", job.ID)
	_, err := w.sessForJob.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func wrapKey2String(key []byte) string {
	if len(key) == 0 {
		return "''"
	}
	return fmt.Sprintf("0x%x", key)
}
