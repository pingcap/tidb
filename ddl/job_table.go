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
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	addingDDLJobGeneral = "/tidb/ddl/add_ddl_job_general"
	addingDDLJobReorg   = "/tidb/ddl/add_ddl_job_reorg"
)

func (dc *ddlCtx) insertRunningDDLJobMap(id int64) {
	dc.runningJobs.Lock()
	defer dc.runningJobs.Unlock()
	dc.runningJobs.runningJobMap[id] = struct{}{}
}

func (dc *ddlCtx) deleteRunningDDLJobMap(id int64) {
	dc.runningJobs.Lock()
	defer dc.runningJobs.Unlock()
	delete(dc.runningJobs.runningJobMap, id)
}

func (dc *ddlCtx) getRunningDDLJobIDs() string {
	dc.resetRunningIDs()
	dc.runningJobs.RLock()
	defer dc.runningJobs.RUnlock()
	for id := range dc.runningJobs.runningJobMap {
		dc.runningJobIDs = append(dc.runningJobIDs, strconv.Itoa(int(id)))
	}
	return strings.Join(dc.runningJobIDs, ",")
}

func (dc *ddlCtx) resetRunningIDs() {
	dc.runningJobIDs = dc.runningJobIDs[:1]
}

const (
	getJobSQL = "select job_meta from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_id, table_id) and %s reorg and job_id not in (%s)"
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
	sql := fmt.Sprintf(getJobSQL, not, d.getRunningDDLJobIDs())
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
				logutil.BgLogger().Warn("[ddl] start general worker watch channel closed", zap.String("watch key", addingDDLJobGeneral))
				notifyDDLJobByEtcdChGeneral = d.etcdCli.Watch(context.Background(), addingDDLJobGeneral)
				time.Sleep(time.Second)
				continue
			}
		case _, ok := <-notifyDDLJobByEtcdChReorg:
			if !ok {
				logutil.BgLogger().Warn("[ddl] start reorg worker watch channel closed", zap.String("watch key", addingDDLJobReorg))
				notifyDDLJobByEtcdChReorg = d.etcdCli.Watch(context.Background(), addingDDLJobReorg)
				time.Sleep(time.Second)
				continue
			}
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
		defer func() {
			pool.put(wk)
			d.deleteRunningDDLJobMap(job.ID)
			asyncNotify(d.ddlJobCh)
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

func runInTxn(se *session, f func(*session) error) (err error) {
	err = se.begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			se.rollback()
			return
		}
		err = se.commit()
	}()
	err = f(se)
	return
}
