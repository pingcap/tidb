// Copyright 2014 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type limitJobTask struct {
	job *model.Job
	// when we combine multiple jobs into one task,
	// append the errChs to this slice.
	errChs   []chan error
	cacheErr error
}

func (t *limitJobTask) NotifyError(err error) {
	for _, errCh := range t.errChs {
		errCh <- err
	}
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
	if err := d.checkAndUpdateClusterState(true); err != nil {
		logutil.BgLogger().Fatal("dispatch loop get cluster state failed, it should not happen, please try restart TiDB", zap.Error(err))
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

func (d *ddl) deliverJobTask(task *limitJobTask) {
	if task.job.LocalMode {
		d.limitJobChV2 <- task
	} else {
		d.limitJobCh <- task
	}
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
			logutil.BgLogger().Info("handle ddl job failed", zap.String("category", "ddl"), zap.Error(err), zap.String("job", job.String()))
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
			d.runningJobs.remove(job)
			asyncNotify(d.ddlJobCh)
			metrics.DDLRunningJobCount.WithLabelValues(pool.tp().String()).Dec()
		}()
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
					cleanMDLInfo(d.sessPool, job.ID, d.etcdCli)
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
			logutil.Logger(logCtx).Info("handle ddl job failed", zap.String("category", "ddl"), zap.Error(err), zap.String("job", job.String()))
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

// addBatchDDLJobs gets global job IDs and puts the DDL jobs in the DDL job table or local worker.
func (d *ddl) addBatchDDLJobs(tasks []*limitJobTask) error {
	var ids []int64
	var err error

	if len(tasks) == 0 {
		return nil
	}

	se, err := d.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer d.sessPool.Put(se)
	job, err := getJobsBySQL(sess.NewSession(se), JobTable, fmt.Sprintf("type = %d", model.ActionFlashbackCluster))
	if err != nil {
		return errors.Trace(err)
	}
	if len(job) != 0 {
		return errors.Errorf("Can't add ddl job, have flashback cluster job")
	}

	var (
		startTS = uint64(0)
		bdrRole = string(ast.BDRRoleNone)
	)

	if newTasks, err := d.combineBatchCreateTableJobs(tasks); err == nil {
		tasks = newTasks
	}

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	// lock to reduce conflict
	d.globalIDLock.Lock()
	err = kv.RunInNewTxn(ctx, d.store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		ids, err = t.GenGlobalIDs(len(tasks))
		if err != nil {
			return errors.Trace(err)
		}

		bdrRole, err = t.GetBDRRole()
		if err != nil {
			return errors.Trace(err)
		}

		startTS = txn.StartTS()

		// for localmode, we still need to check this variable if upgrading below v6.2.
		if variable.DDLForce2Queue.Load() {
			if err := d.checkFlashbackJobInQueue(t); err != nil {
				return err
			}
		}

		return nil
	})
	d.globalIDLock.Unlock()
	if err != nil {
		return errors.Trace(err)
	}

	jobTasks := make([]*model.Job, 0, len(tasks))
	for i, task := range tasks {
		job := task.job
		job.Version = currentVersion
		job.StartTS = startTS
		job.ID = ids[i]
		job.BDRRole = bdrRole

		// BDR mode only affects the DDL not from CDC
		if job.CDCWriteSource == 0 && bdrRole != string(ast.BDRRoleNone) {
			if job.Type == model.ActionMultiSchemaChange && job.MultiSchemaInfo != nil {
				for _, subJob := range job.MultiSchemaInfo.SubJobs {
					if ast.DeniedByBDR(ast.BDRRole(bdrRole), subJob.Type, job) {
						return dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
					}
				}
			} else if ast.DeniedByBDR(ast.BDRRole(bdrRole), job.Type, job) {
				return dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
			}
		}

		setJobStateToQueueing(job)

		// currently doesn't support pause job in local mode.
		if d.stateSyncer.IsUpgradingState() && !hasSysDB(job) && !job.LocalMode {
			if err = pauseRunningJob(sess.NewSession(se), job, model.AdminCommandBySystem); err != nil {
				logutil.BgLogger().Warn("pause user DDL by system failed", zap.String("category", "ddl-upgrading"), zap.Stringer("job", job), zap.Error(err))
				task.cacheErr = err
				continue
			}
			logutil.BgLogger().Info("pause user DDL by system successful", zap.String("category", "ddl-upgrading"), zap.Stringer("job", job))
		}

		if _, err := job.Encode(true); err != nil {
			return err
		}

		jobTasks = append(jobTasks, job)
		injectModifyJobArgFailPoint(job)
	}

	se.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)

	if tasks[0].job.LocalMode {
		for _, task := range tasks {
			d.localJobCh <- task
		}
		return nil
	}
	return errors.Trace(insertDDLJobs2Table(sess.NewSession(se), true, jobTasks...))
}

// combineBatchCreateTableJobs combine batch jobs to another batch jobs.
// currently it only support combine CreateTable to CreateTables.
func (d *ddl) combineBatchCreateTableJobs(tasks []*limitJobTask) ([]*limitJobTask, error) {
	if len(tasks) <= 1 || !tasks[0].job.LocalMode {
		return tasks, nil
	}
	var schemaName string
	jobs := make([]*model.Job, 0, len(tasks))
	for i, task := range tasks {
		if task.job.Type != model.ActionCreateTable {
			return tasks, nil
		}
		if i == 0 {
			schemaName = task.job.SchemaName
		} else if task.job.SchemaName != schemaName {
			return tasks, nil
		}
		jobs = append(jobs, task.job)
	}

	job, err := d.BatchCreateTableWithJobs(jobs)
	if err != nil {
		return tasks, err
	}
	logutil.BgLogger().Info("combine jobs to batch create table job", zap.String("category", "ddl"), zap.Int("len", len(tasks)))

	jobTask := &limitJobTask{job, []chan error{}, nil}
	// combine the error chans.
	for _, j := range tasks {
		jobTask.errChs = append(jobTask.errChs, j.errChs...)
	}
	return []*limitJobTask{jobTask}, nil
}

func (d *ddl) limitDDLJobs(ch chan *limitJobTask, handler func(tasks []*limitJobTask)) {
	defer tidbutil.Recover(metrics.LabelDDL, "limitDDLJobs", nil, true)

	tasks := make([]*limitJobTask, 0, batchAddingJobs)
	for {
		select {
		case task := <-ch:
			tasks = tasks[:0]
			jobLen := len(ch)
			tasks = append(tasks, task)
			for i := 0; i < jobLen; i++ {
				tasks = append(tasks, <-ch)
			}
			handler(tasks)
		case <-d.ctx.Done():
			return
		}
	}
}

// addBatchDDLJobsV1 gets global job IDs and puts the DDL jobs in the DDL queue.
func (d *ddl) addBatchDDLJobsV1(tasks []*limitJobTask) {
	startTime := time.Now()
	var err error
	// DDLForce2Queue is a flag to tell DDL worker to always push the job to the DDL queue.
	toTable := !variable.DDLForce2Queue.Load()
	if toTable {
		err = d.addBatchDDLJobs(tasks)
	} else {
		err = d.addBatchDDLJobs2Queue(tasks)
	}
	var jobs string
	for _, task := range tasks {
		if err == nil {
			err = task.cacheErr
		}
		task.NotifyError(err)
		jobs += task.job.String() + "; "
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerAddDDLJob, task.job.Type.String(),
			metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}
	if err != nil {
		logutil.BgLogger().Warn("add DDL jobs failed", zap.String("category", "ddl"), zap.String("jobs", jobs), zap.Error(err))
	} else {
		logutil.BgLogger().Info("add DDL jobs", zap.String("category", "ddl"), zap.Int("batch count", len(tasks)), zap.String("jobs", jobs), zap.Bool("table", toTable))
	}
}

// addBatchLocalDDLJobs gets global job IDs and delivery the DDL jobs to local TiDB
func (d *ddl) addBatchLocalDDLJobs(tasks []*limitJobTask) {
	err := d.addBatchDDLJobs(tasks)
	if err != nil {
		for _, task := range tasks {
			task.NotifyError(err)
		}
		logutil.BgLogger().Error("add DDL jobs failed", zap.String("category", "ddl"), zap.Bool("local_mode", true), zap.Error(err))
	} else {
		logutil.BgLogger().Info("add DDL jobs", zap.String("category", "ddl"), zap.Bool("local_mode", true), zap.Int("batch count", len(tasks)))
	}
}

func (d *ddl) addBatchDDLJobs2Queue(tasks []*limitJobTask) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	// lock to reduce conflict
	d.globalIDLock.Lock()
	defer d.globalIDLock.Unlock()
	return kv.RunInNewTxn(ctx, d.store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		ids, err := t.GenGlobalIDs(len(tasks))
		if err != nil {
			return errors.Trace(err)
		}

		if err := d.checkFlashbackJobInQueue(t); err != nil {
			return errors.Trace(err)
		}

		for i, task := range tasks {
			job := task.job
			job.Version = currentVersion
			job.StartTS = txn.StartTS()
			job.ID = ids[i]
			setJobStateToQueueing(job)
			if err = buildJobDependence(t, job); err != nil {
				return errors.Trace(err)
			}
			jobListKey := meta.DefaultJobListKey
			if job.MayNeedReorg() {
				jobListKey = meta.AddIndexJobListKey
			}
			injectModifyJobArgFailPoint(job)
			if err = t.EnQueueDDLJob(job, jobListKey); err != nil {
				return errors.Trace(err)
			}
		}
		failpoint.Inject("mockAddBatchDDLJobsErr", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(errors.Errorf("mockAddBatchDDLJobsErr"))
			}
		})
		return nil
	})
}
