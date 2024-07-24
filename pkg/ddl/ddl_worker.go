// Copyright 2015 PingCAP, Inc.
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
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/binloginfo"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	pumpcli "github.com/pingcap/tidb/pkg/tidb-binlog/pump_client"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/resourcegrouptag"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	kvutil "github.com/tikv/client-go/v2/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	// ddlWorkerID is used for generating the next DDL worker ID.
	ddlWorkerID = atomicutil.NewInt32(0)
	// backfillContextID is used for generating the next backfill context ID.
	backfillContextID = atomicutil.NewInt32(0)
	// WaitTimeWhenErrorOccurred is waiting interval when processing DDL jobs encounter errors.
	WaitTimeWhenErrorOccurred = int64(1 * time.Second)

	mockDDLErrOnce = int64(0)
	// TestNotifyBeginTxnCh is used for if the txn is beginning in runInTxn.
	TestNotifyBeginTxnCh = make(chan struct{})
)

// GetWaitTimeWhenErrorOccurred return waiting interval when processing DDL jobs encounter errors.
func GetWaitTimeWhenErrorOccurred() time.Duration {
	return time.Duration(atomic.LoadInt64(&WaitTimeWhenErrorOccurred))
}

// SetWaitTimeWhenErrorOccurred update waiting interval when processing DDL jobs encounter errors.
func SetWaitTimeWhenErrorOccurred(dur time.Duration) {
	atomic.StoreInt64(&WaitTimeWhenErrorOccurred, int64(dur))
}

type workerType byte

const (
	// generalWorker is the worker who handles all DDL statements except “add index”.
	generalWorker workerType = 0
	// addIdxWorker is the worker who handles the operation of adding indexes.
	addIdxWorker workerType = 1
	// loaclWorker is the worker who handles the operation in local TiDB.
	// currently it only handle CreateTable job of fast create table enabled.
	localWorker workerType = 2
)

// worker is used for handling DDL jobs.
// Now we have two kinds of workers.
type worker struct {
	id              int32
	tp              workerType
	addingDDLJobKey string
	ddlJobCh        chan struct{}
	// for local mode worker, it's ctx of 'ddl', else it's the ctx of 'job scheduler'.
	ctx context.Context
	wg  sync.WaitGroup

	sessPool        *sess.Pool    // sessPool is used to new sessions to execute SQL in ddl package.
	sess            *sess.Session // sess is used and only used in running DDL job.
	delRangeManager delRangeManager
	logCtx          context.Context
	seqAllocator    *atomic.Uint64

	*ddlCtx
}

// JobContext is the ddl job execution context.
type JobContext struct {
	// below fields are cache for top sql
	ddlJobCtx          context.Context
	cacheSQL           string
	cacheNormalizedSQL string
	cacheDigest        *parser.Digest
	tp                 string

	resourceGroupName string
	cloudStorageURI   string
}

// NewJobContext returns a new ddl job context.
func NewJobContext() *JobContext {
	return &JobContext{
		ddlJobCtx:          context.Background(),
		cacheSQL:           "",
		cacheNormalizedSQL: "",
		cacheDigest:        nil,
		tp:                 "",
	}
}

func newWorker(ctx context.Context, tp workerType, sessPool *sess.Pool, delRangeMgr delRangeManager, dCtx *ddlCtx) *worker {
	worker := &worker{
		id:              ddlWorkerID.Add(1),
		tp:              tp,
		ddlJobCh:        make(chan struct{}, 1),
		ctx:             ctx,
		ddlCtx:          dCtx,
		sessPool:        sessPool,
		delRangeManager: delRangeMgr,
	}
	worker.addingDDLJobKey = addingDDLJobPrefix + worker.typeStr()
	worker.logCtx = tidblogutil.WithFields(context.Background(), zap.String("worker", worker.String()), zap.String("category", "ddl"))
	return worker
}

func (w *worker) jobLogger(job *model.Job) *zap.Logger {
	logger := tidblogutil.Logger(w.logCtx)
	if job != nil {
		logger = tidblogutil.LoggerWithTraceInfo(
			logger.With(zap.Int64("jobID", job.ID)),
			job.TraceInfo,
		)
	}
	return logger
}

func (w *worker) typeStr() string {
	var str string
	switch w.tp {
	case generalWorker:
		str = "general"
	case addIdxWorker:
		str = "add index"
	case localWorker:
		str = "local worker"
	default:
		str = "unknown"
	}
	return str
}

func (w *worker) String() string {
	return fmt.Sprintf("worker %d, tp %s", w.id, w.typeStr())
}

func (w *worker) Close() {
	startTime := time.Now()
	if w.sess != nil {
		w.sessPool.Put(w.sess.Session())
	}
	w.wg.Wait()
	tidblogutil.Logger(w.logCtx).Info("DDL worker closed", zap.Duration("take time", time.Since(startTime)))
}

func (e *executor) notifyNewJobByEtcd(etcdPath string, jobID int64, jobType string) {
	if e.etcdCli == nil {
		return
	}

	jobIDStr := strconv.FormatInt(jobID, 10)
	timeStart := time.Now()
	err := util.PutKVToEtcd(e.ctx, e.etcdCli, 1, etcdPath, jobIDStr)
	if err != nil {
		logutil.DDLLogger().Info("notify handling DDL job failed",
			zap.String("etcdPath", etcdPath),
			zap.Int64("jobID", jobID),
			zap.String("type", jobType),
			zap.Error(err))
	}
	metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerNotifyDDLJob, jobType, metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
}

func asyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (d *ddl) limitDDLJobs(ch chan *JobWrapper, handler func([]*JobWrapper)) {
	defer tidbutil.Recover(metrics.LabelDDL, "limitDDLJobs", nil, true)

	jobWs := make([]*JobWrapper, 0, batchAddingJobs)
	for {
		select {
		// the channel is never closed
		case jobW := <-ch:
			jobWs = jobWs[:0]
			jobLen := len(ch)
			jobWs = append(jobWs, jobW)
			for i := 0; i < jobLen; i++ {
				jobWs = append(jobWs, <-ch)
			}
			handler(jobWs)
		case <-d.ctx.Done():
			return
		}
	}
}

// addBatchDDLJobsV1 gets global job IDs and puts the DDL jobs in the DDL queue.
func (d *ddl) addBatchDDLJobsV1(jobWs []*JobWrapper) {
	startTime := time.Now()
	var err error
	// DDLForce2Queue is a flag to tell DDL worker to always push the job to the DDL queue.
	toTable := !variable.DDLForce2Queue.Load()
	if toTable {
		err = d.addBatchDDLJobs(jobWs)
	} else {
		err = d.addBatchDDLJobs2Queue(jobWs)
	}
	var jobs string
	for _, jobW := range jobWs {
		if err == nil {
			err = jobW.cacheErr
		}
		jobW.NotifyError(err)
		jobs += jobW.Job.String() + "; "
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerAddDDLJob, jobW.Job.Type.String(),
			metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}
	if err != nil {
		logutil.DDLLogger().Warn("add DDL jobs failed", zap.String("jobs", jobs), zap.Error(err))
	} else {
		logutil.DDLLogger().Info("add DDL jobs",
			zap.Int("batch count", len(jobWs)),
			zap.String("jobs", jobs),
			zap.Bool("table", toTable))
	}
}

// addBatchLocalDDLJobs gets global job IDs and delivery the DDL jobs to local TiDB
func (d *ddl) addBatchLocalDDLJobs(jobWs []*JobWrapper) {
	if newJobWs, err := combineBatchCreateTableJobs(jobWs); err == nil {
		jobWs = newJobWs
	}
	err := d.addBatchDDLJobs(jobWs)
	if err != nil {
		for _, jobW := range jobWs {
			jobW.NotifyError(err)
		}
		logutil.DDLLogger().Error("add DDL jobs failed", zap.Bool("local_mode", true), zap.Error(err))
	} else {
		logutil.DDLLogger().Info("add DDL jobs",
			zap.Bool("local_mode", true),
			zap.Int("batch count", len(jobWs)))
	}
}

// buildJobDependence sets the curjob's dependency-ID.
// The dependency-job's ID must less than the current job's ID, and we need the largest one in the list.
func buildJobDependence(t *meta.Meta, curJob *model.Job) error {
	// Jobs in the same queue are ordered. If we want to find a job's dependency-job, we need to look for
	// it from the other queue. So if the job is "ActionAddIndex" job, we need find its dependency-job from DefaultJobList.
	jobListKey := meta.DefaultJobListKey
	if !curJob.MayNeedReorg() {
		jobListKey = meta.AddIndexJobListKey
	}
	jobs, err := t.GetAllDDLJobsInQueue(jobListKey)
	if err != nil {
		return errors.Trace(err)
	}

	for _, job := range jobs {
		if curJob.ID < job.ID {
			continue
		}
		isDependent, err := curJob.IsDependentOn(job)
		if err != nil {
			return errors.Trace(err)
		}
		if isDependent {
			logutil.DDLLogger().Info("current DDL job depends on other job",
				zap.Stringer("currentJob", curJob),
				zap.Stringer("dependentJob", job))
			curJob.DependencyID = job.ID
			break
		}
	}
	return nil
}

func (d *ddl) addBatchDDLJobs2Queue(jobWs []*JobWrapper) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	// lock to reduce conflict
	d.globalIDLock.Lock()
	defer d.globalIDLock.Unlock()
	return kv.RunInNewTxn(ctx, d.store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		ids, err := t.GenGlobalIDs(len(jobWs))
		if err != nil {
			return errors.Trace(err)
		}

		if err := d.checkFlashbackJobInQueue(t); err != nil {
			return errors.Trace(err)
		}

		for i, jobW := range jobWs {
			job := jobW.Job
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

func (*ddl) checkFlashbackJobInQueue(t *meta.Meta) error {
	jobs, err := t.GetAllDDLJobsInQueue(meta.DefaultJobListKey)
	if err != nil {
		return errors.Trace(err)
	}
	for _, job := range jobs {
		if job.Type == model.ActionFlashbackCluster {
			return errors.Errorf("Can't add ddl job, have flashback cluster job")
		}
	}
	return nil
}

// TODO this failpoint is only checking how job scheduler handle
// corrupted job args, we should test it there by UT, not here.
func injectModifyJobArgFailPoint(jobWs []*JobWrapper) {
	failpoint.Inject("MockModifyJobArg", func(val failpoint.Value) {
		if val.(bool) {
			for _, jobW := range jobWs {
				job := jobW.Job
				// Corrupt the DDL job argument.
				if job.Type == model.ActionMultiSchemaChange {
					if len(job.MultiSchemaInfo.SubJobs) > 0 && len(job.MultiSchemaInfo.SubJobs[0].Args) > 0 {
						job.MultiSchemaInfo.SubJobs[0].Args[0] = 1
					}
				} else if len(job.Args) > 0 {
					job.Args[0] = 1
				}
			}
		}
	})
}

func setJobStateToQueueing(job *model.Job) {
	if job.Type == model.ActionMultiSchemaChange && job.MultiSchemaInfo != nil {
		for _, sub := range job.MultiSchemaInfo.SubJobs {
			sub.State = model.JobStateQueueing
		}
	}
	job.State = model.JobStateQueueing
}

// addBatchDDLJobs gets global job IDs and puts the DDL jobs in the DDL job table or local worker.
func (d *ddl) addBatchDDLJobs(jobWs []*JobWrapper) error {
	var err error

	if len(jobWs) == 0 {
		return nil
	}

	ctx := kv.WithInternalSourceType(d.ctx, kv.InternalTxnDDL)
	se, err := d.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer d.sessPool.Put(se)
	found, err := d.sysTblMgr.HasFlashbackClusterJob(ctx, d.minJobIDRefresher.GetCurrMinJobID())
	if err != nil {
		return errors.Trace(err)
	}
	if found {
		return errors.Errorf("Can't add ddl job, have flashback cluster job")
	}

	var (
		startTS = uint64(0)
		bdrRole = string(ast.BDRRoleNone)
	)

	err = kv.RunInNewTxn(ctx, d.store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)

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
	if err != nil {
		return errors.Trace(err)
	}

	for _, jobW := range jobWs {
		job := jobW.Job
		job.Version = currentVersion
		job.StartTS = startTS
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
				logutil.DDLUpgradingLogger().Warn("pause user DDL by system failed", zap.Stringer("job", job), zap.Error(err))
				jobW.cacheErr = err
				continue
			}
			logutil.DDLUpgradingLogger().Info("pause user DDL by system successful", zap.Stringer("job", job))
		}
	}

	se.GetSessionVars().SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	ddlSe := sess.NewSession(se)
	localMode := jobWs[0].Job.LocalMode
	if localMode {
		if err = fillJobRelatedGIDs(ctx, ddlSe, jobWs); err != nil {
			return err
		}
		for _, jobW := range jobWs {
			if _, err := jobW.Encode(true); err != nil {
				return err
			}
			d.localJobCh <- jobW
		}
		return nil
	}

	if err = GenGIDAndInsertJobsWithRetry(ctx, ddlSe, jobWs); err != nil {
		return errors.Trace(err)
	}
	for _, jobW := range jobWs {
		d.initJobDoneCh(jobW.ID)
	}

	return nil
}

// GenGIDAndInsertJobsWithRetry generate job related global ID and inserts DDL jobs to the DDL job
// table with retry. job id allocation and job insertion are in the same transaction,
// as we want to make sure DDL jobs are inserted in id order, then we can query from
// a min job ID when scheduling DDL jobs to mitigate https://github.com/pingcap/tidb/issues/52905.
// so this function has side effect, it will set table/db/job id of 'jobs'.
func GenGIDAndInsertJobsWithRetry(ctx context.Context, ddlSe *sess.Session, jobWs []*JobWrapper) error {
	count := getRequiredGIDCount(jobWs)
	return genGIDAndCallWithRetry(ctx, ddlSe, count, func(ids []int64) error {
		failpoint.Inject("mockGenGlobalIDFail", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(errors.New("gofail genGlobalIDs error"))
			}
		})
		assignGIDsForJobs(jobWs, ids)
		injectModifyJobArgFailPoint(jobWs)
		return insertDDLJobs2Table(ctx, ddlSe, jobWs...)
	})
}

// fillJobRelatedGIDs similar to GenGIDAndInsertJobsWithRetry, but only fill job related global IDs.
func fillJobRelatedGIDs(ctx context.Context, ddlSe *sess.Session, jobWs []*JobWrapper) error {
	var allocatedIDs []int64
	count := getRequiredGIDCount(jobWs)
	if err := genGIDAndCallWithRetry(ctx, ddlSe, count, func(ids []int64) error {
		allocatedIDs = ids
		return nil
	}); err != nil {
		return errors.Trace(err)
	}

	assignGIDsForJobs(jobWs, allocatedIDs)
	return nil
}

// getRequiredGIDCount returns the count of required global IDs for the jobs. it's calculated
// as: the count of jobs + the count of IDs for the jobs which do NOT have pre-allocated ID.
func getRequiredGIDCount(jobWs []*JobWrapper) int {
	count := len(jobWs)
	idCountForTable := func(info *model.TableInfo) int {
		c := 1
		if partitionInfo := info.GetPartitionInfo(); partitionInfo != nil {
			c += len(partitionInfo.Definitions)
		}
		return c
	}
	for _, jobW := range jobWs {
		if jobW.IDAllocated {
			continue
		}
		switch jobW.Type {
		case model.ActionCreateView, model.ActionCreateSequence, model.ActionCreateTable:
			info := jobW.Args[0].(*model.TableInfo)
			count += idCountForTable(info)
		case model.ActionCreateTables:
			infos := jobW.Args[0].([]*model.TableInfo)
			for _, info := range infos {
				count += idCountForTable(info)
			}
		case model.ActionCreateSchema:
			count++
		}
		// TODO support other type of jobs
	}
	return count
}

// assignGIDsForJobs should be used with getRequiredGIDCount, and len(ids) must equal
// what getRequiredGIDCount returns.
func assignGIDsForJobs(jobWs []*JobWrapper, ids []int64) {
	idx := 0

	assignIDsForTable := func(info *model.TableInfo) {
		info.ID = ids[idx]
		idx++
		if partitionInfo := info.GetPartitionInfo(); partitionInfo != nil {
			for i := range partitionInfo.Definitions {
				partitionInfo.Definitions[i].ID = ids[idx]
				idx++
			}
		}
	}
	for _, jobW := range jobWs {
		switch jobW.Type {
		case model.ActionCreateView, model.ActionCreateSequence, model.ActionCreateTable:
			info := jobW.Args[0].(*model.TableInfo)
			if !jobW.IDAllocated {
				assignIDsForTable(info)
			}
			jobW.TableID = info.ID
		case model.ActionCreateTables:
			if !jobW.IDAllocated {
				infos := jobW.Args[0].([]*model.TableInfo)
				for _, info := range infos {
					assignIDsForTable(info)
				}
			}
		case model.ActionCreateSchema:
			dbInfo := jobW.Args[0].(*model.DBInfo)
			if !jobW.IDAllocated {
				dbInfo.ID = ids[idx]
				idx++
			}
			jobW.SchemaID = dbInfo.ID
		}
		// TODO support other type of jobs
		jobW.ID = ids[idx]
		idx++
	}
}

// genGIDAndCallWithRetry generates global IDs and calls the function with retry.
// generate ID and call function runs in the same transaction.
func genGIDAndCallWithRetry(ctx context.Context, ddlSe *sess.Session, count int, fn func(ids []int64) error) error {
	var resErr error
	for i := uint(0); i < kv.MaxRetryCnt; i++ {
		resErr = func() (err error) {
			if err := ddlSe.Begin(ctx); err != nil {
				return errors.Trace(err)
			}
			defer func() {
				if err != nil {
					ddlSe.Rollback()
				}
			}()
			txn, err := ddlSe.Txn()
			if err != nil {
				return errors.Trace(err)
			}
			txn.SetOption(kv.Pessimistic, true)
			forUpdateTS, err := lockGlobalIDKey(ctx, ddlSe, txn)
			if err != nil {
				return errors.Trace(err)
			}
			txn.GetSnapshot().SetOption(kv.SnapshotTS, forUpdateTS)

			m := meta.NewMeta(txn)
			ids, err := m.GenGlobalIDs(count)
			if err != nil {
				return errors.Trace(err)
			}
			if err = fn(ids); err != nil {
				return errors.Trace(err)
			}
			return ddlSe.Commit(ctx)
		}()

		if resErr != nil && kv.IsTxnRetryableError(resErr) {
			logutil.DDLLogger().Warn("insert job meet retryable error", zap.Error(resErr))
			kv.BackOff(i)
			continue
		}
		break
	}
	return resErr
}

// lockGlobalIDKey locks the global ID key in the meta store. it keeps trying if
// meet write conflict, we cannot have a fixed retry count for this error, see this
// https://github.com/pingcap/tidb/issues/27197#issuecomment-2216315057.
// this part is same as how we implement pessimistic + repeatable read isolation
// level in SQL executor, see doLockKeys.
// NextGlobalID is a meta key, so we cannot use "select xx for update", if we store
// it into a table row or using advisory lock, we will depends on a system table
// that is created by us, cyclic. although we can create a system table without using
// DDL logic, we will only consider change it when we have data dictionary and keep
// it this way now.
// TODO maybe we can unify the lock mechanism with SQL executor in the future, or
// implement it inside TiKV client-go.
func lockGlobalIDKey(ctx context.Context, ddlSe *sess.Session, txn kv.Transaction) (uint64, error) {
	var (
		iteration   uint
		forUpdateTs = txn.StartTS()
		ver         kv.Version
		err         error
	)
	waitTime := ddlSe.GetSessionVars().LockWaitTimeout
	m := meta.NewMeta(txn)
	idKey := m.GlobalIDKey()
	for {
		lockCtx := tikv.NewLockCtx(forUpdateTs, waitTime, time.Now())
		err = txn.LockKeys(ctx, lockCtx, idKey)
		if err == nil || !terror.ErrorEqual(kv.ErrWriteConflict, err) {
			break
		}
		// ErrWriteConflict contains a conflict-commit-ts in most case, but it cannot
		// be used as forUpdateTs, see comments inside handleAfterPessimisticLockError
		ver, err = ddlSe.GetStore().CurrentVersion(oracle.GlobalTxnScope)
		if err != nil {
			break
		}
		forUpdateTs = ver.Ver

		kv.BackOff(iteration)
		// avoid it keep growing and overflow.
		iteration = min(iteration+1, math.MaxInt)
	}
	return forUpdateTs, err
}

// combineBatchCreateTableJobs combine batch jobs to another batch jobs.
// currently it only support combine CreateTable to CreateTables.
func combineBatchCreateTableJobs(jobWs []*JobWrapper) ([]*JobWrapper, error) {
	if len(jobWs) <= 1 {
		return jobWs, nil
	}
	var schemaName string
	jobs := make([]*model.Job, 0, len(jobWs))
	for i, jobW := range jobWs {
		// we don't merge jobs with ID pre-allocated.
		if jobW.Job.Type != model.ActionCreateTable || jobW.IDAllocated {
			return jobWs, nil
		}
		if i == 0 {
			schemaName = jobW.Job.SchemaName
		} else if jobW.Job.SchemaName != schemaName {
			return jobWs, nil
		}
		jobs = append(jobs, jobW.Job)
	}

	job, err := BatchCreateTableWithJobs(jobs)
	if err != nil {
		return jobWs, err
	}
	logutil.DDLLogger().Info("combine jobs to batch create table job", zap.Int("len", len(jobWs)))

	newJobW := &JobWrapper{
		Job:    job,
		ErrChs: []chan error{},
	}
	// combine the error chans.
	for _, j := range jobWs {
		newJobW.ErrChs = append(newJobW.ErrChs, j.ErrChs...)
	}
	return []*JobWrapper{newJobW}, nil
}

func injectFailPointForGetJob(job *model.Job) {
	if job == nil {
		return
	}
	failpoint.Inject("mockModifyJobSchemaId", func(val failpoint.Value) {
		job.SchemaID = int64(val.(int))
	})
	failpoint.Inject("MockModifyJobTableId", func(val failpoint.Value) {
		job.TableID = int64(val.(int))
	})
}

// handleUpdateJobError handles the too large DDL job.
func (w *worker) handleUpdateJobError(t *meta.Meta, job *model.Job, err error) error {
	if err == nil {
		return nil
	}
	if kv.ErrEntryTooLarge.Equal(err) {
		w.jobLogger(job).Warn("update DDL job failed", zap.String("job", job.String()), zap.Error(err))
		w.sess.Rollback()
		err1 := w.sess.Begin(w.ctx)
		if err1 != nil {
			return errors.Trace(err1)
		}
		// Reduce this txn entry size.
		job.BinlogInfo.Clean()
		job.Error = toTError(err)
		job.ErrorCount++
		job.SchemaState = model.StateNone
		job.State = model.JobStateCancelled
		err = w.finishDDLJob(t, job)
	}
	return errors.Trace(err)
}

// updateDDLJob updates the DDL job information.
func (w *worker) updateDDLJob(job *model.Job, updateRawArgs bool) error {
	failpoint.Inject("mockErrEntrySizeTooLarge", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(kv.ErrEntryTooLarge)
		}
	})

	if !updateRawArgs {
		w.jobLogger(job).Info("meet something wrong before update DDL job, shouldn't update raw args",
			zap.String("job", job.String()))
	}
	return errors.Trace(updateDDLJob2Table(w.sess, job, updateRawArgs))
}

// registerMDLInfo registers metadata lock info.
func (w *worker) registerMDLInfo(job *model.Job, ver int64) error {
	if !variable.EnableMDL.Load() {
		return nil
	}
	if ver == 0 {
		return nil
	}
	rows, err := w.sess.Execute(w.ctx, fmt.Sprintf("select table_ids from mysql.tidb_ddl_job where job_id = %d", job.ID), "register-mdl-info")
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return errors.Errorf("can't find ddl job %d", job.ID)
	}
	ownerID := w.ownerManager.ID()
	ids := rows[0].GetString(0)
	var sql string
	if tidbutil.IsSysDB(strings.ToLower(job.SchemaName)) {
		// DDLs that modify system tables could only happen in upgrade process,
		// we should not reference 'owner_id'. Otherwise, there is a circular blocking problem.
		sql = fmt.Sprintf("replace into mysql.tidb_mdl_info (job_id, version, table_ids) values (%d, %d, '%s')", job.ID, ver, ids)
	} else {
		sql = fmt.Sprintf("replace into mysql.tidb_mdl_info (job_id, version, table_ids, owner_id) values (%d, %d, '%s', '%s')", job.ID, ver, ids, ownerID)
	}
	_, err = w.sess.Execute(w.ctx, sql, "register-mdl-info")
	return err
}

// cleanMDLInfo cleans metadata lock info.
func (s *jobScheduler) cleanMDLInfo(job *model.Job, ownerID string) {
	if !variable.EnableMDL.Load() {
		return
	}
	var sql string
	if tidbutil.IsSysDB(strings.ToLower(job.SchemaName)) {
		// DDLs that modify system tables could only happen in upgrade process,
		// we should not reference 'owner_id'. Otherwise, there is a circular blocking problem.
		sql = fmt.Sprintf("delete from mysql.tidb_mdl_info where job_id = %d", job.ID)
	} else {
		sql = fmt.Sprintf("delete from mysql.tidb_mdl_info where job_id = %d and owner_id = '%s'", job.ID, ownerID)
	}
	sctx, _ := s.sessPool.Get()
	defer s.sessPool.Put(sctx)
	se := sess.NewSession(sctx)
	se.GetSessionVars().SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	_, err := se.Execute(s.schCtx, sql, "delete-mdl-info")
	if err != nil {
		logutil.DDLLogger().Warn("unexpected error when clean mdl info", zap.Int64("job ID", job.ID), zap.Error(err))
		return
	}
	// TODO we need clean it when version of JobStateRollbackDone is synced also.
	if job.State == model.JobStateSynced && s.etcdCli != nil {
		path := fmt.Sprintf("%s/%d/", util.DDLAllSchemaVersionsByJob, job.ID)
		_, err = s.etcdCli.Delete(s.schCtx, path, clientv3.WithPrefix())
		if err != nil {
			logutil.DDLLogger().Warn("delete versions failed", zap.Int64("job ID", job.ID), zap.Error(err))
		}
	}
}

// JobNeedGC is called to determine whether delete-ranges need to be generated for the provided job.
//
// NOTICE: BR also uses jobNeedGC to determine whether delete-ranges need to be generated for the provided job.
// Therefore, please make sure any modification is compatible with BR.
func JobNeedGC(job *model.Job) bool {
	if !job.IsCancelled() {
		if job.Warning != nil && dbterror.ErrCantDropFieldOrKey.Equal(job.Warning) {
			// For the field/key not exists warnings, there is no need to
			// delete the ranges.
			return false
		}
		switch job.Type {
		case model.ActionDropSchema, model.ActionDropTable,
			model.ActionTruncateTable, model.ActionDropIndex,
			model.ActionDropPrimaryKey,
			model.ActionDropTablePartition, model.ActionTruncateTablePartition,
			model.ActionDropColumn, model.ActionModifyColumn,
			model.ActionAddIndex, model.ActionAddPrimaryKey,
			model.ActionReorganizePartition, model.ActionRemovePartitioning,
			model.ActionAlterTablePartitioning:
			return true
		case model.ActionMultiSchemaChange:
			for i, sub := range job.MultiSchemaInfo.SubJobs {
				proxyJob := sub.ToProxyJob(job, i)
				needGC := JobNeedGC(&proxyJob)
				if needGC {
					return true
				}
			}
			return false
		}
	}
	return false
}

// finishDDLJob deletes the finished DDL job in the ddl queue and puts it to history queue.
// If the DDL job need to handle in background, it will prepare a background job.
func (w *worker) finishDDLJob(t *meta.Meta, job *model.Job) (err error) {
	startTime := time.Now()
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerFinishDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	if JobNeedGC(job) {
		err = w.delRangeManager.addDelRangeJob(w.ctx, job)
		if err != nil {
			return errors.Trace(err)
		}
	}

	switch job.Type {
	case model.ActionRecoverTable:
		err = finishRecoverTable(w, job)
	case model.ActionFlashbackCluster:
		err = finishFlashbackCluster(w, job)
	case model.ActionRecoverSchema:
		err = finishRecoverSchema(w, job)
	case model.ActionCreateTables:
		if job.IsCancelled() {
			// it may be too large that it can not be added to the history queue, so
			// delete its arguments
			job.Args = nil
		}
	}
	if err != nil {
		return errors.Trace(err)
	}
	// for local mode job, we didn't insert the job to ddl table now.
	// so no need to delete it.
	if !job.LocalMode {
		err = w.deleteDDLJob(job)
		if err != nil {
			return errors.Trace(err)
		}
	}

	job.BinlogInfo.FinishedTS = t.StartTS
	w.jobLogger(job).Info("finish DDL job", zap.String("job", job.String()))
	updateRawArgs := true
	if job.Type == model.ActionAddPrimaryKey && !job.IsCancelled() {
		// ActionAddPrimaryKey needs to check the warnings information in job.Args.
		// Notice: warnings is used to support non-strict mode.
		updateRawArgs = false
	}
	job.SeqNum = w.seqAllocator.Add(1)
	w.removeJobCtx(job)
	failpoint.InjectCall("afterFinishDDLJob", job)
	err = AddHistoryDDLJob(w.ctx, w.sess, t, job, updateRawArgs)
	return errors.Trace(err)
}

func finishRecoverTable(w *worker, job *model.Job) error {
	var (
		recoverInfo           *RecoverInfo
		recoverTableCheckFlag int64
	)
	err := job.DecodeArgs(&recoverInfo, &recoverTableCheckFlag)
	if err != nil {
		return errors.Trace(err)
	}
	if recoverTableCheckFlag == recoverCheckFlagEnableGC {
		err = enableGC(w)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func finishRecoverSchema(w *worker, job *model.Job) error {
	var (
		recoverSchemaInfo      *RecoverSchemaInfo
		recoverSchemaCheckFlag int64
	)
	err := job.DecodeArgs(&recoverSchemaInfo, &recoverSchemaCheckFlag)
	if err != nil {
		return errors.Trace(err)
	}
	if recoverSchemaCheckFlag == recoverCheckFlagEnableGC {
		err = enableGC(w)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (w *JobContext) setDDLLabelForTopSQL(jobQuery string) {
	if !topsqlstate.TopSQLEnabled() || jobQuery == "" {
		return
	}

	if jobQuery != w.cacheSQL || w.cacheDigest == nil {
		w.cacheNormalizedSQL, w.cacheDigest = parser.NormalizeDigest(jobQuery)
		w.cacheSQL = jobQuery
		w.ddlJobCtx = topsql.AttachAndRegisterSQLInfo(context.Background(), w.cacheNormalizedSQL, w.cacheDigest, false)
	} else {
		topsql.AttachAndRegisterSQLInfo(w.ddlJobCtx, w.cacheNormalizedSQL, w.cacheDigest, false)
	}
}

// DDLBackfillers contains the DDL need backfill step.
var DDLBackfillers = map[model.ActionType]string{
	model.ActionAddIndex:            "add_index",
	model.ActionModifyColumn:        "modify_column",
	model.ActionDropIndex:           "drop_index",
	model.ActionReorganizePartition: "reorganize_partition",
}

func getDDLRequestSource(jobType model.ActionType) string {
	if tp, ok := DDLBackfillers[jobType]; ok {
		return kv.InternalTxnBackfillDDLPrefix + tp
	}
	return kv.InternalTxnDDL
}

func (w *JobContext) setDDLLabelForDiagnosis(jobType model.ActionType) {
	if w.tp != "" {
		return
	}
	w.tp = getDDLRequestSource(jobType)
	w.ddlJobCtx = kv.WithInternalSourceAndTaskType(w.ddlJobCtx, w.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
}

func (w *worker) handleJobDone(d *ddlCtx, job *model.Job, t *meta.Meta) error {
	if err := w.checkBeforeCommit(); err != nil {
		return err
	}
	err := w.finishDDLJob(t, job)
	if err != nil {
		w.sess.Rollback()
		return err
	}

	err = w.sess.Commit(w.ctx)
	if err != nil {
		return err
	}
	cleanupDDLReorgHandles(job, w.sess)
	d.notifyJobDone(job.ID)
	return nil
}

func (w *worker) prepareTxn(job *model.Job) (kv.Transaction, error) {
	err := w.sess.Begin(w.ctx)
	if err != nil {
		return nil, err
	}
	failpoint.Inject("mockRunJobTime", func(val failpoint.Value) {
		if val.(bool) {
			time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond) // #nosec G404
		}
	})
	txn, err := w.sess.Txn()
	if err != nil {
		w.sess.Rollback()
		return txn, err
	}
	// Only general DDLs are allowed to be executed when TiKV is disk full.
	if w.tp == addIdxWorker && job.IsRunning() {
		txn.SetDiskFullOpt(kvrpcpb.DiskFullOpt_NotAllowedOnFull)
	}
	w.setDDLLabelForTopSQL(job.ID, job.Query)
	w.setDDLSourceForDiagnosis(job.ID, job.Type)
	jobContext := w.jobContext(job.ID, job.ReorgMeta)
	if tagger := w.getResourceGroupTaggerForTopSQL(job.ID); tagger != nil {
		txn.SetOption(kv.ResourceGroupTagger, tagger)
	}
	txn.SetOption(kv.ResourceGroupName, jobContext.resourceGroupName)
	// set request source type to DDL type
	txn.SetOption(kv.RequestSourceType, jobContext.ddlJobSourceType())
	return txn, err
}

// transitOneJobStep runs one step of the DDL job and persist the new job
// information.
//
// The first return value is the schema version after running the job. If it's
// non-zero, caller should wait for other nodes to catch up.
func (w *worker) transitOneJobStep(d *ddlCtx, job *model.Job) (int64, error) {
	var (
		err error
	)

	txn, err := w.prepareTxn(job)
	if err != nil {
		return 0, err
	}
	var t *meta.Meta
	if variable.EnableFastCreateTable.Load() {
		t = meta.NewMeta(txn, meta.WithUpdateTableName())
	} else {
		t = meta.NewMeta(txn)
	}

	if job.IsDone() || job.IsRollbackDone() || job.IsCancelled() {
		if job.IsDone() {
			job.State = model.JobStateSynced
		}
		// Inject the failpoint to prevent the progress of index creation.
		failpoint.Inject("create-index-stuck-before-ddlhistory", func(v failpoint.Value) {
			if sigFile, ok := v.(string); ok && job.Type == model.ActionAddIndex {
				for {
					time.Sleep(1 * time.Second)
					if _, err := os.Stat(sigFile); err != nil {
						if os.IsNotExist(err) {
							continue
						}
						failpoint.Return(0, errors.Trace(err))
					}
					break
				}
			}
		})
		return 0, w.handleJobDone(d, job, t)
	}
	d.mu.RLock()
	d.mu.hook.OnJobRunBefore(job)
	d.mu.RUnlock()

	// If running job meets error, we will save this error in job Error and retry
	// later if the job is not cancelled.
	schemaVer, updateRawArgs, runJobErr := w.runOneJobStep(d, t, job)

	d.mu.RLock()
	d.mu.hook.OnJobRunAfter(job)
	d.mu.RUnlock()

	if job.IsCancelled() {
		defer d.unlockSchemaVersion(job.ID)
		w.sess.Reset()
		return 0, w.handleJobDone(d, job, t)
	}

	if err = w.checkBeforeCommit(); err != nil {
		d.unlockSchemaVersion(job.ID)
		return 0, err
	}

	if runJobErr != nil && !job.IsRollingback() && !job.IsRollbackDone() {
		// If the running job meets an error
		// and the job state is rolling back, it means that we have already handled this error.
		// Some DDL jobs (such as adding indexes) may need to update the table info and the schema version,
		// then shouldn't discard the KV modification.
		// And the job state is rollback done, it means the job was already finished, also shouldn't discard too.
		// Otherwise, we should discard the KV modification when running job.
		w.sess.Reset()
		// If error happens after updateSchemaVersion(), then the schemaVer is updated.
		// Result in the retry duration is up to 2 * lease.
		schemaVer = 0
	}

	err = w.registerMDLInfo(job, schemaVer)
	if err != nil {
		w.sess.Rollback()
		d.unlockSchemaVersion(job.ID)
		return 0, err
	}
	err = w.updateDDLJob(job, updateRawArgs)
	if err = w.handleUpdateJobError(t, job, err); err != nil {
		w.sess.Rollback()
		d.unlockSchemaVersion(job.ID)
		return 0, err
	}
	writeBinlog(d.binlogCli, txn, job)
	// reset the SQL digest to make topsql work right.
	w.sess.GetSessionVars().StmtCtx.ResetSQLDigest(job.Query)
	err = w.sess.Commit(w.ctx)
	d.unlockSchemaVersion(job.ID)
	if err != nil {
		return 0, err
	}
	w.registerSync(job)

	// If error is non-retryable, we can ignore the sleep.
	if runJobErr != nil && errorIsRetryable(runJobErr, job) {
		w.jobLogger(job).Info("run DDL job failed, sleeps a while then retries it.",
			zap.Duration("waitTime", GetWaitTimeWhenErrorOccurred()), zap.Error(runJobErr))
		// wait a while to retry again. If we don't wait here, DDL will retry this job immediately,
		// which may act like a deadlock.
		select {
		case <-time.After(GetWaitTimeWhenErrorOccurred()):
		case <-w.ctx.Done():
		}
	}

	return schemaVer, nil
}

func (w *worker) checkBeforeCommit() error {
	if !w.ddlCtx.isOwner() && w.tp != localWorker {
		// Since this TiDB instance is not a DDL owner anymore,
		// it should not commit any transaction.
		w.sess.Rollback()
		return dbterror.ErrNotOwner
	}

	if err := w.ctx.Err(); err != nil {
		// The worker context is canceled, it should not commit any transaction.
		return err
	}
	return nil
}

// HandleLocalDDLJob handles local ddl job like fast create table.
// Compare with normal ddl job:
// 1. directly insert the job to history job table(incompatible with CDC).
// 2. no need to wait schema version(only support create table now).
// 3. no register mdl info(only support create table now).
func (w *worker) HandleLocalDDLJob(d *ddlCtx, job *model.Job) (err error) {
	txn, err := w.prepareTxn(job)
	if err != nil {
		return err
	}

	t := meta.NewMeta(txn, meta.WithUpdateTableName())
	d.mu.RLock()
	d.mu.hook.OnJobRunBefore(job)
	d.mu.RUnlock()

	_, _, err = w.runOneJobStep(d, t, job)
	defer d.unlockSchemaVersion(job.ID)
	if err != nil {
		return err
	}
	// no need to rollback for fast create table now.
	if job.IsCancelling() {
		job.State = model.JobStateCancelled
		job.Error = dbterror.ErrCancelledDDLJob
	}
	if job.IsCancelled() {
		w.sess.Reset()
		if err = w.handleJobDone(d, job, t); err != nil {
			return err
		}
		// return job.Error to let caller know the job is cancelled.
		return job.Error
	}

	d.mu.RLock()
	d.mu.hook.OnJobRunAfter(job)
	d.mu.RUnlock()

	writeBinlog(d.binlogCli, txn, job)
	// reset the SQL digest to make topsql work right.
	w.sess.GetSessionVars().StmtCtx.ResetSQLDigest(job.Query)

	job.State = model.JobStateSynced
	return w.handleJobDone(d, job, t)
}

func (w *JobContext) getResourceGroupTaggerForTopSQL() tikvrpc.ResourceGroupTagger {
	if !topsqlstate.TopSQLEnabled() || w.cacheDigest == nil {
		return nil
	}

	digest := w.cacheDigest
	tagger := func(req *tikvrpc.Request) {
		req.ResourceGroupTag = resourcegrouptag.EncodeResourceGroupTag(digest, nil,
			resourcegrouptag.GetResourceGroupLabelByKey(resourcegrouptag.GetFirstKeyFromRequest(req)))
	}
	return tagger
}

func (w *JobContext) ddlJobSourceType() string {
	return w.tp
}

func skipWriteBinlog(job *model.Job) bool {
	switch job.Type {
	// ActionUpdateTiFlashReplicaStatus is a TiDB internal DDL,
	// it's used to update table's TiFlash replica available status.
	case model.ActionUpdateTiFlashReplicaStatus:
		return true
	// Don't sync 'alter table cache|nocache' to other tools.
	// It's internal to the current cluster.
	case model.ActionAlterCacheTable, model.ActionAlterNoCacheTable:
		return true
	}

	return false
}

func writeBinlog(binlogCli *pumpcli.PumpsClient, txn kv.Transaction, job *model.Job) {
	if job.IsDone() || job.IsRollbackDone() ||
		// When this column is in the "delete only" and "delete reorg" states, the binlog of "drop column" has not been written yet,
		// but the column has been removed from the binlog of the write operation.
		// So we add this binlog to enable downstream components to handle DML correctly in this schema state.
		(job.Type == model.ActionDropColumn && job.SchemaState == model.StateDeleteOnly) {
		if skipWriteBinlog(job) {
			return
		}
		binloginfo.SetDDLBinlog(binlogCli, txn, job.ID, int32(job.SchemaState), job.Query)
	}
}

func chooseLeaseTime(t, max time.Duration) time.Duration {
	if t == 0 || t > max {
		return max
	}
	return t
}

// countForPanic records the error count for DDL job.
func (w *worker) countForPanic(job *model.Job) {
	// If run DDL job panic, just cancel the DDL jobs.
	if job.State == model.JobStateRollingback {
		job.State = model.JobStateCancelled
	} else {
		job.State = model.JobStateCancelling
	}
	job.ErrorCount++

	logger := w.jobLogger(job)
	// Load global DDL variables.
	if err1 := loadDDLVars(w); err1 != nil {
		logger.Error("load DDL global variable failed", zap.Error(err1))
	}
	errorCount := variable.GetDDLErrorCountLimit()

	if job.ErrorCount > errorCount {
		msg := fmt.Sprintf("panic in handling DDL logic and error count beyond the limitation %d, cancelled", errorCount)
		logger.Warn(msg)
		job.Error = toTError(errors.New(msg))
		job.State = model.JobStateCancelled
	}
}

// countForError records the error count for DDL job.
func (w *worker) countForError(err error, job *model.Job) error {
	job.Error = toTError(err)
	job.ErrorCount++

	logger := w.jobLogger(job)
	// If job is cancelled, we shouldn't return an error and shouldn't load DDL variables.
	if job.State == model.JobStateCancelled {
		logger.Info("DDL job is cancelled normally", zap.Error(err))
		return nil
	}
	logger.Warn("run DDL job error", zap.Error(err))

	// Load global DDL variables.
	if err1 := loadDDLVars(w); err1 != nil {
		logger.Error("load DDL global variable failed", zap.Error(err1))
	}
	// Check error limit to avoid falling into an infinite loop.
	if job.ErrorCount > variable.GetDDLErrorCountLimit() && job.State == model.JobStateRunning && job.IsRollbackable() {
		logger.Warn("DDL job error count exceed the limit, cancelling it now", zap.Int64("errorCountLimit", variable.GetDDLErrorCountLimit()))
		job.State = model.JobStateCancelling
	}
	return err
}

func (w *worker) processJobPausingRequest(d *ddlCtx, job *model.Job) (isRunnable bool, err error) {
	if job.IsPaused() {
		w.jobLogger(job).Debug("paused DDL job ", zap.String("job", job.String()))
		return false, err
	}
	if job.IsPausing() {
		w.jobLogger(job).Debug("pausing DDL job ", zap.String("job", job.String()))
		job.State = model.JobStatePaused
		return false, pauseReorgWorkers(w, d, job)
	}
	return true, nil
}

// runOneJobStep runs a DDL job *step*. It returns the current schema version in
// this transaction, if the given job.Args has changed, and the error. The *step*
// is defined as the following reasons:
//
// - TiDB uses "Asynchronous Schema Change in F1", one job may have multiple
// *steps* each for a schema state change such as 'delete only' -> 'write only'.
// Combined with caller transitOneJobStepAndWaitSync waiting for other nodes to
// catch up with the returned schema version, we can make sure the cluster will
// only have two adjacent schema state for a DDL object.
//
// - Some types of DDL jobs has defined its own *step*s other than F1 paper.
// These *step*s may not be schema state change, and their purposes are various.
// For example, onLockTables updates the lock state of one table every *step*.
//
// - To provide linearizability we have added extra job state change *step*. For
// example, if job becomes JobStateDone in runOneJobStep, we cannot return to
// user that the job is finished because other nodes in cluster may not be
// synchronized. So JobStateSynced *step* is added to make sure there is
// waitSchemaChanged to wait for all nodes to catch up JobStateDone.
func (w *worker) runOneJobStep(
	d *ddlCtx,
	t *meta.Meta,
	job *model.Job,
) (ver int64, updateRawArgs bool, err error) {
	defer tidbutil.Recover(metrics.LabelDDLWorker, fmt.Sprintf("%s runOneJobStep", w),
		func() {
			w.countForPanic(job)
		}, false)

	// Mock for run ddl job panic.
	failpoint.Inject("mockPanicInRunDDLJob", func(failpoint.Value) {})

	if job.Type != model.ActionMultiSchemaChange {
		w.jobLogger(job).Info("run DDL job", zap.String("category", "ddl"), zap.String("job", job.String()))
	}
	timeStart := time.Now()
	if job.RealStartTS == 0 {
		job.RealStartTS = t.StartTS
	}
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerRunDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()

	if job.IsCancelling() {
		w.jobLogger(job).Debug("cancel DDL job", zap.String("job", job.String()))
		ver, err = convertJob2RollbackJob(w, d, t, job)
		// if job is converted to rollback job, the job.Args may be changed for the
		// rollback logic, so we let caller persist the new arguments.
		updateRawArgs = job.IsRollingback()
		return
	}

	isRunnable, err := w.processJobPausingRequest(d, job)
	if !isRunnable {
		return ver, false, err
	}

	// It would be better to do the positive check, but no idea to list all valid states here now.
	if !job.IsRollingback() {
		job.State = model.JobStateRunning
	}

	prevState := job.State

	// For every type, `schema/table` modification and `job` modification are conducted
	// in the one kv transaction. The `schema/table` modification can be always discarded
	// by kv reset when meets an unhandled error, but the `job` modification can't.
	// So make sure job state and args change is after all other checks or make sure these
	// change has no effect when retrying it.
	switch job.Type {
	case model.ActionCreateSchema:
		ver, err = onCreateSchema(d, t, job)
	case model.ActionModifySchemaCharsetAndCollate:
		ver, err = onModifySchemaCharsetAndCollate(d, t, job)
	case model.ActionDropSchema:
		ver, err = onDropSchema(d, t, job)
	case model.ActionRecoverSchema:
		ver, err = w.onRecoverSchema(d, t, job)
	case model.ActionModifySchemaDefaultPlacement:
		ver, err = onModifySchemaDefaultPlacement(d, t, job)
	case model.ActionCreateTable:
		ver, err = onCreateTable(d, t, job)
	case model.ActionCreateTables:
		ver, err = onCreateTables(d, t, job)
	case model.ActionRepairTable:
		ver, err = onRepairTable(d, t, job)
	case model.ActionCreateView:
		ver, err = onCreateView(d, t, job)
	case model.ActionDropTable, model.ActionDropView, model.ActionDropSequence:
		ver, err = onDropTableOrView(d, t, job)
	case model.ActionDropTablePartition:
		ver, err = w.onDropTablePartition(d, t, job)
	case model.ActionTruncateTablePartition:
		ver, err = w.onTruncateTablePartition(d, t, job)
	case model.ActionExchangeTablePartition:
		ver, err = w.onExchangeTablePartition(d, t, job)
	case model.ActionAddColumn:
		ver, err = onAddColumn(d, t, job)
	case model.ActionDropColumn:
		ver, err = onDropColumn(d, t, job)
	case model.ActionModifyColumn:
		ver, err = w.onModifyColumn(d, t, job)
	case model.ActionSetDefaultValue:
		ver, err = onSetDefaultValue(d, t, job)
	case model.ActionAddIndex:
		ver, err = w.onCreateIndex(d, t, job, false)
	case model.ActionAddPrimaryKey:
		ver, err = w.onCreateIndex(d, t, job, true)
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		ver, err = onDropIndex(d, t, job)
	case model.ActionRenameIndex:
		ver, err = onRenameIndex(d, t, job)
	case model.ActionAddForeignKey:
		ver, err = w.onCreateForeignKey(d, t, job)
	case model.ActionDropForeignKey:
		ver, err = onDropForeignKey(d, t, job)
	case model.ActionTruncateTable:
		ver, err = w.onTruncateTable(d, t, job)
	case model.ActionRebaseAutoID:
		ver, err = onRebaseAutoIncrementIDType(d, t, job)
	case model.ActionRebaseAutoRandomBase:
		ver, err = onRebaseAutoRandomType(d, t, job)
	case model.ActionRenameTable:
		ver, err = onRenameTable(d, t, job)
	case model.ActionShardRowID:
		ver, err = w.onShardRowID(d, t, job)
	case model.ActionModifyTableComment:
		ver, err = onModifyTableComment(d, t, job)
	case model.ActionModifyTableAutoIdCache:
		ver, err = onModifyTableAutoIDCache(d, t, job)
	case model.ActionAddTablePartition:
		ver, err = w.onAddTablePartition(d, t, job)
	case model.ActionModifyTableCharsetAndCollate:
		ver, err = onModifyTableCharsetAndCollate(d, t, job)
	case model.ActionRecoverTable:
		ver, err = w.onRecoverTable(d, t, job)
	case model.ActionLockTable:
		ver, err = onLockTables(d, t, job)
	case model.ActionUnlockTable:
		ver, err = onUnlockTables(d, t, job)
	case model.ActionSetTiFlashReplica:
		ver, err = w.onSetTableFlashReplica(d, t, job)
	case model.ActionUpdateTiFlashReplicaStatus:
		ver, err = onUpdateFlashReplicaStatus(d, t, job)
	case model.ActionCreateSequence:
		ver, err = onCreateSequence(d, t, job)
	case model.ActionAlterIndexVisibility:
		ver, err = onAlterIndexVisibility(d, t, job)
	case model.ActionAlterSequence:
		ver, err = onAlterSequence(d, t, job)
	case model.ActionRenameTables:
		ver, err = onRenameTables(d, t, job)
	case model.ActionAlterTableAttributes:
		ver, err = onAlterTableAttributes(d, t, job)
	case model.ActionAlterTablePartitionAttributes:
		ver, err = onAlterTablePartitionAttributes(d, t, job)
	case model.ActionCreatePlacementPolicy:
		ver, err = onCreatePlacementPolicy(d, t, job)
	case model.ActionDropPlacementPolicy:
		ver, err = onDropPlacementPolicy(d, t, job)
	case model.ActionAlterPlacementPolicy:
		ver, err = onAlterPlacementPolicy(d, t, job)
	case model.ActionAlterTablePartitionPlacement:
		ver, err = onAlterTablePartitionPlacement(d, t, job)
	case model.ActionAlterTablePlacement:
		ver, err = onAlterTablePlacement(d, t, job)
	case model.ActionCreateResourceGroup:
		ver, err = onCreateResourceGroup(w.ctx, d, t, job)
	case model.ActionAlterResourceGroup:
		ver, err = onAlterResourceGroup(d, t, job)
	case model.ActionDropResourceGroup:
		ver, err = onDropResourceGroup(d, t, job)
	case model.ActionAlterCacheTable:
		ver, err = onAlterCacheTable(d, t, job)
	case model.ActionAlterNoCacheTable:
		ver, err = onAlterNoCacheTable(d, t, job)
	case model.ActionFlashbackCluster:
		ver, err = w.onFlashbackCluster(d, t, job)
	case model.ActionMultiSchemaChange:
		ver, err = onMultiSchemaChange(w, d, t, job)
	case model.ActionReorganizePartition, model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		ver, err = w.onReorganizePartition(d, t, job)
	case model.ActionAlterTTLInfo:
		ver, err = onTTLInfoChange(d, t, job)
	case model.ActionAlterTTLRemove:
		ver, err = onTTLInfoRemove(d, t, job)
	case model.ActionAddCheckConstraint:
		ver, err = w.onAddCheckConstraint(d, t, job)
	case model.ActionDropCheckConstraint:
		ver, err = onDropCheckConstraint(d, t, job)
	case model.ActionAlterCheckConstraint:
		ver, err = w.onAlterCheckConstraint(d, t, job)
	default:
		// Invalid job, cancel it.
		job.State = model.JobStateCancelled
		err = dbterror.ErrInvalidDDLJob.GenWithStack("invalid ddl job type: %v", job.Type)
	}

	// there are too many job types, instead let every job type output its own
	// updateRawArgs, we try to use these rules as a generalization:
	//
	// if job has no error, some arguments may be changed, there's no harm to update
	// it.
	updateRawArgs = err == nil
	// if job changed from running to rolling back, arguments may be changed
	if prevState == model.JobStateRunning && job.IsRollingback() {
		updateRawArgs = true
	}

	// Save errors in job if any, so that others can know errors happened.
	if err != nil {
		err = w.countForError(err, job)
	}
	return ver, updateRawArgs, err
}

func loadDDLVars(w *worker) error {
	// Get sessionctx from context resource pool.
	var ctx sessionctx.Context
	ctx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(ctx)
	return util.LoadDDLVars(ctx)
}

func toTError(err error) *terror.Error {
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	if ok {
		return tErr
	}

	// TODO: Add the error code.
	return dbterror.ClassDDL.Synthesize(terror.CodeUnknown, err.Error())
}

// waitSchemaChanged waits for the completion of updating all servers' schema or MDL synced. In order to make sure that happens,
// we wait at most 2 * lease time(sessionTTL, 90 seconds).
func waitSchemaChanged(ctx context.Context, d *ddlCtx, latestSchemaVersion int64, job *model.Job) error {
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		return nil
	}

	timeStart := time.Now()
	var err error
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerWaitSchemaChanged, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()

	if latestSchemaVersion == 0 {
		logutil.DDLLogger().Info("schema version doesn't change", zap.Int64("jobID", job.ID))
		return nil
	}

	err = d.schemaSyncer.OwnerUpdateGlobalVersion(ctx, latestSchemaVersion)
	if err != nil {
		logutil.DDLLogger().Info("update latest schema version failed", zap.Int64("ver", latestSchemaVersion), zap.Error(err))
		if variable.EnableMDL.Load() {
			return err
		}
		if terror.ErrorEqual(err, context.DeadlineExceeded) {
			// If err is context.DeadlineExceeded, it means waitTime(2 * lease) is elapsed. So all the schemas are synced by ticker.
			// There is no need to use etcd to sync. The function returns directly.
			return nil
		}
	}

	return checkAllVersions(ctx, d, job, latestSchemaVersion, timeStart)
}

// waitSchemaSyncedForMDL likes waitSchemaSynced, but it waits for getting the metadata lock of the latest version of this DDL.
func waitSchemaSyncedForMDL(ctx context.Context, d *ddlCtx, job *model.Job, latestSchemaVersion int64) error {
	timeStart := time.Now()
	return checkAllVersions(ctx, d, job, latestSchemaVersion, timeStart)
}

func buildPlacementAffects(oldIDs []int64, newIDs []int64) []*model.AffectedOption {
	if len(oldIDs) == 0 {
		return nil
	}

	affects := make([]*model.AffectedOption, len(oldIDs))
	for i := 0; i < len(oldIDs); i++ {
		affects[i] = &model.AffectedOption{
			OldTableID: oldIDs[i],
			TableID:    newIDs[i],
		}
	}
	return affects
}
