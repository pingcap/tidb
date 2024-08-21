// Copyright 2024 PingCAP, Inc.
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
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/serverstate"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// JobSubmitter collects the DDL jobs and submits them to job tables in batch.
// when fast-create is enabled, it will merge the create-table jobs to a single
// batch create-table job.
// export for testing.
type JobSubmitter struct {
	ctx               context.Context
	etcdCli           *clientv3.Client
	ownerManager      owner.Manager
	store             kv.Storage
	serverStateSyncer serverstate.Syncer
	ddlJobDoneChMap   *generic.SyncMap[int64, chan struct{}]

	// init at ddl start.
	sessPool          *sess.Pool
	sysTblMgr         systable.Manager
	minJobIDRefresher *systable.MinJobIDRefresher

	limitJobCh chan *JobWrapper
	// get notification if any DDL job submitted or finished.
	ddlJobNotifyCh chan struct{}
	globalIDLock   *sync.Mutex
}

func (s *JobSubmitter) submitLoop() {
	defer util.Recover(metrics.LabelDDL, "submitLoop", nil, true)

	jobWs := make([]*JobWrapper, 0, batchAddingJobs)
	ch := s.limitJobCh
	for {
		select {
		// the channel is never closed
		case jobW := <-ch:
			jobWs = jobWs[:0]
			failpoint.InjectCall("afterGetJobFromLimitCh", ch)
			jobLen := len(ch)
			jobWs = append(jobWs, jobW)
			for i := 0; i < jobLen; i++ {
				jobWs = append(jobWs, <-ch)
			}
			s.addBatchDDLJobs(jobWs)
		case <-s.ctx.Done():
			return
		}
	}
}

// addBatchDDLJobs gets global job IDs and puts the DDL jobs in the DDL queue.
func (s *JobSubmitter) addBatchDDLJobs(jobWs []*JobWrapper) {
	startTime := time.Now()
	var (
		err   error
		newWs []*JobWrapper
	)
	// DDLForce2Queue is a flag to tell DDL worker to always push the job to the DDL queue.
	toTable := !variable.DDLForce2Queue.Load()
	fastCreate := variable.EnableFastCreateTable.Load()
	if toTable {
		if fastCreate {
			newWs, err = mergeCreateTableJobs(jobWs)
			if err != nil {
				logutil.DDLLogger().Warn("failed to merge create table jobs", zap.Error(err))
			} else {
				jobWs = newWs
			}
		}
		err = s.addBatchDDLJobs2Table(jobWs)
	} else {
		err = s.addBatchDDLJobs2Queue(jobWs)
	}
	var jobs string
	for _, jobW := range jobWs {
		if err == nil {
			err = jobW.cacheErr
		}
		jobW.NotifyResult(err)
		jobs += jobW.Job.String() + "; "
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerAddDDLJob, jobW.Job.Type.String(),
			metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}
	if err != nil {
		logutil.DDLLogger().Warn("add DDL jobs failed", zap.String("jobs", jobs), zap.Error(err))
		return
	}
	// Notice worker that we push a new job and wait the job done.
	s.notifyNewJobSubmitted()
	logutil.DDLLogger().Info("add DDL jobs",
		zap.Int("batch count", len(jobWs)),
		zap.String("jobs", jobs),
		zap.Bool("table", toTable),
		zap.Bool("fast_create", fastCreate))
}

// mergeCreateTableJobs merges CreateTable jobs to CreateTables.
func mergeCreateTableJobs(jobWs []*JobWrapper) ([]*JobWrapper, error) {
	if len(jobWs) <= 1 {
		return jobWs, nil
	}
	resJobWs := make([]*JobWrapper, 0, len(jobWs))
	mergeableJobWs := make(map[string][]*JobWrapper, len(jobWs))
	for _, jobW := range jobWs {
		// we don't merge jobs with ID pre-allocated.
		if jobW.Type != model.ActionCreateTable || jobW.IDAllocated {
			resJobWs = append(resJobWs, jobW)
			continue
		}
		// ActionCreateTables doesn't support foreign key now.
		tbInfo, ok := jobW.Args[0].(*model.TableInfo)
		if !ok || len(tbInfo.ForeignKeys) > 0 {
			resJobWs = append(resJobWs, jobW)
			continue
		}
		// CreateTables only support tables of same schema now.
		mergeableJobWs[jobW.Job.SchemaName] = append(mergeableJobWs[jobW.Job.SchemaName], jobW)
	}

	for schema, jobs := range mergeableJobWs {
		total := len(jobs)
		if total <= 1 {
			resJobWs = append(resJobWs, jobs...)
			continue
		}
		const maxBatchSize = 8
		batchCount := (total + maxBatchSize - 1) / maxBatchSize
		start := 0
		for _, batchSize := range mathutil.Divide2Batches(total, batchCount) {
			batch := jobs[start : start+batchSize]
			job, err := mergeCreateTableJobsOfSameSchema(batch)
			if err != nil {
				return nil, err
			}
			start += batchSize
			logutil.DDLLogger().Info("merge create table jobs", zap.String("schema", schema),
				zap.Int("total", total), zap.Int("batch_size", batchSize))

			newJobW := &JobWrapper{
				Job:      job,
				ResultCh: make([]chan jobSubmitResult, 0, batchSize),
			}
			// merge the result channels.
			for _, j := range batch {
				newJobW.ResultCh = append(newJobW.ResultCh, j.ResultCh...)
			}
			resJobWs = append(resJobWs, newJobW)
		}
	}
	return resJobWs, nil
}

// buildQueryStringFromJobs takes a slice of Jobs and concatenates their
// queries into a single query string.
// Each query is separated by a semicolon and a space.
// Trailing spaces are removed from each query, and a semicolon is appended
// if it's not already present.
func buildQueryStringFromJobs(jobs []*JobWrapper) string {
	var queryBuilder strings.Builder
	for i, job := range jobs {
		q := strings.TrimSpace(job.Query)
		if !strings.HasSuffix(q, ";") {
			q += ";"
		}
		queryBuilder.WriteString(q)

		if i < len(jobs)-1 {
			queryBuilder.WriteString(" ")
		}
	}
	return queryBuilder.String()
}

// mergeCreateTableJobsOfSameSchema combine CreateTableJobs to BatchCreateTableJob.
func mergeCreateTableJobsOfSameSchema(jobWs []*JobWrapper) (*model.Job, error) {
	if len(jobWs) == 0 {
		return nil, errors.Trace(fmt.Errorf("expect non-empty jobs"))
	}

	var combinedJob *model.Job

	args := make([]*model.TableInfo, 0, len(jobWs))
	involvingSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(jobWs))
	var foreignKeyChecks bool

	// if there is any duplicated table name
	duplication := make(map[string]struct{})
	for _, job := range jobWs {
		if combinedJob == nil {
			combinedJob = job.Clone()
			combinedJob.Type = model.ActionCreateTables
			combinedJob.Args = combinedJob.Args[:0]
			foreignKeyChecks = job.Args[1].(bool)
		}
		// append table job args
		info, ok := job.Args[0].(*model.TableInfo)
		if !ok {
			return nil, errors.Trace(fmt.Errorf("expect model.TableInfo, but got %T", job.Args[0]))
		}
		args = append(args, info)

		if _, ok := duplication[info.Name.L]; ok {
			// return err even if create table if not exists
			return nil, infoschema.ErrTableExists.FastGenByArgs("can not batch create tables with same name")
		}

		duplication[info.Name.L] = struct{}{}

		involvingSchemaInfo = append(involvingSchemaInfo,
			model.InvolvingSchemaInfo{
				Database: job.SchemaName,
				Table:    info.Name.L,
			})
	}

	combinedJob.Args = append(combinedJob.Args, args)
	combinedJob.Args = append(combinedJob.Args, foreignKeyChecks)
	combinedJob.InvolvingSchemaInfo = involvingSchemaInfo
	combinedJob.Query = buildQueryStringFromJobs(jobWs)

	return combinedJob, nil
}

// addBatchDDLJobs2Table gets global job IDs and puts the DDL jobs in the DDL job table.
func (s *JobSubmitter) addBatchDDLJobs2Table(jobWs []*JobWrapper) error {
	var err error

	if len(jobWs) == 0 {
		return nil
	}

	ctx := kv.WithInternalSourceType(s.ctx, kv.InternalTxnDDL)
	se, err := s.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.sessPool.Put(se)
	found, err := s.sysTblMgr.HasFlashbackClusterJob(ctx, s.minJobIDRefresher.GetCurrMinJobID())
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

	err = kv.RunInNewTxn(ctx, s.store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)

		bdrRole, err = t.GetBDRRole()
		if err != nil {
			return errors.Trace(err)
		}
		startTS = txn.StartTS()

		if variable.DDLForce2Queue.Load() {
			if err := s.checkFlashbackJobInQueue(t); err != nil {
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

		if s.serverStateSyncer.IsUpgradingState() && !hasSysDB(job) {
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
	if err = s.GenGIDAndInsertJobsWithRetry(ctx, ddlSe, jobWs); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *JobSubmitter) addBatchDDLJobs2Queue(jobWs []*JobWrapper) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	// lock to reduce conflict
	s.globalIDLock.Lock()
	defer s.globalIDLock.Unlock()
	return kv.RunInNewTxn(ctx, s.store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)

		count := getRequiredGIDCount(jobWs)
		ids, err := t.GenGlobalIDs(count)
		if err != nil {
			return errors.Trace(err)
		}
		assignGIDsForJobs(jobWs, ids)

		if err := s.checkFlashbackJobInQueue(t); err != nil {
			return errors.Trace(err)
		}

		for _, jobW := range jobWs {
			job := jobW.Job
			job.Version = currentVersion
			job.StartTS = txn.StartTS()
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

func (*JobSubmitter) checkFlashbackJobInQueue(t *meta.Meta) error {
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

// GenGIDAndInsertJobsWithRetry generate job related global ID and inserts DDL jobs to the DDL job
// table with retry. job id allocation and job insertion are in the same transaction,
// as we want to make sure DDL jobs are inserted in id order, then we can query from
// a min job ID when scheduling DDL jobs to mitigate https://github.com/pingcap/tidb/issues/52905.
// so this function has side effect, it will set table/db/job id of 'jobs'.
func (s *JobSubmitter) GenGIDAndInsertJobsWithRetry(ctx context.Context, ddlSe *sess.Session, jobWs []*JobWrapper) error {
	savedJobIDs := make([]int64, len(jobWs))
	count := getRequiredGIDCount(jobWs)
	return genGIDAndCallWithRetry(ctx, ddlSe, count, func(ids []int64) error {
		failpoint.Inject("mockGenGlobalIDFail", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(errors.New("gofail genGlobalIDs error"))
			}
		})
		assignGIDsForJobs(jobWs, ids)
		injectModifyJobArgFailPoint(jobWs)
		// job scheduler will start run them after txn commit, we want to make sure
		// the channel exists before the jobs are submitted.
		for i, jobW := range jobWs {
			if savedJobIDs[i] > 0 {
				// in case of retry
				s.ddlJobDoneChMap.Delete(savedJobIDs[i])
			}
			s.ddlJobDoneChMap.Store(jobW.ID, make(chan struct{}, 1))
			savedJobIDs[i] = jobW.ID
		}
		failpoint.Inject("mockGenGIDRetryableError", func() {
			failpoint.Return(kv.ErrTxnRetryable)
		})
		return insertDDLJobs2Table(ctx, ddlSe, jobWs...)
	})
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
			failpoint.InjectCall("onGenGIDRetry")
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

func (s *JobSubmitter) notifyNewJobSubmitted() {
	if s.ownerManager.IsOwner() {
		asyncNotify(s.ddlJobNotifyCh)
		return
	}
	s.notifyNewJobByEtcd()
}

func (s *JobSubmitter) notifyNewJobByEtcd() {
	if s.etcdCli == nil {
		return
	}

	err := ddlutil.PutKVToEtcd(s.ctx, s.etcdCli, 1, addingDDLJobNotifyKey, "0")
	if err != nil {
		logutil.DDLLogger().Info("notify new DDL job failed", zap.Error(err))
	}
}
