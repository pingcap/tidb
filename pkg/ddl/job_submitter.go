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
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// JobSubmitter collects the DDL jobs and submits them to job tables in batch, it's
// also responsible allocating IDs for the jobs. when fast-create is enabled, it
// will merge the create-table jobs to a single batch create-table job.
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
	fastCreate := variable.EnableFastCreateTable.Load()
	if fastCreate {
		newWs, err = mergeCreateTableJobs(jobWs)
		if err != nil {
			logutil.DDLLogger().Warn("failed to merge create table jobs", zap.Error(err))
		} else {
			jobWs = newWs
		}
	}
	err = s.addBatchDDLJobs2Table(jobWs)
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
		args := jobW.JobArgs.(*model.CreateTableArgs)
		if len(args.TableInfo.ForeignKeys) > 0 {
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
			newJobW, err := mergeCreateTableJobsOfSameSchema(batch)
			if err != nil {
				return nil, err
			}
			start += batchSize
			logutil.DDLLogger().Info("merge create table jobs", zap.String("schema", schema),
				zap.Int("total", total), zap.Int("batch_size", batchSize))
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
func mergeCreateTableJobsOfSameSchema(jobWs []*JobWrapper) (*JobWrapper, error) {
	if len(jobWs) == 0 {
		return nil, errors.Trace(fmt.Errorf("expect non-empty jobs"))
	}

	var (
		combinedJob *model.Job
		args        = &model.BatchCreateTableArgs{
			Tables: make([]*model.CreateTableArgs, 0, len(jobWs)),
		}
		involvingSchemaInfo = make([]model.InvolvingSchemaInfo, 0, len(jobWs))
	)

	// if there is any duplicated table name
	duplication := make(map[string]struct{})
	for _, job := range jobWs {
		if combinedJob == nil {
			combinedJob = job.Clone()
			combinedJob.Type = model.ActionCreateTables
		}
		jobArgs := job.JobArgs.(*model.CreateTableArgs)
		args.Tables = append(args.Tables, jobArgs)

		info := jobArgs.TableInfo
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

	combinedJob.InvolvingSchemaInfo = involvingSchemaInfo
	combinedJob.Query = buildQueryStringFromJobs(jobWs)

	newJobW := &JobWrapper{
		Job:      combinedJob,
		JobArgs:  args,
		ResultCh: make([]chan jobSubmitResult, 0, len(jobWs)),
	}
	// merge the result channels.
	for _, j := range jobWs {
		newJobW.ResultCh = append(newJobW.ResultCh, j.ResultCh...)
	}

	return newJobW, nil
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
		t := meta.NewMutator(txn)

		bdrRole, err = t.GetBDRRole()
		if err != nil {
			return errors.Trace(err)
		}
		startTS = txn.StartTS()

		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	for _, jobW := range jobWs {
		job := jobW.Job
		intest.Assert(job.Version != 0, "Job version should not be zero")

		job.StartTS = startTS
		job.BDRRole = bdrRole

		// BDR mode only affects the DDL not from CDC
		if job.CDCWriteSource == 0 && bdrRole != string(ast.BDRRoleNone) {
			if job.Type == model.ActionMultiSchemaChange && job.MultiSchemaInfo != nil {
				for _, subJob := range job.MultiSchemaInfo.SubJobs {
					if DeniedByBDR(ast.BDRRole(bdrRole), subJob.Type, subJob.JobArgs) {
						return dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
					}
				}
			} else if DeniedByBDR(ast.BDRRole(bdrRole), job.Type, jobW.JobArgs) {
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

type gidAllocator struct {
	idx int
	ids []int64
}

func (a *gidAllocator) next() int64 {
	id := a.ids[a.idx]
	a.idx++
	return id
}

func (a *gidAllocator) assignIDsForTable(info *model.TableInfo) {
	info.ID = a.next()
	if partitionInfo := info.GetPartitionInfo(); partitionInfo != nil {
		a.assignIDsForPartitionInfo(partitionInfo)
	}
}

func (a *gidAllocator) assignIDsForPartitionInfo(partitionInfo *model.PartitionInfo) {
	for i := range partitionInfo.Definitions {
		partitionInfo.Definitions[i].ID = a.next()
	}
}

func idCountForTable(info *model.TableInfo) int {
	c := 1
	if partitionInfo := info.GetPartitionInfo(); partitionInfo != nil {
		c += len(partitionInfo.Definitions)
	}
	return c
}

// getRequiredGIDCount returns the count of required global IDs for the jobs. it's calculated
// as: the count of jobs + the count of IDs for the jobs which do NOT have pre-allocated ID.
func getRequiredGIDCount(jobWs []*JobWrapper) int {
	count := len(jobWs)
	for _, jobW := range jobWs {
		if jobW.IDAllocated {
			continue
		}
		switch jobW.Type {
		case model.ActionCreateView, model.ActionCreateSequence, model.ActionCreateTable:
			args := jobW.JobArgs.(*model.CreateTableArgs)
			count += idCountForTable(args.TableInfo)
		case model.ActionCreateTables:
			args := jobW.JobArgs.(*model.BatchCreateTableArgs)
			for _, tblArgs := range args.Tables {
				count += idCountForTable(tblArgs.TableInfo)
			}
		case model.ActionCreateSchema, model.ActionCreateResourceGroup:
			count++
		case model.ActionAlterTablePartitioning:
			args := jobW.JobArgs.(*model.TablePartitionArgs)
			// A new table ID would be needed for
			// the global table, which cannot be the same as the current table id,
			// since this table id will be removed in the final state when removing
			// all the data with this table id.
			count += 1 + len(args.PartInfo.Definitions)
		case model.ActionTruncateTablePartition:
			count += len(jobW.JobArgs.(*model.TruncateTableArgs).OldPartitionIDs)
		case model.ActionAddTablePartition, model.ActionReorganizePartition, model.ActionRemovePartitioning:
			args := jobW.JobArgs.(*model.TablePartitionArgs)
			count += len(args.PartInfo.Definitions)
		case model.ActionTruncateTable:
			count += 1 + len(jobW.JobArgs.(*model.TruncateTableArgs).OldPartitionIDs)
		}
	}
	return count
}

// assignGIDsForJobs should be used with getRequiredGIDCount, and len(ids) must equal
// what getRequiredGIDCount returns.
func assignGIDsForJobs(jobWs []*JobWrapper, ids []int64) {
	alloc := &gidAllocator{ids: ids}
	for _, jobW := range jobWs {
		switch jobW.Type {
		case model.ActionCreateView, model.ActionCreateSequence, model.ActionCreateTable:
			args := jobW.JobArgs.(*model.CreateTableArgs)
			if !jobW.IDAllocated {
				alloc.assignIDsForTable(args.TableInfo)
			}
			jobW.TableID = args.TableInfo.ID
		case model.ActionCreateTables:
			if !jobW.IDAllocated {
				args := jobW.JobArgs.(*model.BatchCreateTableArgs)
				for _, tblArgs := range args.Tables {
					alloc.assignIDsForTable(tblArgs.TableInfo)
				}
			}
		case model.ActionCreateSchema:
			dbInfo := jobW.JobArgs.(*model.CreateSchemaArgs).DBInfo
			if !jobW.IDAllocated {
				dbInfo.ID = alloc.next()
			}
			jobW.SchemaID = dbInfo.ID
		case model.ActionCreateResourceGroup:
			if !jobW.IDAllocated {
				args := jobW.JobArgs.(*model.ResourceGroupArgs)
				args.RGInfo.ID = alloc.next()
			}
		case model.ActionAlterTablePartitioning:
			if !jobW.IDAllocated {
				args := jobW.JobArgs.(*model.TablePartitionArgs)
				alloc.assignIDsForPartitionInfo(args.PartInfo)
				args.PartInfo.NewTableID = alloc.next()
			}
		case model.ActionAddTablePartition, model.ActionReorganizePartition:
			if !jobW.IDAllocated {
				pInfo := jobW.JobArgs.(*model.TablePartitionArgs).PartInfo
				alloc.assignIDsForPartitionInfo(pInfo)
			}
		case model.ActionRemovePartitioning:
			// a special partition is used in this case, and we will use the ID
			// of the partition as the new table ID.
			pInfo := jobW.JobArgs.(*model.TablePartitionArgs).PartInfo
			if !jobW.IDAllocated {
				alloc.assignIDsForPartitionInfo(pInfo)
			}
			pInfo.NewTableID = pInfo.Definitions[0].ID
		case model.ActionTruncateTable, model.ActionTruncateTablePartition:
			if !jobW.IDAllocated {
				args := jobW.JobArgs.(*model.TruncateTableArgs)
				if jobW.Type == model.ActionTruncateTable {
					args.NewTableID = alloc.next()
				}
				partIDs := make([]int64, len(args.OldPartitionIDs))
				for i := range partIDs {
					partIDs[i] = alloc.next()
				}
				args.NewPartitionIDs = partIDs
			}
		}
		jobW.ID = alloc.next()
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

			m := meta.NewMutator(txn)
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
	m := meta.NewMutator(txn)
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

func insertDDLJobs2Table(ctx context.Context, se *sess.Session, jobWs ...*JobWrapper) error {
	failpoint.Inject("mockAddBatchDDLJobsErr", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.Errorf("mockAddBatchDDLJobsErr"))
		}
	})
	if len(jobWs) == 0 {
		return nil
	}
	var sql bytes.Buffer
	sql.WriteString("insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing) values")
	for i, jobW := range jobWs {
		jobW.FillArgsWithSubJobs()
		b, err := jobW.Encode(true)
		if err != nil {
			return err
		}
		if i != 0 {
			sql.WriteString(",")
		}
		fmt.Fprintf(&sql, "(%d, %t, %s, %s, %s, %d, %t)", jobW.ID, jobW.MayNeedReorg(),
			strconv.Quote(job2SchemaIDs(jobW)), strconv.Quote(job2TableIDs(jobW)),
			ddlutil.WrapKey2String(b), jobW.Type, jobW.Started())
	}
	se.GetSessionVars().SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	_, err := se.Execute(ctx, sql.String(), "insert_job")
	logutil.DDLLogger().Debug("add job to mysql.tidb_ddl_job table", zap.String("sql", sql.String()))
	return errors.Trace(err)
}

func makeStringForIDs(ids []int64) string {
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

func job2SchemaIDs(jobW *JobWrapper) string {
	switch jobW.Type {
	case model.ActionRenameTables:
		var ids []int64
		arg := jobW.JobArgs.(*model.RenameTablesArgs)
		ids = make([]int64, 0, len(arg.RenameTableInfos)*2)
		for _, info := range arg.RenameTableInfos {
			ids = append(ids, info.OldSchemaID, info.NewSchemaID)
		}
		return makeStringForIDs(ids)
	case model.ActionRenameTable:
		oldSchemaID := jobW.JobArgs.(*model.RenameTableArgs).OldSchemaID
		ids := []int64{oldSchemaID, jobW.SchemaID}
		return makeStringForIDs(ids)
	case model.ActionExchangeTablePartition:
		args := jobW.JobArgs.(*model.ExchangeTablePartitionArgs)
		return makeStringForIDs([]int64{jobW.SchemaID, args.PTSchemaID})
	default:
		return strconv.FormatInt(jobW.SchemaID, 10)
	}
}

func job2TableIDs(jobW *JobWrapper) string {
	switch jobW.Type {
	case model.ActionRenameTables:
		var ids []int64
		arg := jobW.JobArgs.(*model.RenameTablesArgs)
		ids = make([]int64, 0, len(arg.RenameTableInfos))
		for _, info := range arg.RenameTableInfos {
			ids = append(ids, info.TableID)
		}
		return makeStringForIDs(ids)
	case model.ActionExchangeTablePartition:
		args := jobW.JobArgs.(*model.ExchangeTablePartitionArgs)
		return makeStringForIDs([]int64{jobW.TableID, args.PTTableID})
	case model.ActionTruncateTable:
		newTableID := jobW.JobArgs.(*model.TruncateTableArgs).NewTableID
		return strconv.FormatInt(jobW.TableID, 10) + "," + strconv.FormatInt(newTableID, 10)
	default:
		return strconv.FormatInt(jobW.TableID, 10)
	}
}

func setJobStateToQueueing(job *model.Job) {
	if job.Type == model.ActionMultiSchemaChange && job.MultiSchemaInfo != nil {
		for _, sub := range job.MultiSchemaInfo.SubJobs {
			sub.State = model.JobStateQueueing
		}
	}
	job.State = model.JobStateQueueing
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
