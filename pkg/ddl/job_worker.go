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
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	kvutil "github.com/tikv/client-go/v2/util"
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

// jobContext is the context for execution of a DDL job.
type jobContext struct {
	// below fields are shared by all DDL jobs
	ctx context.Context
	*unSyncedJobTracker
	*schemaVersionManager
	infoCache       *infoschema.InfoCache
	autoidCli       *autoid.ClientDiscover
	store           kv.Storage
	schemaVerSyncer schemaver.Syncer

	// per job fields
	notifyCh chan struct{}
	logger   *zap.Logger

	// per job step fields
	metaMut *meta.Mutator

	// TODO reorg part of code couple this struct so much, remove it later.
	oldDDLCtx *ddlCtx
}

func (c *jobContext) getAutoIDRequirement() autoid.Requirement {
	return &asAutoIDRequirement{
		store:     c.store,
		autoidCli: c.autoidCli,
	}
}

func (c *jobContext) notifyDone() {
	if c.notifyCh != nil {
		// broadcast done event as we might merge multiple jobs into one when fast
		// create table is enabled.
		close(c.notifyCh)
	}
}

type workerType byte

const (
	// generalWorker is the worker who handles all DDL statements except “add index”.
	generalWorker workerType = 0
	// addIdxWorker is the worker who handles the operation of adding indexes.
	addIdxWorker workerType = 1
)

// worker is used for handling DDL jobs.
// Now we have two kinds of workers.
type worker struct {
	id              int32
	tp              workerType
	addingDDLJobKey string
	ddlJobCh        chan struct{}
	// it's the ctx of 'job scheduler'.
	ctx context.Context
	wg  sync.WaitGroup

	sessPool        *sess.Pool    // sessPool is used to new sessions to execute SQL in ddl package.
	sess            *sess.Session // sess is used and only used in running DDL job.
	delRangeManager delRangeManager
	seqAllocator    *atomic.Uint64

	*ddlCtx
}

// ReorgContext contains context info for reorg job.
// TODO there is another reorgCtx, merge them.
type ReorgContext struct {
	// below fields are cache for top sql
	ddlJobCtx          context.Context
	cacheSQL           string
	cacheNormalizedSQL string
	cacheDigest        *parser.Digest
	tp                 string

	resourceGroupName string
	cloudStorageURI   string
}

// NewReorgContext returns a new ddl job context.
func NewReorgContext() *ReorgContext {
	return &ReorgContext{
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
	return worker
}

func (w *worker) typeStr() string {
	var str string
	switch w.tp {
	case generalWorker:
		str = "general"
	case addIdxWorker:
		str = "add index"
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
	logutil.DDLLogger().Info("DDL worker closed", zap.Stringer("worker", w),
		zap.Duration("take time", time.Since(startTime)))
}

func asyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
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
func (w *worker) handleUpdateJobError(jobCtx *jobContext, job *model.Job, err error) error {
	if err == nil {
		return nil
	}
	if kv.ErrEntryTooLarge.Equal(err) {
		jobCtx.logger.Warn("update DDL job failed", zap.String("job", job.String()), zap.Error(err))
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
		err = w.finishDDLJob(jobCtx, job)
	}
	return errors.Trace(err)
}

// updateDDLJob updates the DDL job information.
func (w *worker) updateDDLJob(jobCtx *jobContext, job *model.Job, updateRawArgs bool) error {
	failpoint.Inject("mockErrEntrySizeTooLarge", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(kv.ErrEntryTooLarge)
		}
	})

	if !updateRawArgs {
		jobCtx.logger.Info("meet something wrong before update DDL job, shouldn't update raw args",
			zap.String("job", job.String()))
	}
	return errors.Trace(updateDDLJob2Table(w.ctx, w.sess, job, updateRawArgs))
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
func (w *worker) finishDDLJob(jobCtx *jobContext, job *model.Job) (err error) {
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
	err = w.deleteDDLJob(job)
	if err != nil {
		return errors.Trace(err)
	}

	metaMut := jobCtx.metaMut
	job.BinlogInfo.FinishedTS = metaMut.StartTS
	jobCtx.logger.Info("finish DDL job", zap.String("job", job.String()))
	updateRawArgs := true
	if job.Type == model.ActionAddPrimaryKey && !job.IsCancelled() {
		// ActionAddPrimaryKey needs to check the warnings information in job.Args.
		// Notice: warnings is used to support non-strict mode.
		updateRawArgs = false
	}
	job.SeqNum = w.seqAllocator.Add(1)
	w.removeJobCtx(job)
	failpoint.InjectCall("afterFinishDDLJob", job)
	err = AddHistoryDDLJob(w.ctx, w.sess, metaMut, job, updateRawArgs)
	return errors.Trace(err)
}

func (w *worker) deleteDDLJob(job *model.Job) error {
	sql := fmt.Sprintf("delete from mysql.tidb_ddl_job where job_id = %d", job.ID)
	_, err := w.sess.Execute(context.Background(), sql, "delete_job")
	return errors.Trace(err)
}

func finishRecoverTable(w *worker, job *model.Job) error {
	args, err := model.GetRecoverArgs(job)
	if err != nil {
		return errors.Trace(err)
	}
	if args.CheckFlag == recoverCheckFlagEnableGC {
		err = enableGC(w)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func finishRecoverSchema(w *worker, job *model.Job) error {
	args, err := model.GetRecoverArgs(job)
	if err != nil {
		return errors.Trace(err)
	}
	if args.CheckFlag == recoverCheckFlagEnableGC {
		err = enableGC(w)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (w *ReorgContext) setDDLLabelForTopSQL(jobQuery string) {
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

func (w *ReorgContext) setDDLLabelForDiagnosis(jobType model.ActionType) {
	if w.tp != "" {
		return
	}
	w.tp = getDDLRequestSource(jobType)
	w.ddlJobCtx = kv.WithInternalSourceAndTaskType(w.ddlJobCtx, w.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
}

func (w *worker) handleJobDone(jobCtx *jobContext, job *model.Job) error {
	if err := w.checkBeforeCommit(); err != nil {
		return err
	}
	err := w.finishDDLJob(jobCtx, job)
	if err != nil {
		w.sess.Rollback()
		return err
	}

	err = w.sess.Commit(w.ctx)
	if err != nil {
		return err
	}
	cleanupDDLReorgHandles(job, w.sess)
	jobCtx.notifyDone()
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
func (w *worker) transitOneJobStep(jobCtx *jobContext, job *model.Job) (int64, error) {
	var (
		err error
	)

	txn, err := w.prepareTxn(job)
	if err != nil {
		return 0, err
	}
	jobCtx.metaMut = meta.NewMutator(txn)

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
		return 0, w.handleJobDone(jobCtx, job)
	}
	failpoint.InjectCall("onJobRunBefore", job)

	// If running job meets error, we will save this error in job Error and retry
	// later if the job is not cancelled.
	schemaVer, updateRawArgs, runJobErr := w.runOneJobStep(jobCtx, job)

	failpoint.InjectCall("onJobRunAfter", job)

	if job.IsCancelled() {
		defer jobCtx.unlockSchemaVersion(job.ID)
		w.sess.Reset()
		return 0, w.handleJobDone(jobCtx, job)
	}

	if err = w.checkBeforeCommit(); err != nil {
		jobCtx.unlockSchemaVersion(job.ID)
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
		jobCtx.unlockSchemaVersion(job.ID)
		return 0, err
	}
	err = w.updateDDLJob(jobCtx, job, updateRawArgs)
	if err = w.handleUpdateJobError(jobCtx, job, err); err != nil {
		w.sess.Rollback()
		jobCtx.unlockSchemaVersion(job.ID)
		return 0, err
	}
	// reset the SQL digest to make topsql work right.
	w.sess.GetSessionVars().StmtCtx.ResetSQLDigest(job.Query)
	err = w.sess.Commit(w.ctx)
	jobCtx.unlockSchemaVersion(job.ID)
	if err != nil {
		return 0, err
	}
	jobCtx.addUnSynced(job.ID)

	// If error is non-retryable, we can ignore the sleep.
	if runJobErr != nil && errorIsRetryable(runJobErr, job) {
		jobCtx.logger.Info("run DDL job failed, sleeps a while then retries it.",
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
	if !w.ddlCtx.isOwner() {
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

func (w *ReorgContext) getResourceGroupTaggerForTopSQL() *kv.ResourceGroupTagBuilder {
	if !topsqlstate.TopSQLEnabled() || w.cacheDigest == nil {
		return nil
	}

	digest := w.cacheDigest
	return kv.NewResourceGroupTagBuilder().SetSQLDigest(digest)
}

func (w *ReorgContext) ddlJobSourceType() string {
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

func chooseLeaseTime(t, max time.Duration) time.Duration {
	if t == 0 || t > max {
		return max
	}
	return t
}

// countForPanic records the error count for DDL job.
func (w *worker) countForPanic(jobCtx *jobContext, job *model.Job) {
	// If run DDL job panic, just cancel the DDL jobs.
	if job.State == model.JobStateRollingback {
		job.State = model.JobStateCancelled
	} else {
		job.State = model.JobStateCancelling
	}
	job.ErrorCount++

	logger := jobCtx.logger
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
func (w *worker) countForError(jobCtx *jobContext, job *model.Job, err error) error {
	job.Error = toTError(err)
	job.ErrorCount++

	logger := jobCtx.logger
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

func (*worker) processJobPausingRequest(jobCtx *jobContext, job *model.Job) (isRunnable bool, err error) {
	if job.IsPaused() {
		jobCtx.logger.Debug("paused DDL job ", zap.String("job", job.String()))
		return false, err
	}
	if job.IsPausing() {
		jobCtx.logger.Debug("pausing DDL job ", zap.String("job", job.String()))
		job.State = model.JobStatePaused
		return false, pauseReorgWorkers(jobCtx, job)
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
// updateGlobalVersionAndWaitSynced to wait for all nodes to catch up JobStateDone.
func (w *worker) runOneJobStep(
	jobCtx *jobContext,
	job *model.Job,
) (ver int64, updateRawArgs bool, err error) {
	defer tidbutil.Recover(metrics.LabelDDLWorker, fmt.Sprintf("%s runOneJobStep", w),
		func() {
			w.countForPanic(jobCtx, job)
		}, false)

	// Mock for run ddl job panic.
	failpoint.Inject("mockPanicInRunDDLJob", func(failpoint.Value) {})

	if job.Type != model.ActionMultiSchemaChange {
		jobCtx.logger.Info("run DDL job", zap.String("job", job.String()))
	}
	timeStart := time.Now()
	if job.RealStartTS == 0 {
		job.RealStartTS = jobCtx.metaMut.StartTS
	}
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerRunDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()

	if job.IsCancelling() {
		jobCtx.logger.Debug("cancel DDL job", zap.String("job", job.String()))
		ver, err = convertJob2RollbackJob(w, jobCtx, job)
		// if job is converted to rollback job, the job.Args may be changed for the
		// rollback logic, so we let caller persist the new arguments.
		updateRawArgs = job.IsRollingback()
		return
	}

	isRunnable, err := w.processJobPausingRequest(jobCtx, job)
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
		ver, err = onCreateSchema(jobCtx, job)
	case model.ActionModifySchemaCharsetAndCollate:
		ver, err = onModifySchemaCharsetAndCollate(jobCtx, job)
	case model.ActionDropSchema:
		ver, err = onDropSchema(jobCtx, job)
	case model.ActionRecoverSchema:
		ver, err = w.onRecoverSchema(jobCtx, job)
	case model.ActionModifySchemaDefaultPlacement:
		ver, err = onModifySchemaDefaultPlacement(jobCtx, job)
	case model.ActionCreateTable:
		ver, err = onCreateTable(jobCtx, job)
	case model.ActionCreateTables:
		ver, err = onCreateTables(jobCtx, job)
	case model.ActionRepairTable:
		ver, err = onRepairTable(jobCtx, job)
	case model.ActionCreateView:
		ver, err = onCreateView(jobCtx, job)
	case model.ActionDropTable, model.ActionDropView, model.ActionDropSequence:
		ver, err = onDropTableOrView(jobCtx, job)
	case model.ActionDropTablePartition:
		ver, err = w.onDropTablePartition(jobCtx, job)
	case model.ActionTruncateTablePartition:
		ver, err = w.onTruncateTablePartition(jobCtx, job)
	case model.ActionExchangeTablePartition:
		ver, err = w.onExchangeTablePartition(jobCtx, job)
	case model.ActionAddColumn:
		ver, err = onAddColumn(jobCtx, job)
	case model.ActionDropColumn:
		ver, err = onDropColumn(jobCtx, job)
	case model.ActionModifyColumn:
		ver, err = w.onModifyColumn(jobCtx, job)
	case model.ActionSetDefaultValue:
		ver, err = onSetDefaultValue(jobCtx, job)
	case model.ActionAddIndex:
		ver, err = w.onCreateIndex(jobCtx, job, false)
	case model.ActionAddPrimaryKey:
		ver, err = w.onCreateIndex(jobCtx, job, true)
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		ver, err = onDropIndex(jobCtx, job)
	case model.ActionRenameIndex:
		ver, err = onRenameIndex(jobCtx, job)
	case model.ActionAddForeignKey:
		ver, err = w.onCreateForeignKey(jobCtx, job)
	case model.ActionDropForeignKey:
		ver, err = onDropForeignKey(jobCtx, job)
	case model.ActionTruncateTable:
		ver, err = w.onTruncateTable(jobCtx, job)
	case model.ActionRebaseAutoID:
		ver, err = onRebaseAutoIncrementIDType(jobCtx, job)
	case model.ActionRebaseAutoRandomBase:
		ver, err = onRebaseAutoRandomType(jobCtx, job)
	case model.ActionRenameTable:
		ver, err = onRenameTable(jobCtx, job)
	case model.ActionShardRowID:
		ver, err = w.onShardRowID(jobCtx, job)
	case model.ActionModifyTableComment:
		ver, err = onModifyTableComment(jobCtx, job)
	case model.ActionModifyTableAutoIDCache:
		ver, err = onModifyTableAutoIDCache(jobCtx, job)
	case model.ActionAddTablePartition:
		ver, err = w.onAddTablePartition(jobCtx, job)
	case model.ActionModifyTableCharsetAndCollate:
		ver, err = onModifyTableCharsetAndCollate(jobCtx, job)
	case model.ActionRecoverTable:
		ver, err = w.onRecoverTable(jobCtx, job)
	case model.ActionLockTable:
		ver, err = onLockTables(jobCtx, job)
	case model.ActionUnlockTable:
		ver, err = onUnlockTables(jobCtx, job)
	case model.ActionSetTiFlashReplica:
		ver, err = w.onSetTableFlashReplica(jobCtx, job)
	case model.ActionUpdateTiFlashReplicaStatus:
		ver, err = onUpdateTiFlashReplicaStatus(jobCtx, job)
	case model.ActionCreateSequence:
		ver, err = onCreateSequence(jobCtx, job)
	case model.ActionAlterIndexVisibility:
		ver, err = onAlterIndexVisibility(jobCtx, job)
	case model.ActionAlterSequence:
		ver, err = onAlterSequence(jobCtx, job)
	case model.ActionRenameTables:
		ver, err = onRenameTables(jobCtx, job)
	case model.ActionAlterTableAttributes:
		ver, err = onAlterTableAttributes(jobCtx, job)
	case model.ActionAlterTablePartitionAttributes:
		ver, err = onAlterTablePartitionAttributes(jobCtx, job)
	case model.ActionCreatePlacementPolicy:
		ver, err = onCreatePlacementPolicy(jobCtx, job)
	case model.ActionDropPlacementPolicy:
		ver, err = onDropPlacementPolicy(jobCtx, job)
	case model.ActionAlterPlacementPolicy:
		ver, err = onAlterPlacementPolicy(jobCtx, job)
	case model.ActionAlterTablePartitionPlacement:
		ver, err = onAlterTablePartitionPlacement(jobCtx, job)
	case model.ActionAlterTablePlacement:
		ver, err = onAlterTablePlacement(jobCtx, job)
	case model.ActionCreateResourceGroup:
		ver, err = onCreateResourceGroup(jobCtx, job)
	case model.ActionAlterResourceGroup:
		ver, err = onAlterResourceGroup(jobCtx, job)
	case model.ActionDropResourceGroup:
		ver, err = onDropResourceGroup(jobCtx, job)
	case model.ActionAlterCacheTable:
		ver, err = onAlterCacheTable(jobCtx, job)
	case model.ActionAlterNoCacheTable:
		ver, err = onAlterNoCacheTable(jobCtx, job)
	case model.ActionFlashbackCluster:
		ver, err = w.onFlashbackCluster(jobCtx, job)
	case model.ActionMultiSchemaChange:
		ver, err = onMultiSchemaChange(w, jobCtx, job)
	case model.ActionReorganizePartition, model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		ver, err = w.onReorganizePartition(jobCtx, job)
	case model.ActionAlterTTLInfo:
		ver, err = onTTLInfoChange(jobCtx, job)
	case model.ActionAlterTTLRemove:
		ver, err = onTTLInfoRemove(jobCtx, job)
	case model.ActionAddCheckConstraint:
		ver, err = w.onAddCheckConstraint(jobCtx, job)
	case model.ActionDropCheckConstraint:
		ver, err = onDropCheckConstraint(jobCtx, job)
	case model.ActionAlterCheckConstraint:
		ver, err = w.onAlterCheckConstraint(jobCtx, job)
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
		err = w.countForError(jobCtx, job, err)
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

// updateGlobalVersionAndWaitSynced update global schema version to notify all TiDBs
// to reload info schema, and waits for all servers' schema or MDL synced.
func updateGlobalVersionAndWaitSynced(jobCtx *jobContext, latestSchemaVersion int64, job *model.Job) error {
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		return nil
	}

	var err error

	if latestSchemaVersion == 0 {
		logutil.DDLLogger().Info("schema version doesn't change", zap.Int64("jobID", job.ID))
		return nil
	}

	err = jobCtx.schemaVerSyncer.OwnerUpdateGlobalVersion(jobCtx.ctx, latestSchemaVersion)
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

	return waitVersionSynced(jobCtx, job, latestSchemaVersion)
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
