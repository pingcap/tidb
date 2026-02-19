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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

// DoDDLJob will return
// - nil: found in history DDL job and no job error
// - context.Cancel: job has been sent to worker, but not found in history DDL job before cancel
// - other: found in history DDL job and return that job error
func (e *executor) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	return e.DoDDLJobWrapper(ctx, NewJobWrapper(job, false))
}

func (e *executor) doDDLJob2(ctx sessionctx.Context, job *model.Job, args model.JobArgs) error {
	return e.DoDDLJobWrapper(ctx, NewJobWrapperWithArgs(job, args, false))
}

// DoDDLJobWrapper submit DDL job and wait it finishes.
// When fast create is enabled, we might merge multiple jobs into one, so do not
// depend on job.ID, use JobID from jobSubmitResult.
func (e *executor) DoDDLJobWrapper(ctx sessionctx.Context, jobW *JobWrapper) (resErr error) {
	if traceCtx := ctx.GetTraceCtx(); traceCtx != nil {
		r := tracing.StartRegion(traceCtx, "ddl.DoDDLJobWrapper")
		defer r.End()
	}

	job := jobW.Job
	job.TraceInfo = &tracing.TraceInfo{
		ConnectionID: ctx.GetSessionVars().ConnectionID,
		SessionAlias: ctx.GetSessionVars().SessionAlias,
		TraceID:      traceevent.TraceIDFromContext(ctx.GetTraceCtx()),
	}
	if mci := ctx.GetSessionVars().StmtCtx.MultiSchemaInfo; mci != nil {
		// In multiple schema change, we don't run the job.
		// Instead, we merge all the jobs into one pending job.
		return appendToSubJobs(mci, jobW)
	}
	if err := job.CheckInvolvingSchemaInfo(); err != nil {
		return err
	}
	// Get a global job ID and put the DDL job in the queue.
	setDDLJobQuery(ctx, job)

	if traceevent.IsEnabled(tracing.DDLJob) && ctx.GetTraceCtx() != nil {
		traceevent.TraceEvent(ctx.GetTraceCtx(), tracing.DDLJob, "ddlDelieverJobTask",
			zap.Uint64("ConnID", job.TraceInfo.ConnectionID),
			zap.String("SessionAlias", job.TraceInfo.SessionAlias))
	}
	e.deliverJobTask(jobW)

	failpoint.Inject("mockParallelSameDDLJobTwice", func(val failpoint.Value) {
		if val.(bool) {
			<-jobW.ResultCh[0]
			// The same job will be put to the DDL queue twice.
			job = job.Clone()
			newJobW := NewJobWrapperWithArgs(job, jobW.JobArgs, jobW.IDAllocated)
			e.deliverJobTask(newJobW)
			// The second job result is used for test.
			jobW = newJobW
		}
	})

	var result jobSubmitResult
	select {
	case <-e.ctx.Done():
		logutil.DDLLogger().Info("DoDDLJob will quit because context done")
		return e.ctx.Err()
	case res := <-jobW.ResultCh[0]:
		// worker should restart to continue handling tasks in limitJobCh, and send back through jobW.err
		result = res
	}
	// job.ID must be allocated after previous channel receive returns nil.
	jobID, err := result.jobID, result.err
	defer e.delJobDoneCh(jobID)
	if err != nil {
		// The transaction of enqueuing job is failed.
		return errors.Trace(err)
	}
	failpoint.InjectCall("waitJobSubmitted")

	sessVars := ctx.GetSessionVars()
	sessVars.StmtCtx.IsDDLJobInQueue.Store(true)

	ddlAction := job.Type
	if result.merged {
		logutil.DDLLogger().Info("DDL job submitted", zap.Int64("job_id", jobID), zap.String("query", job.Query), zap.String("merged", "true"))
	} else {
		logutil.DDLLogger().Info("DDL job submitted", zap.Stringer("job", job), zap.String("query", job.Query))
	}

	// lock tables works on table ID, for some DDLs which changes table ID, we need
	// make sure the session still tracks it.
	// we need add it here to avoid this ddl job was executed successfully but the
	// session was killed before return. The session will release all table locks
	// it holds, if we don't add the new locking table id here, the session may forget
	// to release the new locked table id when this ddl job was executed successfully
	// but the session was killed before return.
	if config.TableLockEnabled() {
		HandleLockTablesOnSuccessSubmit(ctx, jobW)
		defer func() {
			HandleLockTablesOnFinish(ctx, jobW, resErr)
		}()
	}

	var historyJob *model.Job

	// Attach the context of the jobId to the calling session so that
	// KILL can cancel this DDL job.
	ctx.GetSessionVars().StmtCtx.DDLJobID = jobID

	// For a job from start to end, the state of it will be none -> delete only -> write only -> reorganization -> public
	// For every state changes, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	// But we use etcd to speed up, normally it takes less than 0.5s now, so we use 0.5s or 1s or 3s as the max value.
	initInterval, _ := getJobCheckInterval(ddlAction, 0)
	ticker := time.NewTicker(chooseLeaseTime(10*e.lease, initInterval))
	startTime := time.Now()
	metrics.JobsGauge.WithLabelValues(ddlAction.String()).Inc()
	defer func() {
		ticker.Stop()
		metrics.JobsGauge.WithLabelValues(ddlAction.String()).Dec()
		metrics.HandleJobHistogram.WithLabelValues(ddlAction.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		recordLastDDLInfo(ctx, historyJob)
	}()
	i := 0
	notifyCh, _ := e.getJobDoneCh(jobID)
	for {
		failpoint.InjectCall("storeCloseInLoop")
		select {
		case _, ok := <-notifyCh:
			if !ok {
				// when fast create enabled, jobs might be merged, and we broadcast
				// the result by closing the channel, to avoid this loop keeps running
				// without sleeping on retryable error, we set it to nil.
				notifyCh = nil
			}
		case <-ticker.C:
			i++
			ticker = updateTickerInterval(ticker, 10*e.lease, ddlAction, i)
		case <-e.ctx.Done():
			logutil.DDLLogger().Info("DoDDLJob will quit because context done")
			return e.ctx.Err()
		}

		// If the connection being killed, we need to CANCEL the DDL job.
		if sessVars.SQLKiller.HandleSignal() == exeerrors.ErrQueryInterrupted {
			if atomic.LoadInt32(&sessVars.ConnectionStatus) == variable.ConnStatusShutdown {
				logutil.DDLLogger().Info("DoDDLJob will quit because context done")
				return context.Canceled
			}
			if sessVars.StmtCtx.DDLJobID != 0 {
				se, err := e.sessPool.Get()
				if err != nil {
					logutil.DDLLogger().Error("get session failed, check again", zap.Error(err))
					continue
				}
				sessVars.StmtCtx.DDLJobID = 0 // Avoid repeat.
				errs, err := CancelJobsBySystem(se, []int64{jobID})
				e.sessPool.Put(se)
				if len(errs) > 0 {
					logutil.DDLLogger().Warn("error canceling DDL job", zap.Error(errs[0]))
				}
				if err != nil {
					logutil.DDLLogger().Warn("Kill command could not cancel DDL job", zap.Error(err))
					continue
				}
			}
		}

		se, err := e.sessPool.Get()
		if err != nil {
			logutil.DDLLogger().Error("get session failed, check again", zap.Error(err))
			continue
		}
		historyJob, err = GetHistoryJobByID(se, jobID)
		e.sessPool.Put(se)
		if err != nil {
			logutil.DDLLogger().Error("get history DDL job failed, check again", zap.Error(err))
			continue
		}
		if historyJob == nil {
			logutil.DDLLogger().Debug("DDL job is not in history, maybe not run", zap.Int64("jobID", jobID))
			continue
		}

		e.checkHistoryJobInTest(ctx, historyJob)

		// If a job is a history job, the state must be JobStateSynced or JobStateRollbackDone or JobStateCancelled.
		if historyJob.IsSynced() {
			// Judge whether there are some warnings when executing DDL under the certain SQL mode.
			if historyJob.ReorgMeta != nil && len(historyJob.ReorgMeta.Warnings) != 0 {
				if len(historyJob.ReorgMeta.Warnings) != len(historyJob.ReorgMeta.WarningsCount) {
					logutil.DDLLogger().Info("DDL warnings doesn't match the warnings count", zap.Int64("jobID", jobID))
				} else {
					for key, warning := range historyJob.ReorgMeta.Warnings {
						keyCount := historyJob.ReorgMeta.WarningsCount[key]
						if keyCount == 1 {
							ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
						} else {
							newMsg := fmt.Sprintf("%d warnings with this error code, first warning: "+warning.GetMsg(), keyCount)
							newWarning := dbterror.ClassTypes.Synthesize(terror.ErrCode(warning.Code()), newMsg)
							ctx.GetSessionVars().StmtCtx.AppendWarning(newWarning)
						}
					}
				}
			}
			appendMultiChangeWarningsToOwnerCtx(ctx, historyJob)

			logutil.DDLLogger().Info("DDL job is finished", zap.Int64("jobID", jobID))
			return nil
		}

		if historyJob.Error != nil {
			logutil.DDLLogger().Info("DDL job is failed", zap.Int64("jobID", jobID))
			return errors.Trace(historyJob.Error)
		}
		panic("When the state is JobStateRollbackDone or JobStateCancelled, historyJob.Error should never be nil")
	}
}

// HandleLockTablesOnSuccessSubmit handles the table lock for the job which is submitted
// successfully. exported for testing purpose.
func HandleLockTablesOnSuccessSubmit(ctx sessionctx.Context, jobW *JobWrapper) {
	if jobW.Type == model.ActionTruncateTable {
		if ok, lockTp := ctx.CheckTableLocked(jobW.TableID); ok {
			ctx.AddTableLock([]model.TableLockTpInfo{{
				SchemaID: jobW.SchemaID,
				TableID:  jobW.JobArgs.(*model.TruncateTableArgs).NewTableID,
				Tp:       lockTp,
			}})
		}
	}
}

// HandleLockTablesOnFinish handles the table lock for the job which is finished.
// exported for testing purpose.
func HandleLockTablesOnFinish(ctx sessionctx.Context, jobW *JobWrapper, ddlErr error) {
	if jobW.Type == model.ActionTruncateTable {
		if ddlErr != nil {
			newTableID := jobW.JobArgs.(*model.TruncateTableArgs).NewTableID
			ctx.ReleaseTableLockByTableIDs([]int64{newTableID})
			return
		}
		if ok, _ := ctx.CheckTableLocked(jobW.TableID); ok {
			ctx.ReleaseTableLockByTableIDs([]int64{jobW.TableID})
		}
	}
}

func (e *executor) getJobDoneCh(jobID int64) (chan struct{}, bool) {
	return e.ddlJobDoneChMap.Load(jobID)
}

func (e *executor) delJobDoneCh(jobID int64) {
	e.ddlJobDoneChMap.Delete(jobID)
}

func (e *executor) deliverJobTask(task *JobWrapper) {
	// TODO this might block forever, as the consumer part considers context cancel.
	e.limitJobCh <- task
}

func updateTickerInterval(ticker *time.Ticker, lease time.Duration, action model.ActionType, i int) *time.Ticker {
	interval, changed := getJobCheckInterval(action, i)
	if !changed {
		return ticker
	}
	// For now we should stop old ticker and create a new ticker
	ticker.Stop()
	return time.NewTicker(chooseLeaseTime(lease, interval))
}

func recordLastDDLInfo(ctx sessionctx.Context, job *model.Job) {
	if job == nil {
		return
	}
	ctx.GetSessionVars().LastDDLInfo.Query = job.Query
	ctx.GetSessionVars().LastDDLInfo.SeqNum = job.SeqNum
}

func setDDLJobQuery(ctx sessionctx.Context, job *model.Job) {
	switch job.Type {
	case model.ActionUpdateTiFlashReplicaStatus, model.ActionUnlockTable:
		job.Query = ""
	default:
		job.Query, _ = ctx.Value(sessionctx.QueryString).(string)
	}
}

var (
	fastDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
	}
	normalDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
	}
	slowDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		1 * time.Second,
		3 * time.Second,
	}
)

func getIntervalFromPolicy(policy []time.Duration, i int) (time.Duration, bool) {
	plen := len(policy)
	if i < plen {
		return policy[i], true
	}
	return policy[plen-1], false
}

func getJobCheckInterval(action model.ActionType, i int) (time.Duration, bool) {
	switch action {
	case model.ActionAddIndex, model.ActionAddPrimaryKey, model.ActionModifyColumn,
		model.ActionReorganizePartition,
		model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		return getIntervalFromPolicy(slowDDLIntervalPolicy, i)
	case model.ActionCreateTable, model.ActionCreateSchema:
		return getIntervalFromPolicy(fastDDLIntervalPolicy, i)
	default:
		return getIntervalFromPolicy(normalDDLIntervalPolicy, i)
	}
}

// NewDDLReorgMeta create a DDL ReorgMeta.
func NewDDLReorgMeta(ctx sessionctx.Context) *model.DDLReorgMeta {
	tzName, tzOffset := ddlutil.GetTimeZone(ctx)
	return &model.DDLReorgMeta{
		SQLMode:           ctx.GetSessionVars().SQLMode,
		Warnings:          make(map[errors.ErrorID]*terror.Error),
		WarningsCount:     make(map[errors.ErrorID]int64),
		Location:          &model.TimeZoneLocation{Name: tzName, Offset: tzOffset},
		ResourceGroupName: ctx.GetSessionVars().StmtCtx.ResourceGroupName,
		Version:           model.CurrentReorgMetaVersion,
	}
}

// RefreshMeta is a internal DDL job. In some cases, BR log restore will EXCHANGE
// PARTITION\DROP TABLE by write meta kv directly, and table info in meta kv
// is inconsistent with info schema. So when BR call AlterTableMode for new table
// will failure. RefreshMeta will reload schema diff to update info schema by
// schema ID and table ID to make sure data in meta kv and info schema is consistent.
func (e *executor) RefreshMeta(sctx sessionctx.Context, args *model.RefreshMetaArgs) error {
	job := &model.Job{
		Version:        model.JobVersion2,
		SchemaID:       args.SchemaID,
		TableID:        args.TableID,
		SchemaName:     args.InvolvedDB,
		Type:           model.ActionRefreshMeta,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{
				Database: args.InvolvedDB,
				Table:    args.InvolvedTable,
			},
		},
	}
	sctx.SetValue(sessionctx.QueryString, "skip")
	err := e.doDDLJob2(sctx, job, args)
	return errors.Trace(err)
}

func getScatterScopeFromSessionctx(sctx sessionctx.Context) string {
	if val, ok := sctx.GetSessionVars().GetSystemVar(vardef.TiDBScatterRegion); ok {
		return val
	}
	logutil.DDLLogger().Info("system variable tidb_scatter_region not found, use default value")
	return vardef.DefTiDBScatterRegion
}

func getEnableDDLAnalyze(sctx sessionctx.Context) string {
	if val, ok := sctx.GetSessionVars().GetSystemVar(vardef.TiDBEnableDDLAnalyze); ok {
		return val
	}
	logutil.DDLLogger().Info("system variable tidb_stats_update_during_ddl not found, use default value")
	return variable.BoolToOnOff(vardef.DefTiDBEnableDDLAnalyze)
}

func getAnalyzeVersion(sctx sessionctx.Context) string {
	if val, ok := sctx.GetSessionVars().GetSystemVar(vardef.TiDBAnalyzeVersion); ok {
		return val
	}
	logutil.DDLLogger().Info("system variable tidb_analyze_version not found, use default value")
	return strconv.Itoa(vardef.DefTiDBAnalyzeVersion)
}

// checkColumnReferencedByPartialCondition checks whether alter column is referenced by a partial index condition
func checkColumnReferencedByPartialCondition(t *model.TableInfo, colName ast.CIStr) error {
	for _, idx := range t.Indices {
		_, ic := model.FindIndexColumnByName(idx.AffectColumn, colName.L)
		if ic != nil {
			return dbterror.ErrModifyColumnReferencedByPartialCondition.GenWithStackByArgs(colName.O, idx.Name.O)
		}
	}

	return nil
}

func isReservedSchemaObjInNextGen(id int64) bool {
	failpoint.Inject("skipCheckReservedSchemaObjInNextGen", func() {
		failpoint.Return(false)
	})
	return kerneltype.IsNextGen() && metadef.IsReservedID(id)
}
