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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"go.uber.org/zap"
)
// GetDropOrTruncateTableInfoFromJobsByStore implements GetDropOrTruncateTableInfoFromJobs
func GetDropOrTruncateTableInfoFromJobsByStore(jobs []*model.Job, gcSafePoint uint64, getTable func(uint64, int64, int64) (*model.TableInfo, error), fn func(*model.Job, *model.TableInfo) (bool, error)) (bool, error) {
	for _, job := range jobs {
		// Check GC safe point for getting snapshot infoSchema.
		err := gcutil.ValidateSnapshotWithGCSafePoint(job.StartTS, gcSafePoint)
		if err != nil {
			return false, err
		}
		if job.Type != model.ActionDropTable && job.Type != model.ActionTruncateTable {
			continue
		}

		tbl, err := getTable(job.StartTS, job.SchemaID, job.TableID)
		if err != nil {
			if meta.ErrDBNotExists.Equal(err) {
				// The dropped/truncated DDL maybe execute failed that caused by the parallel DDL execution,
				// then can't find the table from the snapshot info-schema. Should just ignore error here,
				// see more in TestParallelDropSchemaAndDropTable.
				continue
			}
			return false, err
		}
		if tbl == nil {
			// The dropped/truncated DDL maybe execute failed that caused by the parallel DDL execution,
			// then can't find the table from the snapshot info-schema. Should just ignore error here,
			// see more in TestParallelDropSchemaAndDropTable.
			continue
		}
		finish, err := fn(job, tbl)
		if err != nil || finish {
			return finish, err
		}
	}
	return false, nil
}

// Info is for DDL information.
type Info struct {
	SchemaVer   int64
	ReorgHandle kv.Key       // It's only used for DDL information.
	Jobs        []*model.Job // It's the currently running jobs.
}

// GetDDLInfoWithNewTxn returns DDL information using a new txn.
func GetDDLInfoWithNewTxn(s sessionctx.Context) (*Info, error) {
	se := sess.NewSession(s)
	err := se.Begin(context.Background())
	if err != nil {
		return nil, err
	}
	info, err := GetDDLInfo(s)
	se.Rollback()
	return info, err
}

// GetDDLInfo returns DDL information and only uses for testing.
func GetDDLInfo(s sessionctx.Context) (*Info, error) {
	var err error
	info := &Info{}
	se := sess.NewSession(s)
	txn, err := se.Txn()
	if err != nil {
		return nil, errors.Trace(err)
	}
	t := meta.NewMutator(txn)
	info.Jobs = make([]*model.Job, 0, 2)
	var generalJob, reorgJob *model.Job
	generalJob, reorgJob, err = get2JobsFromTable(se)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if generalJob != nil {
		info.Jobs = append(info.Jobs, generalJob)
	}

	if reorgJob != nil {
		info.Jobs = append(info.Jobs, reorgJob)
	}

	info.SchemaVer, err = t.GetSchemaVersionWithNonEmptyDiff()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if reorgJob == nil {
		return info, nil
	}

	_, info.ReorgHandle, _, _, err = newReorgHandler(se).GetDDLReorgHandle(reorgJob)
	if err != nil {
		if meta.ErrDDLReorgElementNotExist.Equal(err) {
			return info, nil
		}
		return nil, errors.Trace(err)
	}

	return info, nil
}

func get2JobsFromTable(sess *sess.Session) (generalJob, reorgJob *model.Job, err error) {
	ctx := context.Background()
	jobs, err := getJobsBySQL(ctx, sess, "not reorg order by job_id limit 1")
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if len(jobs) != 0 {
		generalJob = jobs[0]
	}
	jobs, err = getJobsBySQL(ctx, sess, "reorg order by job_id limit 1")
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if len(jobs) != 0 {
		reorgJob = jobs[0]
	}
	return generalJob, reorgJob, nil
}

// cancelRunningJob cancel a DDL job that is in the concurrent state.
func cancelRunningJob(job *model.Job,
	byWho model.AdminCommandOperator) (err error) {
	// These states can't be cancelled.
	if job.IsDone() || job.IsSynced() {
		return dbterror.ErrCancelFinishedDDLJob.GenWithStackByArgs(job.ID)
	}

	// If the state is rolling back, it means the work is cleaning the data after cancelling the job.
	if job.IsCancelled() || job.IsRollingback() || job.IsRollbackDone() {
		return nil
	}

	if !job.IsRollbackable() {
		return dbterror.ErrCannotCancelDDLJob.GenWithStackByArgs(job.ID)
	}
	job.State = model.JobStateCancelling
	job.AdminOperator = byWho
	return nil
}

// pauseRunningJob check and pause the running Job
func pauseRunningJob(job *model.Job,
	byWho model.AdminCommandOperator) (err error) {
	if job.IsPausing() || job.IsPaused() {
		return dbterror.ErrPausedDDLJob.GenWithStackByArgs(job.ID)
	}
	if !job.IsPausable() {
		errMsg := fmt.Sprintf("state [%s] or schema state [%s]", job.State.String(), job.SchemaState.String())
		err = dbterror.ErrCannotPauseDDLJob.GenWithStackByArgs(job.ID, errMsg)
		if err != nil {
			return err
		}
	}

	job.State = model.JobStatePausing
	job.AdminOperator = byWho
	return nil
}

// resumePausedJob check and resume the Paused Job
func resumePausedJob(job *model.Job,
	byWho model.AdminCommandOperator) (err error) {
	if !job.IsResumable() {
		errMsg := fmt.Sprintf("job has not been paused, job state:%s, schema state:%s",
			job.State, job.SchemaState)
		return dbterror.ErrCannotResumeDDLJob.GenWithStackByArgs(job.ID, errMsg)
	}
	// The Paused job should only be resumed by who paused it
	if job.AdminOperator != byWho {
		errMsg := fmt.Sprintf("job has been paused by [%s], should not resumed by [%s]",
			job.AdminOperator.String(), byWho.String())
		return dbterror.ErrCannotResumeDDLJob.GenWithStackByArgs(job.ID, errMsg)
	}

	job.State = model.JobStateQueueing

	return nil
}

// processJobs command on the Job according to the process
func processJobs(
	ctx context.Context,
	process func(*model.Job, model.AdminCommandOperator) (err error),
	sessCtx sessionctx.Context,
	ids []int64,
	byWho model.AdminCommandOperator,
) (jobErrs []error, err error) {
	failpoint.Inject("mockFailedCommandOnConcurencyDDL", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("mock failed admin command on ddl jobs"))
		}
	})

	if len(ids) == 0 {
		return nil, nil
	}

	ns := sess.NewSession(sessCtx)
	// We should process (and try) all the jobs in one Transaction.
	for range 3 {
		jobErrs = make([]error, len(ids))
		// Need to figure out which one could not be paused
		jobMap := make(map[int64]int, len(ids))
		idsStr := make([]string, 0, len(ids))
		for idx, id := range ids {
			jobMap[id] = idx
			idsStr = append(idsStr, strconv.FormatInt(id, 10))
		}

		err = ns.Begin(ctx)
		if err != nil {
			return nil, err
		}
		jobs, err := getJobsBySQL(ctx, ns, fmt.Sprintf("job_id in (%s) order by job_id", strings.Join(idsStr, ", ")))
		if err != nil {
			ns.Rollback()
			return nil, err
		}

		for _, job := range jobs {
			i, ok := jobMap[job.ID]
			if !ok {
				logutil.DDLLogger().Debug("Job ID from meta is not consistent with requested job id,",
					zap.Int64("fetched job ID", job.ID))
				jobErrs[i] = dbterror.ErrInvalidDDLJob.GenWithStackByArgs(job.ID)
				continue
			}
			delete(jobMap, job.ID)

			err = process(job, byWho)
			if err != nil {
				jobErrs[i] = err
				continue
			}

			err = updateDDLJob2Table(ctx, ns, job, false)
			if err != nil {
				jobErrs[i] = err
				continue
			}
		}

		failpoint.Inject("mockCommitFailedOnDDLCommand", func(val failpoint.Value) {
			if val.(bool) {
				ns.Rollback()
				failpoint.Return(jobErrs, errors.New("mock commit failed on admin command on ddl jobs"))
			}
		})

		// There may be some conflict during the update, try it again
		if err = ns.Commit(ctx); err != nil {
			ns.Rollback()
			continue
		}

		for id, idx := range jobMap {
			jobErrs[idx] = dbterror.ErrDDLJobNotFound.GenWithStackByArgs(id)
		}

		return jobErrs, nil
	}

	return jobErrs, err
}

// CancelJobs cancels the DDL jobs according to user command.
func CancelJobs(ctx context.Context, se sessionctx.Context, ids []int64) (errs []error, err error) {
	return processJobs(ctx, cancelRunningJob, se, ids, model.AdminCommandByEndUser)
}

// PauseJobs pause all the DDL jobs according to user command.
func PauseJobs(ctx context.Context, se sessionctx.Context, ids []int64) ([]error, error) {
	return processJobs(ctx, pauseRunningJob, se, ids, model.AdminCommandByEndUser)
}

// ResumeJobs resume all the DDL jobs according to user command.
func ResumeJobs(ctx context.Context, se sessionctx.Context, ids []int64) ([]error, error) {
	return processJobs(ctx, resumePausedJob, se, ids, model.AdminCommandByEndUser)
}

// CancelJobsBySystem cancels Jobs because of internal reasons.
func CancelJobsBySystem(se sessionctx.Context, ids []int64) (errs []error, err error) {
	ctx := context.Background()
	return processJobs(ctx, cancelRunningJob, se, ids, model.AdminCommandBySystem)
}

// PauseJobsBySystem pauses Jobs because of internal reasons.
func PauseJobsBySystem(se sessionctx.Context, ids []int64) (errs []error, err error) {
	ctx := context.Background()
	return processJobs(ctx, pauseRunningJob, se, ids, model.AdminCommandBySystem)
}

// ResumeJobsBySystem resumes Jobs that are paused by TiDB itself.
func ResumeJobsBySystem(se sessionctx.Context, ids []int64) (errs []error, err error) {
	ctx := context.Background()
	return processJobs(ctx, resumePausedJob, se, ids, model.AdminCommandBySystem)
}

// pprocessAllJobs processes all the jobs in the job table, 100 jobs at a time in case of high memory usage.
func processAllJobs(
	ctx context.Context,
	process func(*model.Job, model.AdminCommandOperator) (err error),
	se sessionctx.Context,
	byWho model.AdminCommandOperator,
) (map[int64]error, error) {
	var err error
	var jobErrs = make(map[int64]error)

	ns := sess.NewSession(se)
	err = ns.Begin(ctx)
	if err != nil {
		return nil, err
	}

	var jobID int64
	var jobIDMax int64
	var limit = 100
	for {
		var jobs []*model.Job
		jobs, err = getJobsBySQL(ctx, ns, fmt.Sprintf("job_id >= %s order by job_id asc limit %s",
			strconv.FormatInt(jobID, 10),
			strconv.FormatInt(int64(limit), 10)))
		if err != nil {
			ns.Rollback()
			return nil, err
		}

		for _, job := range jobs {
			err = process(job, byWho)
			if err != nil {
				jobErrs[job.ID] = err
				continue
			}

			err = updateDDLJob2Table(ctx, ns, job, false)
			if err != nil {
				jobErrs[job.ID] = err
				continue
			}
		}

		// Just in case the job ID is not sequential
		if len(jobs) > 0 && jobs[len(jobs)-1].ID > jobIDMax {
			jobIDMax = jobs[len(jobs)-1].ID
		}

		// If rows returned is smaller than $limit, then there is no more records
		if len(jobs) < limit {
			break
		}

		jobID = jobIDMax + 1
	}

	err = ns.Commit(ctx)
	if err != nil {
		return nil, err
	}
	return jobErrs, nil
}

// PauseAllJobsBySystem pauses all running Jobs because of internal reasons.
func PauseAllJobsBySystem(se sessionctx.Context) (map[int64]error, error) {
	return processAllJobs(context.Background(), pauseRunningJob, se, model.AdminCommandBySystem)
}

// ResumeAllJobsBySystem resumes all paused Jobs because of internal reasons.
func ResumeAllJobsBySystem(se sessionctx.Context) (map[int64]error, error) {
	return processAllJobs(context.Background(), resumePausedJob, se, model.AdminCommandBySystem)
}

// GetAllDDLJobs get all DDL jobs and sorts jobs by job.ID.
func GetAllDDLJobs(ctx context.Context, se sessionctx.Context) ([]*model.Job, error) {
	return getJobsBySQL(ctx, sess.NewSession(se), "1 order by job_id")
}

// IterAllDDLJobs will iterates running DDL jobs first, return directly if `finishFn` return true or error,
// then iterates history DDL jobs until the `finishFn` return true or error.
func IterAllDDLJobs(ctx sessionctx.Context, txn kv.Transaction, finishFn func([]*model.Job) (bool, error)) error {
	jobs, err := GetAllDDLJobs(context.Background(), ctx)
	if err != nil {
		return err
	}

	finish, err := finishFn(jobs)
	if err != nil || finish {
		return err
	}
	return IterHistoryDDLJobs(txn, finishFn)
}
