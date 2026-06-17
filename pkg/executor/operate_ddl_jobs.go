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

package executor

import (
	"context"
	"fmt"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// CommandDDLJobsExec is the general struct for Cancel/Pause/Resume commands on
// DDL jobs. These command currently by admin have the very similar struct and
// operations, it should be a better idea to have them in the same struct.
type CommandDDLJobsExec struct {
	exec.BaseExecutor

	cursor int
	jobIDs []int64
	errs   []error

	execute func(ctx context.Context, se sessionctx.Context, ids []int64) (errs []error, err error)
}

// Open implements the Executor for all Cancel/Pause/Resume command on DDL jobs
// just with different processes. And, it should not be called directly by the
// Executor.
func (e *CommandDDLJobsExec) Open(ctx context.Context) error {
	// We want to use a global transaction to execute the admin command, so we don't use e.Ctx() here.
	newSess, err := e.GetSysSession()
	if err != nil {
		return err
	}
	e.errs, err = e.execute(ctx, newSess, e.jobIDs)
	e.ReleaseSysSession(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), newSess)
	return err
}

// Next implements the Executor Next interface for Cancel/Pause/Resume
func (e *CommandDDLJobsExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.jobIDs) {
		return nil
	}
	numCurBatch := min(req.Capacity(), len(e.jobIDs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		req.AppendString(0, strconv.FormatInt(e.jobIDs[i], 10))
		if e.errs != nil && e.errs[i] != nil {
			req.AppendString(1, fmt.Sprintf("error: %v", e.errs[i]))
		} else {
			req.AppendString(1, "successful")
		}
	}
	e.cursor += numCurBatch
	return nil
}

// CancelDDLJobsExec represents a cancel DDL jobs executor.
type CancelDDLJobsExec struct {
	*CommandDDLJobsExec
}

// PauseDDLJobsExec indicates an Executor for Pause a DDL Job.
type PauseDDLJobsExec struct {
	*CommandDDLJobsExec
}

// ResumeDDLJobsExec indicates an Executor for Resume a DDL Job.
type ResumeDDLJobsExec struct {
	*CommandDDLJobsExec
}

// AlterDDLJobExec indicates an Executor for alter config of a DDL Job.
type AlterDDLJobExec struct {
	exec.BaseExecutor
	jobID     int64
	AlterOpts []*core.AlterDDLJobOpt
}

// Open implements the Executor Open interface.
func (e *AlterDDLJobExec) Open(ctx context.Context) error {
	newSess, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), newSess)

	return e.processAlterDDLJobConfig(ctx, newSess)
}

func getJobMetaFromTable(
	ctx context.Context,
	se *sess.Session,
	jobID int64,
) (*model.Job, error) {
	sql := fmt.Sprintf("select job_meta from mysql.%s where job_id = %s",
		ddl.JobTable, strconv.FormatInt(jobID, 10))
	rows, err := se.Execute(ctx, sql, "get_job_by_id")
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("ddl job %d is not running", jobID)
	}
	jobBinary := rows[0].GetBytes(0)
	job := model.Job{}
	err = job.Decode(jobBinary)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &job, nil
}

func updateJobMeta2Table(
	ctx context.Context,
	se *sess.Session,
	job *model.Job,
) error {
	b, err := job.Encode(false)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("update mysql.%s set job_meta = %s where job_id = %d",
		ddl.JobTable, util.WrapKey2String(b), job.ID)
	_, err = se.Execute(ctx, sql, "update_job")
	return errors.Trace(err)
}

const alterDDLJobMaxRetryCnt = 3

// processAlterDDLJobConfig try to alter the ddl job configs.
// In case of failure, it will retry alterDDLJobMaxRetryCnt times.
func (e *AlterDDLJobExec) processAlterDDLJobConfig(
	ctx context.Context,
	sessCtx sessionctx.Context,
) (err error) {
	ns := sess.NewSession(sessCtx)
	var job *model.Job
	for tryN := uint(0); tryN < alterDDLJobMaxRetryCnt; tryN++ {
		if err = ns.Begin(ctx); err != nil {
			continue
		}
		job, err = getJobMetaFromTable(ctx, ns, e.jobID)
		if err != nil {
			continue
		}
		if !job.IsAlterable() {
			return fmt.Errorf("unsupported DDL operation: %s. "+
				"Supported DDL operations are: ADD INDEX, MODIFY COLUMN, and ALTER TABLE REORGANIZE PARTITION", job.Type.String())
		}
		if err = e.updateReorgMeta(job, model.AdminCommandByEndUser); err != nil {
			continue
		}
		if err = updateJobMeta2Table(ctx, ns, job); err != nil {
			continue
		}

		failpoint.Inject("mockAlterDDLJobCommitFailed", func(val failpoint.Value) {
			if val.(bool) {
				ns.Rollback()
				failpoint.Return(errors.New("mock commit failed on admin alter ddl jobs"))
			}
		})

		if err = ns.Commit(ctx); err != nil {
			ns.Rollback()
			continue
		}
		return nil
	}
	return err
}

func (e *AlterDDLJobExec) updateReorgMeta(job *model.Job, byWho model.AdminCommandOperator) error {
	for _, opt := range e.AlterOpts {
		if opt.Value == nil {
			continue
		}
		switch opt.Name {
		case core.AlterDDLJobThread:
			cons := opt.Value.(*expression.Constant)
			job.ReorgMeta.SetConcurrency(int(cons.Value.GetInt64()))
			job.AdminOperator = byWho
		case core.AlterDDLJobBatchSize:
			cons := opt.Value.(*expression.Constant)
			job.ReorgMeta.SetBatchSize(int(cons.Value.GetInt64()))
			job.AdminOperator = byWho
		case core.AlterDDLJobMaxWriteSpeed:
			speed, err := core.GetMaxWriteSpeedFromExpression(opt)
			if err != nil {
				return err
			}
			job.ReorgMeta.SetMaxWriteSpeed(int(speed))
			job.AdminOperator = byWho
		default:
			return errors.Errorf("unsupported admin alter ddl jobs config: %s", opt.Name)
		}
	}
	return nil
}
