// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	fstorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	exeerrors "github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// AlterImportJobExec executes ALTER IMPORT JOB.
// It updates import job parameters and related global task meta.
type AlterImportJobExec struct {
	exec.BaseExecutor
	jobID     int64
	AlterOpts []*core.AlterImportJobOpt
}

// Open executes ALTER IMPORT JOB logic. It checks privileges, fetches
// job info, validates its status, updates job parameters in the database,
// and modifies the global task metadata accordingly.
func (e *AlterImportJobExec) Open(ctx context.Context) error {
	taskManager, err := fstorage.GetTaskManager()
	if err != nil {
		return err
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalImportInto)
	hasSuperPriv := false
	if pm := privilege.GetPrivilegeManager(e.Ctx()); pm != nil {
		hasSuperPriv = pm.RequestVerification(e.Ctx().GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv)
	}
	var jobInfo *importer.JobInfo
	if err := taskManager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.GetSQLExecutor()
		var err2 error
		jobInfo, err2 = importer.GetJob(ctx, exec, e.jobID, e.Ctx().GetSessionVars().User.String(), hasSuperPriv)
		return err2
	}); err != nil {
		return err
	}
	if !jobInfo.CanModify() {
		return exeerrors.ErrLoadDataInvalidOperation.FastGenByArgs("ALTER")
	}

	taskKey := importinto.TaskKey(e.jobID)
	task, err := taskManager.GetTaskByKey(ctx, taskKey)
	if err != nil {
		return err
	}
	taskID := task.ID

	currTask, err := taskManager.GetTaskByID(ctx, taskID)
	if err != nil {
		return err
	}
	if !currTask.State.CanMoveToModifying() {
		return errors.Errorf(
			"import into task state is not suitable for modifying for job id %d; "+
				"current task state is %s",
			e.jobID,
			currTask.State,
		)
	}
	opts := make([]*core.AlterImportJobOpt, 0, len(e.AlterOpts))
	for _, opt := range e.AlterOpts {
		opts = append(opts, &core.AlterImportJobOpt{Name: opt.Name, Value: opt.Value})
	}

	mods := make([]proto.Modification, 0, len(opts))
	for _, opt := range opts {
		switch opt.Name {
		case core.AlterImportJobThread:
			val, _ := core.GetThreadOrBatchSizeFromImportExpression(opt)
			cpuCount, err := taskManager.GetCPUCountOfNode(ctx)
			if err != nil {
				return err
			}
			adjustedCurrency := min(val, int64(cpuCount))
			logutil.BgLogger().Info(
				"adjusted concurrency",
				zap.Int64("requested", val),
				zap.Int64("cpuCount", int64(cpuCount)),
				zap.Int64("adjustedConcurrency", adjustedCurrency),
			)
			mods = append(mods, proto.Modification{Type: proto.ModifyConcurrency, To: adjustedCurrency})
		case core.AlterImportJobMaxWriteSpeed:
			val, _ := core.GetMaxWriteSpeedFromImportExpression(opt)
			mods = append(mods, proto.Modification{Type: proto.ModifyMaxWriteSpeed, To: val})
		}
	}

	if err := taskManager.ModifyTaskByID(ctx, jobInfo.ID, &proto.ModifyParam{
		PrevState:     currTask.State,
		Modifications: mods,
	}); err != nil {
		return err
	}

	// Only update job parameters if task modification succeeded.
	return taskManager.WithNewTxn(ctx, func(se sessionctx.Context) error {
		exec := se.GetSQLExecutor()
		return importer.UpdateJobParameters(
			ctx,
			exec,
			e.jobID,
			e.Ctx().GetSessionVars().User.String(),
			hasSuperPriv,
			opts,
		)
	})
}
