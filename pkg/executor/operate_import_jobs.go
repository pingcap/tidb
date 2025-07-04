package executor

import (
	"context"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	fstorage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	exeerrors "github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
)

// AlterImportJobExec executes ALTER IMPORT JOB.
// It updates import job parameters and related global task meta.
type AlterImportJobExec struct {
	exec.BaseExecutor
	jobID     int64
	AlterOpts []*core.AlterImportJobOpt
}

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
	if jobInfo.Status != importer.JobStatusRunning {
		return exeerrors.ErrLoadDataInvalidOperation.FastGenByArgs("ALTER")
	}
	opts := make([]*core.AlterImportJobOpt, 0, len(e.AlterOpts))
	for _, opt := range e.AlterOpts {
		opts = append(opts, &core.AlterImportJobOpt{Name: opt.Name, Value: opt.Value})
	}
	if err := taskManager.WithNewTxn(ctx, func(se sessionctx.Context) error {
		exec := se.GetSQLExecutor()
		return importer.UpdateJobParameters(ctx, exec, e.jobID, opts)
	}); err != nil {
		return err
	}
	mods := make([]proto.Modification, 0, len(opts))
	for _, opt := range opts {
		switch opt.Name {
		case core.AlterImportJobThread:
			val, _ := core.GetThreadOrBatchSizeFromImportExpression(opt)
			mods = append(mods, proto.Modification{Type: proto.ModifyConcurrency, To: val})
		case core.AlterImportJobMaxWriteSpeed:
			val, _ := core.GetMaxWriteSpeedFromImportExpression(opt)
			mods = append(mods, proto.Modification{Type: proto.ModifyMaxWriteSpeed, To: val})
		}
	}
	return taskManager.ModifyTaskByID(ctx, jobInfo.ID, &proto.ModifyParam{PrevState: proto.TaskStateRunning, Modifications: mods})
}
