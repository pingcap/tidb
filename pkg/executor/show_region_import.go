// Copyright 2017 PingCAP, Inc.
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
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	fstorage "github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/pd/client/errs"
	"go.uber.org/zap"
)

func handleImportJobInfo(
	ctx context.Context, location *time.Location,
	info *importer.JobInfo, result *chunk.Chunk,
) error {
	var (
		runInfo *importinto.RuntimeInfo
		err     error
	)

	if info.Status == importer.JobStatusRunning {
		// need to get more info from distributed framework for running jobs
		runInfo, err = importinto.GetRuntimeInfoForJob(ctx, location, info.ID)
		if err != nil {
			return err
		}
		if runInfo.Status == proto.TaskStateAwaitingResolution {
			info.Status = string(runInfo.Status)
			info.ErrorMessage = runInfo.ErrorMsg
		}
	}
	FillOneImportJobInfo(result, info, runInfo)
	return nil
}

const balanceRangeScheduler = "balance-range-scheduler"

func (e *ShowExec) fetchShowDistributionJobs(ctx context.Context) error {
	config, err := infosync.GetSchedulerConfig(ctx, balanceRangeScheduler)
	if err != nil {
		return err
	}
	configs, ok := config.([]any)
	if !ok {
		// it means that no any jobs
		return nil
	}
	jobs := make([]map[string]any, 0, len(configs))
	for _, cfg := range configs {
		job, ok := cfg.(map[string]any)
		if !ok {
			return errs.ErrClientProtoUnmarshal.FastGenByArgs(cfg)
		}
		jobs = append(jobs, job)
	}
	if e.DistributionJobID != nil {
		for _, job := range jobs {
			jobID, ok := job["job-id"].(float64)
			if ok && *e.DistributionJobID == int64(jobID) {
				if err := fillDistributionJobToChunk(ctx, job, e.result); err != nil {
					return err
				}
				break
			}
		}
	} else {
		for _, job := range jobs {
			if err := fillDistributionJobToChunk(ctx, job, e.result); err != nil {
				return err
			}
		}
	}
	return nil
}

// fillDistributionJobToChunk fills the distribution job to the chunk
func fillDistributionJobToChunk(ctx context.Context, job map[string]any, result *chunk.Chunk) error {
	// alias is {db_name}.{table_name}.{partition_name}
	alias := strings.Split(job["alias"].(string), ".")
	logutil.Logger(ctx).Info("fillDistributionJobToChunk", zap.String("alias", job["alias"].(string)))
	if len(alias) != 3 {
		return errs.ErrClientProtoUnmarshal.FastGenByArgs(fmt.Sprintf("alias:%s is invalid", job["alias"].(string)))
	}
	result.AppendUint64(0, uint64(job["job-id"].(float64)))
	result.AppendString(1, alias[0])
	result.AppendString(2, alias[1])
	// partition name maybe empty when the table is not partitioned
	if alias[2] == "" {
		result.AppendNull(3)
	} else {
		result.AppendString(3, alias[2])
	}
	result.AppendString(4, job["engine"].(string))
	result.AppendString(5, job["rule"].(string))
	result.AppendString(6, job["status"].(string))
	timeout := uint64(job["timeout"].(float64))
	result.AppendString(7, time.Duration(timeout).String())
	if create, ok := job["create"]; ok {
		createTime := &time.Time{}
		err := createTime.UnmarshalText([]byte(create.(string)))
		if err != nil {
			return err
		}
		result.AppendTime(8, types.NewTime(types.FromGoTime(*createTime), mysql.TypeDatetime, types.DefaultFsp))
	} else {
		result.AppendNull(8)
	}
	if start, ok := job["start"]; ok {
		startTime := &time.Time{}
		err := startTime.UnmarshalText([]byte(start.(string)))
		if err != nil {
			return err
		}
		result.AppendTime(9, types.NewTime(types.FromGoTime(*startTime), mysql.TypeDatetime, types.DefaultFsp))
	} else {
		result.AppendNull(9)
	}
	if finish, ok := job["finish"]; ok {
		finishedTime := &time.Time{}
		err := finishedTime.UnmarshalText([]byte(finish.(string)))
		if err != nil {
			return err
		}
		result.AppendTime(10, types.NewTime(types.FromGoTime(*finishedTime), mysql.TypeDatetime, types.DefaultFsp))
	} else {
		result.AppendNull(10)
	}
	return nil
}

type groupInfo struct {
	groupKey   string
	jobCount   int64
	pending    int64
	running    int64
	completed  int64
	failed     int64
	canceled   int64
	createTime types.Time
	updateTime types.Time
}

func (e *ShowExec) fetchShowImportGroups(ctx context.Context) error {
	sctx := e.Ctx()

	var hasSuperPriv bool
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		hasSuperPriv = pm.RequestVerification(sctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv)
	}
	// we use sessionCtx from GetTaskManager, user ctx might not have system table privileges.
	taskManager, err := fstorage.GetTaskManager()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return err
	}

	var infos []*importer.JobInfo
	if err = taskManager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.GetSQLExecutor()
		var err2 error
		infos, err2 = importer.GetJobsByGroupKey(ctx, exec, sctx.GetSessionVars().User.String(), e.ImportGroupKey, hasSuperPriv)
		return err2
	}); err != nil {
		return err
	}

	groupMap := make(map[string]*groupInfo)
	for _, info := range infos {
		if _, ok := groupMap[info.GroupKey]; !ok {
			groupMap[info.GroupKey] = &groupInfo{
				groupKey:   info.GroupKey,
				createTime: types.ZeroTime,
				updateTime: types.ZeroTime,
			}
		}

		updateTime := types.ZeroTime
		// See FillOneImportJobInfo, we make the update time calculation same with SHOW IMPORT JOBS.
		if info.Status == importer.JobStatusRunning {
			runInfo, err := importinto.GetRuntimeInfoForJob(ctx, sctx.GetSessionVars().Location(), info.ID)
			if err != nil {
				return err
			}
			updateTime = runInfo.UpdateTime
			if updateTime.IsZero() {
				updateTime = info.UpdateTime
			}
		}

		gInfo := groupMap[info.GroupKey]
		if !info.CreateTime.IsZero() && (gInfo.createTime.IsZero() || info.CreateTime.Compare(gInfo.createTime) < 0) {
			gInfo.createTime = info.CreateTime
		}
		if updateTime.Compare(gInfo.updateTime) > 0 {
			gInfo.updateTime = updateTime
		}

		// See pkg/executor/importer/job.go for job status
		gInfo.jobCount++
		switch info.Status {
		case "pending":
			gInfo.pending++
		case "running":
			gInfo.running++
		case "finished":
			gInfo.completed++
		case "failed":
			gInfo.failed++
		case "cancelled":
			gInfo.canceled++
		}
	}

	for _, gInfo := range groupMap {
		e.result.AppendString(0, gInfo.groupKey)
		e.result.AppendInt64(1, gInfo.jobCount)
		e.result.AppendInt64(2, gInfo.pending)
		e.result.AppendInt64(3, gInfo.running)
		e.result.AppendInt64(4, gInfo.completed)
		e.result.AppendInt64(5, gInfo.failed)
		e.result.AppendInt64(6, gInfo.canceled)
		if gInfo.createTime.IsZero() {
			e.result.AppendNull(7)
		} else {
			e.result.AppendTime(7, gInfo.createTime)
		}
		if gInfo.updateTime.IsZero() {
			e.result.AppendNull(8)
		} else {
			e.result.AppendTime(8, gInfo.updateTime)
		}
	}
	return nil
}

// fetchShowImportJobs fills the result with the schema:
// {"Job_ID", "Group_Key", "Data_Source", "Target_Table", "Table_ID",
// "Phase", "Status", "Source_File_Size", "Imported_Rows",
// "Result_Message", "Create_Time", "Start_Time", "End_Time", "Created_By"}
func (e *ShowExec) fetchShowImportJobs(ctx context.Context) error {
	sctx := e.Ctx()

	var hasSuperPriv bool
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		hasSuperPriv = pm.RequestVerification(sctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv)
	}
	// we use sessionCtx from GetTaskManager, user ctx might not have system table privileges.
	taskManager, err := fstorage.GetTaskManager()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return err
	}

	loc := sctx.GetSessionVars().Location()
	if e.ImportJobID != nil {
		var info *importer.JobInfo
		if err = taskManager.WithNewSession(func(se sessionctx.Context) error {
			exec := se.GetSQLExecutor()
			var err2 error
			info, err2 = importer.GetJob(ctx, exec, *e.ImportJobID, sctx.GetSessionVars().User.String(), hasSuperPriv)
			return err2
		}); err != nil {
			return err
		}
		return handleImportJobInfo(ctx, loc, info, e.result)
	}
	var infos []*importer.JobInfo
	if err = taskManager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.GetSQLExecutor()
		var err2 error
		infos, err2 = importer.GetAllViewableJobs(ctx, exec, sctx.GetSessionVars().User.String(), hasSuperPriv)
		return err2
	}); err != nil {
		return err
	}
	for _, info := range infos {
		if err2 := handleImportJobInfo(ctx, loc, info, e.result); err2 != nil {
			return err2
		}
	}
	// TODO: does not support filtering for now
	return nil
}

// tryFillViewColumnType fill the columns type info of a view.
// Because view's underlying table's column could change or recreate, so view's column type may change over time.
// To avoid this situation we need to generate a logical plan and extract current column types from Schema.
func tryFillViewColumnType(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, dbName ast.CIStr, tbl *model.TableInfo) error {
	if !tbl.IsView() {
		return nil
	}
	ctx = kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	// We need to run the build plan process in another session because there may be
	// multiple goroutines running at the same time while session is not goroutine-safe.
	// Take joining system table as an example, `fetchBuildSideRows` and `fetchProbeSideChunks` can be run concurrently.
	return runWithSystemSession(ctx, sctx, func(s sessionctx.Context) error {
		// Retrieve view columns info.
		planBuilder, _ := plannercore.NewPlanBuilder(
			plannercore.PlanBuilderOptNoExecution{}).Init(s.GetPlanCtx(), is, hint.NewQBHintHandler(nil))
		viewLogicalPlan, err := planBuilder.BuildDataSourceFromView(ctx, dbName, tbl, nil, nil)
		if err != nil {
			return err
		}
		viewSchema := viewLogicalPlan.Schema()
		viewOutputNames := viewLogicalPlan.OutputNames()
		for _, col := range tbl.Columns {
			idx := expression.FindFieldNameIdxByColName(viewOutputNames, col.Name.L)
			if idx >= 0 {
				col.FieldType = *viewSchema.Columns[idx].GetType(sctx.GetExprCtx().GetEvalCtx())
			}
			if col.GetType() == mysql.TypeVarString {
				col.SetType(mysql.TypeVarchar)
			}
		}
		return nil
	})
}

func runWithSystemSession(ctx context.Context, sctx sessionctx.Context, fn func(sessionctx.Context) error) error {
	b := exec.NewBaseExecutor(sctx, nil, 0)
	sysCtx, err := b.GetSysSession()
	if err != nil {
		return err
	}
	defer b.ReleaseSysSession(ctx, sysCtx)

	if err = loadSnapshotInfoSchemaIfNeeded(sysCtx, sctx.GetSessionVars().SnapshotTS); err != nil {
		return err
	}
	// `fn` may use KV transaction, so initialize the txn here
	if err = sessiontxn.NewTxn(ctx, sysCtx); err != nil {
		return err
	}
	defer sysCtx.RollbackTxn(ctx)
	if err = ResetContextOfStmt(sysCtx, &ast.SelectStmt{}); err != nil {
		return err
	}
	return fn(sysCtx)
}
