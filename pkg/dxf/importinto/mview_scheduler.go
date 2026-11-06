// Copyright 2026 PingCAP, Inc.
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

package importinto

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/planner"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	importcore "github.com/pingcap/tidb/pkg/importinto"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	statsstorage "github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/util/backoff"
)

// mviewBuildScheduler shares ingestion mechanics, but its lifecycle is owned by
// a DDL job. It never creates or updates an IMPORT job record.
type mviewBuildScheduler struct {
	*importScheduler
}

// NewMViewBuildScheduler creates the scheduler for a DDL-owned MV build.
func NewMViewBuildScheduler(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
	return &mviewBuildScheduler{importScheduler: newImportScheduler(ctx, task, param)}
}

func (sch *mviewBuildScheduler) Init() (err error) {
	task := sch.GetTask()
	defer func() {
		if err != nil {
			metricsManager.unregister(task.ID)
		}
	}()
	var meta importcore.TaskMeta
	if err := json.Unmarshal(task.Meta, &meta); err != nil {
		return errors.Trace(err)
	}
	if kerneltype.IsClassic() || meta.DDLJobID == 0 || meta.JobID != 0 || meta.Plan.QuerySource == nil ||
		meta.Plan.TableInfo == nil || !meta.Plan.IsGlobalSort() || task.ExtraParams.PrepareMode != proto.PrepareModeDisabled {
		return errors.New("invalid MV build task metadata")
	}
	sch.BaseScheduler.Extension = sch
	return sch.initTask(&meta)
}

func (*mviewBuildScheduler) OnPrepare(context.Context, storage.TaskHandle, *proto.Task) error {
	return errors.New("MV builds must be prepared by the DDL owner")
}

func (sch *mviewBuildScheduler) OnNextSubtasksBatch(ctx context.Context, taskHandle storage.TaskHandle,
	task *proto.Task, _ []string, nextStep proto.Step) ([][]byte, error) {
	var meta importcore.TaskMeta
	if err := json.Unmarshal(task.Meta, &meta); err != nil {
		return nil, errors.Trace(err)
	}
	var inputs []proto.Step
	switch nextStep {
	case proto.ImportStepQuery:
	case proto.ImportStepMergeSort:
		inputs = []proto.Step{sch.sourceStep}
	case proto.ImportStepWriteAndIngest:
		inputs = []proto.Step{sch.sourceStep, proto.ImportStepMergeSort}
	case proto.ImportStepCollectConflicts, proto.ImportStepConflictResolution:
		inputs = []proto.Step{sch.sourceStep, proto.ImportStepMergeSort, proto.ImportStepWriteAndIngest}
	case proto.ImportStepPostProcess:
		sch.switchTiKV2NormalMode(ctx, task, sch.GetLogger())
		inputs = []proto.Step{sch.sourceStep, proto.ImportStepCollectConflicts}
	case proto.StepDone:
		return nil, nil
	default:
		return nil, errors.Errorf("unknown step %d for MV build", nextStep)
	}
	previous := make(map[proto.Step][][]byte, len(inputs))
	for _, step := range inputs {
		metas, err := taskHandle.GetPreviousSubtaskMetas(task.ID, step)
		if err != nil {
			return nil, err
		}
		previous[step] = metas
	}
	planCtx := planner.PlanCtx{
		Ctx:                  ctx,
		TaskID:               task.ID,
		PreviousSubtaskMetas: previous,
		GlobalSort:           true,
		NextTaskStep:         nextStep,
		ExecuteNodesCnt:      task.MaxNodeCount,
		Store:                sch.TaskRuntime.Store(),
		ThreadCnt:            task.GetRuntimeSlots(),
	}
	return sch.planSubtasks(taskHandle, task, &meta, planCtx, sch.GetLogger())
}

func (sch *mviewBuildScheduler) OnDone(ctx context.Context, _ storage.TaskHandle, task *proto.Task) error {
	logger := sch.GetLogger()
	if task.State == proto.TaskStateReverting {
		sch.switchTiKV2NormalMode(ctx, task, logger)
		sch.unregisterTask(ctx, task)
		return nil
	}
	sch.unregisterTask(ctx, task)
	var meta importcore.TaskMeta
	if err := json.Unmarshal(task.Meta, &meta); err != nil {
		return errors.Trace(err)
	}
	manager, err := sch.getTaskKeyspaceManager()
	if err != nil {
		return err
	}
	delta := &statsstorage.DeltaUpdate{
		Delta: variable.TableDelta{
			Delta:    meta.Summary.ImportedRows,
			Count:    meta.Summary.ImportedRows,
			InitTime: time.Now(),
		},
		TableID: meta.Plan.TableInfo.ID,
	}
	backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
	return handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logger,
		func(ctx context.Context) (bool, error) {
			return true, manager.WithNewTxn(ctx, func(se sessionctx.Context) error {
				return updateImportedTableStats(ctx, se, delta, logger)
			})
		},
	)
}
