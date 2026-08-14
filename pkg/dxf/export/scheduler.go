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

package export

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type exportScheduler struct {
	*scheduler.BaseScheduler
	store    kv.Storage
	taskMeta *TaskMeta
	logger   *zap.Logger
}

var _ scheduler.Scheduler = (*exportScheduler)(nil)
var _ scheduler.Extension = (*exportScheduler)(nil)

// NewExportScheduler creates a scheduler for the export task.
func NewExportScheduler(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
	return &exportScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param),
		store:         param.TaskRuntime.Store(),
		logger: logutil.BgLogger().With(
			zap.Int64("task-id", task.ID), zap.String("task-type", string(proto.Export))),
	}
}

// Init implements scheduler.Scheduler.
func (s *exportScheduler) Init() error {
	taskMeta := &TaskMeta{}
	if err := json.Unmarshal(s.GetTask().Meta, taskMeta); err != nil {
		return errors.Annotate(err, "unmarshal export task meta failed")
	}
	s.taskMeta = taskMeta
	s.BaseScheduler.Extension = s
	return s.BaseScheduler.Init()
}

// OnTick implements scheduler.Extension.
func (*exportScheduler) OnTick(context.Context, *proto.Task) {}

// OnPrepare implements scheduler.Extension. In prepare mode it estimates the
// export set's data size after submit, seeds the per-physical sizes for the
// split, and (in nextgen) sizes the task's resources from the total.
func (s *exportScheduler) OnPrepare(ctx context.Context, _ storage.TaskHandle, task *proto.Task) error {
	sizes, total, err := estimateExportSize(ctx, s.store, s.taskMeta)
	if err != nil {
		return err
	}
	s.taskMeta.PhysicalSizes = sizes
	if kerneltype.IsNextGen() {
		if err := s.setResources(ctx, task, total); err != nil {
			return err
		}
	}
	meta, err := json.Marshal(s.taskMeta)
	if err != nil {
		return errors.Trace(err)
	}
	task.Meta = meta
	return nil
}

// setResources sizes the task's slots and node count from the total data size.
func (s *exportScheduler) setResources(ctx context.Context, task *proto.Task, totalSize int64) error {
	nodeCPU, err := scheduler.GetExecCPUNode(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	factors, err := handle.GetScheduleTuneFactors(ctx, s.store.GetKeyspace())
	if err != nil {
		return errors.Trace(err)
	}
	calc := scheduler.NewRCCalc(totalSize, nodeCPU, 0, factors)
	task.RequiredSlots = calc.CalcRequiredSlots()
	task.MaxNodeCount = calc.CalcMaxNodeCountForExport()
	return nil
}

// GetNextStep implements scheduler.Extension.
func (*exportScheduler) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit, proto.StepPrepared:
		return proto.ExportStepDump
	default:
		// PostProcess is inserted here by a later milestone.
		return proto.StepDone
	}
}

// OnNextSubtasksBatch implements scheduler.Extension.
func (s *exportScheduler) OnNextSubtasksBatch(
	ctx context.Context,
	_ storage.TaskHandle,
	task *proto.Task,
	_ []string,
	nextStep proto.Step,
) ([][]byte, error) {
	switch nextStep {
	case proto.ExportStepDump:
		// Size subtasks by the worker concurrency; the framework spreads the
		// resulting subtasks across the scaled-out nodes.
		metas, err := splitTables(ctx, s.store, s.taskMeta, max(task.RequiredSlots, 1))
		if err != nil {
			return nil, err
		}
		s.logger.Info("split export dump subtasks",
			zap.Int("table-cnt", len(s.taskMeta.Tables)), zap.Int("subtask-cnt", len(metas)))
		return metas, nil
	default:
		return nil, errors.Errorf("unexpected nextStep %s", proto.Step2Str(task.Type, nextStep))
	}
}

// OnDone implements scheduler.Extension.
func (s *exportScheduler) OnDone(_ context.Context, _ storage.TaskHandle, task *proto.Task) error {
	s.logger.Info("export task done", zap.Stringer("state", task.State), zap.Error(task.Error))
	return nil
}

// GetEligibleInstances implements scheduler.Extension.
func (*exportScheduler) GetEligibleInstances(context.Context, *proto.Task) ([]string, error) {
	return nil, nil
}

// IsRetryableErr implements scheduler.Extension.
func (*exportScheduler) IsRetryableErr(error) bool {
	return true
}

// ModifyMeta implements scheduler.Extension.
func (*exportScheduler) ModifyMeta(oldMeta []byte, _ []proto.Modification) ([]byte, error) {
	return oldMeta, nil
}

// TaskKey returns the DXF task key from the root id (the table id for EXPORT
// TABLE, the schema id for EXPORT SCHEMA) and the snapshot.
func TaskKey(rootID int64, snapshotTS uint64) string {
	return fmt.Sprintf("export/%d/%d", rootID, snapshotTS)
}
