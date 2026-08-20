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
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/extstore"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// OnPrepare implements scheduler.Extension.
func (s *exportScheduler) OnPrepare(ctx context.Context, _ storage.TaskHandle, task *proto.Task) error {
	tableInfos, err := s.snapshotTableInfos()
	if err != nil {
		return err
	}
	chunks, total, err := generateChunks(ctx, s.store, tableInfos, s.taskMeta)
	if err != nil {
		return err
	}
	if kerneltype.IsNextGen() {
		if err := s.setResources(ctx, task, total); err != nil {
			return err
		}
	}
	preparedPlanPath, err := writePreparedPlan(ctx, task.ID, chunks)
	if err != nil {
		return err
	}
	s.taskMeta.PreparedPlanPath = preparedPlanPath
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

func (s *exportScheduler) snapshotTableInfos() (map[int64]*model.TableInfo, error) {
	reader := meta.NewReader(s.store.GetSnapshot(kv.NewVersion(s.taskMeta.SnapshotTS)))
	tableInfos := make(map[int64]*model.TableInfo, s.taskMeta.tableCount())
	for i := range s.taskMeta.DBs {
		db := &s.taskMeta.DBs[i]
		dbInfo, err := reader.GetDatabase(db.DBID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if dbInfo == nil {
			return nil, errors.Errorf("export: database %d not found in snapshot metadata", db.DBID)
		}
		if dbInfo.State != model.StatePublic {
			return nil, errors.Errorf("export: database %d is not public", db.DBID)
		}
		db.DBName = dbInfo.Name.O
		for _, tableID := range db.TableIDs {
			tableInfo, err := reader.GetTable(db.DBID, tableID)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if tableInfo == nil {
				return nil, errors.Errorf("export: table %d not found in snapshot metadata", tableID)
			}
			if tableInfo.State != model.StatePublic {
				return nil, errors.Errorf("export: table %d is not public", tableID)
			}
			tableInfos[tableID] = tableInfo
		}
	}
	return tableInfos, nil
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
		chunks, err := readPreparedPlan(ctx, s.taskMeta.PreparedPlanPath)
		if err != nil {
			return nil, err
		}
		groups := divideSubtasks(chunks, max(task.MaxNodeCount, 1))
		metas, err := marshalSubtasks(ctx, task.ID, nextStep, groups)
		if err != nil {
			return nil, err
		}
		s.logger.Info("split export dump subtasks",
			zap.Int("table-cnt", s.taskMeta.tableCount()), zap.Int("subtask-cnt", len(metas)))
		return metas, nil
	default:
		return nil, errors.Errorf("unexpected nextStep %s", proto.Step2Str(task.Type, nextStep))
	}
}

func writePreparedPlan(ctx context.Context, taskID int64, chunks []Chunk) (string, error) {
	store, err := extstore.GetGlobalExtStorage(ctx)
	if err != nil {
		return "", errors.Trace(err)
	}
	plan := &SubtaskMeta{Chunks: chunks}
	plan.ExternalPath = dxfutil.PreparedMetaPath(taskID)
	if err := plan.WriteJSONToExternalStorage(ctx, store, plan); err != nil {
		return "", errors.Trace(err)
	}
	return plan.ExternalPath, nil
}

func readPreparedPlan(ctx context.Context, planPath string) ([]Chunk, error) {
	if planPath == "" {
		return nil, errors.New("export: prepared plan path is empty")
	}
	store, err := extstore.GetGlobalExtStorage(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	plan := &SubtaskMeta{}
	plan.ExternalPath = planPath
	if err := plan.ReadJSONFromExternalStorage(ctx, store, plan); err != nil {
		return nil, errors.Trace(err)
	}
	return plan.Chunks, nil
}

// marshalSubtasks serializes each chunk group into a subtask meta, offloading the
// chunk list to external storage so the row stored by the framework stays small.
func marshalSubtasks(ctx context.Context, taskID int64, step proto.Step, groups [][]Chunk) ([][]byte, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	store, err := extstore.GetGlobalExtStorage(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	stepStr := proto.Step2Str(proto.Export, step)
	metas := make([][]byte, 0, len(groups))
	for i, g := range groups {
		sm := &SubtaskMeta{Chunks: g}
		sm.ExternalPath = dxfutil.PlanMetaPath(taskID, stepStr, i+1)
		if err := sm.WriteJSONToExternalStorage(ctx, store, sm); err != nil {
			return nil, errors.Trace(err)
		}
		bs, err := sm.Marshal(sm)
		if err != nil {
			return nil, errors.Trace(err)
		}
		metas = append(metas, bs)
	}
	return metas, nil
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
func (*exportScheduler) IsRetryableErr(err error) bool {
	return isRetryablePlanningError(err)
}

func isRetryablePlanningError(err error) bool {
	if err == nil {
		return false
	}
	if rpcStatus, ok := status.FromError(errors.Cause(err)); ok {
		switch rpcStatus.Code() {
		case codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Unavailable, codes.DataLoss:
			return true
		default:
			return false
		}
	}
	return common.IsRetryableError(err)
}

// ModifyMeta implements scheduler.Extension.
func (*exportScheduler) ModifyMeta(oldMeta []byte, _ []proto.Modification) ([]byte, error) {
	return oldMeta, nil
}

// TaskKey returns the DXF task key from the root id (the table id) and the
// snapshot.
func TaskKey(rootID int64, snapshotTS uint64) string {
	return TaskKeyInKeyspace(keyspace.GetKeyspaceNameBySettings(), rootID, snapshotTS)
}

// TaskKeyInKeyspace returns the task key scoped to keyspaceName in NextGen.
// Classic ignores keyspaceName because its task keys are not keyspace-scoped.
func TaskKeyInKeyspace(keyspaceName string, rootID int64, snapshotTS uint64) string {
	if kerneltype.IsNextGen() {
		return taskKeyInKeyspace(keyspaceName, rootID, snapshotTS)
	}
	return fmt.Sprintf("export/%d/%d", rootID, snapshotTS)
}

func taskKeyInKeyspace(keyspaceName string, rootID int64, snapshotTS uint64) string {
	return fmt.Sprintf("%s/export/%d/%d", keyspaceName, rootID, snapshotTS)
}
