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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/gc"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// gcTTL is the TTL of the export service safepoint in seconds.
	gcTTL = 5 * 60
	// maxRegionsPerSubtask caps the span size when auto-splitting.
	maxRegionsPerSubtask = 4000
)

func gcSafePointID(taskKey string) string {
	return "export-" + taskKey
}

type exportScheduler struct {
	*scheduler.BaseScheduler
	ctx      context.Context
	store    kv.Storage
	taskMeta *TaskMeta
	logger   *zap.Logger

	gcCancel context.CancelFunc
}

var _ scheduler.Scheduler = (*exportScheduler)(nil)

// NewExportScheduler creates a scheduler for the export task.
func NewExportScheduler(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
	return &exportScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param),
		ctx:           ctx,
		store:         param.TaskStore,
		logger:        logutil.BgLogger().With(zap.Int64("task-id", task.ID), zap.String("task-type", string(proto.Export))),
	}
}

// Init implements scheduler.Scheduler.
func (s *exportScheduler) Init() error {
	taskMeta := &TaskMeta{}
	if err := json.Unmarshal(s.GetTask().Meta, taskMeta); err != nil {
		return errors.Annotate(err, "unmarshal export task meta failed")
	}
	s.taskMeta = taskMeta
	if err := s.startGCKeeper(); err != nil {
		s.logger.Warn("start gc safepoint keeper failed, snapshot may be GCed during a long export",
			zap.Error(err))
	}
	s.BaseScheduler.Extension = s
	return s.BaseScheduler.Init()
}

// startGCKeeper keeps a service safepoint at the snapshot TS alive until the
// task is done, so a long export does not fail on GC.
func (s *exportScheduler) startGCKeeper() error {
	pdStore, ok := s.store.(kv.StorageWithPD)
	if !ok {
		s.logger.Warn("storage does not support PD, skip GC safepoint keeper")
		return nil
	}
	mgr := gc.NewManager(pdStore.GetPDClient(), s.store.GetCodec().GetKeyspaceID())
	gcCtx, cancel := context.WithCancel(s.ctx)
	s.gcCancel = cancel
	return gc.StartServiceSafePointKeeper(gcCtx, gc.BRServiceSafePoint{
		ID:       gcSafePointID(s.GetTask().Key),
		TTL:      gcTTL,
		BackupTS: s.taskMeta.SnapshotTS,
	}, mgr)
}

// OnTick implements scheduler.Extension.
func (*exportScheduler) OnTick(context.Context, *proto.Task) {}

// GetNextStep implements scheduler.Extension.
func (*exportScheduler) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.ExportStepDump
	default:
		return proto.StepDone
	}
}

// OnNextSubtasksBatch implements scheduler.Extension.
func (s *exportScheduler) OnNextSubtasksBatch(
	ctx context.Context,
	_ storage.TaskHandle,
	task *proto.Task,
	execIDs []string,
	nextStep proto.Step,
) ([][]byte, error) {
	switch nextStep {
	case proto.ExportStepDump:
		return s.splitDumpSubtasks(ctx, execIDs)
	default:
		return nil, errors.Errorf("unexpected nextStep %s", proto.Step2Str(task.Type, nextStep))
	}
}

func (s *exportScheduler) splitDumpSubtasks(ctx context.Context, execIDs []string) ([][]byte, error) {
	tblInfo := s.taskMeta.TableInfo
	physicalIDs := make([]int64, 0, 1)
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, def := range pi.Definitions {
			physicalIDs = append(physicalIDs, def.ID)
		}
	} else {
		physicalIDs = append(physicalIDs, tblInfo.ID)
	}

	metas := make([][]byte, 0, len(physicalIDs))
	for _, pid := range physicalIDs {
		start, end := physicalTableRange(tblInfo, pid)
		boundaries, err := loadRegionBoundaries(ctx, s.store, start, end)
		if err != nil {
			return nil, err
		}
		regionCnt := len(boundaries) - 1
		groupCnt := s.subtaskCntFor(regionCnt, len(execIDs))
		groups := groupBoundaries(boundaries, groupCnt)
		// RequiredSlots is validated >= 1 at submit and Export has no
		// concurrency-modification entry point.
		writerCnt := s.GetTask().RequiredSlots * s.taskMeta.effectiveWritersPerEncoder()
		for _, g := range groups {
			writerGroups := groupBoundaries(g, writerCnt)
			bounds := make([][]byte, 0, len(writerGroups)+1)
			bounds = append(bounds, writerGroups[0][0])
			for _, wg := range writerGroups {
				bounds = append(bounds, wg[len(wg)-1])
			}
			meta, err := json.Marshal(&SubtaskMeta{
				PhysicalID:   pid,
				WriterBounds: bounds,
			})
			if err != nil {
				return nil, errors.Trace(err)
			}
			metas = append(metas, meta)
		}
		s.logger.Info("split export dump subtasks",
			zap.Int64("physical-id", pid),
			zap.Int("region-cnt", regionCnt),
			zap.Int("subtask-cnt", len(groups)))
	}
	return metas, nil
}

// physicalTableRange returns the record-key range of one physical table.
// For int-handle tables the start must be a well-formed record key
// (TiKV returns nothing for a bare "t<id>_r" prefix start), so it mirrors
// the table reader's FullIntRange.
func physicalTableRange(tblInfo *model.TableInfo, pid int64) (start, end kv.Key) {
	prefix := tablecodec.GenTableRecordPrefix(pid)
	if tblInfo.IsCommonHandle {
		return prefix, prefix.PrefixNext()
	}
	return tablecodec.EncodeRowKeyWithHandle(pid, kv.IntHandle(math.MinInt64)), prefix.PrefixNext()
}

// subtaskCntFor decides how many spans to emit for one physical table. The
// region batch follows add-index's CalculateRegionBatch (cloud branch):
// batch = min(maxRegionsPerSubtask, ceil(regionCnt/nodeCnt)).
func (s *exportScheduler) subtaskCntFor(regionCnt, nodeCnt int) int {
	batch := s.taskMeta.SubtaskRegions
	if batch <= 0 {
		if nodeCnt <= 0 {
			nodeCnt = 1
		}
		avgTasksPerNode := (regionCnt + nodeCnt - 1) / nodeCnt
		batch = min(maxRegionsPerSubtask, avgTasksPerNode)
	}
	batch = max(batch, 1)
	return max(1, (regionCnt+batch-1)/batch)
}

// OnDone implements scheduler.Extension.
func (s *exportScheduler) OnDone(ctx context.Context, _ storage.TaskHandle, task *proto.Task) error {
	if s.gcCancel != nil {
		s.gcCancel()
	}
	if pdStore, ok := s.store.(kv.StorageWithPD); ok {
		mgr := gc.NewManager(pdStore.GetPDClient(), s.store.GetCodec().GetKeyspaceID())
		if err := mgr.DeleteServiceSafePoint(ctx, gc.BRServiceSafePoint{
			ID:  gcSafePointID(task.Key),
			TTL: gcTTL,
		}); err != nil {
			s.logger.Warn("delete export gc safepoint failed, it will expire by TTL", zap.Error(err))
		}
	}
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

// TaskKey returns the task key of an export task on the given table at the
// given snapshot.
func TaskKey(tableID int64, snapshotTS uint64) string {
	return fmt.Sprintf("export/%d/%d", tableID, snapshotTS)
}
