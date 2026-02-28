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

package globalindexcleanup

import (
	"bytes"
	"context"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// Scheduler implements scheduler.Extension for global index cleanup.
type Scheduler struct {
	*scheduler.BaseScheduler
	store kv.Storage
}

var _ scheduler.Extension = (*Scheduler)(nil)

// NewScheduler creates a new Scheduler.
func NewScheduler(ctx context.Context, store kv.Storage, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
	sch := &Scheduler{
		BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param),
		store:         store,
	}
	sch.BaseScheduler.Extension = sch
	return sch
}

// Init implements scheduler.Extension.
func (s *Scheduler) Init() error {
	return s.BaseScheduler.Init()
}

// OnTick implements scheduler.Extension.
func (*Scheduler) OnTick(_ context.Context, _ *proto.Task) {}

// OnNextSubtasksBatch implements scheduler.Extension.
func (s *Scheduler) OnNextSubtasksBatch(
	ctx context.Context,
	_ storage.TaskHandle,
	task *proto.Task,
	execIDs []string,
	nextStep proto.Step,
) ([][]byte, error) {
	logger := logutil.DDLLogger().With(
		zap.Stringer("type", task.Type),
		zap.Int64("task-id", task.ID),
		zap.String("next-step", proto.Step2Str(task.Type, nextStep)),
	)

	switch nextStep {
	case proto.GlobalIndexCleanupStepScanAndDelete:
		return s.generateScanAndDeleteSubtasks(ctx, task, len(execIDs), logger)
	default:
		return nil, nil
	}
}

func (s *Scheduler) generateScanAndDeleteSubtasks(
	ctx context.Context,
	task *proto.Task,
	nodeCnt int,
	logger *zap.Logger,
) ([][]byte, error) {
	var taskMeta CleanupTaskMeta
	if err := taskMeta.Unmarshal(task.Meta); err != nil {
		return nil, errors.Trace(err)
	}

	if len(taskMeta.OldPartitionIDs) == 0 {
		logger.Info("no old partitions to clean up")
		return nil, nil
	}

	logger.Info("generating subtasks for global index cleanup",
		zap.Int64s("old-partition-ids", taskMeta.OldPartitionIDs),
		zap.Int64s("global-index-ids", taskMeta.GlobalIndexIDs),
	)

	allSubtaskMetas := make([][]byte, 0, len(taskMeta.OldPartitionIDs))
	for _, pid := range taskMeta.OldPartitionIDs {
		metas, err := s.generateSubtasksForPartition(ctx, pid, nodeCnt, logger)
		if err != nil {
			return nil, errors.Trace(err)
		}
		allSubtaskMetas = append(allSubtaskMetas, metas...)
	}

	logger.Info("generated subtasks", zap.Int("count", len(allSubtaskMetas)))
	return allSubtaskMetas, nil
}

func (s *Scheduler) generateSubtasksForPartition(
	ctx context.Context,
	partitionID int64,
	nodeCnt int,
	logger *zap.Logger,
) ([][]byte, error) {
	startKey := tablecodec.EncodeTablePrefix(partitionID)
	endKey := tablecodec.EncodeTablePrefix(partitionID + 1)

	var subtaskMetas [][]byte
	backoffer := backoff.NewExponential(50, 2, 3000)
	err := handle.RunWithRetry(ctx, 8, backoffer, logger, func(_ context.Context) (bool, error) {
		regionCache := s.store.(helper.Storage).GetRegionCache()
		regionMetas, err := regionCache.LoadRegionsInKeyRange(
			tikv.NewBackofferWithVars(context.Background(), 20000, nil),
			startKey, endKey,
		)
		if err != nil {
			return false, err
		}

		if len(regionMetas) == 0 {
			// Empty partition.
			return false, nil
		}

		sort.Slice(regionMetas, func(i, j int) bool {
			return bytes.Compare(regionMetas[i].StartKey(), regionMetas[j].StartKey()) < 0
		})

		// Check if regions are continuous.
		shouldRetry := false
		cur := regionMetas[0]
		for _, m := range regionMetas[1:] {
			if !bytes.Equal(cur.EndKey(), m.StartKey()) {
				shouldRetry = true
				break
			}
			cur = m
		}
		if shouldRetry {
			return true, nil
		}

		regionBatch := calculateRegionBatch(len(regionMetas), nodeCnt)
		logger.Info("calculate region batch for partition",
			zap.Int64("partition-id", partitionID),
			zap.Int("total-regions", len(regionMetas)),
			zap.Int("region-batch", regionBatch),
		)

		for i := 0; i < len(regionMetas); i += regionBatch {
			end := min(i+regionBatch, len(regionMetas))
			batch := regionMetas[i:end]

			subtaskMeta := &CleanupSubtaskMeta{
				PhysicalTableID: partitionID,
				RowStart:        batch[0].StartKey(),
				RowEnd:          batch[len(batch)-1].EndKey(),
			}
			if i == 0 {
				subtaskMeta.RowStart = startKey
			}
			if end == len(regionMetas) {
				subtaskMeta.RowEnd = endKey
			}

			metaBytes, err := subtaskMeta.Marshal()
			if err != nil {
				return false, err
			}
			subtaskMetas = append(subtaskMetas, metaBytes)
		}
		return false, nil
	})

	if err != nil {
		return nil, errors.Trace(err)
	}
	return subtaskMetas, nil
}

func calculateRegionBatch(totalRegionCnt int, nodeCnt int) int {
	if nodeCnt <= 0 {
		nodeCnt = 1
	}
	avgTasksPerInstance := (totalRegionCnt + nodeCnt - 1) / nodeCnt
	// Each subtask should contain no more than 1000 regions for cleanup.
	return min(1000, max(1, avgTasksPerInstance))
}

// GetNextStep implements scheduler.Extension.
func (*Scheduler) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.GlobalIndexCleanupStepScanAndDelete
	case proto.GlobalIndexCleanupStepScanAndDelete:
		return proto.StepDone
	default:
		return proto.StepDone
	}
}

// GetEligibleInstances implements scheduler.Extension.
func (*Scheduler) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]string, error) {
	return nil, nil
}

// IsRetryableErr implements scheduler.Extension.
func (*Scheduler) IsRetryableErr(error) bool {
	return true
}

// OnDone implements scheduler.Extension.
func (*Scheduler) OnDone(_ context.Context, _ storage.TaskHandle, _ *proto.Task) error {
	return nil
}

// Close implements scheduler.Extension.
func (s *Scheduler) Close() {
	s.BaseScheduler.Close()
}
