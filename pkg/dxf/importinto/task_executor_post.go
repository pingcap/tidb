// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type postProcessStepExecutor struct {
	taskexecutor.BaseStepExecutor
	taskID       int64
	store        tidbkv.Storage
	taskTbl      taskexecutor.TaskTable
	taskMeta     *TaskMeta
	taskKeyspace string
	logger       *zap.Logger
}

var _ execute.StepExecutor = &postProcessStepExecutor{}

// NewPostProcessStepExecutor creates a new post process step executor.
// exported for testing.
func NewPostProcessStepExecutor(
	taskID int64,
	store tidbkv.Storage,
	taskTbl taskexecutor.TaskTable,
	taskMeta *TaskMeta,
	taskKeyspace string,
	logger *zap.Logger,
) execute.StepExecutor {
	return &postProcessStepExecutor{
		taskID:       taskID,
		store:        store,
		taskTbl:      taskTbl,
		taskMeta:     taskMeta,
		taskKeyspace: taskKeyspace,
		logger:       logger,
	}
}

func (p *postProcessStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := p.logger.With(zap.Int64("subtask-id", subtask.ID))
	logTask := log.BeginTask(logger, "run subtask")
	defer func() {
		logTask.End(zapcore.ErrorLevel, err)
	}()
	stepMeta := PostProcessStepMeta{}
	if err = json.Unmarshal(subtask.Meta, &stepMeta); err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("waitBeforePostProcess", func() {
		time.Sleep(5 * time.Second)
	})
	return p.postProcess(ctx, &stepMeta, logger)
}

type importExecutor struct {
	*taskexecutor.BaseTaskExecutor
	store        tidbkv.Storage
	indicesGenKV map[int64]importer.GenKVIndex
}

// NewImportExecutor creates a new import task executor.
func NewImportExecutor(
	ctx context.Context,
	task *proto.Task,
	param taskexecutor.Param,
) taskexecutor.TaskExecutor {
	metrics := metricsManager.getOrCreateMetrics(task.ID)
	subCtx := metric.WithCommonMetric(ctx, metrics)

	s := &importExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(subCtx, task, param),
		store:            param.Store,
	}
	s.BaseTaskExecutor.Extension = s
	return s
}

func (*importExecutor) IsIdempotent(*proto.Subtask) bool {
	// for local sort, there is no conflict detection and resolution , so it's
	// ok to import data twice.
	// for global-sort, subtasks are safe to retry because duplicate KVs are
	// recorded during encode/merge/ingest steps and conflicted rows are
	// resolved in later steps.
	return true
}

func (*importExecutor) IsRetryableError(err error) bool {
	return common.IsRetryableError(err)
}

func (e *importExecutor) GetStepExecutor(task *proto.Task) (execute.StepExecutor, error) {
	taskMeta := TaskMeta{}
	if err := json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return nil, errors.Trace(err)
	}
	logger := logutil.BgLogger().With(
		zap.Int64("task-id", task.ID),
		zap.String("task-key", task.Key),
		zap.String("step", proto.Step2Str(task.Type, task.Step)),
	)
	indicesGenKV := importer.GetIndicesGenKV(taskMeta.Plan.TableInfo)
	logger.Info("got indices that generate kv", zap.Any("indices", indicesGenKV))

	store := e.store
	if e.store.GetKeyspace() != task.Keyspace {
		var err error
		err = e.GetTaskTable().WithNewSession(func(se sessionctx.Context) error {
			store, err = se.GetSQLServer().GetKSStore(task.Keyspace)
			return err
		})
		if err != nil {
			return nil, err
		}
	}

	switch task.Step {
	case proto.ImportStepImport, proto.ImportStepEncodeAndSort:
		return &importStepExecutor{
			taskID:       task.ID,
			taskMeta:     &taskMeta,
			logger:       logger,
			store:        store,
			indicesGenKV: indicesGenKV,
		}, nil
	case proto.ImportStepMergeSort:
		return &mergeSortStepExecutor{
			task:         &task.TaskBase,
			taskMeta:     &taskMeta,
			logger:       logger,
			store:        store,
			indicesGenKV: indicesGenKV,
		}, nil
	case proto.ImportStepWriteAndIngest:
		return &writeAndIngestStepExecutor{
			taskID:       task.ID,
			taskMeta:     &taskMeta,
			logger:       logger,
			store:        store,
			indicesGenKV: indicesGenKV,
		}, nil
	case proto.ImportStepCollectConflicts:
		return NewCollectConflictsStepExecutor(&task.TaskBase, store, &taskMeta, logger), nil
	case proto.ImportStepConflictResolution:
		return NewConflictResolutionStepExecutor(&task.TaskBase, store, &taskMeta, logger), nil
	case proto.ImportStepPostProcess:
		return NewPostProcessStepExecutor(task.ID, store, e.GetTaskTable(), &taskMeta, task.Keyspace, logger), nil
	default:
		return nil, errors.Errorf("unknown step %d for import task %d", task.Step, task.ID)
	}
}

func (e *importExecutor) Close() {
	task := e.GetTaskBase()
	metricsManager.unregister(task.ID)
	e.BaseTaskExecutor.Close()
}
