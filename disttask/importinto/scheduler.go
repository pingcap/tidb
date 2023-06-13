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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// importStepScheduler is a scheduler for import step.
// Scheduler is equivalent to a Lightning instance.
type importStepScheduler struct {
	taskID        int64
	taskMeta      *TaskMeta
	tableImporter *importer.TableImporter
	sharedVars    sync.Map
	logger        *zap.Logger

	importCtx    context.Context
	importCancel context.CancelFunc
	wg           sync.WaitGroup
}

func (s *importStepScheduler) InitSubtaskExecEnv(ctx context.Context) error {
	s.logger.Info("InitSubtaskExecEnv", zap.Any("taskMeta", s.taskMeta))

	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, s.taskMeta.Plan.TableInfo)
	if err != nil {
		return err
	}
	astArgs, err := importer.ASTArgsFromStmt(s.taskMeta.Stmt)
	if err != nil {
		return err
	}
	controller, err := importer.NewLoadDataController(&s.taskMeta.Plan, tbl, astArgs)
	if err != nil {
		return err
	}
	// todo: this method will load all files, but we only import files related to current subtask.
	if err := controller.InitDataFiles(ctx); err != nil {
		return err
	}

	tableImporter, err := importer.NewTableImporter(&importer.JobImportParam{
		GroupCtx: ctx,
		Progress: asyncloaddata.NewProgress(false),
		Job:      &asyncloaddata.Job{},
	}, controller, s.taskID)
	if err != nil {
		return err
	}
	s.tableImporter = tableImporter

	s.importCtx, s.importCancel = context.WithCancel(context.Background())
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.tableImporter.CheckDiskQuota(s.importCtx)
	}()
	return nil
}

func (s *importStepScheduler) SplitSubtask(ctx context.Context, bs []byte) ([]proto.MinimalTask, error) {
	s.logger.Info("SplitSubtask", zap.Any("taskMeta", s.taskMeta))
	var subtaskMeta ImportStepMeta
	err := json.Unmarshal(bs, &subtaskMeta)
	if err != nil {
		return nil, err
	}

	dataEngine, err := s.tableImporter.OpenDataEngine(ctx, subtaskMeta.ID)
	if err != nil {
		return nil, err
	}
	// Unlike in Lightning, we start an index engine for each subtask, whereas previously there was only a single index engine globally.
	// This is because the scheduler currently does not have a post-processing mechanism.
	// If we import the index in `cleanupSubtaskEnv`, the dispatcher will not wait for the import to complete.
	// Multiple index engines may suffer performance degradation due to range overlap.
	// These issues will be alleviated after we integrate s3 sorter.
	// engineID = -1, -2, -3, ...
	indexEngine, err := s.tableImporter.OpenIndexEngine(ctx, common.IndexEngineID-subtaskMeta.ID)
	if err != nil {
		return nil, err
	}
	sharedVars := &SharedVars{
		TableImporter: s.tableImporter,
		DataEngine:    dataEngine,
		IndexEngine:   indexEngine,
		Progress:      asyncloaddata.NewProgress(false),
		Checksum:      &verification.KVChecksum{},
	}
	s.sharedVars.Store(subtaskMeta.ID, sharedVars)

	miniTask := make([]proto.MinimalTask, 0, len(subtaskMeta.Chunks))
	for _, chunk := range subtaskMeta.Chunks {
		miniTask = append(miniTask, MinimalTaskMeta{
			Plan:       s.taskMeta.Plan,
			Chunk:      chunk,
			SharedVars: sharedVars,
		})
	}
	return miniTask, nil
}

func (s *importStepScheduler) OnSubtaskFinished(ctx context.Context, subtaskMetaBytes []byte) ([]byte, error) {
	s.logger.Info("OnSubtaskFinished", zap.Any("taskMeta", s.taskMeta))
	var subtaskMeta ImportStepMeta
	if err := json.Unmarshal(subtaskMetaBytes, &subtaskMeta); err != nil {
		return nil, err
	}

	val, ok := s.sharedVars.Load(subtaskMeta.ID)
	if !ok {
		return nil, errors.Errorf("sharedVars %d not found", subtaskMeta.ID)
	}
	sharedVars, ok := val.(*SharedVars)
	if !ok {
		return nil, errors.Errorf("sharedVars %d not found", subtaskMeta.ID)
	}

	// TODO: we should close and cleanup engine in all case, since there's no checkpoint.
	s.logger.Info("import data engine", zap.Any("id", subtaskMeta.ID))
	closedDataEngine, err := sharedVars.DataEngine.Close(ctx)
	if err != nil {
		return nil, err
	}
	dataKVCount, err := s.tableImporter.ImportAndCleanup(ctx, closedDataEngine)
	if err != nil {
		return nil, err
	}

	logutil.BgLogger().Info("import index engine", zap.Any("id", subtaskMeta.ID))
	if closedEngine, err := sharedVars.IndexEngine.Close(ctx); err != nil {
		return nil, err
	} else if _, err := s.tableImporter.ImportAndCleanup(ctx, closedEngine); err != nil {
		return nil, err
	}

	sharedVars.mu.Lock()
	defer sharedVars.mu.Unlock()
	subtaskMeta.Checksum.Sum = sharedVars.Checksum.Sum()
	subtaskMeta.Checksum.KVs = sharedVars.Checksum.SumKVS()
	subtaskMeta.Checksum.Size = sharedVars.Checksum.SumSize()
	subtaskMeta.Result = Result{
		ReadRowCnt:   sharedVars.Progress.ReadRowCnt.Load(),
		LoadedRowCnt: uint64(dataKVCount),
	}
	s.sharedVars.Delete(subtaskMeta.ID)
	return json.Marshal(subtaskMeta)
}

func (s *importStepScheduler) CleanupSubtaskExecEnv(_ context.Context) (err error) {
	s.logger.Info("CleanupSubtaskExecEnv", zap.Any("taskMeta", s.taskMeta))
	s.importCancel()
	s.wg.Wait()
	return s.tableImporter.Close()
}

func (s *importStepScheduler) Rollback(context.Context) error {
	// TODO: add rollback
	s.logger.Info("rollback", zap.Any("taskMeta", s.taskMeta))
	return nil
}

type postStepScheduler struct {
	scheduler.EmptyScheduler
	taskID   int64
	taskMeta *TaskMeta
	logger   *zap.Logger
}

var _ scheduler.Scheduler = &postStepScheduler{}

func (p *postStepScheduler) OnSubtaskFinished(ctx context.Context, metaBytes []byte) ([]byte, error) {
	subtaskMeta := &PostProcessStepMeta{}
	if err := json.Unmarshal(metaBytes, subtaskMeta); err != nil {
		return nil, err
	}
	if err := postProcess(ctx, p.taskMeta, subtaskMeta, p.logger); err != nil {
		return nil, err
	}
	return metaBytes, nil
}

func init() {
	prepareFn := func(taskID int64, bs []byte, step int64) (*TaskMeta, *zap.Logger, error) {
		taskMeta := TaskMeta{}
		if err := json.Unmarshal(bs, &taskMeta); err != nil {
			return nil, nil, err
		}
		logger := logutil.BgLogger().With(
			zap.String("component", "scheduler"),
			zap.String("type", proto.ImportInto),
			zap.Int64("task-id", taskID),
			zap.String("step", stepStr(step)),
		)
		logger.Info("create step scheduler", zap.Any("taskMeta", taskMeta))
		return &taskMeta, logger, nil
	}
	scheduler.RegisterTaskType(proto.ImportInto)
	scheduler.RegisterSchedulerConstructor(proto.ImportInto, StepImport,
		func(taskID int64, bs []byte, step int64) (scheduler.Scheduler, error) {
			taskMeta, logger, err := prepareFn(taskID, bs, step)
			if err != nil {
				return nil, err
			}
			return &importStepScheduler{
				taskID:   taskID,
				taskMeta: taskMeta,
				logger:   logger,
			}, nil
		},
		scheduler.WithConcurrentSubtask(),
	)
	scheduler.RegisterSchedulerConstructor(proto.ImportInto, StepPostProcess,
		func(taskID int64, bs []byte, step int64) (scheduler.Scheduler, error) {
			taskMeta, logger, err := prepareFn(taskID, bs, step)
			if err != nil {
				return nil, err
			}
			return &postStepScheduler{
				taskID:   taskID,
				taskMeta: taskMeta,
				logger:   logger,
			}, nil
		},
	)
}
