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

package loaddata

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ImportScheduler is a scheduler for load data.
type ImportScheduler struct {
	taskMeta      *TaskMeta
	indexEngine   *backend.OpenedEngine
	tableImporter *importer.TableImporter
	sharedVars    sync.Map
}

// InitSubtaskExecEnv implements the Scheduler.InitSubtaskExecEnv interface.
func (s *ImportScheduler) InitSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("InitSubtaskExecEnv", zap.Any("taskMeta", s.taskMeta))

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
	if err := controller.InitDataFiles(ctx); err != nil {
		return err
	}

	tableImporter, err := importer.NewTableImporter(&importer.JobImportParam{
		GroupCtx: ctx,
		Progress: asyncloaddata.NewProgress(controller.ImportMode == importer.LogicalImportMode),
		Job: &asyncloaddata.Job{
			ID: s.taskMeta.JobID,
		},
	}, controller)
	if err != nil {
		return err
	}
	s.tableImporter = tableImporter

	indexEngine, err := s.tableImporter.OpenIndexEngine(ctx)
	if err != nil {
		return err
	}
	s.indexEngine = indexEngine
	return nil
}

// SplitSubtask implements the Scheduler.SplitSubtask interface.
func (s *ImportScheduler) SplitSubtask(ctx context.Context, bs []byte) ([]proto.MinimalTask, error) {
	logutil.BgLogger().Info("SplitSubtask", zap.Any("taskMeta", s.taskMeta))
	var subtaskMeta SubtaskMeta
	err := json.Unmarshal(bs, &subtaskMeta)
	if err != nil {
		return nil, err
	}

	dataEngine, err := s.tableImporter.OpenDataEngine(ctx, subtaskMeta.ID)
	if err != nil {
		return nil, err
	}
	sharedVars := &SharedVars{
		TableImporter: s.tableImporter,
		DataEngine:    dataEngine,
		IndexEngine:   s.indexEngine,
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

// OnSubtaskFinished implements the Scheduler.OnSubtaskFinished interface.
func (s *ImportScheduler) OnSubtaskFinished(ctx context.Context, subtaskMetaBytes []byte) ([]byte, error) {
	logutil.BgLogger().Info("OnSubtaskFinished", zap.Any("taskMeta", s.taskMeta))
	var subtaskMeta SubtaskMeta
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

	closedEngine, err := sharedVars.DataEngine.Close(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.tableImporter.ImportAndCleanup(ctx, closedEngine); err != nil {
		return nil, err
	}

	subtaskMeta.Checksum.Sum = sharedVars.Checksum.Sum()
	subtaskMeta.Checksum.KVs = sharedVars.Checksum.SumKVS()
	subtaskMeta.Checksum.Size = sharedVars.Checksum.SumSize()
	return json.Marshal(subtaskMeta)
}

// CleanupSubtaskExecEnv implements the Scheduler.CleanupSubtaskExecEnv interface.
func (s *ImportScheduler) CleanupSubtaskExecEnv(ctx context.Context) (err error) {
	defer func() {
		err2 := s.tableImporter.Close()
		if err == nil {
			err = err2
		}
	}()

	logutil.BgLogger().Info("CleanupSubtaskExecEnv", zap.Any("taskMeta", s.taskMeta))
	// FIXME: if cleanup failed, framework still regard it as success.
	closedIndexEngine, err := s.indexEngine.Close(ctx)
	if err != nil {
		return err
	}
	return s.tableImporter.ImportAndCleanup(ctx, closedIndexEngine)
}

// Rollback implements the Scheduler.Rollback interface.
// TODO: add rollback
func (s *ImportScheduler) Rollback(context.Context) error {
	logutil.BgLogger().Info("rollback", zap.Any("taskMeta", s.taskMeta))
	return nil
}

func init() {
	scheduler.RegisterSchedulerConstructor(
		proto.LoadData,
		func(bs []byte, step int64) (scheduler.Scheduler, error) {
			taskMeta := TaskMeta{}
			if err := json.Unmarshal(bs, &taskMeta); err != nil {
				return nil, err
			}
			logutil.BgLogger().Info("register scheduler constructor", zap.Any("taskMeta", taskMeta))
			return &ImportScheduler{taskMeta: &taskMeta}, nil
		},
		scheduler.WithConcurrentSubtask(),
	)
}
