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

package importinto

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type conflictResolutionStepExecutor struct {
	taskexecutor.EmptyStepExecutor
	taskID   int64
	store    tidbkv.Storage
	taskMeta *TaskMeta
	logger   *zap.Logger

	tableImporter *importer.TableImporter
}

var _ execute.StepExecutor = &conflictResolutionStepExecutor{}

func (e *conflictResolutionStepExecutor) Init(ctx context.Context) error {
	tableImporter, err := getTableImporter(ctx, e.taskID, e.taskMeta, e.store)
	if err != nil {
		return err
	}
	e.tableImporter = tableImporter
	return nil
}

func (e *conflictResolutionStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := e.logger.With(zap.Int64("subtask-id", subtask.ID))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()
	stepMeta := &ConflictResolutionStepMeta{}
	if err = json.Unmarshal(subtask.Meta, stepMeta); err != nil {
		return errors.Trace(err)
	}
	if stepMeta.ExternalPath != "" {
		if err := stepMeta.ReadJSONFromExternalStorage(ctx, e.tableImporter.GlobalSortStore, stepMeta); err != nil {
			return errors.Trace(err)
		}
	}
	for kvGroup, ci := range stepMeta.Infos.ConflictInfos {
		err = handleKVGroupConflicts(ctx, e.logger, subtask.Concurrency, e.getHandler, e.tableImporter.GlobalSortStore, kvGroup, ci, nil)
		failpoint.InjectCall("afterResolveOneKVGroup", &err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *conflictResolutionStepExecutor) getHandler(kvGroup string) conflictKVHandler {
	baseHandler := &baseConflictKVHandler{
		tableImporter: e.tableImporter,
		store:         e.store,
		logger:        e.logger,
		kvGroup:       kvGroup,
	}
	var handler conflictKVHandler = &conflictDataKVHandler{baseConflictKVHandler: baseHandler}
	if kvGroup != dataKVGroup {
		handler = &conflictIndexKVHandler{baseConflictKVHandler: baseHandler}
	}
	return handler
}

func (e *conflictResolutionStepExecutor) Cleanup(_ context.Context) (err error) {
	e.logger.Info("cleanup subtask env")
	return e.tableImporter.Close()
}
