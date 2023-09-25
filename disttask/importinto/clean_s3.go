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
	"strconv"

	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type ImportCleanUpS3 struct {
	ctx  context.Context
	task *proto.Task
}

func newImportCleanUpS3(ctx context.Context, task *proto.Task) dispatcher.CleanUpRoutine {
	return &ImportCleanUpS3{
		ctx:  ctx,
		task: task,
	}
}

func (c *ImportCleanUpS3) CleanUp() error {
	// we can only clean up files after all write&ingest subtasks are finished,
	// since they might share the same file.
	// TODO: maybe add a way to notify user that there are files left in global sorted storage.
	logger := logutil.BgLogger().With(zap.Int64("task-id", c.task.ID))
	callLog := log.BeginTask(logger, "cleanup global sorted data")
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(c.task.Meta, taskMeta)
	if err != nil {
		return err
	}
	if taskMeta.Plan.CloudStorageURI == "" {
		return nil
	}
	defer callLog.End(zap.InfoLevel, nil)

	controller, err := buildController(&taskMeta.Plan, taskMeta.Stmt)
	if err != nil {
		logger.Warn("failed to build controller", zap.Error(err))
		return err
	}
	if err = controller.InitDataStore(c.ctx); err != nil {
		logger.Warn("failed to init data store", zap.Error(err))
		return err
	}
	if err = external.CleanUpFiles(c.ctx, controller.GlobalSortStore,
		strconv.Itoa(int(c.task.ID))); err != nil {
		logger.Warn("failed to clean up files of task", zap.Error(err))
		return err
	}
	// Only redact sensitive info after cleanUpFiles success.
	redactSensitiveInfo(c.task, taskMeta)
	return nil
}

func init() {
	dispatcher.RegisterDispatcherCleanUpFactory(proto.ImportInto, newImportCleanUpS3)
}
