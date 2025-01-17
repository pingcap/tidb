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

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var _ scheduler.CleanUpRoutine = (*ImportCleanUpS3)(nil)

// ImportCleanUpS3 implements scheduler.CleanUpRoutine.
type ImportCleanUpS3 struct {
}

func newImportCleanUpS3() scheduler.CleanUpRoutine {
	return &ImportCleanUpS3{}
}

// CleanUp implements the CleanUpRoutine.CleanUp interface.
func (*ImportCleanUpS3) CleanUp(ctx context.Context, task *proto.Task) error {
	// we can only clean up files after all write&ingest subtasks are finished,
	// since they might share the same file.
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(task.Meta, taskMeta)
	if err != nil {
		return err
	}
	defer redactSensitiveInfo(task, taskMeta)
	// Not use cloud storage, no need to cleanUp.
	if taskMeta.Plan.CloudStorageURI == "" {
		return nil
	}
	logger := logutil.BgLogger().With(zap.Int64("task-id", task.ID))
	callLog := log.BeginTask(logger, "cleanup global sorted data")
	defer callLog.End(zap.InfoLevel, nil)

	controller, err := buildController(&taskMeta.Plan, taskMeta.Stmt)
	if err != nil {
		logger.Warn("failed to build controller", zap.Error(err))
		return err
	}
	if err = controller.InitDataStore(ctx); err != nil {
		logger.Warn("failed to init data store", zap.Error(err))
		return err
	}
	if err = external.CleanUpFiles(ctx, controller.GlobalSortStore,
		strconv.Itoa(int(task.ID))); err != nil {
		logger.Warn("failed to clean up files of task", zap.Error(err))
		return err
	}
	return nil
}

func init() {
	scheduler.RegisterSchedulerCleanUpFactory(proto.ImportInto, newImportCleanUpS3)
}
