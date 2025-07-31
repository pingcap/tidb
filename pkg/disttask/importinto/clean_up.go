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

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var _ scheduler.CleanUpRoutine = (*ImportCleanUp)(nil)

// ImportCleanUp implements scheduler.CleanUpRoutine.
type ImportCleanUp struct {
}

func newImportCleanUpS3() scheduler.CleanUpRoutine {
	return &ImportCleanUp{}
}

// CleanUp implements the CleanUpRoutine.CleanUp interface.
func (*ImportCleanUp) CleanUp(ctx context.Context, task *proto.Task) error {
	// we can only clean up files after all write&ingest subtasks are finished,
	// since they might share the same file.
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(task.Meta, taskMeta)
	if err != nil {
		return err
	}
	defer redactSensitiveInfo(task, taskMeta)

	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	if err = taskManager.WithNewTxn(ctx, func(se sessionctx.Context) error {
		return ddl.AlterTableMode(domain.GetDomain(se).DDLExecutor(), se, model.TableModeNormal, taskMeta.Plan.DBID, taskMeta.Plan.TableInfo.ID)
	}); err != nil {
		return err
	}
	// Not use cloud storage, no need to cleanUp.
	if taskMeta.Plan.CloudStorageURI == "" {
		return nil
	}
	logger := logutil.BgLogger().With(zap.Int64("task-id", task.ID))
	callLog := log.BeginTask(logger, "cleanup global sorted data")
	defer callLog.End(zap.InfoLevel, nil)

	store, err := importer.GetSortStore(ctx, taskMeta.Plan.CloudStorageURI)
	if err != nil {
		logger.Warn("failed to create store", zap.Error(err))
		return err
	}
	defer store.Close()
	if err = external.CleanUpFiles(ctx, store, strconv.Itoa(int(task.ID))); err != nil {
		logger.Warn("failed to clean up files of task", zap.Error(err))
		return err
	}
	return nil
}

func init() {
	scheduler.RegisterSchedulerCleanUpFactory(proto.ImportInto, newImportCleanUpS3)
}
