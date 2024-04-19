// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var _ scheduler.CleanUpRoutine = (*BackfillCleanUpS3)(nil)

// BackfillCleanUpS3 implements scheduler.CleanUpRoutine.
type BackfillCleanUpS3 struct {
}

func newBackfillCleanUpS3() scheduler.CleanUpRoutine {
	return &BackfillCleanUpS3{}
}

// CleanUp implements the CleanUpRoutine.CleanUp interface.
func (*BackfillCleanUpS3) CleanUp(ctx context.Context, task *proto.Task) error {
	var taskMeta BackfillTaskMeta
	if err := json.Unmarshal(task.Meta, &taskMeta); err != nil {
		return err
	}
	// Not use cloud storage, no need to cleanUp.
	if len(taskMeta.CloudStorageURI) == 0 {
		return nil
	}
	backend, err := storage.ParseBackend(taskMeta.CloudStorageURI, nil)
	if err != nil {
		logutil.Logger(ctx).Warn("failed to parse cloud storage uri", zap.Error(err))
		return err
	}
	extStore, err := storage.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		logutil.Logger(ctx).Warn("failed to create cloud storage", zap.Error(err))
		return err
	}
	prefix := strconv.Itoa(int(taskMeta.Job.ID))
	err = external.CleanUpFiles(ctx, extStore, prefix)
	if err != nil {
		logutil.Logger(ctx).Warn("cannot cleanup cloud storage files", zap.Error(err))
		return err
	}
	redactCloudStorageURI(ctx, task, &taskMeta)
	return nil
}

func redactCloudStorageURI(
	ctx context.Context,
	task *proto.Task,
	origin *BackfillTaskMeta,
) {
	origin.CloudStorageURI = ast.RedactURL(origin.CloudStorageURI)
	metaBytes, err := json.Marshal(origin)
	if err != nil {
		logutil.Logger(ctx).Warn("failed to marshal task meta", zap.Error(err))
		return
	}
	task.Meta = metaBytes
}
