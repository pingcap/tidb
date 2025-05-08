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

package importinto_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestGetTaskImportedRows(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := context.WithValue(context.Background(), "etcd", true)
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)

	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)

	// local sort
	taskMeta := importinto.TaskMeta{
		Plan: importer.Plan{},
	}
	bytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	taskID, err := manager.CreateTask(ctx, importinto.TaskKey(111), proto.ImportInto, 1, "", 0, proto.ExtraParams{}, bytes)
	require.NoError(t, err)
	importStepSummaries := []*execute.SubtaskSummary{
		{
			ProcessedRowCount: 1,
		},
		{
			ProcessedRowCount: 2,
		},
	}
	for _, m := range importStepSummaries {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepImport,
			"", nil, m, proto.ImportInto, 11)
	}
	runInfo, err := importinto.GetRuntimeInfoForJob(ctx, 111)
	require.NoError(t, err)
	require.Equal(t, uint64(3), runInfo.ImportRows)

	// global sort
	taskMeta = importinto.TaskMeta{
		Plan: importer.Plan{
			CloudStorageURI: "s3://test-bucket/test-path",
		},
	}
	bytes, err = json.Marshal(taskMeta)
	require.NoError(t, err)
	taskID, err = manager.CreateTask(ctx, importinto.TaskKey(222), proto.ImportInto, 1, "", 0, proto.ExtraParams{}, bytes)
	require.NoError(t, err)
	ingestStepSummaries := []*execute.SubtaskSummary{
		{
			ProcessedRowCount: 11,
		},
		{
			ProcessedRowCount: 22,
		},
	}
	for _, m := range ingestStepSummaries {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepWriteAndIngest,
			"", bytes, m, proto.ImportInto, 11)
	}
	runInfo, err = importinto.GetRuntimeInfoForJob(ctx, 222)
	require.NoError(t, err)
	require.Equal(t, uint64(33), runInfo.ImportRows)
}
