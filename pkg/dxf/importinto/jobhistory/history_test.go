// Copyright 2026 PingCAP, Inc.
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

package jobhistory_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/dxf/importinto/jobhistory"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskkey"
	"github.com/stretchr/testify/require"
)

func TestGetFromHistory(t *testing.T) {
	_, tm, ctx := testutil.InitTableTest(t)
	require.NoError(t, tm.InitMeta(ctx, ":4000", ""))

	const (
		keyspace = "ks1"
		jobID    = int64(9527)
	)
	taskMeta := []byte(`{
		"Plan": {
			"DistSQLScanConcurrency": 16,
			"DesiredTableInfo": {
				"index_info": [{"id": 1}, {"id": 2}],
				"cols": [{"id": 1}, {"id": 2}, {"id": 3}]
			},
			"TotalFileSize": 2147483648
		},
		"Summary": {
			"row-count": 1024
		}
	}`)
	taskID, err := tm.CreateTask(ctx, taskkey.ForJobInKeyspace(keyspace, jobID), proto.ImportInto, keyspace, 8, "", 4, proto.ExtraParams{}, taskMeta)
	require.NoError(t, err)

	encodeID := testutil.InsertSubtask(t, tm, taskID, proto.ImportStepEncodeAndSort, "tidb-1",
		[]byte(`{"kv-group":"data"}`), proto.SubtaskStateSucceed, proto.ImportInto, 8)
	dataID := testutil.InsertSubtask(t, tm, taskID, proto.ImportStepWriteAndIngest, "tidb-1",
		[]byte(`{"kv-group":"data"}`), proto.SubtaskStateSucceed, proto.ImportInto, 8)
	indexID := testutil.InsertSubtask(t, tm, taskID, proto.ImportStepWriteAndIngest, "tidb-1",
		[]byte(`{"kv-group":"index-1"}`), proto.SubtaskStateSucceed, proto.ImportInto, 8)
	invalidDurationID := testutil.InsertSubtask(t, tm, taskID, proto.ImportStepPostProcess, "tidb-1",
		[]byte(`{"kv-group":"data"}`), proto.SubtaskStateSucceed, proto.ImportInto, 8)

	updateSubtask := func(id int64, startTime, endTime int64, summary, meta string) {
		_, err = tm.ExecuteSQLWithNewSession(ctx, `
			update mysql.tidb_background_subtask
			set start_time = %?, state_update_time = %?, summary = %?, meta = %?
			where id = %?`,
			startTime, endTime, summary, []byte(meta), id)
		require.NoError(t, err)
	}
	updateSubtask(encodeID, 100, 700, `{"bytes": 1073741824}`, `{"kv-group":"data"}`)
	updateSubtask(dataID, 700, 2500, `{"bytes": 1073741824}`, `{"kv-group":"data"}`)
	updateSubtask(indexID, 900, 2100, `{"bytes": 536870912}`, `{"kv-group":"index-1"}`)
	updateSubtask(invalidDurationID, 0, 0, `{"bytes": 0}`, `{"kv-group":"data"}`)

	task, err := tm.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	require.NoError(t, tm.TransferTasks2History(ctx, []*proto.Task{task}))

	info, err := jobhistory.GetFromHistory(ctx, tm, keyspace, jobID)
	require.NoError(t, err)
	require.Equal(t, jobID, info.JobID)
	require.Equal(t, keyspace, info.Keyspace)
	require.Equal(t, taskID, info.TaskID)
	require.Equal(t, string(proto.TaskStatePending), info.State)
	require.Equal(t, 8, info.Concurrency)
	require.Equal(t, 4, info.MaxNodeCount)
	require.Equal(t, 16, info.DistSQLScanConcurrency)
	require.Equal(t, 2, info.IndexCount)
	require.Equal(t, 3, info.ColumnCount)
	require.Equal(t, "2GiB", info.FileSize)
	require.Equal(t, "1GiB", info.DataKVSize)
	require.Equal(t, "512MiB", info.IndexKVSize)
	require.NotEmpty(t, info.PerCoreSpeed)
	require.NotEmpty(t, info.OverallSpeed)
	require.EqualValues(t, 1024, info.RowCount)
	require.EqualValues(t, 2097152, info.RowLength)
	require.Equal(t, "40m0s", info.Duration.Total)
	require.Equal(t, "10m0s", info.Duration.Encode)
	require.Equal(t, "30m0s", info.Duration.Ingest)
	require.Empty(t, info.Duration.MergeSort)
	require.Empty(t, info.Duration.CollectConflicts)
	require.Empty(t, info.Duration.ResolveConflicts)
	require.Empty(t, info.Duration.PostProcess)

	_, err = jobhistory.GetFromHistory(ctx, tm, keyspace, jobID+1)
	require.ErrorContains(t, err, "not found in history")
}
