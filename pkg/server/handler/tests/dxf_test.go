// Copyright 2025 PingCAP, Inc.
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

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/schstatus"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto/jobhistory"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskkey"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func runAndCheckReqFn(t *testing.T, code int, resMsg string, doReqFn func() (*http.Response, error)) []byte {
	t.Helper()
	resp, err := doReqFn()
	require.NoError(t, err)
	require.Equal(t, code, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), resMsg)
	require.NoError(t, resp.Body.Close())
	return body
}

func TestDXFScheduleAPI(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("DXF schedule API is only supported in nextgen kernel and only available in the SYSTEM keyspace")
	}
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)
	t.Run("schedule status api", func(t *testing.T) {
		// invalid method
		resp, err := ts.PostStatus("/dxf/schedule/status", "", bytes.NewBuffer([]byte("")))
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "This api only support GET method")
		require.NoError(t, resp.Body.Close())
		// success
		resp, err = ts.FetchStatus("/dxf/schedule/status")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		status := schstatus.Status{}
		require.NoError(t, json.Unmarshal(body, &status))
		require.Equal(t, 1, status.TiDBWorker.RequiredCount)
	})

	t.Run("schedule api", func(t *testing.T) {
		resp, err := ts.FetchStatus("/dxf/schedule")
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "This api only support POST method")
		require.NoError(t, resp.Body.Close())
		// success
		for _, c := range []struct {
			action  string
			enabled bool
		}{
			{action: "pause_scale_in", enabled: true},
			{action: "resume_scale_in", enabled: false},
		} {
			resp, err = ts.PostStatus(fmt.Sprintf("/dxf/schedule?action=%s", c.action), "", bytes.NewBuffer([]byte("")))
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			body, err = io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			param := schstatus.TTLFlag{}
			require.NoError(t, json.Unmarshal(body, &param))
			require.Equal(t, c.enabled, param.Enabled)
			if param.Enabled {
				require.Equal(t, time.Hour, param.TTL)
				require.WithinRange(t, param.ExpireTime, time.Now().Add(param.TTL-time.Minute), time.Now().Add(param.TTL))
			}
		}
	})

	t.Run("active task api", func(t *testing.T) {
		resp, err := ts.PostStatus("/dxf/task/active", "", bytes.NewBuffer([]byte("")))
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "This api only support GET method")
		require.NoError(t, resp.Body.Close())

		tm, err := storage.GetTaskManager()
		require.NoError(t, err)
		ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)
		require.NoError(t, tm.InitMeta(ctx, ":4000", ""))
		_, err = tm.ExecuteSQLWithNewSession(ctx, "delete from mysql.tidb_global_task")
		require.NoError(t, err)

		_, err = tm.CreateTask(ctx, "active-key-1", proto.ImportInto, "SYSTEM", 8, "", 0, proto.ExtraParams{}, []byte("test"))
		require.NoError(t, err)
		_, err = tm.CreateTask(ctx, "active-key-2", proto.ImportInto, "ks1", 8, "", 0, proto.ExtraParams{}, []byte("test"))
		require.NoError(t, err)
		_, err = tm.CreateTask(ctx, "active-key-3", proto.ImportInto, "ks1", 8, "", 0, proto.ExtraParams{}, []byte("test"))
		require.NoError(t, err)

		resp, err = ts.FetchStatus("/dxf/task/active")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		out := struct {
			Total       int64            `json:"total"`
			PerKeyspace map[string]int64 `json:"per_keyspace"`
		}{}
		require.NoError(t, json.Unmarshal(body, &out))
		require.EqualValues(t, 3, out.Total)
		require.EqualValues(t, 1, out.PerKeyspace["SYSTEM"])
		require.EqualValues(t, 2, out.PerKeyspace["ks1"])
	})

	t.Run("import-into history job info api", func(t *testing.T) {
		setupTaskManager := func(t *testing.T) (*storage.TaskManager, context.Context) {
			t.Helper()
			tm, err := storage.GetTaskManager()
			require.NoError(t, err)
			ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)
			require.NoError(t, tm.InitMeta(ctx, ":4000", ""))
			for _, sql := range []string{
				"delete from mysql.tidb_background_subtask",
				"delete from mysql.tidb_background_subtask_history",
				"delete from mysql.tidb_global_task",
				"delete from mysql.tidb_global_task_history",
			} {
				_, err = tm.ExecuteSQLWithNewSession(ctx, sql)
				require.NoError(t, err)
			}
			return tm, ctx
		}

		t.Run("validation", func(t *testing.T) {
			runAndCheckReqFn(t, http.StatusBadRequest, "This api only support GET method", func() (*http.Response, error) {
				return ts.PostStatus("/dxf/import-into/history/job/ks1/9527", "", bytes.NewBuffer([]byte("")))
			})
			runAndCheckReqFn(t, http.StatusBadRequest, "invalid job id", func() (*http.Response, error) {
				return ts.FetchStatus("/dxf/import-into/history/job/ks1/invalid")
			})
			runAndCheckReqFn(t, http.StatusBadRequest, "invalid or empty target keyspace", func() (*http.Response, error) {
				return ts.FetchStatus("/dxf/import-into/history/job/ks.1/9527")
			})
		})

		t.Run("not found in history", func(t *testing.T) {
			_, _ = setupTaskManager(t)
			runAndCheckReqFn(t, http.StatusNotFound, "not found in history", func() (*http.Response, error) {
				return ts.FetchStatus("/dxf/import-into/history/job/ks1/9527")
			})
		})

		t.Run("success", func(t *testing.T) {
			tm, ctx := setupTaskManager(t)

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
			taskID, err := tm.CreateTask(ctx, taskkey.ForJobInKeyspace("ks1", 9527), proto.ImportInto, "ks1", 8, "", 4, proto.ExtraParams{}, taskMeta)
			require.NoError(t, err)

			_, err = tm.ExecuteSQLWithNewSession(ctx, `
				insert into mysql.tidb_background_subtask(
					step, task_key, exec_id, meta, state, type, concurrency, ordinal, create_time, checkpoint, summary, start_time, state_update_time
				) values
					(%?, %?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), '{}', %?, %?, %?),
					(%?, %?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), '{}', %?, %?, %?),
					(%?, %?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), '{}', %?, %?, %?),
					(%?, %?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), '{}', %?, %?, %?)`,
				proto.ImportStepEncodeAndSort, taskID, "tidb-1", []byte(`{"kv-group":"data"}`), proto.SubtaskStateSucceed, proto.Type2Int(proto.ImportInto), 8, 1, `{"bytes": 1073741824}`, 100, 700,
				proto.ImportStepWriteAndIngest, taskID, "tidb-1", []byte(`{"kv-group":"data"}`), proto.SubtaskStateSucceed, proto.Type2Int(proto.ImportInto), 8, 1, `{"bytes": 1073741824}`, 700, 2500,
				proto.ImportStepWriteAndIngest, taskID, "tidb-1", []byte(`{"kv-group":"index-1"}`), proto.SubtaskStateSucceed, proto.Type2Int(proto.ImportInto), 8, 2, `{"bytes": 536870912}`, 900, 2100,
				proto.ImportStepPostProcess, taskID, "tidb-1", []byte(`{"kv-group":"data"}`), proto.SubtaskStateSucceed, proto.Type2Int(proto.ImportInto), 8, 3, `{"bytes": 0}`, 0, 0,
			)
			require.NoError(t, err)

			task, err := tm.GetTaskByID(ctx, taskID)
			require.NoError(t, err)
			require.NoError(t, tm.TransferTasks2History(ctx, []*proto.Task{task}))

			body := runAndCheckReqFn(t, http.StatusOK, "", func() (*http.Response, error) {
				return ts.FetchStatus("/dxf/import-into/history/job/ks1/9527")
			})
			out := jobhistory.Info{}
			require.NoError(t, json.Unmarshal(body, &out))
			require.EqualValues(t, 9527, out.JobID)
			require.EqualValues(t, taskID, out.TaskID)
			require.Equal(t, "ks1", out.Keyspace)
			require.Equal(t, 16, out.DistSQLScanConcurrency)
			require.Equal(t, 2, out.IndexCount)
			require.Equal(t, 3, out.ColumnCount)
			require.EqualValues(t, 1024, out.RowCount)
			require.Equal(t, "40m0s", out.Duration.Total)
			require.Equal(t, "10m0s", out.Duration.Encode)
			require.Equal(t, "30m0s", out.Duration.Ingest)
			require.Empty(t, out.Duration.PostProcess)
		})
	})
}

func TestDXFScheduleTuneAPI(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only supported in nextgen kernel and only available in the SYSTEM keyspace")
	}
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)

	// no keyspace
	resp, err := ts.FetchStatus("/dxf/schedule/tune")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "invalid or empty target keyspace")
	require.NoError(t, resp.Body.Close())
	// when not set, return default value
	resp, err = ts.FetchStatus("/dxf/schedule/tune?keyspace=aaa")
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "failed to load keyspace")
	require.NoError(t, resp.Body.Close())
	// invalid value
	for _, v := range []float64{0.9, 10.1} {
		resp, err = ts.PostStatus(fmt.Sprintf("/dxf/schedule/tune?keyspace=SYSTEM&amplify_factor=%f", v), "", bytes.NewBuffer([]byte("")))
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		body, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), "is out of range")
		require.NoError(t, resp.Body.Close())
	}
	// success
	resp, err = ts.PostStatus("/dxf/schedule/tune?keyspace=SYSTEM&amplify_factor=2&ttl=10h", "", bytes.NewBuffer([]byte("")))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	ttlFactors := &schstatus.TTLTuneFactors{}
	require.NoError(t, json.Unmarshal(body, ttlFactors))
	require.Equal(t, 10*time.Hour, ttlFactors.TTL)
	require.EqualValues(t, 2.0, ttlFactors.AmplifyFactor)
	// get again
	resp, err = ts.FetchStatus("/dxf/schedule/tune?keyspace=SYSTEM")
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	factors := &schstatus.TuneFactors{}
	require.NoError(t, json.Unmarshal(body, factors))
	require.EqualValues(t, &schstatus.TuneFactors{AmplifyFactor: 2}, factors)
	require.NoError(t, resp.Body.Close())
}
