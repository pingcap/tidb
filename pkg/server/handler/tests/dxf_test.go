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
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/schstatus"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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

func TestDXFAPI(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("DXF API is only supported in nextgen kernel and only available in the SYSTEM keyspace")
	}
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)")
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)

	t.Run("schedule status api", func(t *testing.T) {
		// invalid method
		runAndCheckReqFn(t, http.StatusBadRequest, "This api only support GET method", func() (*http.Response, error) {
			return ts.PostStatus("/dxf/schedule/status", "", bytes.NewBuffer([]byte("")))
		})
		// success
		body := runAndCheckReqFn(t, http.StatusOK, "", func() (*http.Response, error) {
			return ts.FetchStatus("/dxf/schedule/status")
		})
		status := schstatus.Status{}
		require.NoError(t, json.Unmarshal(body, &status))
		require.Equal(t, 1, status.TiDBWorker.RequiredCount)
	})

	t.Run("schedule api", func(t *testing.T) {
		runAndCheckReqFn(t, http.StatusBadRequest, "This api only support POST method", func() (*http.Response, error) {
			return ts.FetchStatus("/dxf/schedule")
		})
		// success
		for _, c := range []struct {
			action  string
			enabled bool
		}{
			{action: "pause_scale_in", enabled: true},
			{action: "resume_scale_in", enabled: false},
		} {
			body := runAndCheckReqFn(t, http.StatusOK, "", func() (*http.Response, error) {
				return ts.PostStatus(fmt.Sprintf("/dxf/schedule?action=%s", c.action), "", bytes.NewBuffer([]byte("")))
			})
			param := schstatus.TTLFlag{}
			require.NoError(t, json.Unmarshal(body, &param))
			require.Equal(t, c.enabled, param.Enabled)
			if param.Enabled {
				require.Equal(t, time.Hour, param.TTL)
				require.WithinRange(t, param.ExpireTime, time.Now().Add(param.TTL-time.Minute), time.Now().Add(param.TTL))
			}
		}
	})

	t.Run("task max_runtime_slots api", func(t *testing.T) {
		runAndCheckReqFn(t, http.StatusBadRequest, "This api only support POST method", func() (*http.Response, error) {
			return ts.FetchStatus("/dxf/task/1/max_runtime_slots")
		})
		tm, err := storage.GetTaskManager()
		require.NoError(t, err)
		ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)
		require.NoError(t, tm.InitMeta(ctx, ":4000", ""))
		id, err := tm.CreateTask(ctx, "key1", proto.ImportInto, "", 8, "", 0, proto.ExtraParams{}, []byte("test"))
		require.NoError(t, err)

		for _, c := range [][2]string{
			{"/dxf/task/0/max_runtime_slots", "invalid task ID"},
			{"/dxf/task/aa/max_runtime_slots", "invalid task ID"},
			{"/dxf/task/1/max_runtime_slots", "invalid value "},
			{"/dxf/task/1/max_runtime_slots?value=aa", "invalid value "},
			{"/dxf/task/1/max_runtime_slots?value=0", "invalid value "},
			{"/dxf/task/1/max_runtime_slots?value=1&target_step=a", "invalid target step"},
			{"/dxf/task/1/max_runtime_slots?value=1&target_step=1&target_step=aa", "invalid target step"},
			{"/dxf/task/1123123/max_runtime_slots?value=1&target_step=1", "task not found"},
			{fmt.Sprintf("/dxf/task/%d/max_runtime_slots?value=10", id), "max runtime slots should be less than required slots(8)"},
			{fmt.Sprintf("/dxf/task/%d/max_runtime_slots?value=6&target_step=100", id), "invalid target step 100 for task type ImportInto"},
		} {
			path, errMsg := c[0], c[1]
			runAndCheckReqFn(t, http.StatusBadRequest, errMsg, func() (*http.Response, error) {
				return ts.PostStatus(path, "", bytes.NewBuffer([]byte("")))
			})
		}

		body := runAndCheckReqFn(t, http.StatusOK, "", func() (*http.Response, error) {
			return ts.PostStatus(fmt.Sprintf("/dxf/task/%d/max_runtime_slots?value=6&target_step=3", id), "", bytes.NewBuffer([]byte("")))
		})
		out := struct {
			RequiredSlots   int      `json:"required_slots"`
			MaxRuntimeSlots int      `json:"max_runtime_slots"`
			TargetSteps     []string `json:"target_steps"`
		}{}
		require.NoError(t, json.Unmarshal(body, &out))
		require.Equal(t, 8, out.RequiredSlots)
		require.Equal(t, 6, out.MaxRuntimeSlots)
		require.Equal(t, []string{"encode"}, out.TargetSteps)
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
	runAndCheckReqFn(t, http.StatusBadRequest, "invalid or empty target keyspace", func() (*http.Response, error) {
		return ts.FetchStatus("/dxf/schedule/tune")
	})
	// when not set, return default value
	runAndCheckReqFn(t, http.StatusBadRequest, "failed to load keyspace", func() (*http.Response, error) {
		return ts.FetchStatus("/dxf/schedule/tune?keyspace=aaa")
	})
	// invalid value
	for _, v := range []float64{0.9, 10.1} {
		runAndCheckReqFn(t, http.StatusBadRequest, "is out of range", func() (*http.Response, error) {
			return ts.PostStatus(fmt.Sprintf("/dxf/schedule/tune?keyspace=SYSTEM&amplify_factor=%f", v), "", bytes.NewBuffer([]byte("")))
		})
	}
	// success
	body := runAndCheckReqFn(t, http.StatusOK, "", func() (*http.Response, error) {
		return ts.PostStatus("/dxf/schedule/tune?keyspace=SYSTEM&amplify_factor=2&ttl=10h", "", bytes.NewBuffer([]byte("")))
	})
	ttlFactors := &schstatus.TTLTuneFactors{}
	require.NoError(t, json.Unmarshal(body, ttlFactors))
	require.Equal(t, 10*time.Hour, ttlFactors.TTL)
	require.EqualValues(t, 2.0, ttlFactors.AmplifyFactor)
	// get again
	body = runAndCheckReqFn(t, http.StatusOK, "", func() (*http.Response, error) {
		return ts.FetchStatus("/dxf/schedule/tune?keyspace=SYSTEM")
	})
	factors := &schstatus.TuneFactors{}
	require.NoError(t, json.Unmarshal(body, factors))
	require.EqualValues(t, &schstatus.TuneFactors{AmplifyFactor: 2}, factors)
}
