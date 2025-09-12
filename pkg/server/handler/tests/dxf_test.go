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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/schstatus"
	"github.com/stretchr/testify/require"
)

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
}
