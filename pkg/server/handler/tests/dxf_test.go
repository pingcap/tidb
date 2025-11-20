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
