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

//go:build nextgen

package standby

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestOnServerShutdownStarterReportsFree(t *testing.T) {
	resetStandbyTestState(t)

	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(deploymode.Premium))
	})
	t.Setenv("GracefulCloseConnectionsTimeout", "1ms")

	require.NoError(t, os.WriteFile(tidbNormalRestartLogPath, []byte("keyspace-1:idle"), 0644))
	t.Cleanup(func() {
		_ = os.Remove(tidbNormalRestartLogPath)
	})

	mgrCli := &mockManagerClient{}
	controller := NewLoadKeyspaceController(mgrCli)
	svr := server.NewTestServer(config.NewConfig())
	svr.SetNeedRequestMgrFree()

	controller.OnServerShutdown(svr)

	require.True(t, mgrCli.called)
	require.Equal(t, "keyspace-1:idle", mgrCli.gotReason)
	require.Equal(t, terminatingState, state)
}

func TestStatusReturnsExportIDForStarter(t *testing.T) {
	resetStandbyTestState(t)

	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(deploymode.Premium))
	})

	mu.Lock()
	activateRequest = ActivateRequest{
		KeyspaceName: "ks1",
		ExportID:     "export-1",
	}
	mu.Unlock()

	recorder := httptest.NewRecorder()
	statusHandler(recorder, httptest.NewRequest("GET", "/tidb-pool/status", nil))

	require.JSONEq(t, `{"state": "standby", "keyspace_name": "ks1", "export_id": "export-1"}`, recorder.Body.String())
}

func TestExitRejectsInvalidQueryParams(t *testing.T) {
	resetStandbyTestState(t)
	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "ks1"
	})

	controller := NewLoadKeyspaceController()
	_, mux := controller.Handler(server.NewTestServer(config.NewConfig()))

	tests := []struct {
		name  string
		query url.Values
		want  string
	}{
		{
			name: "invalid graceful",
			query: url.Values{
				"keyspace": {"ks1"},
				"graceful": {"tru"},
			},
			want: "invalid graceful\n",
		},
		{
			name: "invalid wait",
			query: url.Values{
				"keyspace": {"ks1"},
				"wait":     {"-1"},
			},
			want: "invalid wait\n",
		},
		{
			name: "invalid skip auto id owner",
			query: url.Values{
				"keyspace":           {"ks1"},
				"skip_auto_id_owner": {"maybe"},
			},
			want: "invalid skip_auto_id_owner\n",
		},
		{
			name: "invalid need manager free",
			query: url.Values{
				"keyspace":      {"ks1"},
				"need_mgr_free": {"maybe"},
			},
			want: "invalid need_mgr_free\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest("GET", "/tidb-pool/exit?"+tt.query.Encode(), nil))

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Equal(t, tt.want, recorder.Body.String())
		})
	}
}
