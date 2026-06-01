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
	"syscall"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/stretchr/testify/require"
)

type blockingShutdownServer struct {
	forceShutdown      bool
	needRequestMgrFree bool
	waitStarted        chan struct{}
	waitDone           chan struct{}
}

func newBlockingShutdownServer() *blockingShutdownServer {
	return &blockingShutdownServer{
		needRequestMgrFree: true,
		waitStarted:        make(chan struct{}),
		waitDone:           make(chan struct{}),
	}
}

func (s *blockingShutdownServer) AutoIDServiceClose() {}

func (s *blockingShutdownServer) GetForceShutdown() bool {
	return s.forceShutdown
}

func (s *blockingShutdownServer) GetNeedRequestMgrFree() bool {
	return s.needRequestMgrFree
}

func (s *blockingShutdownServer) IsAutoIDOwner() bool {
	return false
}

func (s *blockingShutdownServer) SetForceShutdown() {
	s.forceShutdown = true
}

func (s *blockingShutdownServer) SetNeedRequestMgrFree() {
	s.needRequestMgrFree = true
}

func (s *blockingShutdownServer) WaitZeroConn() {
	close(s.waitStarted)
	<-s.waitDone
}

func TestOnServerShutdownStarterReportsFree(t *testing.T) {
	resetStandbyTestState(t)

	originalMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})

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

	t.Run("reports after zero connection wait succeeds", func(t *testing.T) {
		mgrCli := &mockManagerClient{}
		controller := NewLoadKeyspaceController(mgrCli)
		controller.setCloseConnWait(time.Second)
		svr := newBlockingShutdownServer()

		done := make(chan struct{})
		go func() {
			controller.OnServerShutdown(svr)
			close(done)
		}()

		select {
		case <-svr.waitStarted:
		case <-time.After(time.Second):
			require.Fail(t, "wait zero connection was not called")
		}
		require.False(t, mgrCli.called)

		close(svr.waitDone)
		select {
		case <-done:
		case <-time.After(time.Second):
			require.Fail(t, "shutdown did not finish")
		}
		require.True(t, mgrCli.called)
	})

	t.Run("does not report free after zero connection wait timeout", func(t *testing.T) {
		mgrCli := &mockManagerClient{}
		controller := NewLoadKeyspaceController(mgrCli)
		controller.setCloseConnWait(10 * time.Millisecond)
		svr := newBlockingShutdownServer()

		controller.OnServerShutdown(svr)
		require.False(t, mgrCli.called)
		close(svr.waitDone)
	})
}

func TestOnServerShutdownStarterRetriesManagerFree(t *testing.T) {
	resetStandbyTestState(t)

	originalMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})

	mgrCli := &mockManagerClient{failures: 2}
	controller := NewLoadKeyspaceController(mgrCli)
	svr := server.NewTestServer(config.NewConfig())
	svr.SetNeedRequestMgrFree()

	controller.OnServerShutdown(svr)

	require.True(t, mgrCli.called)
	require.Equal(t, 3, mgrCli.calls)

	t.Run("returns false after exhausting retries", func(t *testing.T) {
		mgrCli := &mockManagerClient{failures: managerFreeMaxAttempts}
		controller := NewLoadKeyspaceController(mgrCli)

		require.False(t, controller.reportManagerFree("manager free failed"))
		require.Equal(t, managerFreeMaxAttempts, mgrCli.calls)
	})
}

func TestStatusReturnsExportIDForStarter(t *testing.T) {
	resetStandbyTestState(t)

	originalMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})

	mu.Lock()
	activateRequest = ActivateRequest{
		KeyspaceName: "ks1",
		ExportID:     `export"\1`,
	}
	mu.Unlock()

	recorder := httptest.NewRecorder()
	statusHandler(recorder, httptest.NewRequest("GET", "/tidb-pool/status", nil))

	require.JSONEq(t, `{"state": "standby", "keyspace_name": "ks1", "export_id": "export\"\\1"}`, recorder.Body.String())
}

func TestExitRejectsInvalidQueryParams(t *testing.T) {
	resetStandbyTestState(t)
	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "ks1"
	})

	controller := NewLoadKeyspaceController(nil)
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
			name: "negative wait",
			query: url.Values{
				"keyspace": {"ks1"},
				"wait":     {"-1"},
			},
			want: "invalid wait\n",
		},
		{
			name: "overflow wait",
			query: url.Values{
				"keyspace": {"ks1"},
				"wait":     {"9223372036854775807"},
			},
			want: "invalid wait\n",
		},
		{
			name: "too large wait",
			query: url.Values{
				"keyspace": {"ks1"},
				"wait":     {"24h1s"},
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

	t.Run("close connection wait is instance scoped", func(t *testing.T) {
		controller.setCloseConnWait(3 * time.Second)
		require.Equal(t, 3*time.Second, controller.getCloseConnWait())
		controller.setCloseConnWait(0)
		require.Zero(t, controller.getCloseConnWait())
	})
}

func TestExitGracefulUsesTermSignalWithoutManagerFree(t *testing.T) {
	resetStandbyTestState(t)
	t.Cleanup(func() {
		_ = os.Remove(tidbNormalRestartLogPath)
	})
	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "ks1"
	})

	originalMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})

	var gotSig syscall.Signal
	oldExit := tidbExit
	tidbExit = func(sig syscall.Signal) {
		gotSig = sig
	}
	t.Cleanup(func() {
		tidbExit = oldExit
	})

	controller := NewLoadKeyspaceController(nil)
	_, mux := controller.Handler(server.NewTestServer(config.NewConfig()))
	query := url.Values{
		"keyspace": {"ks1"},
		"graceful": {"true"},
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/tidb-pool/exit?"+query.Encode(), nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, syscall.SIGTERM, gotSig)
	require.Equal(t, defaultCloseConnWait, controller.getCloseConnWait())
}

func TestExitRejectsManagerFreeWithoutNotifier(t *testing.T) {
	resetStandbyTestState(t)
	t.Cleanup(func() {
		_ = os.Remove(tidbNormalRestartLogPath)
	})
	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "ks1"
	})

	originalMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})

	oldExit := tidbExit
	tidbExit = func(syscall.Signal) {
		require.Fail(t, "exit should not be dispatched")
	}
	t.Cleanup(func() {
		tidbExit = oldExit
	})

	t.Run("uses default wait", func(t *testing.T) {
		oldExit := tidbExit
		var gotSig syscall.Signal
		tidbExit = func(sig syscall.Signal) {
			gotSig = sig
		}
		t.Cleanup(func() {
			tidbExit = oldExit
		})

		for _, wait := range []string{"", "0", "0s"} {
			gotSig = 0
			controller := NewLoadKeyspaceController(&mockManagerClient{})
			svr := server.NewTestServer(config.NewConfig())
			_, mux := controller.Handler(svr)
			query := url.Values{
				"keyspace":      {"ks1"},
				"graceful":      {"true"},
				"need_mgr_free": {"true"},
			}
			if wait != "" {
				query.Set("wait", wait)
			}
			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/tidb-pool/exit?"+query.Encode(), nil))

			require.Equal(t, http.StatusOK, recorder.Code)
			require.Equal(t, syscall.SIGTERM, gotSig)
			require.True(t, svr.GetNeedRequestMgrFree())
			require.Equal(t, defaultCloseConnWait, controller.getCloseConnWait())
		}
	})

	t.Run("requires notifier", func(t *testing.T) {
		controller := NewLoadKeyspaceController(nil)
		_, mux := controller.Handler(server.NewTestServer(config.NewConfig()))
		query := url.Values{
			"keyspace":      {"ks1"},
			"graceful":      {"true"},
			"need_mgr_free": {"true"},
			"wait":          {"1s"},
		}
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/tidb-pool/exit?"+query.Encode(), nil))

		require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
		require.Equal(t, "manager notifier is unavailable\n", recorder.Body.String())
	})
}

func TestParseExitWait(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{name: "empty"},
		{name: "zero", value: "0"},
		{name: "zero duration", value: "0s"},
		{name: "duration", value: "1h30m", want: time.Hour + 30*time.Minute},
		{name: "legacy seconds", value: "3600", want: time.Hour},
		{name: "max", value: "24h", want: 24 * time.Hour},
		{name: "above max", value: "24h1s", wantErr: true},
		{name: "overflow", value: "9223372036854775807", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseExitWait(tt.value)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
