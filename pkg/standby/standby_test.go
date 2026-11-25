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

package standby

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/stretchr/testify/require"
)

type mockManagerClient struct {
	called    bool
	calls     int
	failures  int
	gotReason string
}

type mockReadyServer struct {
	called bool
	err    error
}

func (s *mockReadyServer) InitTiDBListener() error {
	s.called = true
	return s.err
}

func (c *mockManagerClient) Free(_ context.Context, exitReason string) error {
	c.called = true
	c.calls++
	c.gotReason = exitReason
	if c.calls <= c.failures {
		return errors.New("temporary error")
	}
	return nil
}

func resetStandbyTestState(t *testing.T) {
	t.Helper()
	mu.Lock()
	oldState := state
	oldActivateRequest := activateRequest
	oldActivationTimeout := activationTimeout
	oldActivateCh := activateCh
	state = standbyState
	activateRequest = ActivateRequest{}
	activationTimeout = 0
	activateCh = make(chan struct{}, 1)
	mu.Unlock()

	t.Cleanup(func() {
		mu.Lock()
		state = oldState
		activateRequest = oldActivateRequest
		activationTimeout = oldActivationTimeout
		activateCh = oldActivateCh
		mu.Unlock()
	})
}

func TestActivateRequestMetadata(t *testing.T) {
	resetStandbyTestState(t)

	var req ActivateRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"keyspace_name": "ks",
		"metadata": {
			"meta_a": "value_a"
		}
	}`), &req))
	require.Equal(t, map[string]string{
		"meta_a": "value_a",
	}, req.Metadata)

	mu.Lock()
	activateRequest = req
	mu.Unlock()

	controller := NewLoadKeyspaceController(nil)
	metadata := controller.ActivationMetadata()
	require.Equal(t, req.Metadata, metadata)
	metadata["meta_a"] = "changed"
	require.Equal(t, "value_a", controller.ActivationMetadata()["meta_a"])
}

func TestActivateRequiresKeyspaceName(t *testing.T) {
	resetStandbyTestState(t)
	controller := NewLoadKeyspaceController(nil)
	_, mux := controller.Handler(nil)
	req := httptest.NewRequest(http.MethodPost, "/tidb-pool/activate", strings.NewReader(`{}`))
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusBadRequest, resp.Code)
}

func TestActivateWaitsUntilServerReady(t *testing.T) {
	resetStandbyTestState(t)
	controller := NewLoadKeyspaceController(nil)
	_, mux := controller.Handler(nil)

	req := httptest.NewRequest(http.MethodPost, "/tidb-pool/activate", strings.NewReader(`{"keyspace_name":"ks"}`))
	resp := httptest.NewRecorder()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		mux.ServeHTTP(resp, req)
	}()
	t.Cleanup(func() {
		controller.EndStandby(nil)
		wg.Wait()
	})

	require.Eventually(t, func() bool {
		mu.RLock()
		defer mu.RUnlock()
		return state == activatedState
	}, time.Second, time.Millisecond*10)
	select {
	case <-done:
		require.Fail(t, "activate request returned before server ready")
	default:
	}

	readyServer := &mockReadyServer{}
	require.NoError(t, controller.PrepareForActivation(readyServer))
	wg.Wait()

	require.True(t, readyServer.called)
	require.Equal(t, http.StatusOK, resp.Code)
	require.JSONEq(t, `{"state":"activated","keyspace_name":"ks"}`, resp.Body.String())
}

func TestActivateAbort(t *testing.T) {
	for _, tt := range []struct {
		name       string
		completed  bool
		startupErr error
		timeout    bool
	}{
		{name: "during activation"},
		{name: "after successful activation", completed: true},
		{name: "after failed activation", completed: true, startupErr: errors.New("listener failed")},
		{name: "timeout during activation", timeout: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				resetStandbyTestState(t)
				var signals []syscall.Signal
				oldExit := tidbExit
				tidbExit = func(sig syscall.Signal) {
					signals = append(signals, sig)
				}
				t.Cleanup(func() {
					tidbExit = oldExit
				})

				controller := NewLoadKeyspaceController(nil)
				_, mux := controller.Handler(nil)
				if tt.completed {
					go func() {
						<-activateCh
						controller.EndStandby(tt.startupErr)
					}()
					req := httptest.NewRequest(http.MethodPost, "/tidb-pool/activate", strings.NewReader(`{"keyspace_name":"ks"}`))
					resp := httptest.NewRecorder()
					mux.ServeHTTP(resp, req)
					if tt.startupErr == nil {
						require.Equal(t, http.StatusOK, resp.Code)
						require.JSONEq(t, `{"state":"activated","keyspace_name":"ks"}`, resp.Body.String())
					} else {
						require.Equal(t, http.StatusInternalServerError, resp.Code)
						require.Equal(t, tt.startupErr.Error(), resp.Body.String())
					}
				}

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tt.timeout {
					activationTimeout = 1
				} else {
					cancel()
				}
				// Repeated requests exercise both select cases after activation completes.
				for i := range 64 {
					req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/tidb-pool/activate", strings.NewReader(`{"keyspace_name":"ks"}`))
					resp := httptest.NewRecorder()
					mux.ServeHTTP(resp, req)
					if tt.timeout && i == 0 {
						require.Equal(t, http.StatusRequestTimeout, resp.Code)
					}
					// Wait for any asynchronous shutdown before checking signals or retrying.
					synctest.Wait()
				}

				if tt.completed {
					require.Empty(t, signals)
					require.Equal(t, tt.startupErr, controller.startServerErr)
				} else {
					require.Equal(t, []syscall.Signal{syscall.SIGTERM}, signals)
					if tt.timeout {
						require.EqualError(t, controller.startServerErr, "timeout waiting for activation")
					} else {
						require.EqualError(t, controller.startServerErr, "client closed connection")
					}
				}
			})
		})
	}
}

func TestOnServerShutdownNoopOutsideStarter(t *testing.T) {
	resetStandbyTestState(t)
	if kerneltype.IsNextGen() {
		original := deploymode.Get()
		require.NoError(t, deploymode.Set(deploymode.Premium))
		t.Cleanup(func() {
			require.NoError(t, deploymode.Set(original))
		})
	}
	require.False(t, deploymode.IsStarter())

	mgrCli := &mockManagerClient{}
	controller := NewLoadKeyspaceController(mgrCli)
	svr := server.NewTestServer(config.NewConfig())
	svr.SetNeedRequestMgrFree()

	controller.OnServerShutdown(svr)

	require.False(t, mgrCli.called)
}

func TestStatusDoesNotReturnExportIDOutsideStarter(t *testing.T) {
	resetStandbyTestState(t)
	if kerneltype.IsNextGen() {
		original := deploymode.Get()
		require.NoError(t, deploymode.Set(deploymode.Premium))
		t.Cleanup(func() {
			require.NoError(t, deploymode.Set(original))
		})
	}
	require.False(t, deploymode.IsStarter())

	mu.Lock()
	activateRequest = ActivateRequest{
		KeyspaceName: "ks1",
		ExportID:     "export-1",
	}
	mu.Unlock()

	recorder := httptest.NewRecorder()
	statusHandler(recorder, httptest.NewRequest("GET", "/tidb-pool/status", nil))

	require.JSONEq(t, `{"state": "standby", "keyspace_name": "ks1"}`, recorder.Body.String())
}
