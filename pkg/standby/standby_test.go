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
	"testing"
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
