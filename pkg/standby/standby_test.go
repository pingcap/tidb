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
	"errors"
	"net/http/httptest"
	"testing"

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
	state = standbyState
	activateRequest = ActivateRequest{}
	mu.Unlock()

	t.Cleanup(func() {
		mu.Lock()
		state = oldState
		activateRequest = oldActivateRequest
		mu.Unlock()
	})
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
