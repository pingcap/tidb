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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActivateRequestMetadata(t *testing.T) {
	var req ActivateRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"keyspace_name": "ks",
		"keyspace_id": 42,
		"metadata": {
			"meta_a": "value_a"
		}
	}`), &req))
	require.NotNil(t, req.KeyspaceID)
	require.Equal(t, uint32(42), *req.KeyspaceID)
	require.Equal(t, map[string]string{
		"meta_a": "value_a",
	}, req.Metadata)

	var zeroKeyspaceIDReq ActivateRequest
	require.NoError(t, json.Unmarshal([]byte(`{"keyspace_name":"ks","keyspace_id":0}`), &zeroKeyspaceIDReq))
	require.NotNil(t, zeroKeyspaceIDReq.KeyspaceID)
	require.Equal(t, uint32(0), *zeroKeyspaceIDReq.KeyspaceID)

	mu.Lock()
	originalRequest := activateRequest
	activateRequest = req
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		activateRequest = originalRequest
		mu.Unlock()
	})

	controller := NewLoadKeyspaceController()
	metadata := controller.ActivationMetadata()
	require.Equal(t, req.Metadata, metadata)
	metadata["meta_a"] = "changed"
	require.Equal(t, "value_a", controller.ActivationMetadata()["meta_a"])

	keyspaceID := controller.ActivationKeyspaceID()
	require.Equal(t, uint32(42), keyspaceID)
	require.Equal(t, uint32(42), controller.ActivationKeyspaceID())
}

func TestActivateRequiresKeyspaceID(t *testing.T) {
	controller := NewLoadKeyspaceController()
	_, mux := controller.Handler(nil)
	req := httptest.NewRequest(http.MethodPost, "/tidb-pool/activate", strings.NewReader(`{"keyspace_name":"ks"}`))
	resp := httptest.NewRecorder()

	mux.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
}

func TestActivateRejectsMismatchedKeyspaceID(t *testing.T) {
	mu.Lock()
	originalState, originalRequest := state, activateRequest
	state = standbyState
	activateRequest = ActivateRequest{}
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		state = originalState
		activateRequest = originalRequest
		mu.Unlock()
	})

	controller := NewLoadKeyspaceController()
	_, mux := controller.Handler(nil)
	firstRespCode := make(chan int, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/tidb-pool/activate", strings.NewReader(`{"keyspace_name":"ks","keyspace_id":42}`))
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		firstRespCode <- resp.Code
	}()
	t.Cleanup(func() {
		controller.EndStandby(errors.New("test done"))
		require.Equal(t, http.StatusInternalServerError, <-firstRespCode)
	})

	<-activateCh
	req := httptest.NewRequest(http.MethodPost, "/tidb-pool/activate", strings.NewReader(`{"keyspace_name":"ks","keyspace_id":43}`))
	resp := httptest.NewRecorder()

	mux.ServeHTTP(resp, req)

	require.Equal(t, http.StatusPreconditionFailed, resp.Code)
}
