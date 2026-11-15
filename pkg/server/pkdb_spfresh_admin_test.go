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

package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDecodeSPFreshAdminBodyAcceptsWorkerOwnedPatch(t *testing.T) {
	err := decodeSPFreshAdminBody([]byte(`{
		"build": {"task_ingest_txn_concurrency": 6, "global_max_inflight_bytes": "1gib"},
		"search": {"default_search_beam_size": 64}
	}`))
	require.NoError(t, err)
}

func TestDecodeSPFreshAdminBodyRequiresOneJSONObject(t *testing.T) {
	for _, body := range []string{"", "null", "[]", "{} {}", "{invalid"} {
		require.Error(t, decodeSPFreshAdminBody([]byte(body)), body)
	}
}

func TestFanoutSPFreshAdminConfigReturnsEffectivePerNodeResults(t *testing.T) {
	postBody := []byte(`{"build":{"task_ingest_txn_concurrency":6}}`)
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "42", r.URL.Query().Get("cluster_id"))
		require.Equal(t, http.MethodPost, r.Method)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"ok","effective":{"build.task_ingest_txn_concurrency":6}}`))
	}))
	defer good.Close()

	invalid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte("unsupported"))
	}))
	defer invalid.Close()

	timeout := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write([]byte(`{"result":"ok","effective":{}}`))
	}))
	defer timeout.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	nodes := fanoutSPFreshAdminConfig(
		ctx,
		http.DefaultClient,
		42,
		[]string{good.URL, invalid.URL, timeout.URL},
		http.MethodPost,
		postBody,
	)
	require.Len(t, nodes, 3)
	require.Equal(t, "ok", nodes[0].Result)
	require.JSONEq(t, `{"build.task_ingest_txn_concurrency":6}`, string(nodes[0].Effective))
	require.Equal(t, "unsupported", nodes[1].Result)
	require.Equal(t, "timeout", nodes[2].Result)
	require.Equal(t, map[string]any{"build.task_ingest_txn_concurrency": float64(6)}, effectiveSPFreshAdminConfig(nodes))
}

func TestAllSPFreshAdminNodesOKRequiresEveryWorker(t *testing.T) {
	require.False(t, allSPFreshAdminNodesOK(nil))
	require.True(t, allSPFreshAdminNodesOK([]spfreshAdminNodeResult{{Result: "ok"}}))
	require.False(t, allSPFreshAdminNodesOK([]spfreshAdminNodeResult{{Result: "ok"}, {Result: "timeout"}}))
}

func TestSPFreshAdminAggregateStatusDistinguishesUniformValidationFailure(t *testing.T) {
	require.Equal(t, http.StatusOK, spfreshAdminAggregateStatus(http.MethodGet, []spfreshAdminNodeResult{{Result: "ok"}}))
	require.Equal(t, http.StatusUnprocessableEntity, spfreshAdminAggregateStatus(http.MethodPost, []spfreshAdminNodeResult{{Result: "unsupported"}, {Result: "unsupported"}}))
	require.Equal(t, http.StatusBadRequest, spfreshAdminAggregateStatus(http.MethodPost, []spfreshAdminNodeResult{{Result: "invalid"}}))
	require.Equal(t, http.StatusServiceUnavailable, spfreshAdminAggregateStatus(http.MethodGet, []spfreshAdminNodeResult{{Result: "ok"}, {Result: "timeout"}}))
	require.Equal(t, http.StatusServiceUnavailable, spfreshAdminAggregateStatus(http.MethodPost, []spfreshAdminNodeResult{{Result: "unsupported"}, {Result: "timeout"}}))
}
