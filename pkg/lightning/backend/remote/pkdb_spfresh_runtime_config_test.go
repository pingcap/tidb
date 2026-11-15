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

package remote

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
)

func TestSPFreshRuntimeConfigControlRequests(t *testing.T) {
	expected := model.DefaultSPFreshVectorRuntimeConfig()
	desired := model.SPFreshVectorRuntimeConfig{
		HammingFactor:      64,
		Rabitq4Factor:      3,
		MaxFixupWorkers:    37,
		MaxFixups:          400,
		StalledOpTimeoutMS: 2000,
		MaxInsertAttempts:  64,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("cluster_id"); got != "42" {
			t.Errorf("cluster_id = %q, want 42", got)
		}
		if r.URL.Path != spfreshRuntimeConfigPath {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		var req spfreshRuntimeConfigUpdateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode update request: %v", err)
		}
		if req.KeyspaceID != 3 || req.TableID != 4 || req.IndexID != 5 {
			t.Errorf("update identity = %+v", req)
		}
		if req.Expected != expected {
			t.Errorf("update expected = %+v, want %+v", req.Expected, expected)
		}
		if req.Desired != desired {
			t.Errorf("update desired = %+v, want %+v", req.Desired, desired)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	if err := UpdateSPFreshRuntimeConfig(context.Background(), server.Client(), server.URL, 42, 3, 4, 5, expected, desired); err != nil {
		t.Fatalf("UpdateSPFreshRuntimeConfig() error = %v", err)
	}
}
