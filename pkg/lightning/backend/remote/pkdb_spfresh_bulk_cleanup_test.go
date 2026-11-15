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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSPFreshBulkCleanupRequiresExplicitWorkerRetirement(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		response string
		success  bool
	}{
		{"drained", 200, `{"retired":true}`, true},
		{"busy", 429, "busy", false},
		{"missing-ack", 200, `{}`, false},
		{"not-retired", 200, `{"retired":false}`, false},
		{"unknown-field", 200, `{"retired":true,"extra":1}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, http.MethodPost, r.Method)
				require.Equal(t, "/spfresh/bulk-retire", r.URL.Path)
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.JSONEq(t, `{"build_id":"run-j17"}`, string(body))
				w.WriteHeader(tc.status)
				_, err = io.WriteString(w, tc.response)
				require.NoError(t, err)
			}))
			defer server.Close()
			err := RetireSPFreshBulkBuild(context.Background(), server.Client(), server.URL, "run-j17")
			if tc.success {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, 1, calls)
		})
	}
}
