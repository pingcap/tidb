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

	"github.com/stretchr/testify/require"
)

func TestCommitSPFreshBulkReadyRootKind(t *testing.T) {
	for _, test := range []struct {
		name       string
		rootLevel  uint32
		rootIsLeaf bool
		wantError  bool
	}{
		{name: "leaf root", rootLevel: 1, rootIsLeaf: true},
		{name: "interior root", rootLevel: 2, rootIsLeaf: false},
		{name: "wrong leaf kind", rootLevel: 1, rootIsLeaf: false, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request SPFreshBulkReadyRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				if err := json.NewEncoder(w).Encode(SPFreshBulkReadyResponse{
					BuildID: request.BuildID, SourceSnapshotTS: request.SourceSnapshotTS,
					IndexTS: request.BulkImportTS, RootLevel: request.RootLevel,
					RootIsLeaf: test.rootIsLeaf,
				}); err != nil {
					t.Errorf("write Ready response: %v", err)
				}
			}))
			defer server.Close()

			_, err := CommitSPFreshBulkReady(context.Background(), server.Client(), server.URL, SPFreshBulkReadyRequest{
				BuildID: "build", SourceSnapshotTS: 100, BulkImportTS: 200, RootLevel: test.rootLevel,
			})
			if test.wantError {
				require.ErrorContains(t, err, "Ready response does not match request")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
