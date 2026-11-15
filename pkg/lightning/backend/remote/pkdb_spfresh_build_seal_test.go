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
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/stretchr/testify/require"
)

func testSPFreshBuildSealRequest() SPFreshBuildSealRequest {
	return SPFreshBuildSealRequest{BuildID: "build-1", KeyspaceID: 1, TableID: 10, IndexID: 3, BaseSnapshotTS: 123}
}

func TestSPFreshSealIdentitySurvivesAcceptedPoll(t *testing.T) {
	attempts := 0
	identity := testSPFreshBuildSealRequest()
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var received SPFreshBuildSealRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&received))
		require.Equal(t, identity, received)
		attempts++
		if attempts == 1 {
			resp := jsonResponse(r, `{"status":"sealing","next-retry-at-ms":0}`)
			resp.StatusCode = http.StatusAccepted
			return resp, nil
		}
		// The second request may be answered using durable proof only.
		return jsonResponse(r, `{"task-id":"task-1","status":"sealed","finished":true}`), nil
	})}
	require.NoError(t, SealSPFreshBuildShard(context.Background(), client, "http://spfresh-build.test", 7, "task-1", identity))
	require.Equal(t, 2, attempts)
}

func TestSPFreshSealLostProofIsTerminal(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusConflict} {
		attempts := 0
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			attempts++
			resp := jsonResponse(r, "local build task and durable seal proof are missing; rebuild required")
			resp.StatusCode = status
			return resp, nil
		})}
		err := SealSPFreshBuildShard(context.Background(), client, "http://spfresh-build.test", 7, "task-1", testSPFreshBuildSealRequest())
		require.Error(t, err)
		require.False(t, common.IsRetryableError(err))
		require.Equal(t, 1, attempts)
	}
}
