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

package ddl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
)

// MockSPFreshBuildLifecycle isolates SQL metadata tests from the Storage service.
// Storage tests exercise protection, renewal and generation fencing.
func MockSPFreshBuildLifecycle(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockSPFreshBuildTransport", "return(true)")
	old := requestSPFreshBuildLifecycle
	requestSPFreshBuildLifecycle = func(context.Context, kv.Storage, []string, int64, int64, uint64, string) error { return nil }
	t.Cleanup(func() { requestSPFreshBuildLifecycle = old })
}

type lifecycleTestPD struct{ pd.Client }

func (lifecycleTestPD) GetClusterID(context.Context) uint64 { return 42 }

type lifecycleTestStore struct{ kv.Storage }

func (lifecycleTestStore) GetPDHTTPClient() pdhttp.Client { return nil }
func (lifecycleTestStore) GetPDClient() pd.Client         { return lifecycleTestPD{} }
func (lifecycleTestStore) GetCodec() tikv.Codec {
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Id: 7})
	if err != nil {
		panic(err)
	}
	return codec
}

func TestSPFreshBuildLifecycleRetriesSameIdentity(t *testing.T) {
	for _, action := range []string{"begin", "abort"} {
		t.Run(action, func(t *testing.T) {
			var bodies []map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				require.Equal(t, "/spfresh/build-lifecycle", req.URL.Path)
				require.Equal(t, "42", req.URL.Query().Get("cluster_id"))
				var body map[string]any
				require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
				bodies = append(bodies, body)
				if len(bodies) == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
				} else {
					w.WriteHeader(http.StatusNoContent)
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, requestSPFreshBuildLifecycle(ctx, lifecycleTestStore{}, []string{server.URL}, 11, 2, 100, action))
			require.Len(t, bodies, 2)
			require.Equal(t, bodies[0], bodies[1])
			require.Equal(t, map[string]any{"action": action, "base_ts": float64(100), "keyspace_id": float64(7), "table_id": float64(11), "index_id": float64(2)}, bodies[0])
		})
	}
}

func TestSPFreshBuildLifecycleRejectsFenceWithoutRetry(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { calls++; w.WriteHeader(http.StatusConflict) }))
	defer server.Close()
	require.ErrorContains(t, requestSPFreshBuildLifecycle(context.Background(), lifecycleTestStore{}, []string{server.URL}, 11, 2, 100, "begin"), "HTTP 409")
	require.Equal(t, 1, calls)
}

func TestSPFreshBuildRejectsMissingOrInvalidDurableGeneration(t *testing.T) {
	for _, generation := range []uint64{0, 99, 101} {
		_, err := spfreshBuildGeneration(&model.Job{RealStartTS: 100, SchemaState: model.StateWriteReorganization, ReorgMeta: &model.DDLReorgMeta{SPFreshBuildGeneration: generation}})
		require.Error(t, err)
	}
}
