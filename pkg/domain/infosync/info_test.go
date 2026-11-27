// Copyright 2020 PingCAP, Inc.
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

package infosync

import (
	"context"
	"encoding/json"
<<<<<<< HEAD
=======
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
>>>>>>> 727915525b9 (*: fix affinity group creation and SHOW AFFINITY (#73804))
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
<<<<<<< HEAD
=======
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/servicediscovery"
>>>>>>> 727915525b9 (*: fix affinity group creation and SHOW AFFINITY (#73804))
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestAffinityCreatePDHTTPStatus(t *testing.T) {
	const body = "group already exists"
	for _, tt := range []struct {
		name      string
		status    int
		body      string
		truncated bool
	}{
		{"bad request", http.StatusBadRequest, body, false},
		{"conflict", http.StatusConflict, body, false},
		{"misleading body", http.StatusBadRequest, "request pd http api failed with status: '409 Conflict'", false},
		{"missing API", http.StatusNotFound, body, false},
		{"failed precondition", http.StatusPreconditionFailed, body, false},
		{"truncated conflict", http.StatusConflict, body, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var getCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch {
				case req.URL.Path != pdhttp.AffinityGroups:
					t.Errorf("unexpected PD request: %s %s", req.Method, req.URL)
					w.WriteHeader(http.StatusInternalServerError)
				case req.Method == http.MethodGet:
					getCalls.Add(1)
					ids := req.URL.Query()["ids"]
					if len(ids) != 1 || ids[0] != "g1" {
						t.Errorf("expected GET with ids=g1, got ids=%v", ids)
					}
					_, _ = io.WriteString(w, `{"affinity_groups":{"g1":{"id":"g1","range_count":1}}}`)
				case req.Method == http.MethodPost && req.URL.Query().Get("skip_exist_check") == "true":
					if tt.truncated {
						w.Header().Set("Content-Length", strconv.Itoa(len(tt.body)+1))
					}
					w.WriteHeader(tt.status)
					_, _ = io.WriteString(w, tt.body)
				default:
					t.Errorf("unexpected PD request: %s %s", req.Method, req.URL)
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))
			defer server.Close()

			discovery := servicediscovery.NewMockServiceDiscovery([]string{server.URL}, nil)
			require.NoError(t, discovery.Init())
			client := pdhttp.NewClientWithServiceDiscovery("test", discovery).WithRespHandler(pdResponseHandler)
			defer client.Close()
			groups := map[string][]pdhttp.AffinityGroupKeyRange{
				"g1": {{StartKey: []byte("a"), EndKey: []byte("b")}},
			}
			err := affinity.NewPDManager(client).CreateAffinityGroupsIfNotExists(context.Background(), groups)
			if tt.status == http.StatusConflict {
				require.EqualValues(t, 1, getCalls.Load())
				require.NoError(t, err)
				return
			}
			require.Zero(t, getCalls.Load())
			require.EqualError(t, err, "[domain:8243]"+tt.body)
			require.True(t, ErrHTTPServiceError.Equal(err))
			var statusError interface{ HTTPStatusCode() int }
			require.ErrorAs(t, err, &statusError)
			require.Equal(t, tt.status, statusError.HTTPStatusCode())
		})
	}

	t.Run("gateway paths", func(t *testing.T) {
		for _, path := range []string{
			"/gateway" + pdhttp.AffinityGroups,
			"/gateway" + pdhttp.AffinityGroups + "/policies",
		} {
			for _, status := range []int{http.StatusNotFound, http.StatusPreconditionFailed} {
				t.Run(path+"/"+strconv.Itoa(status), func(t *testing.T) {
					recorder := httptest.NewRecorder()
					recorder.WriteHeader(status)
					_, err := io.WriteString(recorder, body)
					require.NoError(t, err)
					resp := recorder.Result()
					resp.Request = httptest.NewRequest(http.MethodPost, path, nil)

					err = pdResponseHandler(resp, nil)
					require.EqualError(t, err, "[domain:8243]"+body)
					var statusError interface{ HTTPStatusCode() int }
					require.ErrorAs(t, err, &statusError)
					require.Equal(t, status, statusError.HTTPStatusCode())
				})
			}
		}
	})
}

func TestAffinityGetPDHTTPNotFound(t *testing.T) {
	ids := make([]string, 101)
	for i := range ids {
		ids[i] = "g" + strconv.Itoa(i)
	}
	for _, firstBatchOK := range []bool{false, true} {
		t.Run("first_batch_succeeds="+strconv.FormatBool(firstBatchOK), func(t *testing.T) {
			var batchCalls, scanCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet || req.URL.Path != pdhttp.AffinityGroups {
					t.Errorf("unexpected PD request: %s %s", req.Method, req.URL)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				batchIDs := req.URL.Query()["ids"]
				if len(batchIDs) == 0 {
					scanCalls.Add(1)
				} else if batchCalls.Add(1) == 1 && firstBatchOK {
					_, _ = io.WriteString(w, `{"affinity_groups":{"g0":{"id":"g0","range_count":1}}}`)
					return
				}
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, "unsupported affinity GET")
			}))
			defer server.Close()

			discovery := servicediscovery.NewMockServiceDiscovery([]string{server.URL}, nil)
			require.NoError(t, discovery.Init())
			client := pdhttp.NewClientWithServiceDiscovery("test", discovery).WithRespHandler(pdResponseHandler)
			defer client.Close()
			t.Cleanup(affinity.SetPDClientForTest(client))
			result, err := affinity.GetGroupStates(context.Background(), ids)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.EqualValues(t, 1, scanCalls.Load())
			if firstBatchOK {
				require.EqualValues(t, 2, batchCalls.Load())
				require.Len(t, result, 1)
				require.Equal(t, "g0", result["g0"].ID)
				require.Nil(t, result["g100"])
			} else {
				require.EqualValues(t, 1, batchCalls.Load())
				require.Empty(t, result)
			}
		})
	}
}

func TestPDResponseHandlerPlacementCompatibility(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusPreconditionFailed, http.StatusBadRequest} {
		recorder := httptest.NewRecorder()
		recorder.WriteHeader(status)
		_, err := io.WriteString(recorder, "placement rule error")
		require.NoError(t, err)
		resp := recorder.Result()
		resp.Request = httptest.NewRequest(http.MethodGet, "/pd/api/v1/config/placement-rule", nil)
		err = pdResponseHandler(resp, nil)
		if status == http.StatusBadRequest {
			require.EqualError(t, err, "[domain:8243]placement rule error")
			require.True(t, ErrHTTPServiceError.Equal(err))
			require.ErrorIs(t, err, ErrHTTPServiceError)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestPutBundlesRetry(t *testing.T) {
	_, err := GlobalInfoSyncerInit(context.TODO(), "test", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, false, nil)
	require.NoError(t, err)

	bundle, err := placement.NewBundleFromOptions(&model.PlacementSettings{PrimaryRegion: "r1", Regions: "r1,r2"})
	require.NoError(t, err)
	bundle = bundle.Reset(placement.RuleIndexTable, []int64{1024})

	t.Run("serviceErrorShouldNotRetry", func(t *testing.T) {
		require.NoError(t, PutRuleBundles(context.TODO(), []*placement.Bundle{{ID: bundle.ID}}))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError", "1*return(true)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError"))
		}()

		err := PutRuleBundlesWithRetry(context.TODO(), []*placement.Bundle{bundle}, 3, time.Millisecond)
		require.Error(t, err)
		require.Equal(t, "[domain:8243]mock service error", err.Error())

		got, err := GetRuleBundle(context.TODO(), bundle.ID)
		require.NoError(t, err)
		require.True(t, got.IsEmpty())
	})

	t.Run("nonServiceErrorShouldRetry", func(t *testing.T) {
		require.NoError(t, PutRuleBundles(context.TODO(), []*placement.Bundle{{ID: bundle.ID}}))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError", "3*return(false)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError"))
		}()

		err := PutRuleBundlesWithRetry(context.TODO(), []*placement.Bundle{bundle}, 3, time.Millisecond)
		require.NoError(t, err)

		got, err := GetRuleBundle(context.TODO(), bundle.ID)
		require.NoError(t, err)

		gotJSON, err := json.Marshal(got)
		require.NoError(t, err)

		expectJSON, err := json.Marshal(bundle)
		require.NoError(t, err)

		require.Equal(t, expectJSON, gotJSON)
	})

	t.Run("nonServiceErrorRetryAndFail", func(t *testing.T) {
		require.NoError(t, PutRuleBundles(context.TODO(), []*placement.Bundle{{ID: bundle.ID}}))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError", "4*return(false)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError"))
		}()

		err := PutRuleBundlesWithRetry(context.TODO(), []*placement.Bundle{bundle}, 3, time.Millisecond)
		require.Error(t, err)
		require.Equal(t, "mock other error", err.Error())

		got, err := GetRuleBundle(context.TODO(), bundle.ID)
		require.NoError(t, err)
		require.True(t, got.IsEmpty())
	})
}

func TestTiFlashManager(t *testing.T) {
	ctx := context.Background()
	_, err := GlobalInfoSyncerInit(ctx, "test", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, false, nil)
	tiflash := NewMockTiFlash()
	SetMockTiFlash(tiflash)

	require.NoError(t, err)

	// SetTiFlashPlacementRule/GetTiFlashGroupRules
	rule := MakeNewRule(1, 2, []string{"a"})
	require.NoError(t, SetTiFlashPlacementRule(ctx, rule))
	rules, err := GetTiFlashGroupRules(ctx, "tiflash")
	require.NoError(t, err)
	require.Equal(t, 1, len(rules))
	require.Equal(t, "table-1-r", rules[0].ID)
	require.Equal(t, 2, rules[0].Count)
	require.Equal(t, []string{"a"}, rules[0].LocationLabels)
	require.Equal(t, false, rules[0].Override, false)
	require.Equal(t, placement.RuleIndexTiFlash, rules[0].Index)

	// GetTiFlashStoresStat
	stats, err := GetTiFlashStoresStat(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Count)

	// DeleteTiFlashPlacementRules
	require.NoError(t, DeleteTiFlashPlacementRules(ctx, []int64{1}))
	rules, err = GetTiFlashGroupRules(ctx, "tiflash")
	require.NoError(t, err)
	require.Equal(t, 0, len(rules))

	// ConfigureTiFlashPDForTable
	require.Nil(t, ConfigureTiFlashPDForTable(1, 2, &[]string{"a"}))
	rules, err = GetTiFlashGroupRules(ctx, "tiflash")
	require.NoError(t, err)
	require.Equal(t, 1, len(rules))

	// ConfigureTiFlashPDForPartitions
	ConfigureTiFlashPDForPartitions(true, &[]model.PartitionDefinition{
		{
			ID:       2,
			Name:     ast.NewCIStr("p1"),
			LessThan: []string{},
		},
		{
			ID:       3,
			Name:     ast.NewCIStr("p2"),
			LessThan: []string{},
		},
	}, 3, &[]string{}, 100)
	rules, err = GetTiFlashGroupRules(ctx, "tiflash")
	require.NoError(t, err)
	// Have table a and partitions p1, p2
	require.Equal(t, 3, len(rules))
	z, ok := tiflash.SyncStatus[2]
	require.Equal(t, true, ok)
	require.Equal(t, true, z.Accel)
	z, ok = tiflash.SyncStatus[3]
	require.Equal(t, true, ok)
	require.Equal(t, true, z.Accel)

	CloseTiFlashManager(ctx)
}

func TestInfoSyncerMarshal(t *testing.T) {
	info := &serverinfo.ServerInfo{
		StaticInfo: serverinfo.StaticInfo{
			VersionInfo: serverinfo.VersionInfo{
				Version: "8.8.8",
				GitHash: "123456",
			},
			ID:             "tidb1",
			IP:             "127.0.0.1",
			Port:           4000,
			StatusPort:     10080,
			Lease:          "1s",
			StartTimestamp: 10000,
			ServerIDGetter: func() uint64 { return 0 },
			JSONServerID:   1,
		},
		DynamicInfo: serverinfo.DynamicInfo{
			Labels: map[string]string{"zone": "ap-northeast-1a"},
		},
	}
	data, err := json.Marshal(info)
	require.NoError(t, err)
	require.Equal(t, data, []byte(`{"version":"8.8.8","git_hash":"123456",`+
		`"ddl_id":"tidb1","ip":"127.0.0.1","listening_port":4000,"status_port":10080,"lease":"1s","start_timestamp":10000,`+
		`"server_id":1,"labels":{"zone":"ap-northeast-1a"}}`))
	var decodeInfo *serverinfo.ServerInfo
	err = json.Unmarshal(data, &decodeInfo)
	require.NoError(t, err)
	require.Nil(t, decodeInfo.ServerIDGetter)
	require.Equal(t, info.Version, decodeInfo.Version)
	require.Equal(t, info.GitHash, decodeInfo.GitHash)
	require.Equal(t, info.ID, decodeInfo.ID)
	require.Equal(t, info.IP, decodeInfo.IP)
	require.Equal(t, info.Port, decodeInfo.Port)
	require.Equal(t, info.StatusPort, decodeInfo.StatusPort)
	require.Equal(t, info.Lease, decodeInfo.Lease)
	require.Equal(t, info.StartTimestamp, decodeInfo.StartTimestamp)
	require.Equal(t, info.JSONServerID, decodeInfo.JSONServerID)
	require.Equal(t, info.Labels, decodeInfo.Labels)
}
