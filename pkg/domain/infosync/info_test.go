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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	keyspacepb "github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/servicediscovery"
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

	t.Run("circuitBreakerReturnsOnTimeout", func(t *testing.T) {
		restore := config.RestoreFunc()
		defer restore()
		config.UpdateGlobal(func(conf *config.Config) {
			// Set a short columnar collect timeout to trigger the circuit breaker quickly.
			conf.CSE.ColumnarCollectTimeout = 50 * time.Millisecond
		})

		// Block forever so progress collection can only finish via the circuit-breaker timeout.
		// Cancellation propagation is covered separately in helper.CollectColumnarStatusWithCtx tests.
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		defer server.Close()

		tikvStores := map[int64]pdhttp.StoreInfo{
			1: {
				Store: pdhttp.MetaStore{
					ID:            1,
					StatusAddress: strings.TrimPrefix(server.URL, "http://"),
					StateName:     "Up",
				},
			},
		}

		// Must trigger the circuit breaker due to the short timeout.
		progress, circuitBreakerTriggered, err := MustGetTiFlashProgressWithCircuitBreaker(context.Background(), 1024, 1, nil, tikvStores)
		require.NoError(t, err)
		require.True(t, circuitBreakerTriggered)
		require.Equal(t, 1.0, progress)
	})

	t.Run("storageClassStatusCollectsCounters", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"ready":2,"total":3}`))
		}))
		defer server.Close()

		tikvStores := map[int64]pdhttp.StoreInfo{
			3: {Store: pdhttp.MetaStore{ID: 3, StatusAddress: strings.TrimPrefix(server.URL, "http://"), StateName: "Up"}},
			1: {Store: pdhttp.MetaStore{ID: 1, StatusAddress: strings.TrimPrefix(server.URL, "http://"), StateName: "Offline"}},
			2: {Store: pdhttp.MetaStore{ID: 2, StatusAddress: "127.0.0.1:1", StateName: "Tombstone"}},
		}
		statuses, err := CollectStorageClassStatus(context.Background(), 1024, model.StorageClassTierIA, tikvStores)
		require.NoError(t, err)
		require.Equal(t, []StorageClassStoreStatus{
			{StoreID: 1, Ready: 2, Total: 3},
			{StoreID: 3, Ready: 2, Total: 3},
		}, statuses)
	})

	t.Run("storageClassStatusCancelsPendingRequests", func(t *testing.T) {
		requestStarted := make(chan struct{})
		requestCanceled := make(chan struct{})
		schemaVersionSeen := make(chan bool, 1)
		blockingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(requestStarted)
			<-r.Context().Done()
			close(requestCanceled)
		}))
		defer blockingServer.Close()

		failedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-requestStarted
			schemaVersionSeen <- r.URL.Query().Has("schema_version")
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		}))
		defer failedServer.Close()

		requestCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tikvStores := map[int64]pdhttp.StoreInfo{
			1: {
				Store: pdhttp.MetaStore{
					ID:            1,
					StatusAddress: strings.TrimPrefix(failedServer.URL, "http://"),
					StateName:     "Up",
				},
			},
			2: {
				Store: pdhttp.MetaStore{
					ID:            2,
					StatusAddress: strings.TrimPrefix(blockingServer.URL, "http://"),
					StateName:     "Up",
				},
			},
		}

		statuses, err := CollectStorageClassStatus(requestCtx, 1024, model.StorageClassTierIA, tikvStores)
		require.ErrorContains(t, err, "status 503")
		require.Nil(t, statuses)
		require.False(t, <-schemaVersionSeen)
		select {
		case <-requestCanceled:
		case <-time.After(time.Second):
			t.Fatal("expected pending storage class status request to be canceled")
		}
	})

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

type mockKeyspaceConfigPDHTTPClient struct {
	pdhttp.Client
	t              *testing.T
	expectedName   string
	expectedParams *pdhttp.UpdateKeyspaceConfigParams
	retErr         error
}

func (m *mockKeyspaceConfigPDHTTPClient) UpdateKeyspaceConfig(
	ctx context.Context,
	keyspaceName string,
	params *pdhttp.UpdateKeyspaceConfigParams,
) (*keyspacepb.KeyspaceMeta, error) {
	require.NotNil(m.t, ctx)
	require.Equal(m.t, m.expectedName, keyspaceName)
	require.Equal(m.t, m.expectedParams, params)
	if m.retErr != nil {
		return nil, m.retErr
	}
	return &keyspacepb.KeyspaceMeta{}, nil
}

func TestSetKeyspaceConfig(t *testing.T) {
	_, err := GlobalInfoSyncerInit(context.TODO(), "test", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, false, nil)
	require.NoError(t, err)

	value := "True"
	precondition := "False"
	expected := &pdhttp.UpdateKeyspaceConfigParams{
		Config: map[string]*string{
			"serverless_is_bootstrapped_for_restore": &value,
		},
		Preconditions: map[string]*string{
			"serverless_is_bootstrapped_for_restore": &precondition,
		},
	}
	restore := SetPDHttpCliForTest(&mockKeyspaceConfigPDHTTPClient{
		t:              t,
		expectedName:   "test-keyspace",
		expectedParams: expected,
	})
	defer restore()

	input := pdhttp.UpdateKeyspaceConfigParams{
		Config: map[string]*string{
			"serverless_is_bootstrapped_for_restore": &value,
		},
		Preconditions: map[string]*string{
			"serverless_is_bootstrapped_for_restore": &precondition,
		},
	}

	require.NoError(t, SetKeyspaceConfig(context.Background(), "test-keyspace", input))
}

func TestSetKeyspaceConfigWithoutPDHTTPClient(t *testing.T) {
	_, err := GlobalInfoSyncerInit(context.TODO(), "test", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, false, nil)
	require.NoError(t, err)

	err = SetKeyspaceConfig(context.Background(), "test-keyspace", pdhttp.UpdateKeyspaceConfigParams{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "pd http cli is nil")
}

func TestSetKeyspaceConfigPropagatesPDHTTPError(t *testing.T) {
	_, err := GlobalInfoSyncerInit(context.TODO(), "test", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, false, nil)
	require.NoError(t, err)

	expectedErr := errors.New("update keyspace config failed")
	restore := SetPDHttpCliForTest(&mockKeyspaceConfigPDHTTPClient{
		t:            t,
		expectedName: "test-keyspace",
		expectedParams: &pdhttp.UpdateKeyspaceConfigParams{
			Config: map[string]*string{"k": nil},
		},
		retErr: expectedErr,
	})
	defer restore()

	err = SetKeyspaceConfig(context.Background(), "test-keyspace", pdhttp.UpdateKeyspaceConfigParams{
		Config: map[string]*string{"k": nil},
	})
	require.ErrorIs(t, err, expectedErr)
}

type mockAffinityStatusPDHTTPClient struct {
	pdhttp.Client
	callerID     string
	respHandler  func(*http.Response, any) error
	statusCode   *int
	body         *string
	lastCallerID *string
}

func (m *mockAffinityStatusPDHTTPClient) WithCallerID(callerID string) pdhttp.Client {
	cp := *m
	cp.callerID = callerID
	return &cp
}

func (m *mockAffinityStatusPDHTTPClient) WithRespHandler(handler func(*http.Response, any) error) pdhttp.Client {
	cp := *m
	cp.respHandler = handler
	return &cp
}

func (m *mockAffinityStatusPDHTTPClient) GetAllAffinityGroups(_ context.Context) (map[string]*pdhttp.AffinityGroupState, error) {
	if m.lastCallerID != nil {
		*m.lastCallerID = m.callerID
	}
	code := *m.statusCode
	body := *m.body
	if m.respHandler != nil {
		req := httptest.NewRequest(http.MethodGet, "http://pd/pd/api/v1/affinity-groups", nil)
		rec := httptest.NewRecorder()
		rec.WriteHeader(code)
		_, _ = rec.WriteString(body)
		resp := rec.Result()
		resp.Request = req
		var res map[string]*pdhttp.AffinityGroupState
		return res, m.respHandler(resp, &res)
	}
	return nil, fmt.Errorf("request pd http api failed with status: '%d %s', body: '%s'", code, http.StatusText(code), body)
}

func (m *mockAffinityStatusPDHTTPClient) GetAffinityGroups(ctx context.Context, _ []string) (map[string]*pdhttp.AffinityGroupState, error) {
	return m.GetAllAffinityGroups(ctx)
}

func (m *mockAffinityStatusPDHTTPClient) CreateAffinityGroups(ctx context.Context, _ map[string][]pdhttp.AffinityGroupKeyRange, _ ...pdhttp.CreateAffinityGroupsOption) (map[string]*pdhttp.AffinityGroupState, error) {
	return m.GetAllAffinityGroups(ctx)
}

func (m *mockAffinityStatusPDHTTPClient) BatchDeleteAffinityGroups(ctx context.Context, _ []string, _ bool) error {
	_, err := m.GetAllAffinityGroups(ctx)
	return err
}

func TestGlobalInfoSyncerInitPreservesPDHTTPStatusForAffinity(t *testing.T) {
	statusCode := http.StatusNotFound
	body := "404 page not found"
	var lastCallerID string
	cli := &mockAffinityStatusPDHTTPClient{
		statusCode:   &statusCode,
		body:         &body,
		lastCallerID: &lastCallerID,
	}
	t.Cleanup(func() { affinity.InitManager(nil) })

	is, err := GlobalInfoSyncerInit(context.Background(), "test", func() uint64 { return 1 }, nil, nil, nil, cli, keyspace.CodecV1, false, nil)
	require.NoError(t, err)

	// InfoSyncer keeps its own pdResponseHandler and caller ID.
	isCli, ok := is.pdHTTPCli.(*mockAffinityStatusPDHTTPClient)
	require.True(t, ok)
	require.Equal(t, "tidb-info-syncer", isCli.callerID)
	require.NotNil(t, isCli.respHandler)

	// Affinity receives the raw client without pdResponseHandler, so 404 is preserved:
	// - CreateGroupsIfNotExists reports ErrPDNotSupportAffinity.
	// - GetGroups / GetGroupStates (SHOW AFFINITY) return empty map without error.
	// - DeleteGroups (force) is an idempotent no-op.
	// - GetAllGroups returns unsupported error so reconciler skips silently.
	groups := map[string][]pdhttp.AffinityGroupKeyRange{
		"_tidb_t_1": {{StartKey: []byte("a"), EndKey: []byte("b")}},
	}
	err = affinity.CreateGroupsIfNotExists(context.Background(), groups)
	require.ErrorIs(t, err, affinity.ErrPDNotSupportAffinity)
	require.Equal(t, "tidb-affinity", lastCallerID)

	got, err := affinity.GetGroups(context.Background(), []string{"_tidb_t_1"})
	require.NoError(t, err)
	require.Empty(t, got)
	gotStates, err := affinity.GetGroupStates(context.Background(), []string{"_tidb_t_1"})
	require.NoError(t, err)
	require.Empty(t, gotStates)
	require.NoError(t, affinity.DeleteGroups(context.Background(), []string{"_tidb_t_1"}))

	_, err = affinity.GetAllGroups(context.Background())
	require.Error(t, err)
	require.True(t, affinity.IsUnsupportedError(err))

	// 500 with "unsupported" in the body preserves status 500 and is not misclassified.
	statusCode = http.StatusInternalServerError
	body = "unsupported internal state"
	_, err = affinity.GetAllGroups(context.Background())
	require.Error(t, err)
	require.False(t, affinity.IsUnsupportedError(err))
}
