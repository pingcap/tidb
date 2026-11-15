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

package executor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestFetchSPFreshStatusBatchChunksRequests(t *testing.T) {
	ResetSPFreshVectorSearchForTest()
	oldClusterID := spfreshClusterIDCache.Swap(7)
	t.Cleanup(func() {
		ResetSPFreshVectorSearchForTest()
		spfreshClusterIDCache.Store(oldClusterID)
	})

	var (
		mu        sync.Mutex
		batchLens []int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, spfreshStatusBatchPath, r.URL.Path)
		require.Equal(t, "7", r.URL.Query().Get("cluster_id"))

		var req spfreshStatusBatchRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.NoError(t, r.Body.Close())
		mu.Lock()
		batchLens = append(batchLens, len(req.Indexes))
		mu.Unlock()

		resp := spfreshStatusBatchResponse{
			Statuses: make([]spfreshStatusBatchStatus, 0, len(req.Indexes)),
		}
		for _, index := range req.Indexes {
			resp.Statuses = append(resp.Statuses, spfreshStatusBatchStatus{
				KeyspaceID:     req.KeyspaceID,
				TableID:        index.TableID,
				IndexID:        index.IndexID,
				Dimensions:     3,
				DistanceMetric: "l2",
				AppliedBaseTS:  101,
			})
		}
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	t.Cleanup(srv.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{srv.URL}
	})

	indexes := make([]spfreshStatusBatchIndex, maxSPFreshStatusBatchIndexes+1)
	for i := range indexes {
		indexes[i] = spfreshStatusBatchIndex{
			TableID: int64(i + 1),
			IndexID: int64(i + 1000),
		}
	}

	statuses, err := fetchSPFreshStatusBatch(context.Background(), nil, 1, indexes)
	require.NoError(t, err)
	require.Len(t, statuses, len(indexes))

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []int{maxSPFreshStatusBatchIndexes, 1}, batchLens)
}

func TestFetchSPFreshStatusBatchFallbackAfterEndpointTimeout(t *testing.T) {
	oldTimeout := spfreshStatusEndpointTimeout
	spfreshStatusEndpointTimeout = 50 * time.Millisecond
	t.Cleanup(func() {
		spfreshStatusEndpointTimeout = oldTimeout
	})

	var slowHits atomic.Int32
	releaseSlow := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, spfreshStatusBatchPath, r.URL.Path)
		slowHits.Add(1)
		<-releaseSlow
	}))
	t.Cleanup(slow.Close)
	t.Cleanup(func() {
		close(releaseSlow)
	})

	var goodHits atomic.Int32
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, spfreshStatusBatchPath, r.URL.Path)
		require.Equal(t, "7", r.URL.Query().Get("cluster_id"))
		goodHits.Add(1)

		var req spfreshStatusBatchRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.NoError(t, r.Body.Close())
		require.Len(t, req.Indexes, 1)

		lagSeconds := uint64(0)
		require.NoError(t, json.NewEncoder(w).Encode(spfreshStatusBatchResponse{
			Statuses: []spfreshStatusBatchStatus{{
				KeyspaceID:     req.KeyspaceID,
				TableID:        req.Indexes[0].TableID,
				IndexID:        req.Indexes[0].IndexID,
				Dimensions:     3,
				DistanceMetric: "l2",
				AppliedBaseTS:  101,
				LagSeconds:     &lagSeconds,
			}},
		}))
	}))
	t.Cleanup(good.Close)

	start := time.Now()
	statuses, err := fetchSPFreshStatusBatchOnce(
		context.Background(),
		[]*spfreshEndpointState{{endpoint: slow.URL}, {endpoint: good.URL}},
		7,
		1,
		[]spfreshStatusBatchIndex{{TableID: 11, IndexID: 22}},
	)
	require.NoError(t, err)
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, int32(1), slowHits.Load())
	require.Equal(t, int32(1), goodHits.Load())

	status, ok := statuses[spfreshIndexStatusKey{tableID: 11, indexID: 22}]
	require.True(t, ok)
	require.Equal(t, uint32(1), status.KeyspaceID)
	require.Equal(t, int64(11), status.TableID)
	require.Equal(t, int64(22), status.IndexID)
	require.Equal(t, uint64(3), status.Dimensions)
	require.Equal(t, "l2", status.DistanceMetric)
	require.Equal(t, uint64(101), status.AppliedBaseTS)
}

func TestFetchSPFreshStatusBatchFallbackAfterRetryableStatus(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
	}{
		{name: "request_timeout", statusCode: http.StatusRequestTimeout},
		{name: "too_many_requests", statusCode: http.StatusTooManyRequests},
		{name: "internal", statusCode: http.StatusInternalServerError},
		{name: "bad_gateway", statusCode: http.StatusBadGateway},
		{name: "service_unavailable", statusCode: http.StatusServiceUnavailable},
		{name: "gateway_timeout", statusCode: http.StatusGatewayTimeout},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var badHits atomic.Int32
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, spfreshStatusBatchPath, r.URL.Path)
				badHits.Add(1)
				http.Error(w, "retryable", tc.statusCode)
			}))
			t.Cleanup(bad.Close)

			var goodHits atomic.Int32
			good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, spfreshStatusBatchPath, r.URL.Path)
				goodHits.Add(1)

				var req spfreshStatusBatchRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				require.NoError(t, r.Body.Close())
				require.Len(t, req.Indexes, 1)

				lagSeconds := uint64(0)
				require.NoError(t, json.NewEncoder(w).Encode(spfreshStatusBatchResponse{
					Statuses: []spfreshStatusBatchStatus{{
						KeyspaceID:     req.KeyspaceID,
						TableID:        req.Indexes[0].TableID,
						IndexID:        req.Indexes[0].IndexID,
						Dimensions:     3,
						DistanceMetric: "l2",
						AppliedBaseTS:  101,
						LagSeconds:     &lagSeconds,
					}},
				}))
			}))
			t.Cleanup(good.Close)

			statuses, err := fetchSPFreshStatusBatchOnce(
				context.Background(),
				[]*spfreshEndpointState{{endpoint: bad.URL}, {endpoint: good.URL}},
				7,
				1,
				[]spfreshStatusBatchIndex{{TableID: 11, IndexID: 22}},
			)
			require.NoError(t, err)
			require.Equal(t, int32(1), badHits.Load())
			require.Equal(t, int32(1), goodHits.Load())

			status, ok := statuses[spfreshIndexStatusKey{tableID: 11, indexID: 22}]
			require.True(t, ok)
			require.Equal(t, uint32(1), status.KeyspaceID)
			require.Equal(t, uint64(101), status.AppliedBaseTS)
		})
	}
}

func TestFetchSPFreshStatusBatchRejectsOversizedSuccessResponse(t *testing.T) {
	var oversizedHits atomic.Int32
	oversized := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, spfreshStatusBatchPath, r.URL.Path)
		oversizedHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, err := io.CopyN(w, zeroReader{}, maxSPFreshStatusBatchResponseBytes+1)
		require.NoError(t, err)
	}))
	t.Cleanup(oversized.Close)

	var goodHits atomic.Int32
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, spfreshStatusBatchPath, r.URL.Path)
		goodHits.Add(1)
		var req spfreshStatusBatchRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		lagSeconds := uint64(0)
		require.NoError(t, json.NewEncoder(w).Encode(spfreshStatusBatchResponse{
			Statuses: []spfreshStatusBatchStatus{{
				KeyspaceID:     req.KeyspaceID,
				TableID:        req.Indexes[0].TableID,
				IndexID:        req.Indexes[0].IndexID,
				Dimensions:     3,
				DistanceMetric: "l2",
				AppliedBaseTS:  101,
				LagSeconds:     &lagSeconds,
			}},
		}))
	}))
	t.Cleanup(good.Close)

	states := []*spfreshEndpointState{{endpoint: oversized.URL}, {endpoint: good.URL}}
	statuses, err := fetchSPFreshStatusBatchOnce(
		context.Background(),
		states,
		7,
		1,
		[]spfreshStatusBatchIndex{{TableID: 11, IndexID: 22}},
	)
	require.NoError(t, err)
	require.Equal(t, int32(1), oversizedHits.Load())
	require.Equal(t, int32(1), goodHits.Load())
	require.Positive(t, states[0].failures.Load())
	require.Contains(t, statuses, spfreshIndexStatusKey{tableID: 11, indexID: 22})
}
