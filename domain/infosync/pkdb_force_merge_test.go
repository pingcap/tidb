// Copyright 2024 PingCAP, Inc.
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
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/util/pdapi"
	"github.com/stretchr/testify/require"
)

func TestAddForceMergeRangesBatchesRequests(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []addForceMergeRangesRequest
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			t.Errorf("unexpected method: %s", req.Method)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.URL.Path != path.Join(pdapi.Regions, "force-merge") {
			t.Errorf("unexpected path: %s", req.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Errorf("read request body failed: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := req.Body.Close(); err != nil {
			t.Errorf("close request body failed: %v", err)
		}

		var payload addForceMergeRangesRequest
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("unmarshal request body failed: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		mu.Lock()
		requests = append(requests, payload)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, err = w.Write([]byte(`{}`))
		if err != nil {
			t.Errorf("write response failed: %v", err)
		}
	}))
	defer server.Close()

	originalGetForceMergePDAddrs := getForceMergePDAddrs
	getForceMergePDAddrs = func() ([]string, error) {
		return []string{server.URL}, nil
	}
	defer func() {
		getForceMergePDAddrs = originalGetForceMergePDAddrs
	}()

	originalForceMergeBatchSleep := forceMergeBatchSleep
	sleepDurations := make([]time.Duration, 0, 2)
	forceMergeBatchSleep = func(ctx context.Context, d time.Duration) error {
		sleepDurations = append(sleepDurations, d)
		return nil
	}
	defer func() {
		forceMergeBatchSleep = originalForceMergeBatchSleep
	}()

	ranges := buildForceMergeKeyRangesForTest(forceMergeMaxBatchSize*2 + 1)
	require.NoError(t, AddForceMergeRanges(context.Background(), ranges))

	require.Len(t, requests, 3)
	require.Len(t, requests[0].StartKeysHex, forceMergeMaxBatchSize)
	require.Len(t, requests[0].EndKeysHex, forceMergeMaxBatchSize)
	require.Len(t, requests[1].StartKeysHex, forceMergeMaxBatchSize)
	require.Len(t, requests[1].EndKeysHex, forceMergeMaxBatchSize)
	require.Len(t, requests[2].StartKeysHex, 1)
	require.Len(t, requests[2].EndKeysHex, 1)

	require.Equal(t, hex.EncodeToString(ranges[0].StartKey), requests[0].StartKeysHex[0])
	require.Equal(t, hex.EncodeToString(ranges[forceMergeMaxBatchSize-1].EndKey), requests[0].EndKeysHex[forceMergeMaxBatchSize-1])
	require.Equal(t, hex.EncodeToString(ranges[forceMergeMaxBatchSize].StartKey), requests[1].StartKeysHex[0])
	require.Equal(t, hex.EncodeToString(ranges[forceMergeMaxBatchSize*2].EndKey), requests[2].EndKeysHex[0])

	require.Equal(t, []time.Duration{forceMergeBatchSleepTime, forceMergeBatchSleepTime}, sleepDurations)
}

func TestAddForceMergeRangesStopsWhenSleepReturnsError(t *testing.T) {
	var (
		mu          sync.Mutex
		requestsNum int
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != path.Join(pdapi.Regions, "force-merge") {
			t.Errorf("unexpected path: %s", req.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		requestsNum++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{}`))
		if err != nil {
			t.Errorf("write response failed: %v", err)
		}
	}))
	defer server.Close()

	originalGetForceMergePDAddrs := getForceMergePDAddrs
	getForceMergePDAddrs = func() ([]string, error) {
		return []string{server.URL}, nil
	}
	defer func() {
		getForceMergePDAddrs = originalGetForceMergePDAddrs
	}()

	originalForceMergeBatchSleep := forceMergeBatchSleep
	forceMergeBatchSleep = func(ctx context.Context, d time.Duration) error {
		return context.Canceled
	}
	defer func() {
		forceMergeBatchSleep = originalForceMergeBatchSleep
	}()

	ranges := buildForceMergeKeyRangesForTest(forceMergeMaxBatchSize + 1)
	err := AddForceMergeRanges(context.Background(), ranges)
	require.Error(t, err)
	require.Contains(t, err.Error(), context.Canceled.Error())

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, requestsNum)
}

func buildForceMergeKeyRangesForTest(n int) []ForceMergeKeyRange {
	ranges := make([]ForceMergeKeyRange, 0, n)
	for i := 0; i < n; i++ {
		startKey := []byte{byte(i >> 16), byte(i >> 8), byte(i)}
		endKey := []byte{byte(i >> 16), byte(i >> 8), byte(i), 0xff}
		ranges = append(ranges, ForceMergeKeyRange{
			StartKey: startKey,
			EndKey:   endKey,
		})
	}
	return ranges
}
