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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/util/pdapi"
	"github.com/stretchr/testify/require"
)

var forceMergeInfoSyncerTestMu sync.Mutex

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

		payload, err := decodeAddForceMergeRangesRequest(req)
		if err != nil {
			t.Errorf("decode request body failed: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		mu.Lock()
		requests = append(requests, payload)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	setForceMergeInfoSyncerForTest(t, []string{server.URL})

	originalSleep := forceMergeBatchSleep
	sleepDurations := make([]time.Duration, 0, 2)
	forceMergeBatchSleep = func(ctx context.Context, d time.Duration) error {
		sleepDurations = append(sleepDurations, d)
		return nil
	}
	defer func() {
		forceMergeBatchSleep = originalSleep
	}()

	ranges := buildForceMergeKeyRangesForTest(forceMergeMaxBatchSize*2 + 1)
	require.NoError(t, AddForceMergeRanges(context.Background(), ranges))

	mu.Lock()
	defer mu.Unlock()
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
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	setForceMergeInfoSyncerForTest(t, []string{server.URL})

	originalSleep := forceMergeBatchSleep
	forceMergeBatchSleep = func(ctx context.Context, d time.Duration) error {
		return context.Canceled
	}
	defer func() {
		forceMergeBatchSleep = originalSleep
	}()

	err := AddForceMergeRanges(context.Background(), buildForceMergeKeyRangesForTest(forceMergeMaxBatchSize+1))
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, requestsNum)
}

func TestAddForceMergeRangesRetriesWithFreshBody(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []addForceMergeRangesRequest
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != path.Join(pdapi.Regions, "force-merge") {
			t.Errorf("unexpected path: %s", req.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		payload, err := decodeAddForceMergeRangesRequest(req)
		if err != nil {
			t.Errorf("decode request body failed: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		mu.Lock()
		requests = append(requests, payload)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	badServer := httptest.NewServer(http.NotFoundHandler())
	badURL := badServer.URL
	badServer.Close()

	originalTimeout := forceMergePDRequestTimeout
	forceMergePDRequestTimeout = 500 * time.Millisecond
	defer func() {
		forceMergePDRequestTimeout = originalTimeout
	}()

	setForceMergeInfoSyncerForTest(t, []string{badURL, server.URL})

	ranges := buildForceMergeKeyRangesForTest(2)
	require.NoError(t, AddForceMergeRanges(context.Background(), ranges))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, requests, 1)
	require.Equal(t, []string{
		hex.EncodeToString(ranges[0].StartKey),
		hex.EncodeToString(ranges[1].StartKey),
	}, requests[0].StartKeysHex)
	require.Equal(t, []string{
		hex.EncodeToString(ranges[0].EndKey),
		hex.EncodeToString(ranges[1].EndKey),
	}, requests[0].EndKeysHex)
}

func TestAddForceMergeRangesUsesPerRequestTimeout(t *testing.T) {
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
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	setForceMergeInfoSyncerForTest(t, []string{server.URL})

	originalTimeout := forceMergePDRequestTimeout
	forceMergePDRequestTimeout = 20 * time.Millisecond
	defer func() {
		forceMergePDRequestTimeout = originalTimeout
	}()

	err := AddForceMergeRanges(context.Background(), buildForceMergeKeyRangesForTest(1))
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func setForceMergeInfoSyncerForTest(t *testing.T, addrs []string) {
	t.Helper()

	forceMergeInfoSyncerTestMu.Lock()
	t.Cleanup(func() {
		forceMergeInfoSyncerTestMu.Unlock()
	})

	var oldInfoSyncer *InfoSyncer
	oldValue := globalInfoSyncer.Load()
	if oldValue != nil {
		oldInfoSyncer = oldValue.(*InfoSyncer)
	}

	oldGetPDAddrsForForceMergeFunc := getPDAddrsForForceMergeFunc
	getPDAddrsForForceMergeFunc = func(*InfoSyncer) ([]string, error) {
		return addrs, nil
	}
	setGlobalInfoSyncer(&InfoSyncer{})

	t.Cleanup(func() {
		if oldInfoSyncer != nil {
			setGlobalInfoSyncer(oldInfoSyncer)
		} else {
			globalInfoSyncer = atomic.Value{}
		}
		getPDAddrsForForceMergeFunc = oldGetPDAddrsForForceMergeFunc
	})
}

func decodeAddForceMergeRangesRequest(req *http.Request) (addForceMergeRangesRequest, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return addForceMergeRangesRequest{}, err
	}
	if err := req.Body.Close(); err != nil {
		return addForceMergeRangesRequest{}, err
	}

	var payload addForceMergeRangesRequest
	if err := json.Unmarshal(body, &payload); err != nil {
		return addForceMergeRangesRequest{}, err
	}
	return payload, nil
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
