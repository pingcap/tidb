// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikvhandler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

type fakeCacheStore struct {
	helper.Storage
	status        tikv.StoreCacheStatus
	refresh       tikv.StoreCacheRefreshResult
	ks            string
	cid           uint64
	resetCalled   bool
	refreshCalled bool
	resetErr      error
	onRefresh     func(ctx context.Context)
}

func (f *fakeCacheStore) GetStoreCacheStatus(storeID uint64) tikv.StoreCacheStatus {
	f.status.StoreID = storeID
	return f.status
}

func (f *fakeCacheStore) RefreshStoreCache(ctx context.Context, storeID uint64) tikv.StoreCacheRefreshResult {
	f.refreshCalled = true
	if f.onRefresh != nil {
		f.onRefresh(ctx)
	}
	f.refresh.StoreID = storeID
	return f.refresh
}

func (f *fakeCacheStore) ResetStoreCacheRefresh(storeID uint64) error {
	_ = storeID
	f.resetCalled = true
	if f.resetErr != nil {
		return f.resetErr
	}
	f.status.Failed = 0
	f.refresh.Failed = 0
	return nil
}

func (f *fakeCacheStore) GetClusterID() uint64 { return f.cid }
func (f *fakeCacheStore) GetKeyspace() string  { return f.ks }

func TestRegionCacheHandlerGetPost(t *testing.T) {
	fake := &fakeCacheStore{
		status:  tikv.StoreCacheStatus{Matched: 3, Ready: false, ObservedAt: 1},
		refresh: tikv.StoreCacheRefreshResult{Scanned: 3, Matched: 3, Updated: 2, Remaining: 1, Ready: false, ObservedAt: 2},
		ks:      "ks1",
		cid:     99,
	}
	h := NewRegionCacheHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: fake}})

	req := httptest.NewRequest(http.MethodGet, "/regions/cache/status?store_id=7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var got regionCacheHTTPResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, 3, got.Remaining)
	require.False(t, got.Ready)
	require.Empty(t, got.Stores)
	require.NotContains(t, w.Body.String(), `"store_id"`)
	require.NotContains(t, w.Body.String(), `"scanned"`)
	require.NotContains(t, w.Body.String(), `"matched"`)
	require.NotContains(t, w.Body.String(), `"updated"`)
	require.Equal(t, int64(1), got.ObservedAt)

	req = httptest.NewRequest(http.MethodGet, "/regions/cache/status?store_id=7&detail=1", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Stores, 1)
	require.Equal(t, "ks1", got.Stores[0].Keyspace)
	require.Equal(t, uint64(99), got.Stores[0].ClusterID)
	require.Equal(t, int64(1), got.Stores[0].ObservedAt)

	req = httptest.NewRequest(http.MethodPost, "/regions/cache/refresh?store_id=7", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	got = regionCacheHTTPResult{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, 1, got.Remaining)
	require.False(t, got.Ready)
	require.Empty(t, got.Stores)

	req = httptest.NewRequest(http.MethodGet, "/regions/cache/status", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)

	req = httptest.NewRequest(http.MethodPut, "/regions/cache/status?store_id=7", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestRegionCacheHandlerRouteMethods(t *testing.T) {
	fake := &fakeCacheStore{
		status:  tikv.StoreCacheStatus{Matched: 1, Ready: false},
		refresh: tikv.StoreCacheRefreshResult{Matched: 1, Updated: 1, Ready: true},
		ks:      "ks1",
		cid:     1,
	}
	h := NewRegionCacheHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: fake}})
	r := mux.NewRouter()
	r.Handle("/regions/cache/status", h).Methods(http.MethodGet)
	r.Handle("/regions/cache/refresh", h).Methods(http.MethodPost)

	req := httptest.NewRequest(http.MethodPost, "/regions/cache/status?store_id=7", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)

	req = httptest.NewRequest(http.MethodGet, "/regions/cache/refresh?store_id=7", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)

	req = httptest.NewRequest(http.MethodGet, "/regions/cache/status?store_id=7", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	req = httptest.NewRequest(http.MethodPost, "/regions/cache/refresh?store_id=7", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestRegionCacheHandlerResetThenRefresh(t *testing.T) {
	fake := &fakeCacheStore{
		status:  tikv.StoreCacheStatus{Matched: 0, Failed: 2, Ready: false},
		refresh: tikv.StoreCacheRefreshResult{Matched: 0, Failed: 0, Remaining: 0, Ready: true},
		ks:      "ks1",
		cid:     1,
	}
	h := NewRegionCacheHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: fake}})
	req := httptest.NewRequest(http.MethodPost, "/regions/cache/refresh?store_id=7&reset=1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, fake.resetCalled)
	require.True(t, fake.refreshCalled)
	var got regionCacheHTTPResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.True(t, got.Ready)
}

func TestRegionCacheHandlerResetBusySkipsRefresh(t *testing.T) {
	fake := &fakeCacheStore{
		status:   tikv.StoreCacheStatus{Matched: 1, Failed: 1, Ready: false},
		refresh:  tikv.StoreCacheRefreshResult{Matched: 1, Updated: 1, Ready: true},
		resetErr: tikv.ErrStoreCacheRefreshBusy,
		ks:       "ks1",
		cid:      1,
	}
	h := NewRegionCacheHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: fake}})
	req := httptest.NewRequest(http.MethodPost, "/regions/cache/refresh?store_id=7&reset=1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, fake.resetCalled)
	require.False(t, fake.refreshCalled)
	var got regionCacheHTTPResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.False(t, got.Ready)
	require.Equal(t, 0, got.Failed)
	require.False(t, got.InProgress)
	require.Contains(t, got.Errors, tikv.ErrStoreCacheRefreshBusy.Error())
}

func TestRegionCacheHandlerResetRespectsCancel(t *testing.T) {
	fake := &fakeCacheStore{
		refresh: tikv.StoreCacheRefreshResult{Matched: 1, Updated: 1, Ready: true},
		ks:      "ks1",
		cid:     1,
	}
	h := NewRegionCacheHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: fake}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/regions/cache/refresh?store_id=7&reset=1", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.False(t, fake.resetCalled)
	require.False(t, fake.refreshCalled)
	var got regionCacheHTTPResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.False(t, got.Ready)
	require.Contains(t, got.Errors, context.Canceled.Error())
}

func TestRegionCacheHandlerSumsSystemAndBusinessStores(t *testing.T) {
	sys := &fakeCacheStore{
		status: tikv.StoreCacheStatus{Matched: 2, Failed: 0, Ready: false, ObservedAt: 1},
		ks:     "SYSTEM",
		cid:    11,
	}
	biz := &fakeCacheStore{
		status: tikv.StoreCacheStatus{Matched: 3, Failed: 1, Ready: false, ObservedAt: 2},
		ks:     "keyspace1",
		cid:    11,
	}
	h := NewRegionCacheHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: sys}})
	h.listed = []regionCacheStore{sys, biz}

	req := httptest.NewRequest(http.MethodGet, "/regions/cache/status?store_id=7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var got regionCacheHTTPResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, 5, got.Remaining)
	require.Equal(t, 1, got.Failed)
	require.False(t, got.Ready)
	require.Empty(t, got.Stores)

	req = httptest.NewRequest(http.MethodGet, "/regions/cache/status?store_id=7&detail=1", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Stores, 2)
	require.Equal(t, "SYSTEM", got.Stores[0].Keyspace)
	require.Equal(t, "keyspace1", got.Stores[1].Keyspace)
}

func TestRegionCacheHandlerPostContinuesAfterOneStoreFails(t *testing.T) {
	failing := &fakeCacheStore{
		refresh: tikv.StoreCacheRefreshResult{Scanned: 2, Matched: 2, Updated: 0, Failed: 2, Remaining: 2, Ready: false, Errors: []string{"still leader"}},
		ks:      "SYSTEM",
		cid:     11,
	}
	ok := &fakeCacheStore{
		refresh: tikv.StoreCacheRefreshResult{Scanned: 1, Matched: 1, Updated: 1, Failed: 0, Remaining: 0, Ready: true},
		ks:      "keyspace1",
		cid:     11,
	}
	h := NewRegionCacheHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: failing}})
	h.listed = []regionCacheStore{failing, ok}

	req := httptest.NewRequest(http.MethodPost, "/regions/cache/refresh?store_id=7&detail=1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, failing.refreshCalled)
	require.True(t, ok.refreshCalled)
	var got regionCacheHTTPResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.False(t, got.Ready)
	require.Equal(t, 2, got.Failed)
	require.Equal(t, 2, got.Remaining)
	require.Len(t, got.Stores, 2)
	require.Equal(t, "SYSTEM", got.Stores[0].Keyspace)
	require.False(t, got.Stores[0].Ready)
	require.Equal(t, "keyspace1", got.Stores[1].Keyspace)
	require.True(t, got.Stores[1].Ready)
}

func TestRegionCacheHandlerPostStopsLaterStoresAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	first := &fakeCacheStore{
		refresh: tikv.StoreCacheRefreshResult{Scanned: 1, Matched: 1, Updated: 1, Remaining: 0, Ready: true},
		ks:      "SYSTEM",
		cid:     11,
		onRefresh: func(context.Context) {
			cancel()
		},
	}
	second := &fakeCacheStore{
		refresh: tikv.StoreCacheRefreshResult{Scanned: 4, Matched: 4, Updated: 4, Remaining: 0, Ready: true},
		ks:      "keyspace1",
		cid:     11,
	}
	h := NewRegionCacheHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: first}})
	h.listed = []regionCacheStore{first, second}

	req := httptest.NewRequest(http.MethodPost, "/regions/cache/refresh?store_id=7", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, first.refreshCalled)
	require.False(t, second.refreshCalled)
	var got regionCacheHTTPResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.False(t, got.Ready)
	require.False(t, got.InProgress)
	require.Empty(t, got.Stores)
	require.Contains(t, got.Errors, context.Canceled.Error())
}

func TestRegionCacheHandlerZeroCountsKeepNotReady(t *testing.T) {
	fake := &fakeCacheStore{
		refresh: tikv.StoreCacheRefreshResult{Remaining: 0, Failed: 0, Ready: false, Errors: []string{"context canceled"}, ObservedAt: 9},
		ks:      "ks1",
		cid:     1,
	}
	h := NewRegionCacheHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: fake}})
	req := httptest.NewRequest(http.MethodPost, "/regions/cache/refresh?store_id=7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var got regionCacheHTTPResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, 0, got.Remaining)
	require.Equal(t, 0, got.Failed)
	require.False(t, got.Ready)
	require.False(t, got.InProgress)
	require.Contains(t, got.Errors, "context canceled")
	require.Equal(t, int64(9), got.ObservedAt)
}

func TestRegionCacheHandlerDiagnosticErrorsDoNotBlockReady(t *testing.T) {
	fake := &fakeCacheStore{
		refresh: tikv.StoreCacheRefreshResult{Remaining: 0, Failed: 0, Ready: true, Errors: []string{"stale probe timeout"}, ObservedAt: 11},
		ks:      "ks1",
		cid:     1,
	}
	h := NewRegionCacheHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: fake}})
	req := httptest.NewRequest(http.MethodPost, "/regions/cache/refresh?store_id=7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var got regionCacheHTTPResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, 0, got.Remaining)
	require.Equal(t, 0, got.Failed)
	require.True(t, got.Ready)
	require.Contains(t, got.Errors, "stale probe timeout")
	require.Equal(t, int64(11), got.ObservedAt)
}
