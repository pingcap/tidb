// Copyright 2026 PingCAP, Inc.

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
	status      tikv.StoreCacheStatus
	refresh     tikv.StoreCacheRefreshResult
	ks          string
	cid         uint64
	resetCalled bool
}

func (f *fakeCacheStore) GetStoreCacheStatus(storeID uint64) tikv.StoreCacheStatus {
	f.status.StoreID = storeID
	return f.status
}

func (f *fakeCacheStore) RefreshStoreCache(ctx context.Context, storeID uint64) tikv.StoreCacheRefreshResult {
	_ = ctx
	f.refresh.StoreID = storeID
	return f.refresh
}

func (f *fakeCacheStore) ResetStoreCacheRefresh(storeID uint64) {
	_ = storeID
	f.resetCalled = true
	f.status.Failed = 0
	f.refresh.Failed = 0
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
	require.Equal(t, uint64(7), got.StoreID)
	require.Equal(t, 3, got.Matched)
	require.False(t, got.Ready)
	require.Len(t, got.Stores, 1)
	require.Equal(t, "ks1", got.Stores[0].Keyspace)
	require.Equal(t, uint64(99), got.Stores[0].ClusterID)

	req = httptest.NewRequest(http.MethodPost, "/regions/cache/refresh?store_id=7", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, 2, got.Updated)
	require.Equal(t, 1, got.Remaining)
	require.Len(t, got.Stores, 1)
	require.Equal(t, 2, got.Stores[0].Updated)

	req = httptest.NewRequest(http.MethodGet, "/regions/cache/status", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)

	req = httptest.NewRequest(http.MethodPut, "/regions/cache/status?store_id=7", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestRegionCacheHandlerKeepsPerStoreIdentity(t *testing.T) {
	primary := &fakeCacheStore{
		status:  tikv.StoreCacheStatus{Matched: 2, Failed: 1, Ready: false, ObservedAt: 1},
		refresh: tikv.StoreCacheRefreshResult{Scanned: 2, Matched: 2, Updated: 1, Failed: 1, Remaining: 1, Ready: false, ObservedAt: 2},
		ks:      "ks1",
		cid:     11,
	}
	h := NewRegionCacheHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: primary}})

	req := httptest.NewRequest(http.MethodGet, "/regions/cache/status?store_id=7", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var got regionCacheHTTPResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, 2, got.Matched)
	require.Equal(t, 1, got.Failed)
	require.False(t, got.Ready)
	require.Len(t, got.Stores, 1)
	require.Equal(t, "ks1", got.Stores[0].Keyspace)
	require.Equal(t, uint64(11), got.Stores[0].ClusterID)
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
	var got regionCacheHTTPResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, 1, got.Matched)
	require.Equal(t, 0, got.Updated)

	req = httptest.NewRequest(http.MethodPost, "/regions/cache/refresh?store_id=7", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, 1, got.Updated)
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
	var got regionCacheHTTPResult
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.True(t, got.Ready)
}
