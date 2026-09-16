// Copyright 2026 PingCAP, Inc.
package tikvhandler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

type fakeCacheStore struct {
	helper.Storage
	status  tikv.StoreCacheStatus
	refresh tikv.StoreCacheRefreshResult
	ks      string
	cid     uint64
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
	require.Equal(t, "ks1", got.Keyspace)

	req = httptest.NewRequest(http.MethodPost, "/regions/cache/refresh?store_id=7", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, 2, got.Updated)
	require.Equal(t, 1, got.Remaining)

	req = httptest.NewRequest(http.MethodGet, "/regions/cache/status", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}
