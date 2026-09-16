// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package tikvhandler

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/tikv/client-go/v2/tikv"
)

type regionCacheStore interface {
	GetStoreCacheStatus(storeID uint64) tikv.StoreCacheStatus
	RefreshStoreCache(ctx context.Context, storeID uint64) tikv.StoreCacheRefreshResult
	GetClusterID() uint64
	GetKeyspace() string
}

// RegionCacheHandler serves GET /regions/cache/status and POST /regions/cache/refresh.
type RegionCacheHandler struct {
	*handler.TikvHandlerTool
}

// NewRegionCacheHandler creates a RegionCacheHandler.
func NewRegionCacheHandler(tool *handler.TikvHandlerTool) *RegionCacheHandler {
	return &RegionCacheHandler{tool}
}

type regionCacheHTTPResult struct {
	StoreID    uint64   `json:"store_id"`
	Keyspace   string   `json:"keyspace,omitempty"`
	ClusterID  uint64   `json:"cluster_id,omitempty"`
	Scanned    int      `json:"scanned"`
	Matched    int      `json:"matched"`
	Updated    int      `json:"updated"`
	Remaining  int      `json:"remaining"`
	Ready      bool     `json:"ready"`
	Errors     []string `json:"errors,omitempty"`
	ObservedAt int64    `json:"observed_at"`
}

func parseStoreID(req *http.Request) (uint64, error) {
	raw := req.URL.Query().Get("store_id")
	if raw == "" {
		return 0, errors.New("store_id is required")
	}
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		return 0, errors.New("store_id is invalid")
	}
	return id, nil
}

func collectRegionCacheStores(primary kv.Storage) []regionCacheStore {
	seen := map[string]struct{}{}
	var out []regionCacheStore
	add := func(s kv.Storage) {
		st, ok := s.(regionCacheStore)
		if !ok {
			return
		}
		key := st.GetKeyspace() + "/" + strconv.FormatUint(st.GetClusterID(), 10)
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		out = append(out, st)
	}
	add(primary)
	for _, s := range driver.ListCachedStores() {
		add(s)
	}
	return out
}

func (h *RegionCacheHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	storeID, err := parseStoreID(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stores := collectRegionCacheStores(h.Store)
	if len(stores) == 0 {
		http.Error(w, "store does not support region cache refresh", http.StatusNotImplemented)
		return
	}

	var out regionCacheHTTPResult
	out.StoreID = storeID
	switch req.Method {
	case http.MethodGet:
		ready := true
		for _, st := range stores {
			status := st.GetStoreCacheStatus(storeID)
			out.Matched += status.Matched
			out.Remaining += status.Matched
			out.ObservedAt = status.ObservedAt
			out.ClusterID = st.GetClusterID()
			if st.GetKeyspace() != "" && out.Keyspace == "" {
				out.Keyspace = st.GetKeyspace()
			}
			if !status.Ready {
				ready = false
			}
		}
		out.Ready = ready
	case http.MethodPost:
		ready := true
		for _, st := range stores {
			res := st.RefreshStoreCache(req.Context(), storeID)
			out.Scanned += res.Scanned
			out.Matched += res.Matched
			out.Updated += res.Updated
			out.Remaining += res.Remaining
			out.Errors = append(out.Errors, res.Errors...)
			out.ObservedAt = res.ObservedAt
			out.ClusterID = st.GetClusterID()
			if st.GetKeyspace() != "" && out.Keyspace == "" {
				out.Keyspace = st.GetKeyspace()
			}
			if !res.Ready {
				ready = false
			}
		}
		if len(out.Errors) > 8 {
			out.Errors = out.Errors[:8]
		}
		out.Ready = ready && out.Remaining == 0
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
