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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/tikv/client-go/v2/tikv"
)

type regionCacheStore interface {
	GetStoreCacheStatus(storeID uint64) tikv.StoreCacheStatus
	RefreshStoreCache(ctx context.Context, storeID uint64) tikv.StoreCacheRefreshResult
	ResetStoreCacheRefresh(storeID uint64) error
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

type regionCacheStoreResult struct {
	Keyspace   string   `json:"keyspace,omitempty"`
	ClusterID  uint64   `json:"cluster_id,omitempty"`
	Scanned    int      `json:"scanned"`
	Matched    int      `json:"matched"`
	Updated    int      `json:"updated"`
	Failed     int      `json:"failed"`
	Remaining  int      `json:"remaining"`
	Ready      bool     `json:"ready"`
	InProgress bool     `json:"in_progress,omitempty"`
	Errors     []string `json:"errors,omitempty"`
}

type regionCacheHTTPResult struct {
	StoreID    uint64                   `json:"store_id"`
	Scanned    int                      `json:"scanned"`
	Matched    int                      `json:"matched"`
	Updated    int                      `json:"updated"`
	Failed     int                      `json:"failed"`
	Remaining  int                      `json:"remaining"`
	Ready      bool                     `json:"ready"`
	Errors     []string                 `json:"errors,omitempty"`
	ObservedAt int64                    `json:"observed_at"`
	Stores     []regionCacheStoreResult `json:"stores"`
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

func parseReset(req *http.Request) bool {
	raw := req.URL.Query().Get("reset")
	return raw == "1" || raw == "true"
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

	out := regionCacheHTTPResult{StoreID: storeID, Ready: true, Stores: make([]regionCacheStoreResult, 0, len(stores))}
	switch req.Method {
	case http.MethodGet:
		for _, st := range stores {
			status := st.GetStoreCacheStatus(storeID)
			item := regionCacheStoreResult{
				Keyspace:   st.GetKeyspace(),
				ClusterID:  st.GetClusterID(),
				Matched:    status.Matched,
				Failed:     status.Failed,
				Remaining:  status.Matched,
				Ready:      status.Ready,
				InProgress: status.InProgress,
			}
			out.Stores = append(out.Stores, item)
			out.Matched += item.Matched
			out.Failed += item.Failed
			out.Remaining += item.Remaining
			out.ObservedAt = status.ObservedAt
			if !item.Ready {
				out.Ready = false
			}
		}
	case http.MethodPost:
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Minute)
		defer cancel()
		reset := parseReset(req)
		for _, st := range stores {
			if ctx.Err() != nil {
				out.Ready = false
				out.Errors = append(out.Errors, ctx.Err().Error())
				break
			}
			if reset {
				if err := st.ResetStoreCacheRefresh(storeID); err != nil {
					item := regionCacheStoreResult{
						Keyspace:  st.GetKeyspace(),
						ClusterID: st.GetClusterID(),
						Ready:     false,
						Errors:    []string{err.Error()},
					}
					out.Stores = append(out.Stores, item)
					out.Errors = append(out.Errors, err.Error())
					out.Ready = false
					continue
				}
			}
			res := st.RefreshStoreCache(ctx, storeID)
			item := regionCacheStoreResult{
				Keyspace:  st.GetKeyspace(),
				ClusterID: st.GetClusterID(),
				Scanned:   res.Scanned,
				Matched:   res.Matched,
				Updated:   res.Updated,
				Failed:    res.Failed,
				Remaining: res.Remaining,
				Ready:     res.Ready,
				Errors:    res.Errors,
			}
			out.Stores = append(out.Stores, item)
			out.Scanned += item.Scanned
			out.Matched += item.Matched
			out.Updated += item.Updated
			out.Failed += item.Failed
			out.Remaining += item.Remaining
			out.Errors = append(out.Errors, item.Errors...)
			out.ObservedAt = res.ObservedAt
			if !item.Ready {
				out.Ready = false
			}
		}
		if len(out.Errors) > 8 {
			out.Errors = out.Errors[:8]
		}
		if out.Remaining != 0 || out.Failed != 0 {
			out.Ready = false
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
