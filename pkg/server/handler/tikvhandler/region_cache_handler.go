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
	listed []regionCacheStore
}

func (h *RegionCacheHandler) cacheStores() []regionCacheStore {
	if h.listed != nil {
		return h.listed
	}
	return collectRegionCacheStores(h.Store)
}

// NewRegionCacheHandler creates a RegionCacheHandler.
func NewRegionCacheHandler(tool *handler.TikvHandlerTool) *RegionCacheHandler {
	return &RegionCacheHandler{TikvHandlerTool: tool}
}

type regionCacheStoreDetail struct {
	Keyspace   string   `json:"keyspace,omitempty"`
	ClusterID  uint64   `json:"cluster_id,omitempty"`
	Ready      bool     `json:"ready"`
	Remaining  int      `json:"remaining"`
	Failed     int      `json:"failed"`
	InProgress bool     `json:"in_progress,omitempty"`
	Errors     []string `json:"errors,omitempty"`
}

type regionCacheHTTPResult struct {
	Ready      bool                     `json:"ready"`
	Remaining  int                      `json:"remaining"`
	Failed     int                      `json:"failed"`
	InProgress bool                     `json:"in_progress,omitempty"`
	Errors     []string                 `json:"errors,omitempty"`
	Stores     []regionCacheStoreDetail `json:"stores,omitempty"`
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

func parseDetail(req *http.Request) bool {
	raw := req.URL.Query().Get("detail")
	return raw == "1" || raw == "true"
}

func (out *regionCacheHTTPResult) finish() {
	if len(out.Errors) > 8 {
		out.Errors = out.Errors[:8]
	}
	out.Ready = out.Remaining == 0 && out.Failed == 0 && !out.InProgress
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
	stores := h.cacheStores()
	if len(stores) == 0 {
		http.Error(w, "store does not support region cache refresh", http.StatusNotImplemented)
		return
	}

	detail := parseDetail(req)
	out := regionCacheHTTPResult{}
	switch req.Method {
	case http.MethodGet:
		for _, st := range stores {
			status := st.GetStoreCacheStatus(storeID)
			item := regionCacheStoreDetail{
				Keyspace:   st.GetKeyspace(),
				ClusterID:  st.GetClusterID(),
				Remaining:  status.Matched,
				Failed:     status.Failed,
				InProgress: status.InProgress,
			}
			item.Ready = item.Remaining == 0 && item.Failed == 0 && !item.InProgress
			out.Remaining += item.Remaining
			out.Failed += item.Failed
			if item.InProgress {
				out.InProgress = true
			}
			out.Errors = append(out.Errors, item.Errors...)
			if detail {
				out.Stores = append(out.Stores, item)
			}
		}
	case http.MethodPost:
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Minute)
		defer cancel()
		reset := parseReset(req)
		for _, st := range stores {
			if ctx.Err() != nil {
				out.InProgress = true
				out.Errors = append(out.Errors, ctx.Err().Error())
				break
			}
			if reset {
				if err := st.ResetStoreCacheRefresh(storeID); err != nil {
					item := regionCacheStoreDetail{
						Keyspace:  st.GetKeyspace(),
						ClusterID: st.GetClusterID(),
						Ready:     false,
						Errors:    []string{err.Error()},
					}
					out.Failed++
					out.Errors = append(out.Errors, err.Error())
					if detail {
						out.Stores = append(out.Stores, item)
					}
					continue
				}
			}
			res := st.RefreshStoreCache(ctx, storeID)
			item := regionCacheStoreDetail{
				Keyspace:  st.GetKeyspace(),
				ClusterID: st.GetClusterID(),
				Remaining: res.Remaining,
				Failed:    res.Failed,
				Errors:    res.Errors,
			}
			item.Ready = item.Remaining == 0 && item.Failed == 0
			out.Remaining += item.Remaining
			out.Failed += item.Failed
			out.Errors = append(out.Errors, item.Errors...)
			if detail {
				out.Stores = append(out.Stores, item)
			}
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	out.finish()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
