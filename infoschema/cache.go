// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package infoschema

import (
	"sort"
	"sync"

	"github.com/pingcap/tidb/metrics"
)

// InfoCache handles information schema, including getting and setting.
// The cache behavior, however, is transparent and under automatic management.
// It only promised to cache the infoschema, if it is newer than all the cached.
type InfoCache struct {
	mu sync.RWMutex
	// cache is sorted by SchemaVersion in descending order
	cache []InfoSchema
}

// NewCache creates a new InfoCache.
func NewCache(capcity int) *InfoCache {
	return &InfoCache{cache: make([]InfoSchema, 0, capcity)}
}

// GetLatest gets the newest information schema.
func (h *InfoCache) GetLatest() InfoSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()
	metrics.InfoCacheCounters.WithLabelValues("get").Inc()
	if len(h.cache) > 0 {
		metrics.InfoCacheCounters.WithLabelValues("hit").Inc()
		return h.cache[0]
	}
	return nil
}

// GetByVersion gets the information schema based on schemaVersion. Returns nil if it is not loaded.
func (h *InfoCache) GetByVersion(version int64) InfoSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()
	metrics.InfoCacheCounters.WithLabelValues("get").Inc()
	i := sort.Search(len(h.cache), func(i int) bool {
		return h.cache[i].SchemaMetaVersion() <= version
	})
	if i < len(h.cache) && h.cache[i].SchemaMetaVersion() == version {
		metrics.InfoCacheCounters.WithLabelValues("hit").Inc()
		return h.cache[i]
	}
	return nil
}

// Insert will **TRY** to insert the infoschema into the cache.
// It only promised to cache the newest infoschema.
// It returns 'true' if it is cached, 'false' otherwise.
func (h *InfoCache) Insert(is InfoSchema) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	version := is.SchemaMetaVersion()
	i := sort.Search(len(h.cache), func(i int) bool {
		return h.cache[i].SchemaMetaVersion() <= version
	})

	// cached entry
	if i < len(h.cache) && h.cache[i].SchemaMetaVersion() == version {
		return true
	}

	if len(h.cache) < cap(h.cache) {
		// has free space, grown the slice
		h.cache = h.cache[:len(h.cache)+1]
		copy(h.cache[i+1:], h.cache[i:])
		h.cache[i] = is
		return true
	} else if i < len(h.cache) {
		// drop older schema
		copy(h.cache[i+1:], h.cache[i:])
		h.cache[i] = is
		return true
	}
	// older than all cached schemas, refuse to cache it
	return false
}
