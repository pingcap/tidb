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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package infoschema

import (
	"sort"
	"sync"

	"github.com/pingcap/tidb/metrics"
)

var (
	getLatestCounter  = metrics.InfoCacheCounters.WithLabelValues("get", "latest")
	getTSCounter      = metrics.InfoCacheCounters.WithLabelValues("get", "ts")
	getVersionCounter = metrics.InfoCacheCounters.WithLabelValues("get", "version")

	hitLatestCounter  = metrics.InfoCacheCounters.WithLabelValues("hit", "latest")
	hitTSCounter      = metrics.InfoCacheCounters.WithLabelValues("hit", "ts")
	hitVersionCounter = metrics.InfoCacheCounters.WithLabelValues("hit", "version")
)

// InfoCache handles information schema, including getting and setting.
// The cache behavior, however, is transparent and under automatic management.
// It only promised to cache the infoschema, if it is newer than all the cached.
type InfoCache struct {
	mu sync.RWMutex
	// cache is sorted by SchemaVersion in descending order
	cache []InfoSchema
	// record SnapshotTS of the latest schema Insert.
	maxUpdatedSnapshotTS uint64
}

// NewCache creates a new InfoCache.
func NewCache(capcity int) *InfoCache {
	return &InfoCache{cache: make([]InfoSchema, 0, capcity)}
}

// GetLatest gets the newest information schema.
func (h *InfoCache) GetLatest() InfoSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()
	getLatestCounter.Inc()
	if len(h.cache) > 0 {
		hitLatestCounter.Inc()
		return h.cache[0]
	}
	return nil
}

// GetByVersion gets the information schema based on schemaVersion. Returns nil if it is not loaded.
func (h *InfoCache) GetByVersion(version int64) InfoSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()
	getVersionCounter.Inc()
	i := sort.Search(len(h.cache), func(i int) bool {
		return h.cache[i].SchemaMetaVersion() <= version
	})
	if i < len(h.cache) && h.cache[i].SchemaMetaVersion() == version {
		hitVersionCounter.Inc()
		return h.cache[i]
	}
	return nil
}

// GetBySnapshotTS gets the information schema based on snapshotTS.
// If the snapshotTS is new than maxUpdatedSnapshotTS, that's mean it can directly use
// the latest infoschema. otherwise, will return nil.
func (h *InfoCache) GetBySnapshotTS(snapshotTS uint64) InfoSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()

	getTSCounter.Inc()
	if snapshotTS >= h.maxUpdatedSnapshotTS {
		if len(h.cache) > 0 {
			hitTSCounter.Inc()
			return h.cache[0]
		}
	}
	return nil
}

// Insert will **TRY** to insert the infoschema into the cache.
// It only promised to cache the newest infoschema.
// It returns 'true' if it is cached, 'false' otherwise.
func (h *InfoCache) Insert(is InfoSchema, snapshotTS uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	version := is.SchemaMetaVersion()
	i := sort.Search(len(h.cache), func(i int) bool {
		return h.cache[i].SchemaMetaVersion() <= version
	})

	if h.maxUpdatedSnapshotTS < snapshotTS {
		h.maxUpdatedSnapshotTS = snapshotTS
	}

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
