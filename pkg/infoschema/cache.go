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

	infoschema_metrics "github.com/pingcap/tidb/pkg/infoschema/metrics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// InfoCache handles information schema, including getting and setting.
// The cache behavior, however, is transparent and under automatic management.
// It only promised to cache the infoschema, if it is newer than all the cached.
type InfoCache struct {
	mu sync.RWMutex
	// cache is sorted by both SchemaVersion and timestamp in descending order, assume they have same order
	cache []schemaAndTimestamp
}

type schemaAndTimestamp struct {
	infoschema InfoSchema
	timestamp  int64
}

// NewCache creates a new InfoCache.
func NewCache(capacity int) *InfoCache {
	return &InfoCache{
		cache: make([]schemaAndTimestamp, 0, capacity),
	}
}

// ReSize re-size the cache.
func (h *InfoCache) ReSize(capacity int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cap(h.cache) == capacity {
		return
	}
	oldCache := h.cache
	h.cache = make([]schemaAndTimestamp, 0, capacity)
	for i, v := range oldCache {
		if i >= capacity {
			break
		}
		h.cache = append(h.cache, v)
	}
}

// Size returns the size of the cache, export for test.
func (h *InfoCache) Size() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.cache)
}

// Reset resets the cache.
func (h *InfoCache) Reset(capacity int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cache = make([]schemaAndTimestamp, 0, capacity)
}

// GetLatest gets the newest information schema.
func (h *InfoCache) GetLatest() InfoSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()
	infoschema_metrics.GetLatestCounter.Inc()
	if len(h.cache) > 0 {
		infoschema_metrics.HitLatestCounter.Inc()
		return h.cache[0].infoschema
	}
	return nil
}

// Len returns the size of the cache
func (h *InfoCache) Len() int {
	return len(h.cache)
}

func (h *InfoCache) getSchemaByTimestampNoLock(ts uint64) (InfoSchema, bool) {
	logutil.BgLogger().Debug("SCHEMA CACHE get schema", zap.Uint64("timestamp", ts))
	// search one by one instead of binary search, because the timestamp of a schema could be 0
	// this is ok because the size of h.cache is small (currently set to 16)
	// moreover, the most likely hit element in the array is the first one in steady mode
	// thus it may have better performance than binary search
	for i, is := range h.cache {
		if is.timestamp == 0 || ts < uint64(is.timestamp) {
			// is.timestamp == 0 means the schema ts is unknown, so we can't use it, then just skip it.
			// ts < is.timestamp means the schema is newer than ts, so we can't use it too, just skip it to find the older one.
			continue
		}
		// ts >= is.timestamp must be true after the above condition.
		if i == 0 {
			// the first element is the latest schema, so we can return it directly.
			return is.infoschema, true
		}
		if h.cache[i-1].infoschema.SchemaMetaVersion() == is.infoschema.SchemaMetaVersion()+1 && uint64(h.cache[i-1].timestamp) > ts {
			// This first condition is to make sure the schema version is continuous. If last(cache[i-1]) schema-version is 10,
			// but current(cache[i]) schema-version is not 9, then current schema is not suitable for ts.
			// The second condition is to make sure the cache[i-1].timestamp > ts >= cache[i].timestamp, then the current schema is suitable for ts.
			return is.infoschema, true
		}
		// current schema is not suitable for ts, then break the loop to avoid the unnecessary search.
		break
	}

	logutil.BgLogger().Debug("SCHEMA CACHE no schema found")
	return nil, false
}

// GetByVersion gets the information schema based on schemaVersion. Returns nil if it is not loaded.
func (h *InfoCache) GetByVersion(version int64) InfoSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.getByVersionNoLock(version)
}

func (h *InfoCache) getByVersionNoLock(version int64) InfoSchema {
	infoschema_metrics.GetVersionCounter.Inc()
	i := sort.Search(len(h.cache), func(i int) bool {
		return h.cache[i].infoschema.SchemaMetaVersion() <= version
	})

	// `GetByVersion` is allowed to load the latest schema that is less than argument `version`.
	// Consider cache has values [10, 9, _, _, 6, 5, 4, 3, 2, 1], version 8 and 7 is empty because of the diff is empty.
	// If we want to get version 8, we can return version 6 because v7 and v8 do not change anything, they are totally the same,
	// in this case the `i` will not be 0.
	// If i == 0, it means the argument version is `10`, or greater than `10`, if `version` is 10
	// `h.cache[i].SchemaMetaVersion() == version` will be true, so we can return the latest schema, return nil if not.
	// The following code is equivalent to:
	// ```
	//		if h.GetLatest().SchemaMetaVersion() < version {
	//			return nil
	//		}
	//
	//		if i < len(h.cache) {
	//			hitVersionCounter.Inc()
	//			return h.cache[i]
	//		}
	// ```

	if i < len(h.cache) && (i != 0 || h.cache[i].infoschema.SchemaMetaVersion() == version) {
		infoschema_metrics.HitVersionCounter.Inc()
		return h.cache[i].infoschema
	}
	return nil
}

// GetBySnapshotTS gets the information schema based on snapshotTS.
// It searches the schema cache and find the schema with max schema ts that equals or smaller than given snapshot ts
// Where the schema ts is the commitTs of the txn creates the schema diff
func (h *InfoCache) GetBySnapshotTS(snapshotTS uint64) InfoSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()

	infoschema_metrics.GetTSCounter.Inc()
	if schema, ok := h.getSchemaByTimestampNoLock(snapshotTS); ok {
		infoschema_metrics.HitTSCounter.Inc()
		return schema
	}
	return nil
}

// Insert will **TRY** to insert the infoschema into the cache.
// It only promised to cache the newest infoschema.
// It returns 'true' if it is cached, 'false' otherwise.
// schemaTs is the commitTs of the txn creates the schema diff, which indicates since when the schema version is taking effect
func (h *InfoCache) Insert(is InfoSchema, schemaTS uint64) bool {
	logutil.BgLogger().Debug("INSERT SCHEMA", zap.Uint64("schema ts", schemaTS), zap.Int64("schema version", is.SchemaMetaVersion()))
	h.mu.Lock()
	defer h.mu.Unlock()

	version := is.SchemaMetaVersion()

	// assume this is the timestamp order as well
	i := sort.Search(len(h.cache), func(i int) bool {
		return h.cache[i].infoschema.SchemaMetaVersion() <= version
	})

	// cached entry
	if i < len(h.cache) && h.cache[i].infoschema.SchemaMetaVersion() == version {
		// update timestamp if it is not 0 and cached one is 0
		if schemaTS > 0 && h.cache[i].timestamp == 0 {
			h.cache[i].timestamp = int64(schemaTS)
		}
		return true
	}

	if len(h.cache) < cap(h.cache) {
		// has free space, grown the slice
		h.cache = h.cache[:len(h.cache)+1]
		copy(h.cache[i+1:], h.cache[i:])
		h.cache[i] = schemaAndTimestamp{
			infoschema: is,
			timestamp:  int64(schemaTS),
		}
	} else if i < len(h.cache) {
		// drop older schema
		copy(h.cache[i+1:], h.cache[i:])
		h.cache[i] = schemaAndTimestamp{
			infoschema: is,
			timestamp:  int64(schemaTS),
		}
	} else {
		// older than all cached schemas, refuse to cache it
		return false
	}

	return true
}
