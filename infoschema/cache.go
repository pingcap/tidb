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

	infoschema_metrics "github.com/pingcap/tidb/infoschema/metrics"
	"github.com/pingcap/tidb/util/logutil"
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
		if is.timestamp == 0 || (i > 0 && h.cache[i-1].infoschema.SchemaMetaVersion() != is.infoschema.SchemaMetaVersion()+1) {
			// the schema version doesn't have a timestamp or there is a gap in the schema cache
			// ignore all the schema cache equals or less than this version in search by timestamp
			break
		}
		if ts >= uint64(is.timestamp) {
			// found the largest version before the given ts
			return is.infoschema, true
		}
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
