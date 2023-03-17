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
	"errors"
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

func (h *InfoCache) getSchemaByTimestampNoLock(ts uint64) (InfoSchema, error) {
	logutil.BgLogger().Debug("SCHEMA CACHE get schema", zap.Uint64("timestamp", ts))
	i := sort.Search(len(h.cache), func(i int) bool {
		return uint64(h.cache[i].timestamp) <= ts
	})
	// if the request timestamp is earlier then the oldest schema, then it is a cache miss
	if i < len(h.cache) {
		return h.cache[i].infoschema, nil
	}

	logutil.BgLogger().Debug("SCHEMA CACHE no schema found")
	return nil, errors.New("no cached schema")
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
// If the snapshotTS is new than maxUpdatedSnapshotTS, that's mean it can directly use
// the latest infoschema. otherwise, will return nil.
func (h *InfoCache) GetBySnapshotTS(snapshotTS uint64) InfoSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()

	infoschema_metrics.GetTSCounter.Inc()
	if schema, err := h.getSchemaByTimestampNoLock(snapshotTS); err == nil {
		infoschema_metrics.HitTSCounter.Inc()
		return schema
	}
	return nil
}

// Insert will **TRY** to insert the infoschema into the cache.
// It only promised to cache the newest infoschema.
// It returns 'true' if it is cached, 'false' otherwise.
// snapshotTS is the timestap of the schema update, schemaTS is the timestamp of the schema taking effect
func (h *InfoCache) Insert(is InfoSchema, snapshotTS, schemaTS uint64) bool {
	logutil.BgLogger().Debug("INSERT SCHEMA", zap.Uint64("snapshot ts", snapshotTS), zap.Uint64("schema ts", schemaTS))
	h.mu.Lock()
	defer h.mu.Unlock()

	version := is.SchemaMetaVersion()

	// assume this is the timestamp order as well
	i := sort.Search(len(h.cache), func(i int) bool {
		return h.cache[i].infoschema.SchemaMetaVersion() <= version
	})

	// cached entry
	if i < len(h.cache) && h.cache[i].infoschema.SchemaMetaVersion() == version {
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
