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
	"slices"
	"sort"
	"sync"
	"time"

	infoschema_metrics "github.com/pingcap/tidb/pkg/infoschema/metrics"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/store/helper"
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

	// emptySchemaVersions stores schema version which has no schema_diff.
	emptySchemaVersions map[int64]struct{}

	r    autoid.Requirement
	Data *Data

	// first known schema version records the first known schema version, all schemas between [firstKnownSchemaVersion, latest)
	// are known as long as we keep the DDL history correctly.
	firstKnownSchemaVersion int64

	lastCheckVersion int64
	lastCheckTime    time.Time
}

type schemaAndTimestamp struct {
	infoschema InfoSchema
	timestamp  int64
}

// NewCache creates a new InfoCache.
func NewCache(r autoid.Requirement, capacity int) *InfoCache {
	infoData := NewData()
	return &InfoCache{
		cache:               make([]schemaAndTimestamp, 0, capacity),
		emptySchemaVersions: make(map[int64]struct{}),
		r:                   r,
		Data:                infoData,
	}
}

// GetAndResetRecentInfoSchemaTS provides the min start ts for infosync.InfoSyncer.
// It works like this:
//
//	There is a background infosync worker calling ReportMinStartTS() function periodically.
//	At the beginning of each round, the Data.recentMinTS here is reset to current TS.
//	If InfoSchemaV2 APIs are called, there is an internal keepAlive() function will also be called.
//	The keepAlive() function will compare the InfoSchemaV2's ts with Data.recentMinTS, and
//	update the Data.recentMinTS to smaller one.
//
// In a nutshell, every round of ReportMinStartTS(), the minimal known TS used by InfoSchemaV2 APIs will be reported.
// Some corner cases might happen: the caller take an InfoSchemaV2 instance and not use it immediately.
// Seveval rounds later, that InfoSchema is used and its TS is reported to block GC safepoint advancing.
// But that's too late, the GC has been done, "GC life time is shorter than transaction duration" error still happen.
func (h *InfoCache) GetAndResetRecentInfoSchemaTS(now uint64) uint64 {
	ret := h.Data.recentMinTS.Load()
	h.Data.recentMinTS.Store(now)
	return ret
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

// Upsert is Resert and Insert combined, used during infoschema v1 v2 switch.
func (h *InfoCache) Upsert(is InfoSchema, schemaTS uint64) func() {
	h.mu.Lock()
	defer h.mu.Unlock()

	old := h.cache
	h.cache = make([]schemaAndTimestamp, 0, cap(h.cache))
	h.cache = append(h.cache, schemaAndTimestamp{
		infoschema: is,
		timestamp:  int64(schemaTS),
	})
	h.firstKnownSchemaVersion = is.SchemaMetaVersion()

	return func() {
		// TODO: It's a bit tricky here, somewhere is holding the reference of the old infoschema.
		// So GC can not release this object, leading to memory leak.
		// Here we destroy the old infoschema on purpose, so someone use it would panic and
		// we get to know where it is referenced.
		for _, oldItem := range old {
			switch raw := oldItem.infoschema.(type) {
			case *infoSchema:
				*raw = infoSchema{}
			case *infoschemaV2:
				*raw = infoschemaV2{}
			}
		}
		logutil.BgLogger().Info("reset the old infoschema after v1 v2 switch, using the stale object will panic")
	}
}

// GetLatest gets the newest information schema.
func (h *InfoCache) GetLatest() InfoSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()
	infoschema_metrics.GetLatestCounter.Inc()
	if len(h.cache) > 0 {
		infoschema_metrics.HitLatestCounter.Inc()
		ret := h.cache[0].infoschema
		return ret
	}
	return nil
}

// Len returns the size of the cache
func (h *InfoCache) Len() int {
	return len(h.cache)
}

// GetEmptySchemaVersions returns emptySchemaVersions, exports for testing.
func (h *InfoCache) GetEmptySchemaVersions() map[int64]struct{} {
	return h.emptySchemaVersions
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

		if uint64(h.cache[i-1].timestamp) > ts {
			// The first condition is to make sure the cache[i-1].timestamp > ts >= cache[i].timestamp, then the current schema is suitable for ts.
			lastVersion := h.cache[i-1].infoschema.SchemaMetaVersion()
			currentVersion := is.infoschema.SchemaMetaVersion()
			if lastVersion == currentVersion+1 {
				// This condition is to make sure the schema version is continuous. If last(cache[i-1]) schema-version is 10,
				// but current(cache[i]) schema-version is not 9, then current schema may not suitable for ts.
				return is.infoschema, true
			}
			if lastVersion > currentVersion {
				found := true
				for ver := currentVersion + 1; ver < lastVersion; ver++ {
					_, ok := h.emptySchemaVersions[ver]
					if !ok {
						found = false
						break
					}
				}
				if found {
					// This condition is to make sure the schema version is continuous. If last(cache[i-1]) schema-version is 10, and
					// current(cache[i]) schema-version is 8, then there is a gap exist, and if all the gap version can be found in cache.emptySchemaVersions
					// which means those gap versions don't have schema info, then current schema is also suitable for ts.
					return is.infoschema, true
				}
			}
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

	// `GetByVersion` is allowed to load the latest schema that is less than argument
	// `version` when the argument `version` <= the latest schema version.
	// if `version` > the latest schema version, always return nil, loadInfoSchema
	// will use this behavior to decide whether to load schema diffs or full reload.
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
	// upsert is a full reset of InfoCache, after upsert, the DDL history might lost and the assumption does not hold anymore.
	// For example:
	//     Before
	//              infoschema 51
	//              infoschema 52
	//              infoschema 53
	//              infoschema 54
	//              infoschema 55
	//              infoschema 56
	//     After Upsert()
	//              infoschema 56
	//     Then load historial snapshot version 51
	//              infoschema 51
	//              infoschema 56
	// Now, request for schema version 55, return infoschem 51 would be wrong!
	//
	if i == len(h.cache) {
		return nil
	}

	if h.cache[i].infoschema.SchemaMetaVersion() == version ||
		(i != 0 && h.cache[i].infoschema.SchemaMetaVersion() >= h.firstKnownSchemaVersion) {
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

const gcCheckInterval = 128

// Insert will **TRY** to insert the infoschema into the cache.
// It only promised to cache the newest infoschema.
// It returns 'true' if it is cached, 'false' otherwise.
// schemaTs is the commitTs of the txn creates the schema diff, which indicates since when the schema version is taking effect
func (h *InfoCache) Insert(is InfoSchema, schemaTS uint64) bool {
	logutil.BgLogger().Debug("INSERT SCHEMA", zap.Uint64("schema ts", schemaTS), zap.Int64("schema version", is.SchemaMetaVersion()))
	h.mu.Lock()
	defer h.mu.Unlock()

	version := is.SchemaMetaVersion()
	if h.lastCheckVersion == 0 {
		h.lastCheckVersion = version
		h.lastCheckTime = time.Now()
	} else if version > h.lastCheckVersion+gcCheckInterval && time.Since(h.lastCheckTime) > time.Minute {
		h.lastCheckVersion = version
		h.lastCheckTime = time.Now()
		go h.gcOldVersion()
	}

	// assume this is the timestamp order as well
	i := sort.Search(len(h.cache), func(i int) bool {
		return h.cache[i].infoschema.SchemaMetaVersion() <= version
	})

	// cached entry
	if i < len(h.cache) && h.cache[i].infoschema.SchemaMetaVersion() == version {
		xisV2, _ := IsV2(h.cache[i].infoschema)
		yisV2, _ := IsV2(is)
		if xisV2 == yisV2 {
			// update timestamp if it is not 0 and cached one is 0
			if schemaTS > 0 && h.cache[i].timestamp == 0 {
				h.cache[i].timestamp = int64(schemaTS)
			} else if xisV2 {
				// update infoschema if it's infoschema v2
				h.cache[i].infoschema = is
			}
			return true
		}

		// replace the old with the new one
		h.cache[i].infoschema = is
	}

	if len(h.cache) < cap(h.cache) {
		// has free space, grown the slice
		h.cache = h.cache[:len(h.cache)+1]
		copy(h.cache[i+1:], h.cache[i:])
		h.cache[i] = schemaAndTimestamp{
			infoschema: is,
			timestamp:  int64(schemaTS),
		}
		if len(h.cache) == 1 {
			h.firstKnownSchemaVersion = is.SchemaMetaVersion()
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

// InsertEmptySchemaVersion inserts empty schema version into a map. If exceeded the cache capacity, remove the oldest version.
func (h *InfoCache) InsertEmptySchemaVersion(version int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.emptySchemaVersions[version] = struct{}{}
	if len(h.emptySchemaVersions) > cap(h.cache) {
		// remove oldest version.
		versions := make([]int64, 0, len(h.emptySchemaVersions))
		for ver := range h.emptySchemaVersions {
			versions = append(versions, ver)
		}
		slices.Sort(versions)
		for _, ver := range versions {
			delete(h.emptySchemaVersions, ver)
			if len(h.emptySchemaVersions) <= cap(h.cache) {
				break
			}
		}
	}
}

func (h *InfoCache) gcOldVersion() {
	tikvStore, ok := h.r.Store().(helper.Storage)
	if !ok {
		return
	}

	newHelper := helper.NewHelper(tikvStore)
	version, err := meta.GetOldestSchemaVersion(newHelper)
	if err != nil {
		logutil.BgLogger().Warn("failed to GC old schema version", zap.Error(err))
		return
	}
	start := time.Now()
	deleted, total := h.Data.GCOldVersion(version)
	logutil.BgLogger().Info("GC compact old schema version",
		zap.Int64("current version", h.lastCheckVersion),
		zap.Int64("oldest version", version),
		zap.Int("deleted", deleted),
		zap.Int64("total", total),
		zap.Duration("takes", time.Since(start)))
}
