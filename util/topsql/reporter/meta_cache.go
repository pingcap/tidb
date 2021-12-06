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

package reporter

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/util/kvcache"
)

const (
	defReportMetaCacheCapacity uint  = 100000
	defReportMetaCacheTimeSecs int64 = 1 * 60 * 60
)

// ReportedMetaCache caches the reported sql/plan meta.
type ReportedMetaCache struct {
	// digest -> unix seconds timestamp
	lru     *kvcache.SimpleLRUCache
	ttlSecs int64
}

type reportMetaCacheKey []byte

func (key reportMetaCacheKey) Hash() []byte {
	return key
}

type reportMetaCacheValue struct {
	ts int64 // unix seconds timestamp
}

// NewReportedMetaCache returns a new ReportedMetaCache.
func NewReportedMetaCache(capacity uint, ttlSecs int64) *ReportedMetaCache {
	return &ReportedMetaCache{
		lru:     kvcache.NewSimpleLRUCache(capacity, 0.1, 0),
		ttlSecs: ttlSecs,
	}
}

// removeRepeatReportData removes the reported sql/plan meta data, to avoid repeated reporting.
func (mc *ReportedMetaCache) removeRepeatReportData(data *reportData) {
	now := time.Now().Unix()
	mc.removeRepeatReportMeta(data.normalizedSQLMap, now)
	mc.removeRepeatReportMeta(data.normalizedPlanMap, now)
}

func (mc *ReportedMetaCache) removeRepeatReportMeta(metaMap *sync.Map, now int64) {
	metaMap.Range(func(key, _ interface{}) bool {
		k := reportMetaCacheKey(key.(string))
		v, ok := mc.lru.Get(k)
		if !ok {
			mc.lru.Put(k, reportMetaCacheValue{ts: now})
			return true
		}
		value := v.(reportMetaCacheValue)
		if value.ts+mc.ttlSecs >= now {
			// remove reported meta data.
			metaMap.Delete(key)
		} else {
			value.ts = now
			mc.lru.Put(k, value)
		}
		return true
	})
}
