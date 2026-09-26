// Copyright 2026 PingCAP, Inc.
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

package expression

import (
	"sync"

	"github.com/pingcap/tidb/pkg/util/kvcache"
)

// sqlDigestTextCacheCapacity bounds the number of digest-to-text pairs kept in the
// cache. A digest is the hash of the normalized SQL text, so the mapping is immutable
// and entries never need invalidation. A typical entry is a 64-byte digest plus a
// normalized statement of a few hundred bytes, so this capacity keeps the cache in
// the tens of megabytes.
const sqlDigestTextCacheCapacity = 100000

// sqlDigestCacheKey implements kvcache.Key for a SQL digest.
type sqlDigestCacheKey string

func (k sqlDigestCacheKey) Hash() []byte {
	return []byte(k)
}

// digestTextCache is a process-wide cache of digest-to-normalized-text pairs.
// Digest reverse lookups (`tidb_decode_sql_digests` and the digest text columns of
// transaction, deadlock and lock-wait system tables) run a retrieval per evaluation,
// and periodic workloads resolve the same digests over and over; the cache lets
// repeated lookups skip statement summary queries entirely, including the history
// file reads when statement summary persistence is enabled.
type digestTextCache struct {
	mu    sync.Mutex
	cache *kvcache.SimpleLRUCache
}

var sqlDigestTextCache = newDigestTextCache(sqlDigestTextCacheCapacity)

func newDigestTextCache(capacity uint) *digestTextCache {
	return &digestTextCache{cache: kvcache.NewSimpleLRUCache(capacity, 0, 0)}
}

func (c *digestTextCache) get(digest string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.cache.Get(sqlDigestCacheKey(digest))
	if !ok {
		return "", false
	}
	text, ok := value.(string)
	return text, ok
}

func (c *digestTextCache) put(digest, text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Put(sqlDigestCacheKey(digest), text)
}

// lookupSQLDigestTextCache fills the unresolved digests of `sqlDigestsMap` from the
// cache and reports whether every digest is resolved afterwards.
func lookupSQLDigestTextCache(sqlDigestsMap map[string]string) (allResolved bool) {
	allResolved = true
	for digest, text := range sqlDigestsMap {
		if len(text) > 0 {
			continue
		}
		if cached, ok := sqlDigestTextCache.get(digest); ok && len(cached) > 0 {
			sqlDigestsMap[digest] = cached
			continue
		}
		allResolved = false
	}
	return allResolved
}

// storeSQLDigestTextCache puts the resolved digests of `sqlDigestsMap` into the cache.
func storeSQLDigestTextCache(sqlDigestsMap map[string]string) {
	for digest, text := range sqlDigestsMap {
		if len(text) > 0 {
			sqlDigestTextCache.put(digest, text)
		}
	}
}
