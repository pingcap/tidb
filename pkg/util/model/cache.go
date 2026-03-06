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

package model

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/syncutil"
)

// CacheOptions controls the process-level artifact cache.
type CacheOptions struct {
	Capacity uint
	TTL      time.Duration
	Now      func() time.Time
}

// ArtifactCache is a process-level cache for model artifacts.
type ArtifactCache struct {
	cache *kvcache.SimpleLRUCache
	mu    syncutil.Mutex
	ttl   time.Duration
	now   func() time.Time
}

// NewArtifactCache constructs a new process-level cache.
func NewArtifactCache(opts CacheOptions) *ArtifactCache {
	if opts.Capacity == 0 {
		panic("model artifact cache capacity must be positive")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	cache := kvcache.NewSimpleLRUCache(opts.Capacity, 0, 0)
	c := &ArtifactCache{
		cache: cache,
		ttl:   opts.TTL,
		now:   now,
	}
	cache.SetOnEvict(func(_ kvcache.Key, value kvcache.Value) {
		entry := value.(cacheEntry)
		cleanupLocalArtifact(entry.artifact)
	})
	return c
}

// StatementCache is a statement-local cache for artifacts.
type StatementCache struct {
	entries map[cacheKey]Artifact
}

// NewStatementCache constructs a statement-local cache.
func NewStatementCache() *StatementCache {
	return &StatementCache{entries: make(map[cacheKey]Artifact)}
}

// Get returns an artifact from the statement cache.
func (c *StatementCache) Get(meta ArtifactMeta) (Artifact, bool) {
	if c == nil {
		return Artifact{}, false
	}
	artifact, ok := c.entries[newCacheKey(meta)]
	return artifact, ok
}

// Put stores an artifact in the statement cache.
func (c *StatementCache) Put(meta ArtifactMeta, artifact Artifact) {
	if c == nil {
		return
	}
	c.entries[newCacheKey(meta)] = artifact
}

// GetOrLoad returns an artifact from the process cache or loads it.
func (c *ArtifactCache) GetOrLoad(ctx context.Context, meta ArtifactMeta, loader ArtifactLoader) (Artifact, error) {
	if c != nil {
		if artifact, ok := c.Get(meta); ok {
			return artifact, nil
		}
	}
	artifact, err := loader.Load(ctx, meta)
	if err != nil {
		return Artifact{}, err
	}
	if c != nil {
		c.Put(meta, artifact)
	}
	return artifact, nil
}

// Get returns an artifact from the process cache.
func (c *ArtifactCache) Get(meta ArtifactMeta) (Artifact, bool) {
	if c == nil {
		return Artifact{}, false
	}
	key := newCacheKey(meta)
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.cache.Get(key)
	if !ok {
		return Artifact{}, false
	}
	entry := value.(cacheEntry)
	if c.ttl > 0 && c.now().Sub(entry.cachedAt) > c.ttl {
		c.cache.Delete(key)
		cleanupLocalArtifact(entry.artifact)
		return Artifact{}, false
	}
	return entry.artifact, true
}

// Put stores an artifact in the process cache.
func (c *ArtifactCache) Put(meta ArtifactMeta, artifact Artifact) {
	if c == nil {
		return
	}
	key := newCacheKey(meta)
	c.mu.Lock()
	defer c.mu.Unlock()
	if oldValue, ok := c.cache.Get(key); ok {
		oldEntry := oldValue.(cacheEntry)
		if oldEntry.artifact.LocalPath != artifact.LocalPath {
			cleanupLocalArtifact(oldEntry.artifact)
		}
	}
	c.cache.Put(key, cacheEntry{artifact: artifact, cachedAt: c.now()})
}

// GetArtifact fetches using statement cache, then process cache, then loader.
func GetArtifact(ctx context.Context, meta ArtifactMeta, stmtCache *StatementCache, procCache *ArtifactCache, loader ArtifactLoader) (Artifact, error) {
	if stmtCache != nil {
		if artifact, ok := stmtCache.Get(meta); ok {
			return artifact, nil
		}
	}
	artifact, err := procCache.GetOrLoad(ctx, meta, loader)
	if err != nil {
		return Artifact{}, err
	}
	if stmtCache != nil {
		stmtCache.Put(meta, artifact)
	}
	return artifact, nil
}

type cacheKey struct {
	modelID int64
	version int64
}

func newCacheKey(meta ArtifactMeta) cacheKey {
	return cacheKey{modelID: meta.ModelID, version: meta.Version}
}

// Hash implements kvcache.Key.
func (k cacheKey) Hash() []byte {
	return []byte(fmt.Sprintf("%d:%d", k.modelID, k.version))
}

type cacheEntry struct {
	artifact Artifact
	cachedAt time.Time
}

func cleanupLocalArtifact(artifact Artifact) {
	if artifact.LocalPath == "" {
		return
	}
	_ = os.RemoveAll(artifact.LocalPath)
}
