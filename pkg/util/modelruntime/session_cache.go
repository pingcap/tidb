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

package modelruntime

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/yalue/onnxruntime_go"
)

type dynamicSession interface {
	Run(inputs, outputs []onnxruntime_go.Value) error
	RunWithOptions(inputs, outputs []onnxruntime_go.Value, opts *onnxruntime_go.RunOptions) error
	Destroy() error
}

// SessionCacheOptions controls the process-level session cache.
type SessionCacheOptions struct {
	Capacity uint
	TTL      time.Duration
	Now      func() time.Time
}

// SessionKey identifies an ONNX runtime session.
type SessionKey string

// Hash implements kvcache.Key.
func (k SessionKey) Hash() []byte {
	return []byte(k)
}

// SessionKeyFromParts builds a cache key from model identity and IO names.
func SessionKeyFromParts(modelID, version int64, inputNames, outputNames []string) SessionKey {
	return SessionKey(fmt.Sprintf("%d:%d:%s:%s", modelID, version, strings.Join(inputNames, ","), strings.Join(outputNames, ",")))
}

// SessionCache is a process-level cache for ONNX runtime sessions.
type SessionCache struct {
	cache *kvcache.SimpleLRUCache
	mu    syncutil.Mutex
	ttl   time.Duration
	now   func() time.Time
}

const defaultSessionCacheCapacity = 64

var processSessionCache = NewSessionCache(SessionCacheOptions{Capacity: defaultSessionCacheCapacity})

// GetProcessSessionCache returns the process-level session cache.
func GetProcessSessionCache() *SessionCache {
	return processSessionCache
}

// SetProcessSessionCache replaces the process-level session cache (used in tests).
func SetProcessSessionCache(cache *SessionCache) {
	processSessionCache = cache
}

// NewSessionCache constructs a new session cache.
func NewSessionCache(opts SessionCacheOptions) *SessionCache {
	if opts.Capacity == 0 {
		panic("model session cache capacity must be positive")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	cache := kvcache.NewSimpleLRUCache(opts.Capacity, 0, 0)
	sc := &SessionCache{
		cache: cache,
		ttl:   opts.TTL,
		now:   now,
	}
	cache.SetOnEvict(func(_ kvcache.Key, value kvcache.Value) {
		entry := value.(*sessionEntry)
		_ = entry.session.Destroy()
		metrics.ModelSessionCacheCounter.WithLabelValues("evict").Inc()
	})
	return sc
}

// GetOrCreate returns a cached session or creates a new one.
func (c *SessionCache) GetOrCreate(key SessionKey, create func() (dynamicSession, error)) (*sessionEntry, error) {
	if c == nil {
		return nil, nil
	}
	if entry, ok := c.Get(key); ok {
		return entry, nil
	}
	session, err := create()
	if err != nil {
		return nil, err
	}
	entry := &sessionEntry{session: session, cachedAt: c.now()}
	c.Put(key, entry)
	return entry, nil
}

// Get returns a cached session entry.
func (c *SessionCache) Get(key SessionKey) (*sessionEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.cache.Get(key)
	if !ok {
		metrics.ModelSessionCacheCounter.WithLabelValues("miss").Inc()
		return nil, false
	}
	entry := value.(*sessionEntry)
	if c.ttl > 0 && c.now().Sub(entry.cachedAt) > c.ttl {
		c.cache.Delete(key)
		_ = entry.session.Destroy()
		metrics.ModelSessionCacheCounter.WithLabelValues("miss").Inc()
		metrics.ModelSessionCacheCounter.WithLabelValues("evict").Inc()
		return nil, false
	}
	metrics.ModelSessionCacheCounter.WithLabelValues("hit").Inc()
	return entry, true
}

// Put stores a session entry.
func (c *SessionCache) Put(key SessionKey, entry *sessionEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Put(key, entry)
}

// SessionCacheSnapshotEntry captures metadata about a cached session.
type SessionCacheSnapshotEntry struct {
	ModelID     int64
	Version     int64
	InputNames  []string
	OutputNames []string
	CachedAt    time.Time
	TTL         time.Duration
	ExpiresAt   *time.Time
}

// SnapshotEntries returns a point-in-time snapshot of cached sessions.
func (c *SessionCache) SnapshotEntries() []SessionCacheSnapshotEntry {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := c.cache.Keys()
	values := c.cache.Values()
	if len(keys) == 0 {
		return nil
	}
	now := c.now()
	entries := make([]SessionCacheSnapshotEntry, 0, len(keys))
	for i, key := range keys {
		sessionKey, ok := key.(SessionKey)
		if !ok {
			continue
		}
		entry, ok := values[i].(*sessionEntry)
		if !ok {
			continue
		}
		if c.ttl > 0 && now.Sub(entry.cachedAt) > c.ttl {
			continue
		}
		modelID, version, inputs, outputs, ok := parseSessionKey(sessionKey)
		if !ok {
			continue
		}
		snapshot := SessionCacheSnapshotEntry{
			ModelID:     modelID,
			Version:     version,
			InputNames:  inputs,
			OutputNames: outputs,
			CachedAt:    entry.cachedAt,
			TTL:         c.ttl,
		}
		if c.ttl > 0 {
			expiresAt := entry.cachedAt.Add(c.ttl)
			snapshot.ExpiresAt = &expiresAt
		}
		entries = append(entries, snapshot)
	}
	return entries
}

func parseSessionKey(key SessionKey) (modelID int64, version int64, inputs []string, outputs []string, ok bool) {
	parts := strings.SplitN(string(key), ":", 4)
	if len(parts) != 4 {
		return 0, 0, nil, nil, false
	}
	var err error
	modelID, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, nil, nil, false
	}
	version, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, nil, nil, false
	}
	inputs = splitSessionNames(parts[2])
	outputs = splitSessionNames(parts[3])
	return modelID, version, inputs, outputs, true
}

func splitSessionNames(names string) []string {
	if names == "" {
		return nil
	}
	return strings.Split(names, ",")
}

type sessionEntry struct {
	session  dynamicSession
	cachedAt time.Time
	mu       syncutil.Mutex
}

func (s *sessionEntry) run(inputs, outputs []onnxruntime_go.Value, opts *onnxruntime_go.RunOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if opts != nil {
		return s.session.RunWithOptions(inputs, outputs, opts)
	}
	return s.session.Run(inputs, outputs)
}
