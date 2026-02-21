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
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/yalue/onnxruntime_go"
)

type dynamicSession interface {
	Run(inputs, outputs []onnxruntime_go.Value) error
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
		return nil, false
	}
	entry := value.(*sessionEntry)
	if c.ttl > 0 && c.now().Sub(entry.cachedAt) > c.ttl {
		c.cache.Delete(key)
		_ = entry.session.Destroy()
		return nil, false
	}
	return entry, true
}

// Put stores a session entry.
func (c *SessionCache) Put(key SessionKey, entry *sessionEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Put(key, entry)
}

type sessionEntry struct {
	session  dynamicSession
	cachedAt time.Time
	mu       syncutil.Mutex
}

func (s *sessionEntry) run(inputs, outputs []onnxruntime_go.Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.session.Run(inputs, outputs)
}
