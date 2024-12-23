// Copyright 2022 PingCAP, Inc.
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

package bindinfo

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/bindinfo/internal/logutil"
	"github.com/pingcap/tidb/pkg/bindinfo/norm"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"go.uber.org/zap"
)

// GetBindingReturnNil is only for test
var GetBindingReturnNil = stringutil.StringerStr("GetBindingReturnNil")

// GetBindingReturnNilBool is only for test
var GetBindingReturnNilBool atomic.Bool

// GetBindingReturnNilAlways is only for test
var GetBindingReturnNilAlways = stringutil.StringerStr("getBindingReturnNilAlways")

// LoadBindingNothing is only for test
var LoadBindingNothing = stringutil.StringerStr("LoadBindingNothing")

// digestBiMap represents a bidirectional map between noDBDigest and sqlDigest, used to support cross-db binding.
// One noDBDigest can map to multiple sqlDigests, but one sqlDigest can only map to one noDBDigest.
type digestBiMap interface {
	// Add adds a pair of noDBDigest and sqlDigest.
	// noDBDigest is the digest calculated after eliminating all DB names, e.g. `select * from test.t` -> `select * from t` -> noDBDigest.
	// sqlDigest is the digest where all DB names are kept, e.g. `select * from test.t` -> exactDigest.
	Add(noDBDigest, sqlDigest string)

	// Del deletes the pair of noDBDigest and sqlDigest.
	Del(sqlDigest string)

	// NoDBDigest2SQLDigest converts noDBDigest to sqlDigest.
	NoDBDigest2SQLDigest(noDBDigest string) []string

	// SQLDigest2NoDBDigest converts sqlDigest to noDBDigest.
	SQLDigest2NoDBDigest(sqlDigest string) string

	// Copy copies this digestBiMap.
	Copy() digestBiMap
}

type digestBiMapImpl struct {
	mu                   sync.RWMutex
	noDBDigest2SQLDigest map[string][]string // noDBDigest --> sqlDigests
	sqlDigest2noDBDigest map[string]string   // sqlDigest --> noDBDigest
}

func newDigestBiMap() digestBiMap {
	return &digestBiMapImpl{
		noDBDigest2SQLDigest: make(map[string][]string),
		sqlDigest2noDBDigest: make(map[string]string),
	}
}

// Add adds a pair of noDBDigest and sqlDigest.
// noDBDigest is the digest calculated after eliminating all DB names, e.g. `select * from test.t` -> `select * from t` -> noDBDigest.
// sqlDigest is the digest where all DB names are kept, e.g. `select * from test.t` -> exactDigest.
func (b *digestBiMapImpl) Add(noDBDigest, sqlDigest string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.noDBDigest2SQLDigest[noDBDigest] = append(b.noDBDigest2SQLDigest[noDBDigest], sqlDigest)
	b.sqlDigest2noDBDigest[sqlDigest] = noDBDigest
}

// Del deletes the pair of noDBDigest and sqlDigest.
func (b *digestBiMapImpl) Del(sqlDigest string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	noDBDigest, ok := b.sqlDigest2noDBDigest[sqlDigest]
	if !ok {
		return
	}
	digestList := b.noDBDigest2SQLDigest[noDBDigest]
	for i := range digestList { // remove sqlDigest from this list
		if digestList[i] == sqlDigest {
			// Deleting binding is a low-frequently operation, so the O(n) performance is enough.
			digestList = append(digestList[:i], digestList[i+1:]...)
			break
		}
	}
	if len(digestList) == 0 {
		delete(b.noDBDigest2SQLDigest, noDBDigest)
	} else {
		b.noDBDigest2SQLDigest[noDBDigest] = digestList
	}
	delete(b.sqlDigest2noDBDigest, sqlDigest)
}

// NoDBDigest2SQLDigest converts noDBDigest to sqlDigest.
func (b *digestBiMapImpl) NoDBDigest2SQLDigest(noDBDigest string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.noDBDigest2SQLDigest[noDBDigest]
}

// SQLDigest2NoDBDigest converts sqlDigest to noDBDigest.
func (b *digestBiMapImpl) SQLDigest2NoDBDigest(sqlDigest string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.sqlDigest2noDBDigest[sqlDigest]
}

// Copy copies this digestBiMap.
func (b *digestBiMapImpl) Copy() digestBiMap {
	b.mu.RLock()
	defer b.mu.RUnlock()
	noDBDigest2SQLDigest := make(map[string][]string, len(b.noDBDigest2SQLDigest))
	for k, list := range b.noDBDigest2SQLDigest {
		newList := make([]string, len(list))
		copy(newList, list)
		noDBDigest2SQLDigest[k] = newList
	}
	sqlDigest2noDBDigest := make(map[string]string, len(b.sqlDigest2noDBDigest))
	for k, v := range b.sqlDigest2noDBDigest {
		sqlDigest2noDBDigest[k] = v
	}
	return &digestBiMapImpl{
		noDBDigest2SQLDigest: noDBDigest2SQLDigest,
		sqlDigest2noDBDigest: sqlDigest2noDBDigest,
	}
}

// BindingCache is the interface for the cache of the SQL plan bindings.
type BindingCache interface {
	// MatchingBinding supports cross-db matching on bindings.
	MatchingBinding(sctx sessionctx.Context, noDBDigest string, tableNames []*ast.TableName) (bindings Binding, isMatched bool)
	// GetBinding gets the binding for the specified sqlDigest.
	GetBinding(sqlDigest string) Bindings
	// GetAllBindings gets all the bindings in the cache.
	GetAllBindings() Bindings
	// SetBinding sets the binding for the specified sqlDigest.
	SetBinding(sqlDigest string, meta Bindings) (err error)
	// RemoveBinding removes the binding for the specified sqlDigest.
	RemoveBinding(sqlDigest string)
	// SetMemCapacity sets the memory capacity for the cache.
	SetMemCapacity(capacity int64)
	// GetMemUsage gets the memory usage of the cache.
	GetMemUsage() int64
	// GetMemCapacity gets the memory capacity of the cache.
	GetMemCapacity() int64
	// Size returns the number of items in the cache.
	Size() int
}

// bindingCache uses the LRU cache to store the bindings.
// The key of the LRU cache is original sql, the value is a slice of Bindings.
// Note: The bindingCache should be accessed with lock.
type bindingCache struct {
	lock        sync.RWMutex
	digestBiMap digestBiMap             // mapping between noDBDigest and sqlDigest, used to support cross-db binding.
	cache       *kvcache.SimpleLRUCache // the underlying cache to store the bindings.
	memCapacity int64
	memTracker  *memory.Tracker // track memory usage.
	// TODO: use the underlying cache structure to track and control the memory usage, and remove
	// memCapacity and memTracker to simplify the code.

	// loadBindingFromStorageFunc is used to load binding from storage if cache miss.
	loadBindingFromStorageFunc func(sctx sessionctx.Context, sqlDigest string) (Bindings, error)
}

type bindingCacheKey string

func (key bindingCacheKey) Hash() []byte {
	return hack.Slice(string(key))
}

func calcBindCacheKVMem(key bindingCacheKey, value Bindings) int64 {
	var valMem int64
	valMem += int64(value.size())
	return int64(len(key.Hash())) + valMem
}

func newBindCache(bindingLoad func(sctx sessionctx.Context, sqlDigest string) (Bindings, error)) BindingCache {
	// since bindingCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	cache := kvcache.NewSimpleLRUCache(mathutil.MaxUint, 0, 0)
	c := bindingCache{
		cache:                      cache,
		digestBiMap:                newDigestBiMap(),
		memCapacity:                variable.MemQuotaBindingCache.Load(),
		memTracker:                 memory.NewTracker(memory.LabelForBindCache, -1),
		loadBindingFromStorageFunc: bindingLoad,
	}
	return &c
}

// get gets a cache item according to cache key. It's not thread-safe.
// Note: Only other functions of the bindingCache file can use this function.
// Don't use this function directly in other files in bindinfo package.
// The return value is not read-only, but it is only can be used in other functions which are also in the bind_cache.go.
func (c *bindingCache) get(key bindingCacheKey) Bindings {
	value, hit := c.cache.Get(key)
	if !hit {
		return nil
	}
	typedValue := value.(Bindings)
	return typedValue
}

// set inserts an item to the cache. It's not thread-safe.
// Only other functions of the bindingCache can use this function.
// The set operation will return error message when the memory usage of binding_cache exceeds its capacity.
func (c *bindingCache) set(key bindingCacheKey, value Bindings) (ok bool, err error) {
	mem := calcBindCacheKVMem(key, value)
	if mem > c.memCapacity { // ignore this kv pair if its size is too large
		err = errors.New("The memory usage of all available bindings exceeds the cache's mem quota. As a result, all available bindings cannot be held on the cache. Please increase the value of the system variable 'tidb_mem_quota_binding_cache' and execute 'admin reload bindings' to ensure that all bindings exist in the cache and can be used normally")
		return
	}
	bindings := c.get(key)
	if bindings != nil {
		// Remove the origin key-value pair.
		mem -= calcBindCacheKVMem(key, bindings)
	}
	for mem+c.memTracker.BytesConsumed() > c.memCapacity {
		err = errors.New("The memory usage of all available bindings exceeds the cache's mem quota. As a result, all available bindings cannot be held on the cache. Please increase the value of the system variable 'tidb_mem_quota_binding_cache' and execute 'admin reload bindings' to ensure that all bindings exist in the cache and can be used normally")
		evictedKey, evictedValue, evicted := c.cache.RemoveOldest()
		if !evicted {
			return
		}
		c.memTracker.Consume(-calcBindCacheKVMem(evictedKey.(bindingCacheKey), evictedValue.(Bindings)))
	}
	c.memTracker.Consume(mem)
	c.cache.Put(key, value)
	ok = true
	return
}

// delete remove an item from the cache. It's not thread-safe.
// Only other functions of the bindingCache can use this function.
func (c *bindingCache) delete(key bindingCacheKey) bool {
	bindings := c.get(key)
	if bindings != nil {
		mem := calcBindCacheKVMem(key, bindings)
		c.cache.Delete(key)
		c.memTracker.Consume(-mem)
		return true
	}
	return false
}

func (c *bindingCache) shouldMetric() bool {
	return c.loadBindingFromStorageFunc != nil // only metric for GlobalBindingCache, whose loadBindingFromStorageFunc is not nil.
}

func (c *bindingCache) MatchingBinding(sctx sessionctx.Context, noDBDigest string, tableNames []*ast.TableName) (matchedBinding Binding, isMatched bool) {
	matchedBinding, isMatched, missingSQLDigest := c.getFromMemory(sctx, noDBDigest, tableNames)
	if len(missingSQLDigest) == 0 {
		if c.shouldMetric() && isMatched {
			metrics.BindingCacheHitCounter.Inc()
		}
		return
	}
	if c.shouldMetric() {
		metrics.BindingCacheMissCounter.Inc()
	}
	if c.loadBindingFromStorageFunc == nil {
		return
	}
	c.loadFromStore(sctx, missingSQLDigest) // loadFromStore's SetBinding has a Mutex inside, so it's safe to call it without lock
	matchedBinding, isMatched, _ = c.getFromMemory(sctx, noDBDigest, tableNames)
	return
}

func (c *bindingCache) getFromMemory(sctx sessionctx.Context, noDBDigest string, tableNames []*ast.TableName) (matchedBinding Binding, isMatched bool, missingSQLDigest []string) {
	if c.Size() == 0 {
		return
	}
	leastWildcards := len(tableNames) + 1
	enableCrossDBBinding := sctx.GetSessionVars().EnableFuzzyBinding
	for _, sqlDigest := range c.digestBiMap.NoDBDigest2SQLDigest(noDBDigest) {
		bindings := c.GetBinding(sqlDigest)
		if intest.InTest {
			if sctx.Value(GetBindingReturnNil) != nil {
				if GetBindingReturnNilBool.CompareAndSwap(false, true) {
					bindings = nil
				}
			}
			if sctx.Value(GetBindingReturnNilAlways) != nil {
				bindings = nil
			}
		}
		if bindings != nil {
			for _, binding := range bindings {
				numWildcards, matched := crossDBMatchBindingTableName(sctx.GetSessionVars().CurrentDB, tableNames, binding.TableNames)
				if matched && numWildcards > 0 && sctx != nil && !enableCrossDBBinding {
					continue // cross-db binding is disabled, skip this binding
				}
				if matched && numWildcards < leastWildcards {
					matchedBinding = binding
					isMatched = true
					leastWildcards = numWildcards
					break
				}
			}
		} else {
			missingSQLDigest = append(missingSQLDigest, sqlDigest)
		}
	}
	return matchedBinding, isMatched, missingSQLDigest
}

func (c *bindingCache) loadFromStore(sctx sessionctx.Context, missingSQLDigest []string) {
	if intest.InTest && sctx.Value(LoadBindingNothing) != nil {
		return
	}
	defer func(start time.Time) {
		sctx.GetSessionVars().StmtCtx.AppendWarning(errors.New("loading binding from storage takes " + time.Since(start).String()))
	}(time.Now())

	for _, sqlDigest := range missingSQLDigest {
		start := time.Now()
		bindings, err := c.loadBindingFromStorageFunc(sctx, sqlDigest)
		if err != nil {
			logutil.BindLogger().Warn("failed to load binding from storage",
				zap.String("sqlDigest", sqlDigest),
				zap.Error(err),
				zap.Duration("duration", time.Since(start)),
			)
			continue
		}
		// put binding into the cache
		oldBinding := c.GetBinding(sqlDigest)
		newBindings := removeDeletedBindings(merge(oldBinding, bindings))
		if len(newBindings) > 0 {
			err = c.SetBinding(sqlDigest, newBindings)
			if err != nil {
				// When the memory capacity of bing_cache is not enough,
				// there will be some memory-related errors in multiple places.
				// Only needs to be handled once.
				logutil.BindLogger().Warn("update binding cache error", zap.Error(err))
			}
		}
	}
}

// GetBinding gets the Bindings from the cache.
// The return value is not read-only, but it shouldn't be changed in the caller functions.
// The function is thread-safe.
func (c *bindingCache) GetBinding(sqlDigest string) Bindings {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.get(bindingCacheKey(sqlDigest))
}

// GetAllBindings return all the bindings from the bindingCache.
// The return value is not read-only, but it shouldn't be changed in the caller functions.
// The function is thread-safe.
func (c *bindingCache) GetAllBindings() Bindings {
	c.lock.RLock()
	defer c.lock.RUnlock()
	values := c.cache.Values()
	bindings := make(Bindings, 0, len(values))
	for _, vals := range values {
		bindings = append(bindings, vals.(Bindings)...)
	}
	return bindings
}

// SetBinding sets the Bindings to the cache.
// The function is thread-safe.
func (c *bindingCache) SetBinding(sqlDigest string, bindings Bindings) (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// prepare noDBDigests for all bindings
	noDBDigests := make([]string, 0, len(bindings))
	p := parser.New()
	for _, binding := range bindings {
		stmt, err := p.ParseOneStmt(binding.BindSQL, binding.Charset, binding.Collation)
		if err != nil {
			return err
		}
		_, noDBDigest := norm.NormalizeStmtForBinding(stmt, norm.WithoutDB(true))
		noDBDigests = append(noDBDigests, noDBDigest)
	}

	for i, binding := range bindings {
		c.digestBiMap.Add(noDBDigests[i], binding.SQLDigest)
	}

	// NOTE: due to LRU eviction, the underlying BindingCache state might be inconsistent with noDBDigest2SQLDigest and
	// sqlDigest2noDBDigest, but it's acceptable, just return cache-miss in that case.
	cacheKey := bindingCacheKey(sqlDigest)
	_, err = c.set(cacheKey, bindings)
	return
}

// RemoveBinding removes the Bindings which has same originSQL with specified Bindings.
// The function is thread-safe.
func (c *bindingCache) RemoveBinding(sqlDigest string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.digestBiMap.Del(sqlDigest)
	c.delete(bindingCacheKey(sqlDigest))
}

// SetMemCapacity sets the memory capacity for the cache.
// The function is thread-safe.
func (c *bindingCache) SetMemCapacity(capacity int64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	// Only change the capacity size without affecting the cached bindings
	c.memCapacity = capacity
}

// GetMemUsage get the memory Usage for the cache.
// The function is thread-safe.
func (c *bindingCache) GetMemUsage() int64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.memTracker.BytesConsumed()
}

// GetMemCapacity get the memory capacity for the cache.
// The function is thread-safe.
func (c *bindingCache) GetMemCapacity() int64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.memCapacity
}

func (c *bindingCache) Size() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.cache.Size()
}
