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

// CrossDBBindingCache is based on BindingCache, and provide some more advanced features, like
// cross-db matching, loading binding if cache miss automatically (TODO).
type CrossDBBindingCache interface {
	// MatchingBinding supports cross-db matching on bindings.
	MatchingBinding(sctx sessionctx.Context, noDBDigest string, tableNames []*ast.TableName) (bindings Binding, isMatched bool)

	// Copy copies this cache.
	Copy() (c CrossDBBindingCache, err error)

	BindingCache
}

type crossDBBindingCache struct {
	BindingCache

	mu sync.RWMutex

	// noDBDigest2SQLDigest is used to support cross-db matching.
	// noDBDigest is the digest calculated after eliminating all DB names, e.g. `select * from test.t` -> `select * from t` -> noDBDigest.
	// sqlDigest is the digest where all DB names are kept, e.g. `select * from test.t` -> exactDigest.
	noDBDigest2SQLDigest map[string][]string // noDBDigest --> sqlDigests

	sqlDigest2noDBDigest map[string]string // sqlDigest --> noDBDigest

	// loadBindingFromStorageFunc is used to load binding from storage if cache miss.
	loadBindingFromStorageFunc func(sctx sessionctx.Context, sqlDigest string) (Bindings, error)
}

func newCrossDBBindingCache(loadBindingFromStorageFunc func(sessionctx.Context, string) (Bindings, error)) CrossDBBindingCache {
	return &crossDBBindingCache{
		BindingCache:               newBindCache(),
		noDBDigest2SQLDigest:       make(map[string][]string),
		sqlDigest2noDBDigest:       make(map[string]string),
		loadBindingFromStorageFunc: loadBindingFromStorageFunc,
	}
}

func (cc *crossDBBindingCache) shouldMetric() bool {
	return cc.loadBindingFromStorageFunc != nil // only metric for GlobalBindingCache, whose loadBindingFromStorageFunc is not nil.
}

func (cc *crossDBBindingCache) MatchingBinding(sctx sessionctx.Context, noDBDigest string, tableNames []*ast.TableName) (matchedBinding Binding, isMatched bool) {
	matchedBinding, isMatched, missingSQLDigest := cc.getFromMemory(sctx, noDBDigest, tableNames)
	if len(missingSQLDigest) == 0 {
		if cc.shouldMetric() && isMatched {
			metrics.BindingCacheHitCounter.Inc()
		}
		return
	}
	if cc.shouldMetric() {
		metrics.BindingCacheMissCounter.Inc()
	}
	if cc.loadBindingFromStorageFunc == nil {
		return
	}
	cc.loadFromStore(sctx, missingSQLDigest) // loadFromStore's SetBinding has a Mutex inside, so it's safe to call it without lock
	matchedBinding, isMatched, _ = cc.getFromMemory(sctx, noDBDigest, tableNames)
	return
}

func (cc *crossDBBindingCache) getFromMemory(sctx sessionctx.Context, noDBDigest string, tableNames []*ast.TableName) (matchedBinding Binding, isMatched bool, missingSQLDigest []string) {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	bindingCache := cc.BindingCache
	if bindingCache.Size() == 0 {
		return
	}
	leastWildcards := len(tableNames) + 1
	enableCrossDBBinding := sctx.GetSessionVars().EnableFuzzyBinding
	for _, sqlDigest := range cc.noDBDigest2SQLDigest[noDBDigest] {
		bindings := bindingCache.GetBinding(sqlDigest)
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

func (cc *crossDBBindingCache) loadFromStore(sctx sessionctx.Context, missingSQLDigest []string) {
	if intest.InTest && sctx.Value(LoadBindingNothing) != nil {
		return
	}
	defer func(start time.Time) {
		sctx.GetSessionVars().StmtCtx.AppendWarning(errors.New("loading binding from storage takes " + time.Since(start).String()))
	}(time.Now())

	for _, sqlDigest := range missingSQLDigest {
		start := time.Now()
		bindings, err := cc.loadBindingFromStorageFunc(sctx, sqlDigest)
		if err != nil {
			logutil.BindLogger().Warn("failed to load binding from storage",
				zap.String("sqlDigest", sqlDigest),
				zap.Error(err),
				zap.Duration("duration", time.Since(start)),
			)
			continue
		}
		// put binding into the cache
		oldBinding := cc.GetBinding(sqlDigest)
		newBindings := removeDeletedBindings(merge(oldBinding, bindings))
		if len(newBindings) > 0 {
			err = cc.SetBinding(sqlDigest, newBindings)
			if err != nil {
				// When the memory capacity of bing_cache is not enough,
				// there will be some memory-related errors in multiple places.
				// Only needs to be handled once.
				logutil.BindLogger().Warn("update binding cache error", zap.Error(err))
			}
		}
	}
}

func (cc *crossDBBindingCache) SetBinding(sqlDigest string, bindings Bindings) (err error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

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
		cc.noDBDigest2SQLDigest[noDBDigests[i]] = append(cc.noDBDigest2SQLDigest[noDBDigests[i]], binding.SQLDigest)
		cc.sqlDigest2noDBDigest[binding.SQLDigest] = noDBDigests[i]
	}
	// NOTE: due to LRU eviction, the underlying BindingCache state might be inconsistent with noDBDigest2SQLDigest and
	// sqlDigest2noDBDigest, but it's acceptable, just return cache-miss in that case.
	return cc.BindingCache.SetBinding(sqlDigest, bindings)
}

func (cc *crossDBBindingCache) RemoveBinding(sqlDigest string) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	noDBDigest, ok := cc.sqlDigest2noDBDigest[sqlDigest]
	if !ok {
		return
	}
	digestList := cc.noDBDigest2SQLDigest[noDBDigest]
	for i := range digestList { // remove sqlDigest from this list
		if digestList[i] == sqlDigest {
			digestList = append(digestList[:i], digestList[i+1:]...)
			break
		}
	}
	cc.noDBDigest2SQLDigest[noDBDigest] = digestList
	delete(cc.sqlDigest2noDBDigest, sqlDigest)
	cc.BindingCache.RemoveBinding(sqlDigest)
}

func (cc *crossDBBindingCache) Copy() (c CrossDBBindingCache, err error) {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	bc, err := cc.BindingCache.CopyBindingCache()
	if err != nil {
		return nil, err
	}
	sql2noDBDigest := make(map[string]string, len(cc.sqlDigest2noDBDigest))
	for k, v := range cc.sqlDigest2noDBDigest {
		sql2noDBDigest[k] = v
	}
	noDBDigest2SQLDigest := make(map[string][]string, len(cc.noDBDigest2SQLDigest))
	for k, list := range cc.noDBDigest2SQLDigest {
		newList := make([]string, len(list))
		copy(newList, list)
		noDBDigest2SQLDigest[k] = newList
	}
	return &crossDBBindingCache{
		BindingCache:               bc,
		noDBDigest2SQLDigest:       noDBDigest2SQLDigest,
		sqlDigest2noDBDigest:       sql2noDBDigest,
		loadBindingFromStorageFunc: cc.loadBindingFromStorageFunc,
	}, nil
}

// BindingCache is the interface for the cache of the SQL plan bindings.
type BindingCache interface {
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
	// CopyBindingCache copies the cache.
	CopyBindingCache() (newCache BindingCache, err error)
	// Size returns the number of items in the cache.
	Size() int
}

// bindingCache uses the LRU cache to store the bindings.
// The key of the LRU cache is original sql, the value is a slice of Bindings.
// Note: The bindingCache should be accessed with lock.
type bindingCache struct {
	lock        sync.Mutex
	cache       *kvcache.SimpleLRUCache
	memCapacity int64
	memTracker  *memory.Tracker // track memory usage.
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

func newBindCache() BindingCache {
	// since bindingCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	cache := kvcache.NewSimpleLRUCache(mathutil.MaxUint, 0, 0)
	c := bindingCache{
		cache:       cache,
		memCapacity: variable.MemQuotaBindingCache.Load(),
		memTracker:  memory.NewTracker(memory.LabelForBindCache, -1),
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

// GetBinding gets the Bindings from the cache.
// The return value is not read-only, but it shouldn't be changed in the caller functions.
// The function is thread-safe.
func (c *bindingCache) GetBinding(sqlDigest string) Bindings {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.get(bindingCacheKey(sqlDigest))
}

// GetAllBindings return all the bindings from the bindingCache.
// The return value is not read-only, but it shouldn't be changed in the caller functions.
// The function is thread-safe.
func (c *bindingCache) GetAllBindings() Bindings {
	c.lock.Lock()
	defer c.lock.Unlock()
	values := c.cache.Values()
	bindings := make(Bindings, 0, len(values))
	for _, vals := range values {
		bindings = append(bindings, vals.(Bindings)...)
	}
	return bindings
}

// SetBinding sets the Bindings to the cache.
// The function is thread-safe.
func (c *bindingCache) SetBinding(sqlDigest string, meta Bindings) (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	cacheKey := bindingCacheKey(sqlDigest)
	_, err = c.set(cacheKey, meta)
	return
}

// RemoveBinding removes the Bindings which has same originSQL with specified Bindings.
// The function is thread-safe.
func (c *bindingCache) RemoveBinding(sqlDigest string) {
	c.lock.Lock()
	defer c.lock.Unlock()
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

// CopyBindingCache copies a new bindingCache from the origin cache.
// The function is thread-safe.
func (c *bindingCache) CopyBindingCache() (BindingCache, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	var err error
	newCache := newBindCache().(*bindingCache)
	if c.memTracker.BytesConsumed() > newCache.GetMemCapacity() {
		err = errors.New("The memory usage of all available bindings exceeds the cache's mem quota. As a result, all available bindings cannot be held on the cache. Please increase the value of the system variable 'tidb_mem_quota_binding_cache' and execute 'admin reload bindings' to ensure that all bindings exist in the cache and can be used normally")
	}
	keys := c.cache.Keys()
	for _, key := range keys {
		cacheKey := key.(bindingCacheKey)
		v := c.get(cacheKey)
		if _, err := newCache.set(cacheKey, v); err != nil {
			return nil, err
		}
	}
	return newCache, err
}

func (c *bindingCache) Size() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.cache.Size()
}
