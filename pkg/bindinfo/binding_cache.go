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
	"sync"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

// digestBiMap represents a bidirectional map between noDBDigest and sqlDigest, used to support cross-db binding.
// One noDBDigest can map to multiple sqlDigests, but one sqlDigest can only map to one noDBDigest.
type digestBiMap interface {
	// Add adds a pair of noDBDigest and sqlDigest.
	// noDBDigest is the digest calculated after eliminating all DB names, e.g. `select * from test.t` -> `select * from t` -> noDBDigest.
	// sqlDigest is the digest where all DB names are kept, e.g. `select * from test.t` -> exactDigest.
	Add(noDBDigest, sqlDigest string)

	// Del deletes the pair of noDBDigest and sqlDigest.
	Del(sqlDigest string)

	// All returns all the sqlDigests.
	All() (sqlDigests []string)

	// NoDBDigest2SQLDigest converts noDBDigest to sqlDigest.
	NoDBDigest2SQLDigest(noDBDigest string) []string

	// SQLDigest2NoDBDigest converts sqlDigest to noDBDigest.
	SQLDigest2NoDBDigest(sqlDigest string) string
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

// All returns all the sqlDigests.
func (b *digestBiMapImpl) All() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	sqlDigests := make([]string, 0, len(b.sqlDigest2noDBDigest))
	for sqlDigest := range b.sqlDigest2noDBDigest {
		sqlDigests = append(sqlDigests, sqlDigest)
	}
	return sqlDigests
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

// BindingCache is the interface for the cache of the SQL plan bindings.
type BindingCache interface {
	// MatchingBinding supports cross-db matching on bindings.
	MatchingBinding(sctx sessionctx.Context, noDBDigest string, tableNames []*ast.TableName) (binding *Binding, isMatched bool)
	// GetBinding gets the binding for the specified sqlDigest.
	GetBinding(sqlDigest string) *Binding
	// GetAllBindings gets all the bindings in the cache.
	GetAllBindings() []*Binding
	// SetBinding sets the binding for the specified sqlDigest.
	SetBinding(sqlDigest string, binding *Binding) (err error)
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
	// Close closes the cache.
	Close()
}

// bindingCache uses the LRU cache to store the bindings.
// The key of the LRU cache is original sql, the value is a slice of Bindings.
// Note: The bindingCache should be accessed with lock.
type bindingCache struct {
	digestBiMap digestBiMap      // mapping between noDBDigest and sqlDigest, used to support cross-db binding.
	cache       *ristretto.Cache // the underlying cache to store the bindings.
}

func newBindCache() BindingCache {
	cache, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e6,
		MaxCost:     vardef.MemQuotaBindingCache.Load(),
		BufferItems: 64,
		Cost: func(value any) int64 {
			return int64(value.(*Binding).size())
		},
		Metrics:            true,
		IgnoreInternalCost: true,
	})
	c := bindingCache{
		cache:       cache,
		digestBiMap: newDigestBiMap(),
	}
	return &c
}

func (c *bindingCache) MatchingBinding(sctx sessionctx.Context, noDBDigest string, tableNames []*ast.TableName) (matchedBinding *Binding, isMatched bool) {
	if c.Size() == 0 {
		return
	}
	possibleBindings := make([]*Binding, 0, 2)
	for _, sqlDigest := range c.digestBiMap.NoDBDigest2SQLDigest(noDBDigest) {
		binding := c.GetBinding(sqlDigest)
		if binding != nil {
			possibleBindings = append(possibleBindings, binding)
		}
		// TODO: handle cache miss safely.
	}
	matchedBinding, isMatched = crossDBMatchBindings(sctx, tableNames, possibleBindings)
	return
}

// GetBinding gets the Bindings from the cache.
// The return value is not read-only, but it shouldn't be changed in the caller functions.
// The function is thread-safe.
func (c *bindingCache) GetBinding(sqlDigest string) *Binding {
	v, ok := c.cache.Get(sqlDigest)
	if !ok {
		return nil
	}
	return v.(*Binding)
}

// GetAllBindings return all the bindings from the bindingCache.
// The return value is not read-only, but it shouldn't be changed in the caller functions.
// The function is thread-safe.
func (c *bindingCache) GetAllBindings() []*Binding {
	sqlDigests := c.digestBiMap.All()
	bindings := make([]*Binding, 0, len(sqlDigests))
	for _, sqlDigest := range sqlDigests {
		bindings = append(bindings, c.GetBinding(sqlDigest))
	}
	return bindings
}

// SetBinding sets the Bindings to the cache.
// The function is thread-safe.
func (c *bindingCache) SetBinding(sqlDigest string, binding *Binding) (err error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(binding.BindSQL, binding.Charset, binding.Collation)
	if err != nil {
		return err
	}
	_, noDBDigest := NormalizeStmtForBinding(stmt, WithoutDB(true))
	c.digestBiMap.Add(noDBDigest, sqlDigest)
	// NOTE: due to LRU eviction, the underlying BindingCache state might be inconsistent with digestBiMap,
	// but it's acceptable, the optimizer will load the binding when cache-miss.
	// NOTE: the Set might fail if the operation is too frequent, but binding update is a low-frequently operation, so
	// this risk seems acceptable.
	// TODO: handle the Set failure more gracefully.
	c.cache.Set(sqlDigest, binding, 0)
	c.cache.Wait()
	return
}

// RemoveBinding removes the Bindings which has same originSQL with specified Bindings.
// The function is thread-safe.
func (c *bindingCache) RemoveBinding(sqlDigest string) {
	c.digestBiMap.Del(sqlDigest)
	c.cache.Del(sqlDigest)
}

// SetMemCapacity sets the memory capacity for the cache.
// The function is thread-safe.
func (c *bindingCache) SetMemCapacity(capacity int64) {
	c.cache.UpdateMaxCost(capacity)
}

// GetMemUsage get the memory Usage for the cache.
// The function is thread-safe.
func (c *bindingCache) GetMemUsage() int64 {
	return int64(c.cache.Metrics.CostAdded() - c.cache.Metrics.CostEvicted())
}

// GetMemCapacity get the memory capacity for the cache.
// The function is thread-safe.
func (c *bindingCache) GetMemCapacity() int64 {
	return c.cache.MaxCost()
}

func (c *bindingCache) Size() int {
	return int(c.cache.Metrics.KeysAdded() - c.cache.Metrics.KeysEvicted())
}

// Close closes the cache.
func (c *bindingCache) Close() {
	c.cache.Clear()
	c.cache.Close()
	c.cache.Wait()
}
