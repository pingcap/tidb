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

package udf

import (
	"strings"
	"sync"
)

// Cache caches UDF definitions for quick lookup.
type Cache struct {
	// byName maps "schema.name" to definition
	byName map[string]*Definition

	// byID maps ID to definition
	byID map[int64]*Definition

	mu sync.RWMutex
}

// NewCache creates a new UDF cache.
func NewCache() *Cache {
	return &Cache{
		byName: make(map[string]*Definition),
		byID:   make(map[int64]*Definition),
	}
}

// Put adds or updates a UDF definition in the cache.
func (c *Cache) Put(def *Definition) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.makeKey(def.SchemaName, def.Name)
	c.byName[key] = def
	c.byID[def.ID] = def
}

// GetByName retrieves a UDF by schema and name.
func (c *Cache) GetByName(schemaName, name string) *Definition {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := c.makeKey(schemaName, name)
	return c.byName[key]
}

// GetByID retrieves a UDF by ID.
func (c *Cache) GetByID(id int64) *Definition {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.byID[id]
}

// Remove removes a UDF from the cache.
func (c *Cache) Remove(schemaName, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.makeKey(schemaName, name)
	if def, ok := c.byName[key]; ok {
		delete(c.byName, key)
		delete(c.byID, def.ID)
	}
}

// RemoveByID removes a UDF from the cache by ID.
func (c *Cache) RemoveByID(id int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if def, ok := c.byID[id]; ok {
		key := c.makeKey(def.SchemaName, def.Name)
		delete(c.byName, key)
		delete(c.byID, id)
	}
}

// Clear removes all entries from the cache.
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.byName = make(map[string]*Definition)
	c.byID = make(map[int64]*Definition)
}

// List returns all cached UDF definitions.
func (c *Cache) List() []*Definition {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*Definition, 0, len(c.byID))
	for _, def := range c.byID {
		result = append(result, def)
	}
	return result
}

// ListBySchema returns UDF definitions for a specific schema.
func (c *Cache) ListBySchema(schemaName string) []*Definition {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schemaName = strings.ToLower(schemaName)
	result := make([]*Definition, 0)
	for _, def := range c.byID {
		if strings.ToLower(def.SchemaName) == schemaName {
			result = append(result, def)
		}
	}
	return result
}

// Size returns the number of cached UDFs.
func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.byID)
}

// makeKey creates a cache key from schema and name.
func (c *Cache) makeKey(schemaName, name string) string {
	return strings.ToLower(schemaName) + "." + strings.ToLower(name)
}

// Invalidate checks if a definition needs to be updated based on version.
func (c *Cache) Invalidate(id int64, newVersion uint64) bool {
	c.mu.RLock()
	def, ok := c.byID[id]
	c.mu.RUnlock()

	if !ok {
		return true // Not cached, need to fetch
	}

	return def.Version < newVersion
}

// GlobalCache is the global UDF cache instance, shared across all components.
var GlobalCache = NewCache()
