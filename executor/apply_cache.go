// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/chunk"
)

type applyCache struct {
	cache *ristretto.Cache
}

type applyCacheValue struct {
	Key  []byte
	Data *chunk.List
}

func newApplyCache() (*applyCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,
		MaxCost:     1e7,
		BufferItems: 64,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	c := applyCache{
		cache: cache,
	}
	return &c, nil
}

// Get gets a cache item according to cache key.
func (c *applyCache) Get(key []byte) *applyCacheValue {
	if c == nil {
		return nil
	}
	value, hit := c.cache.Get(key)
	if !hit {
		return nil
	}
	typedValue := value.(*applyCacheValue)
	// ristretto does not handle hash collision, so check the key equality after getting a value.
	if !bytes.Equal(typedValue.Key, key) {
		return nil
	}
	return typedValue
}

// Set inserts an item to the cache.
func (c *applyCache) Set(key []byte, value *applyCacheValue) bool {
	if c == nil {
		return false
	}
	// Always ensure that the `Key` in `value` is the `key` we received.
	value.Key = key
	return c.cache.Set(key, value, 1)
}
