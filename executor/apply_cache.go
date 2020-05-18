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
	"github.com/hashicorp/golang-lru"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type applyCache struct {
	cache *lru.Cache
}

type applyCacheValue struct {
	Data *chunk.List
}

func newApplyCache(ctx sessionctx.Context) (*applyCache, error) {
	cache, err := lru.New(int(ctx.GetSessionVars().ApplyCacheQuota))
	if err != nil {
		return nil, errors.Trace(err)
	}
	c := applyCache{
		cache: cache,
	}
	return &c, nil
}

// Get gets a cache item according to cache key.
func (c *applyCache) Get(key string) *applyCacheValue {
	if c == nil {
		return nil
	}
	value, hit := c.cache.Get(key)
	if !hit {
		return nil
	}
	typedValue := value.(*applyCacheValue)
	return typedValue
}

// Set inserts an item to the cache.
func (c *applyCache) Set(key string, value *applyCacheValue) bool {
	if c == nil {
		return false
	}
	return c.cache.Add(key, value)
}
