// Copyright 2015 PingCAP, Inc.
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

package kv

import "github.com/juju/errors"

var _ Snapshot = (*cacheSnapshot)(nil)

// cacheSnapshot wraps a snapshot and supports cache for read.
type cacheSnapshot struct {
	cache              MemBuffer
	snapshot           Snapshot
	lazyConditionPairs MemBuffer
	opts               Options
}

// NewCacheSnapshot creates a new snapshot with cache embedded.
func NewCacheSnapshot(snapshot Snapshot, lazyConditionPairs MemBuffer, opts Options) Snapshot {
	return &cacheSnapshot{
		cache:              p.Get().(MemBuffer),
		snapshot:           snapshot,
		lazyConditionPairs: lazyConditionPairs,
		opts:               opts,
	}
}

// Get gets value from snapshot and saves it in cache.
func (c *cacheSnapshot) Get(k Key) ([]byte, error) {
	v, err := c.cache.Get(k)
	if IsErrNotFound(err) {
		if _, ok := c.opts.Get(PresumeKeyNotExists); ok {
			err = c.lazyConditionPairs.Set(k, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return nil, errors.Trace(ErrNotExist)
		}
	}
	if IsErrNotFound(err) {
		if opt, ok := c.opts.Get(RangePrefetchOnCacheMiss); ok {
			if limit, ok := opt.(int); ok && limit > 0 {
				vals, err2 := c.RangeGet(k, nil, limit)
				if err2 != nil {
					return nil, errors.Trace(err2)
				}
				if val, ok := vals[string(k)]; ok {
					v, err = val, nil
				}
			}
		}
	}
	if IsErrNotFound(err) {
		v, err = c.snapshot.Get(k)
		if err == nil {
			err = c.cache.Set([]byte(k), v)
		}
		return v, errors.Trace(err)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return v, nil
}

// BatchGet gets a batch of values from snapshot and saves them in cache.
func (c *cacheSnapshot) BatchGet(keys []Key) (map[string][]byte, error) {
	m := make(map[string][]byte)
	var missedKeys []Key
	for _, k := range keys {
		v, err := c.cache.Get(k)
		if IsErrNotFound(err) {
			missedKeys = append(missedKeys, k)
			continue
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(v) > 0 {
			m[string(k)] = v
		}
		// If len(v) == 0, it means that the key does not exist.
	}

	values, err := c.snapshot.BatchGet(missedKeys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, k := range missedKeys {
		ks := string(k)
		if v, ok := values[ks]; ok && len(v) > 0 {
			err = c.cache.Set(k, v)
			m[ks] = v
		} else {
			err = c.cache.Set(k, nil)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return m, nil
}

// RangeGet gets values from snapshot and saves them in cache.
// The range should be [start, end] as Snapshot.RangeGet() indicated.
func (c *cacheSnapshot) RangeGet(start, end Key, limit int) (map[string][]byte, error) {
	values, err := c.snapshot.RangeGet(start, end, limit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for k, v := range values {
		err = c.cache.Set([]byte(k), v)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return values, nil
}

// NewIterator creates an iterator of snapshot.
func (c *cacheSnapshot) NewIterator(param interface{}) Iterator {
	return newUnionIter(c.cache.NewIterator(param), c.snapshot.NewIterator(param))
}

// Release reset membuffer and release snapshot.
func (c *cacheSnapshot) Release() {
	if c.cache != nil {
		c.cache.Release()
		p.Put(c.cache)
		c.cache = nil
	}
	if c.snapshot != nil {
		c.snapshot.Release()
	}
}
