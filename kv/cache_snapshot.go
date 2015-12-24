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
			err = cachePut(c.lazyConditionPairs, k, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return nil, errors.Trace(ErrNotExist)
		}
	}
	if IsErrNotFound(err) {
		v, err = c.snapshot.Get(k)
		if err == nil {
			err = cachePut(c.cache, k, v)
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
		v, ok := values[ks]
		if ok && len(v) > 0 {
			m[ks] = v
		}
		err = cachePut(c.cache, k, v)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return m, nil
}

// Seek creates an iterator of snapshot.
func (c *cacheSnapshot) Seek(k Key) (Iterator, error) {
	cacheIter, err := c.cache.Seek(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshotIter, err := c.snapshot.Seek(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newUnionIter(cacheIter, snapshotIter), nil
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

func cachePut(m Mutator, k Key, v []byte) error {
	if len(v) == 0 {
		return m.Delete(k)
	}
	return m.Set(k, v)
}
