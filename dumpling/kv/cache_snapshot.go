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

// CacheSnapshot wraps a snapshot and supports cache for read.
type CacheSnapshot struct {
	// Cache is an in-memory Store for caching KVs.
	Cache MemBuffer
	// Snapshot is a snapshot of a KV store.
	Snapshot Snapshot
}

// Get gets the value for key k from CacheSnapshot.
func (c *CacheSnapshot) Get(k Key) ([]byte, error) {
	v, err := c.Cache.Get(k)
	if IsErrNotFound(err) {
		v, err = c.Snapshot.Get(k)
		if err == nil {
			c.Cache.Set([]byte(k), v)
		}
		return v, errors.Trace(err)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return v, nil
}

// Fetch fetches a batch of values from KV store and saves in cache for later use.
func (c *CacheSnapshot) Fetch(keys []Key) error {
	var missKeys []Key
	for _, k := range keys {
		if _, err := c.Cache.Get(k); IsErrNotFound(err) {
			missKeys = append(missKeys, k)
		}
	}

	values, err := c.Snapshot.BatchGet(missKeys)
	if err != nil {
		return errors.Trace(err)
	}

	for k, v := range values {
		c.Cache.Set([]byte(k), v)
	}
	return nil
}

// Scan scans a batch of values from KV store and saves in cache for later use.
func (c *CacheSnapshot) Scan(start, end Key, limit int) error {
	values, err := c.Snapshot.Scan(start, end, limit)
	if err != nil {
		return errors.Trace(err)
	}
	for k, v := range values {
		c.Cache.Set([]byte(k), v)
	}
	return nil
}

// NewIterator creates an iterator of CacheSnapshot.
func (c *CacheSnapshot) NewIterator(param interface{}) Iterator {
	cacheIt := c.Cache.NewIterator(param)
	snapshotIt := c.Snapshot.NewIterator(param)
	return newUnionIter(cacheIt, snapshotIt)
}

// Release reset membuffer and release snapshot.
func (c *CacheSnapshot) Release() {
	c.Snapshot.Release()
}
