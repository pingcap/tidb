// Copyright 2023 PingCAP, Inc.
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

package internal

import (
	"github.com/pingcap/tidb/pkg/statistics"
)

// StatsCacheInner is the interface to manage the statsCache, it can be implemented by map, lru cache or other structures.
// TODO: this cache shouldn't be aware of *statistics.Table, e.g.
//  1. change Put(int64, *stats.Table) to Put(k, v interface{}, vMem int64).
//  2. remove remove the Version method.
type StatsCacheInner interface {
	// Get gets the cache.
	Get(tid int64) (*statistics.Table, bool)
	// Put puts a cache.
	Put(tid int64, tbl *statistics.Table) bool
	// Del deletes a cache.
	Del(int64)
	// Cost returns the memory usage of the cache.
	Cost() int64
	// Values returns the values of the cache.
	Values() []*statistics.Table
	// Len returns the length of the cache.
	Len() int
	// Copy returns a copy of the cache
	Copy() StatsCacheInner
	// SetCapacity sets the capacity of the cache
	SetCapacity(int64)
	// Close stops the cache
	Close()
}
