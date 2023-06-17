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

package cache

import "github.com/pingcap/tidb/statistics"

// StatsCacheInner is the interface to manage the statsCache, it can be implemented by map, lru cache or other structures.
type StatsCacheInner interface {
	GetByQuery(int64) (*statistics.Table, bool)
	Get(int64) (*statistics.Table, bool)
	PutByQuery(int64, *statistics.Table)
	Put(int64, *statistics.Table)
	Del(int64)
	Cost() int64
	Keys() []int64
	Values() []*statistics.Table
	Map() map[int64]*statistics.Table
	Len() int
	FreshMemUsage()
	Copy() StatsCacheInner
	SetCapacity(int64)
	// Front returns the front element's owner tableID, only used for test
	Front() int64
}
