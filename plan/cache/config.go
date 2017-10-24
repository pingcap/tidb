// Copyright 2017 PingCAP, Inc.
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

package cache

import (
	"github.com/pingcap/tidb/util/kvcache"
)

var (
	// PlanCacheEnabled stores the global config "plan-cache-enabled".
	PlanCacheEnabled bool
	// PlanCacheShards stores the global config "plan-cache-shards".
	PlanCacheShards int64
	// PlanCacheCapacity stores the global config "plan-cache-capacity".
	PlanCacheCapacity int64
	// GlobalPlanCache stores the global plan cache for every session in a tidb-server.
	GlobalPlanCache *kvcache.ShardedLRUCache

	// PreparedPlanCacheEnabled stores the global config "prepared-plan-cache-enabled".
	PreparedPlanCacheEnabled bool
	// PreparedPlanCacheCapacity stores the global config "prepared-plan-cache-capacity".
	PreparedPlanCacheCapacity int64
)
