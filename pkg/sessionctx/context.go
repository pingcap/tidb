// Copyright 2018 PingCAP, Inc.
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

package sessionctx

import (
	"context"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/tikv/client-go/v2/oracle"
)

// SessionPlanCache is an interface for prepare and non-prepared plan cache
type SessionPlanCache interface {
	Get(key string, paramTypes any) (value any, ok bool)
	Put(key string, value, paramTypes any)
	Delete(key string)
	DeleteAll()
	Size() int
	SetCapacity(capacity uint) error
	Close()
}

// InstancePlanCache represents the instance/node level plan cache.
// Value and Opts should always be *PlanCacheValue and *PlanCacheMatchOpts, use any to avoid cycle-import.
type InstancePlanCache interface {
	// Get gets the cached value from the cache according to key and opts.
	Get(key string, paramTypes any) (value any, ok bool)
	// Put puts the key and value into the cache.
	Put(key string, value, paramTypes any) (succ bool)
	// All returns all cached values.
	// Returned values are read-only, don't modify them.
	All() (values []any)
	// Evict evicts some cached values.
	Evict(evictAll bool) (detailInfo string, numEvicted int)
	// Size returns the number of cached values.
	Size() int64
	// MemUsage returns the total memory usage of this plan cache.
	MemUsage() int64
	// GetLimits returns the soft and hard memory limits of this plan cache.
	GetLimits() (softLimit, hardLimit int64)
	// SetLimits sets the soft and hard memory limits of this plan cache.
	SetLimits(softLimit, hardLimit int64)
}

type basicCtxType int

func (t basicCtxType) String() string {
	switch t {
	case QueryString:
		return "query_string"
	case Initing:
		return "initing"
	case LastExecuteDDL:
		return "last_execute_ddl"
	}
	return "unknown"
}

// Context keys.
const (
	// QueryString is the key for original query string.
	QueryString basicCtxType = 1
	// Initing is the key for indicating if the server is running bootstrap or upgrade job.
	Initing basicCtxType = 2
	// LastExecuteDDL is the key for whether the session execute a ddl command last time.
	LastExecuteDDL basicCtxType = 3
)

// ValidateSnapshotReadTS strictly validates that readTS does not exceed the PD timestamp.
// For read requests to the storage, the check can be implicitly performed when sending the RPC request. So this
// function is only needed when it's not proper to delay the check to when RPC requests are being sent (e.g., `BEGIN`
// statements that don't make reading operation immediately).
func ValidateSnapshotReadTS(ctx context.Context, store kv.Storage, readTS uint64, isStaleRead bool) error {
	return store.GetOracle().ValidateReadTS(ctx, readTS, isStaleRead, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
}
