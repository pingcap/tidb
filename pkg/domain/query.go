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

package domain

import (
	"context"

	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/util"
)

// QueryDomain supplies a fixed schema and fully loaded statistics to one SELECT.
// Its Domain is only for query planning/execution: it must not be registered in
// the process domain map or passed to Domain.Init, Start or Close.
// Close releases only query-owned resources, leaving the store and source pool open.
type QueryDomain struct {
	*Domain
}

// NewQueryDomain creates an isolated query environment without schema reload,
// stats loading workers, DDL ownership or server registration. serverID is the
// hosting process's current ID, used by MPP; this domain does not own that ID.
func NewQueryDomain(
	ctx context.Context, store kv.Storage, schema infoschema.InfoSchema,
	stats []*statistics.Table, sourcePool util.DestroyableSessionPool, serverID uint64,
) (*QueryDomain, error) {
	d := &Domain{store: store, serverID: serverID, infoCache: infoschema.NewCache(store, 1)}
	d.infoCache.Insert(schema, 0)
	d.advancedSysSessionPool = syssession.NewAdvancedSessionPool(1, func() (syssession.SessionContext, error) {
		resource, err := sourcePool.Get()
		if err != nil {
			return nil, err
		}
		return &queryStatsSession{SessionContext: resource.(syssession.SessionContext), schema: schema, sourcePool: sourcePool}, nil
	})
	h, err := handle.NewHandle(ctx, 0, d.advancedSysSessionPool, nil, nil, nil, nil)
	if err != nil {
		d.advancedSysSessionPool.Close()
		return nil, err
	}
	// The caller has already checked the loaded statistics against its budget.
	// Keep this snapshot resident regardless of the process cache quota. Leave
	// room for cache entry overhead in addition to the statistics themselves.
	var capacity int64
	for _, table := range stats {
		capacity += table.MemoryUsage().TotalMemUsage + 4096
	}
	h.SetStatsCacheCapacity(max(capacity, 4096))
	h.UpdateStatsCache(statstypes.CacheUpdate{Updated: stats})
	h.WaitForAsyncUpdates()
	close(h.InitStatsDone)
	d.statsHandle.Store(h)
	return &QueryDomain{Domain: d}, nil
}

// Close releases the statistics handle and borrowed auxiliary sessions. It must
// run after query executors close and never release the hosting process's ID.
func (d *QueryDomain) Close() {
	d.StatsHandle().Close()
	d.advancedSysSessionPool.Close()
}

// Cache misses can inspect database metadata through the handle's session pool.
// Supply the submitted schema and return ownership through the original pool.
type queryStatsSession struct {
	syssession.SessionContext
	schema     infoschema.InfoSchema
	sourcePool util.DestroyableSessionPool
}

func (s *queryStatsSession) GetInfoSchema() infoschemactx.MetaOnlyInfoSchema       { return s.schema }
func (s *queryStatsSession) GetLatestInfoSchema() infoschemactx.MetaOnlyInfoSchema { return s.schema }
func (s *queryStatsSession) GetLatestISWithoutSessExt() infoschemactx.MetaOnlyInfoSchema {
	return s.schema
}
func (s *queryStatsSession) Close() { s.sourcePool.Destroy(s.SessionContext) }
