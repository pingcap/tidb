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

package usage

import (
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

// NewSessionIndexUsageCollector creates a new Collector on the list.
// The returned value's type should be *usage.SessionIndexUsageCollector, use interface{} to avoid cycle import now.
func (u *statsUsageImpl) NewSessionIndexUsageCollector() *indexusage.SessionIndexUsageCollector {
	return u.idxUsageCollector.SpawnSessionCollector()
}

// GCIndexUsage removes unnecessary index usage data.
func (u *statsUsageImpl) GCIndexUsage() error {
	return util.CallWithSCtx(u.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		schema := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
		u.idxUsageCollector.GCIndexUsage(func(id int64) (*model.TableInfo, bool) {
			tbl, ok := schema.TableByID(id)
			if !ok {
				return nil, false
			}
			return tbl.Meta(), true
		})
		return nil
	})
}

// StartWorker starts for the collector worker.
func (u *statsUsageImpl) StartWorker() {
	// TODO: after migrating other usage stats to also use the same collector, also start their worker here.
	u.idxUsageCollector.StartWorker()
}

// Close closes and waits for the index usage collector worker.
func (u *statsUsageImpl) Close() {
	u.idxUsageCollector.Close()
}

// GetIndexUsage returns the index usage information
func (u *statsUsageImpl) GetIndexUsage(tableID int64, indexID int64) indexusage.Sample {
	return u.idxUsageCollector.GetIndexUsage(tableID, indexID)
}
