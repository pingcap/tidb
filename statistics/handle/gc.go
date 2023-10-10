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

package handle

import (
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics/handle/storage"
	statsutil "github.com/pingcap/tidb/statistics/handle/util"
)

// GCStats will garbage collect the useless stats' info.
// For dropped tables, we will first update their version
// so that other tidb could know that table is deleted.
func (h *Handle) GCStats(is infoschema.InfoSchema, ddlLease time.Duration) (err error) {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.GCStats(sctx, h.getTableByPhysicalID, h.MarkExtendedStatsDeleted, is, h.Lease(), ddlLease)
	})
}

// ClearOutdatedHistoryStats clear outdated historical stats
func (h *Handle) ClearOutdatedHistoryStats() error {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.ClearOutdatedHistoryStats(sctx)
	})
}

// DeleteTableStatsFromKV deletes table statistics from kv.
// A statsID refers to statistic of a table or a partition.
func (h *Handle) DeleteTableStatsFromKV(statsIDs []int64) (err error) {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.DeleteTableStatsFromKV(sctx, statsIDs)
	}, statsutil.FlagWrapTxn)
}
