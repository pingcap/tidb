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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handle

import (
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics/handle/autoanalyze"
	utilstats "github.com/pingcap/tidb/statistics/handle/util"
)

func (h *Handle) callWithSCtx(f func(sctx sessionctx.Context) error, flags ...int) (err error) {
	return utilstats.CallWithSCtx(h.pool, f, flags...)
}

const (
	// StatsOwnerKey is the stats owner path that is saved to etcd.
	StatsOwnerKey = "/tidb/stats/owner"
	// StatsPrompt is the prompt for stats owner manager.
	StatsPrompt = "stats"
)

// HandleAutoAnalyze analyzes the newly created table or index.
func (h *Handle) HandleAutoAnalyze(is infoschema.InfoSchema) (analyzed bool) {
	_ = h.callWithSCtx(func(sctx sessionctx.Context) error {
		analyzed = autoanalyze.HandleAutoAnalyze(sctx, &autoanalyze.Opt{
			StatsLease:              h.Lease(),
			GetLockedTables:         h.GetLockedTables,
			GetTableStats:           h.GetTableStats,
			GetPartitionStats:       h.GetPartitionStats,
			SysProcTracker:          h.sysProcTracker,
			AutoAnalyzeProcIDGetter: h.autoAnalyzeProcIDGetter,
		}, is)
		return nil
	})
	return
}

// GetCurrentPruneMode returns the current latest partitioning table prune mode.
func (h *Handle) GetCurrentPruneMode() (mode string, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		mode = sctx.GetSessionVars().PartitionPruneMode.Load()
		return nil
	})
	return
}
