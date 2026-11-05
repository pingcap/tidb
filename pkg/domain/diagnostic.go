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

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/statistics/handle"
)

// StartDiagnostic starts query services without DDL execution or system-table
// maintenance. The independent server-ID lease stays active.
func (do *Domain) StartDiagnostic() error {
	cfg := config.GetGlobalConfig()
	if cfg.EnableGlobalKill && do.etcdClient != nil {
		do.wg.Add(1)
		go do.serverIDKeeper()
	}
	do.wg.Run(func() {
		do.isSyncer.SyncLoop(do.ctx)
	}, "loadSchemaInLoop")
	do.wg.Run(do.topNSlowQueryLoop, "topNSlowQueryLoop")
	return nil
}

// LoadStatsDiagnostic creates the stats handle and starts only readers.
// No DDL subscription, ownership, statement-delta collection, auto analyze,
// statistics GC, or usage/history persistence is started.
func (do *Domain) LoadStatsDiagnostic(ctx context.Context, concurrency int) error {
	statsHandle, err := handle.NewHandle(ctx, do.statsLease, do.advancedSysSessionPool,
		&do.sysProcesses, nil, do.NextConnID, do.ReleaseConnID)
	if err != nil {
		return err
	}
	do.statsHandle.Store(statsHandle)
	// Initial stats loading waits for the SessionManager, which is installed
	// after BootstrapSession returns. Keep it off the bootstrap goroutine.
	if do.statsLease >= 0 {
		do.wg.Run(do.loadStatsWorker, "loadStatsWorker")
	} else {
		// A negative lease disables periodic refresh, but diagnostic startup
		// still loads existing stats once and signals InitStatsDone.
		do.wg.Run(func() {
			do.initStats(do.ctx)
		}, "initStats")
	}
	do.StartLoadStatsSubWorkers(concurrency)
	if do.statsLease > 0 {
		do.wg.Run(do.asyncLoadHistogram, "asyncLoadHistogram")
	}
	return nil
}
