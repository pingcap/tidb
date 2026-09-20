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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics/handle"
)

// StartDiagnostic starts query services without DDL execution or system-table
// maintenance. Domain.Init still registers this server, so its registration and
// read timestamps must be kept alive until Domain registration is isolated too.
func (do *Domain) StartDiagnostic() error {
	cfg := config.GetGlobalConfig()
	if cfg.EnableGlobalKill && do.etcdClient != nil {
		do.wg.Add(1)
		go do.serverIDKeeper()
	}
	do.wg.Run(func() {
		do.info.ServerInfoSyncer().ServerInfoSyncLoop(do.store, do.exit)
	}, "infoSyncerKeeper")
	if !cfg.SkipRegisterToDashboard {
		do.wg.Run(func() {
			do.info.ServerInfoSyncer().TopologySyncLoop(do.exit)
		}, "topologySyncerKeeper")
	}
	do.wg.Run(func() {
		do.isSyncer.SyncLoop(do.ctx)
	}, "loadSchemaInLoop")
	do.wg.Run(do.topNSlowQueryLoop, "topNSlowQueryLoop")
	if kv.IsUserKS(do.store) {
		if err := do.loadSysKSInfoSchema(); err != nil {
			return err
		}
	}
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

// LoadDiagnosticMetadata reads the actual bootstrap version and applies startup
// settings locally. Missing settings use defaults without writing them back to
// storage.
func LoadDiagnosticMetadata(ctx context.Context, store kv.Storage) (int64, error) {
	var ver int64
	var schemaCacheSize uint64
	var enableMDL bool
	err := kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		reader := meta.NewReader(txn)
		var err error
		ver, err = reader.GetBootstrapVersion()
		if err != nil {
			return err
		}
		if ver == 0 {
			return errors.New("diagnostic mode requires an already bootstrapped keyspace")
		}
		var missing bool
		schemaCacheSize, missing, err = reader.GetSchemaCacheSize()
		if err != nil {
			return err
		}
		if missing {
			schemaCacheSize = vardef.DefTiDBSchemaCacheSize
		}
		enableMDL, missing, err = reader.GetMetadataLock()
		if missing {
			enableMDL = true
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	vardef.SchemaCacheSize.Store(schemaCacheSize)
	vardef.SchemaCacheSizeOriginText.Store(strconv.FormatUint(schemaCacheSize, 10))
	vardef.SetEnableMDL(enableMDL)
	return ver, nil
}
