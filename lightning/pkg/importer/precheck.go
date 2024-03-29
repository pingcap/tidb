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

package importer

import (
	"context"

	"github.com/pingcap/errors"
	ropts "github.com/pingcap/tidb/lightning/pkg/importer/opts"
	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	pdhttp "github.com/tikv/pd/client/http"
)

type precheckContextKey string

const taskManagerKey precheckContextKey = "PRECHECK/TASK_MANAGER"

// WithPrecheckKey returns a new context with the given key and value.
func WithPrecheckKey(ctx context.Context, key precheckContextKey, val any) context.Context {
	return context.WithValue(ctx, key, val)
}

// PrecheckItemBuilder is used to build precheck items
type PrecheckItemBuilder struct {
	cfg           *config.Config
	dbMetas       []*mydump.MDDatabaseMeta
	preInfoGetter PreImportInfoGetter
	checkpointsDB checkpoints.DB
	pdAddrsGetter func(context.Context) []string
}

// NewPrecheckItemBuilderFromConfig creates a new PrecheckItemBuilder from config
// pdHTTPCli **must not** be nil for local backend
func NewPrecheckItemBuilderFromConfig(
	ctx context.Context,
	cfg *config.Config,
	pdHTTPCli pdhttp.Client,
	opts ...ropts.PrecheckItemBuilderOption,
) (*PrecheckItemBuilder, error) {
	var gerr error
	builderCfg := new(ropts.PrecheckItemBuilderConfig)
	for _, o := range opts {
		o(builderCfg)
	}
	targetDB, err := DBFromConfig(ctx, cfg.TiDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	targetInfoGetter, err := NewTargetInfoGetterImpl(cfg, targetDB, pdHTTPCli)
	if err != nil {
		return nil, errors.Trace(err)
	}
	mdl, err := mydump.NewLoader(ctx, mydump.NewLoaderCfg(cfg), builderCfg.MDLoaderSetupOptions...)
	if err != nil {
		if mdl == nil {
			return nil, errors.Trace(err)
		}
		// here means the partial result is returned, so we can continue on processing
		gerr = err
	}
	dbMetas := mdl.GetDatabases()
	srcStorage := mdl.GetStore()
	preInfoGetter, err := NewPreImportInfoGetter(
		cfg,
		dbMetas,
		srcStorage,
		targetInfoGetter,
		nil, // ioWorkers
		nil, // encBuilder
		builderCfg.PreInfoGetterOptions...,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cpdb, err := checkpoints.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return NewPrecheckItemBuilder(cfg, dbMetas, preInfoGetter, cpdb, pdHTTPCli), gerr
}

// NewPrecheckItemBuilder creates a new PrecheckItemBuilder
func NewPrecheckItemBuilder(
	cfg *config.Config,
	dbMetas []*mydump.MDDatabaseMeta,
	preInfoGetter PreImportInfoGetter,
	checkpointsDB checkpoints.DB,
	pdHTTPCli pdhttp.Client,
) *PrecheckItemBuilder {
	pdAddrsGetter := func(context.Context) []string {
		return []string{cfg.TiDB.PdAddr}
	}
	// in tests we may not have a pdCli
	if pdHTTPCli != nil {
		pdAddrsGetter = func(ctx context.Context) []string {
			leaderInfo, err := pdHTTPCli.GetLeader(ctx)
			if err != nil {
				return []string{cfg.TiDB.PdAddr}
			}
			addrs := leaderInfo.GetClientUrls()
			if len(addrs) == 0 {
				return []string{cfg.TiDB.PdAddr}
			}
			return addrs
		}
	}
	return &PrecheckItemBuilder{
		cfg:           cfg,
		dbMetas:       dbMetas,
		preInfoGetter: preInfoGetter,
		checkpointsDB: checkpointsDB,
		pdAddrsGetter: pdAddrsGetter,
	}
}

// BuildPrecheckItem builds a Checker by the given checkID
func (b *PrecheckItemBuilder) BuildPrecheckItem(checkID precheck.CheckItemID) (precheck.Checker, error) {
	switch checkID {
	case precheck.CheckLargeDataFile:
		return NewLargeFileCheckItem(b.cfg, b.dbMetas), nil
	case precheck.CheckSourcePermission:
		return NewStoragePermissionCheckItem(b.cfg), nil
	case precheck.CheckTargetTableEmpty:
		return NewTableEmptyCheckItem(b.cfg, b.preInfoGetter, b.dbMetas, b.checkpointsDB), nil
	case precheck.CheckSourceSchemaValid:
		return NewSchemaCheckItem(b.cfg, b.preInfoGetter, b.dbMetas, b.checkpointsDB), nil
	case precheck.CheckCheckpoints:
		return NewCheckpointCheckItem(b.cfg, b.preInfoGetter, b.dbMetas, b.checkpointsDB), nil
	case precheck.CheckCSVHeader:
		return NewCSVHeaderCheckItem(b.cfg, b.preInfoGetter, b.dbMetas), nil
	case precheck.CheckTargetClusterSize:
		return NewClusterResourceCheckItem(b.preInfoGetter), nil
	case precheck.CheckTargetClusterEmptyRegion:
		return NewEmptyRegionCheckItem(b.preInfoGetter, b.dbMetas), nil
	case precheck.CheckTargetClusterRegionDist:
		return NewRegionDistributionCheckItem(b.preInfoGetter, b.dbMetas), nil
	case precheck.CheckTargetClusterVersion:
		return NewClusterVersionCheckItem(b.preInfoGetter, b.dbMetas), nil
	case precheck.CheckLocalDiskPlacement:
		return NewLocalDiskPlacementCheckItem(b.cfg), nil
	case precheck.CheckLocalTempKVDir:
		return NewLocalTempKVDirCheckItem(b.cfg, b.preInfoGetter, b.dbMetas), nil
	case precheck.CheckTargetUsingCDCPITR:
		return NewCDCPITRCheckItem(b.cfg, b.pdAddrsGetter), nil
	default:
		return nil, errors.Errorf("unsupported check item: %v", checkID)
	}
}

// GetPreInfoGetter gets the pre restore info getter from the builder.
func (b *PrecheckItemBuilder) GetPreInfoGetter() PreImportInfoGetter {
	return b.preInfoGetter
}
