package importer

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	ropts "github.com/pingcap/tidb/br/pkg/lightning/importer/opts"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/precheck"
	pd "github.com/tikv/pd/client"
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
}

// NewPrecheckItemBuilderFromConfig creates a new PrecheckItemBuilder from config
// pdCli **must not** be nil for local backend
func NewPrecheckItemBuilderFromConfig(ctx context.Context, cfg *config.Config, pdCli pd.Client, opts ...ropts.PrecheckItemBuilderOption) (*PrecheckItemBuilder, error) {
	var gerr error
	builderCfg := new(ropts.PrecheckItemBuilderConfig)
	for _, o := range opts {
		o(builderCfg)
	}
	targetDB, err := DBFromConfig(ctx, cfg.TiDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	targetInfoGetter, err := NewTargetInfoGetterImpl(cfg, targetDB, pdCli)
	if err != nil {
		return nil, errors.Trace(err)
	}
	mdl, err := mydump.NewMyDumpLoader(ctx, cfg, builderCfg.MDLoaderSetupOptions...)
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
	return NewPrecheckItemBuilder(cfg, dbMetas, preInfoGetter, cpdb), gerr
}

// NewPrecheckItemBuilder creates a new PrecheckItemBuilder
func NewPrecheckItemBuilder(
	cfg *config.Config,
	dbMetas []*mydump.MDDatabaseMeta,
	preInfoGetter PreImportInfoGetter,
	checkpointsDB checkpoints.DB,
) *PrecheckItemBuilder {
	return &PrecheckItemBuilder{
		cfg:           cfg,
		dbMetas:       dbMetas,
		preInfoGetter: preInfoGetter,
		checkpointsDB: checkpointsDB,
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
		return NewCDCPITRCheckItem(b.cfg), nil
	default:
		return nil, errors.Errorf("unsupported check item: %v", checkID)
	}
}

// GetPreInfoGetter gets the pre restore info getter from the builder.
func (b *PrecheckItemBuilder) GetPreInfoGetter() PreImportInfoGetter {
	return b.preInfoGetter
}
