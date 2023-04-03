package importer

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	ropts "github.com/pingcap/tidb/br/pkg/lightning/importer/opts"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
)

// CheckItemID is the ID of a precheck item
type CheckItemID string

// CheckItemID constants
const (
	CheckLargeDataFile            CheckItemID = "CHECK_LARGE_DATA_FILES"
	CheckSourcePermission         CheckItemID = "CHECK_SOURCE_PERMISSION"
	CheckTargetTableEmpty         CheckItemID = "CHECK_TARGET_TABLE_EMPTY"
	CheckSourceSchemaValid        CheckItemID = "CHECK_SOURCE_SCHEMA_VALID"
	CheckCheckpoints              CheckItemID = "CHECK_CHECKPOINTS"
	CheckCSVHeader                CheckItemID = "CHECK_CSV_HEADER"
	CheckTargetClusterSize        CheckItemID = "CHECK_TARGET_CLUSTER_SIZE"
	CheckTargetClusterEmptyRegion CheckItemID = "CHECK_TARGET_CLUSTER_EMPTY_REGION"
	CheckTargetClusterRegionDist  CheckItemID = "CHECK_TARGET_CLUSTER_REGION_DISTRIBUTION"
	CheckTargetClusterVersion     CheckItemID = "CHECK_TARGET_CLUSTER_VERSION"
	CheckLocalDiskPlacement       CheckItemID = "CHECK_LOCAL_DISK_PLACEMENT"
	CheckLocalTempKVDir           CheckItemID = "CHECK_LOCAL_TEMP_KV_DIR"
	CheckTargetUsingCDCPITR       CheckItemID = "CHECK_TARGET_USING_CDC_PITR"
)

// CheckResult is the result of a precheck item
type CheckResult struct {
	Item     CheckItemID
	Severity CheckType
	Passed   bool
	Message  string
}

// PrecheckItem is the interface for precheck items
type PrecheckItem interface {
	// Check checks whether it meet some prerequisites for importing
	// If the check is skipped, the returned `CheckResult` is nil
	Check(ctx context.Context) (*CheckResult, error)
	GetCheckItemID() CheckItemID
}

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
func NewPrecheckItemBuilderFromConfig(ctx context.Context, cfg *config.Config, opts ...ropts.PrecheckItemBuilderOption) (*PrecheckItemBuilder, error) {
	var gerr error
	builderCfg := new(ropts.PrecheckItemBuilderConfig)
	for _, o := range opts {
		o(builderCfg)
	}
	targetDB, err := DBFromConfig(ctx, cfg.TiDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	targetInfoGetter, err := NewTargetInfoGetterImpl(cfg, targetDB)
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

// BuildPrecheckItem builds a PrecheckItem by the given checkID
func (b *PrecheckItemBuilder) BuildPrecheckItem(checkID CheckItemID) (PrecheckItem, error) {
	switch checkID {
	case CheckLargeDataFile:
		return NewLargeFileCheckItem(b.cfg, b.dbMetas), nil
	case CheckSourcePermission:
		return NewStoragePermissionCheckItem(b.cfg), nil
	case CheckTargetTableEmpty:
		return NewTableEmptyCheckItem(b.cfg, b.preInfoGetter, b.dbMetas, b.checkpointsDB), nil
	case CheckSourceSchemaValid:
		return NewSchemaCheckItem(b.cfg, b.preInfoGetter, b.dbMetas, b.checkpointsDB), nil
	case CheckCheckpoints:
		return NewCheckpointCheckItem(b.cfg, b.preInfoGetter, b.dbMetas, b.checkpointsDB), nil
	case CheckCSVHeader:
		return NewCSVHeaderCheckItem(b.cfg, b.preInfoGetter, b.dbMetas), nil
	case CheckTargetClusterSize:
		return NewClusterResourceCheckItem(b.preInfoGetter), nil
	case CheckTargetClusterEmptyRegion:
		return NewEmptyRegionCheckItem(b.preInfoGetter, b.dbMetas), nil
	case CheckTargetClusterRegionDist:
		return NewRegionDistributionCheckItem(b.preInfoGetter, b.dbMetas), nil
	case CheckTargetClusterVersion:
		return NewClusterVersionCheckItem(b.preInfoGetter, b.dbMetas), nil
	case CheckLocalDiskPlacement:
		return NewLocalDiskPlacementCheckItem(b.cfg), nil
	case CheckLocalTempKVDir:
		return NewLocalTempKVDirCheckItem(b.cfg, b.preInfoGetter, b.dbMetas), nil
	case CheckTargetUsingCDCPITR:
		return NewCDCPITRCheckItem(b.cfg), nil
	default:
		return nil, errors.Errorf("unsupported check item: %v", checkID)
	}
}

// GetPreInfoGetter gets the pre restore info getter from the builder.
func (b *PrecheckItemBuilder) GetPreInfoGetter() PreImportInfoGetter {
	return b.preInfoGetter
}
