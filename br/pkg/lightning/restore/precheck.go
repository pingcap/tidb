package restore

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
)

type CheckItemID string

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
)

type CheckResult struct {
	Item     CheckItemID
	Severity CheckType
	Passed   bool
	Message  string
}

type PrecheckItem interface {
	// Check checks whether it meet some prerequisites for importing
	// If the check is skipped, the returned `CheckResult` is nil
	Check(ctx context.Context) (*CheckResult, error)
	GetCheckItemID() CheckItemID
}

type precheckContextKey string

const taskManagerKey precheckContextKey = "PRECHECK/TASK_MANAGER"

func WithPrecheckKey(ctx context.Context, key precheckContextKey, val any) context.Context {
	return context.WithValue(ctx, key, val)
}

type PrecheckItemBuilder struct {
	cfg           *config.Config
	dbMetas       []*mydump.MDDatabaseMeta
	preInfoGetter PreRestoreInfoGetter
	checkpointsDB checkpoints.DB
}

func NewPrecheckItemBuilderFromConfig(ctx context.Context, cfg *config.Config) (*PrecheckItemBuilder, error) {
	targetDB, err := DBFromConfig(ctx, cfg.TiDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	targetInfoGetter, err := NewTargetInfoGetterImpl(cfg, targetDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	mdl, err := mydump.NewMyDumpLoader(ctx, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	dbMetas := mdl.GetDatabases()
	srcStorage := mdl.GetStore()
	preInfoGetter, err := NewPreRestoreInfoGetter(
		cfg,
		dbMetas,
		srcStorage,
		targetInfoGetter,
		nil, // ioWorkers
		nil, // encBuilder
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cpdb, err := checkpoints.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return NewPrecheckItemBuilder(cfg, dbMetas, preInfoGetter, cpdb), nil
}

func NewPrecheckItemBuilder(
	cfg *config.Config,
	dbMetas []*mydump.MDDatabaseMeta,
	preInfoGetter PreRestoreInfoGetter,
	checkpointsDB checkpoints.DB,
) *PrecheckItemBuilder {
	return &PrecheckItemBuilder{
		cfg:           cfg,
		dbMetas:       dbMetas,
		preInfoGetter: preInfoGetter,
		checkpointsDB: checkpointsDB,
	}
}

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
		return NewClusterRestoureCheckItem(b.preInfoGetter), nil
	case CheckTargetClusterEmptyRegion:
		return NewEmptyRegionCheckItem(b.preInfoGetter, b.dbMetas), nil
	case CheckTargetClusterRegionDist:
		return NewRegionDistributionCheckItem(b.preInfoGetter, b.dbMetas), nil
	case CheckTargetClusterVersion:
		return NewClusterVersionCheckItem(b.preInfoGetter, b.dbMetas), nil
	case CheckLocalDiskPlacement:
		return NewLocalDiskPlacementCheckItem(b.cfg), nil
	case CheckLocalTempKVDir:
		return NewLocalTempKVDirCheckItem(b.cfg, b.preInfoGetter), nil
	default:
		return nil, errors.Errorf("unsupported check item: %v", checkID)
	}
}
