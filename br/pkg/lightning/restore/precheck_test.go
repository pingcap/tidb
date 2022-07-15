package restore

import (
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/restore/mock"
	"github.com/stretchr/testify/require"
)

func TestPrecheckBuilderBasic(t *testing.T) {
	var err error
	mockSrc, err := mock.NewMockImportSource(nil)
	require.NoError(t, err)
	mockTarget := mock.NewMockTargetInfo()
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendLocal

	preInfoGetter, err := NewPreRestoreInfoGetter(cfg, mockSrc.GetAllDBFileMetas(), mockSrc.GetStorage(), mockTarget, nil, nil)
	require.NoError(t, err)
	theCheckBuilder := NewPrecheckItemBuilder(cfg, mockSrc.GetAllDBFileMetas(), preInfoGetter, nil)
	for _, checkItemID := range []CheckItemID{
		CheckLargeDataFile,
		CheckSourcePermission,
		CheckTargetTableEmpty,
		CheckSourceSchemaValid,
		CheckCheckpoints,
		CheckCSVHeader,
		CheckTargetClusterSize,
		CheckTargetClusterEmptyRegion,
		CheckTargetClusterRegionDist,
		CheckTargetClusterVersion,
		CheckLocalDiskPlacement,
		CheckLocalTempKVDir,
	} {
		theChecker, err := theCheckBuilder.BuildPrecheckItem(checkItemID)
		require.NoError(t, err)
		require.Equal(t, checkItemID, theChecker.GetCheckItemID())
	}
}
