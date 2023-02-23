// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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
