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
	"testing"

	"github.com/pingcap/tidb/lightning/pkg/importer/mock"
	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/stretchr/testify/require"
)

func TestPrecheckBuilderBasic(t *testing.T) {
	var err error
	mockSrc, err := mock.NewImportSource(nil)
	require.NoError(t, err)
	mockTarget := mock.NewTargetInfo()
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendLocal

	preInfoGetter, err := NewPreImportInfoGetter(cfg, mockSrc.GetAllDBFileMetas(), mockSrc.GetStorage(), mockTarget, nil, nil)
	require.NoError(t, err)
	theCheckBuilder := NewPrecheckItemBuilder(cfg, mockSrc.GetAllDBFileMetas(), preInfoGetter, nil, nil)
	for _, checkItemID := range []precheck.CheckItemID{
		precheck.CheckLargeDataFile,
		precheck.CheckSourcePermission,
		precheck.CheckTargetTableEmpty,
		precheck.CheckSourceSchemaValid,
		precheck.CheckCheckpoints,
		precheck.CheckCSVHeader,
		precheck.CheckTargetClusterSize,
		precheck.CheckTargetClusterEmptyRegion,
		precheck.CheckTargetClusterRegionDist,
		precheck.CheckTargetClusterVersion,
		precheck.CheckLocalDiskPlacement,
		precheck.CheckLocalTempKVDir,
	} {
		theChecker, err := theCheckBuilder.BuildPrecheckItem(checkItemID)
		require.NoError(t, err)
		require.Equal(t, checkItemID, theChecker.GetCheckItemID())
	}
}
