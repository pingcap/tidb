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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	tidb "github.com/pingcap/tidb/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPrepareSortDir(t *testing.T) {
	dir := t.TempDir()
	tidbCfg := &tidb.Config{
		Port:    4000,
		TempDir: dir,
	}
	e := &LoadDataController{
		logger: zap.NewNop(),
	}
	importDir := filepath.Join(dir, "import-4000")

	// dir not exist, create it
	sortDir, err := prepareSortDir(e, 1, tidbCfg)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(importDir, "1"), sortDir)
	info, err := os.Stat(importDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	info, err = os.Stat(sortDir)
	require.True(t, os.IsNotExist(err))
	require.Nil(t, info)

	// dir is a file, remove it and create dir
	require.NoError(t, os.Remove(importDir))
	_, err = os.Create(importDir)
	require.NoError(t, err)
	sortDir, err = prepareSortDir(e, 2, tidbCfg)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(importDir, "2"), sortDir)
	info, err = os.Stat(importDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())

	// dir already exist, do nothing
	sortDir, err = prepareSortDir(e, 3, tidbCfg)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(importDir, "3"), sortDir)
	info, err = os.Stat(importDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())

	// sortdir already exist, remove it
	require.NoError(t, os.Mkdir(sortDir, 0755))
	sortDir, err = prepareSortDir(e, 3, tidbCfg)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(importDir, "3"), sortDir)
	info, err = os.Stat(importDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	info, err = os.Stat(sortDir)
	require.True(t, os.IsNotExist(err))
	require.Nil(t, info)
}

func TestLoadDataControllerGetAdjustedMaxEngineSize(t *testing.T) {
	tests := []struct {
		totalSize     int64
		maxEngineSize config.ByteSize
		want          int64
	}{
		{1, 500, 1},
		{499, 500, 499},
		{500, 500, 500},
		{749, 500, 749},
		{750, 500, 375},
		{1249, 500, 625},
		{1250, 500, 417},
		// ceil(100/3)
		{100, 30, 34},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d/%d", tt.totalSize, tt.maxEngineSize), func(t *testing.T) {
			e := &LoadDataController{
				TotalFileSize: tt.totalSize,
				Plan:          &Plan{MaxEngineSize: tt.maxEngineSize},
			}
			if got := e.getAdjustedMaxEngineSize(); got != tt.want {
				t.Errorf("getAdjustedMaxEngineSize() = %v, want %v", got, tt.want)
			}
		})
	}
}
