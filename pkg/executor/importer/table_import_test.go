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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
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
	sortDir, err := prepareSortDir(e, "1", tidbCfg)
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
	sortDir, err = prepareSortDir(e, "2", tidbCfg)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(importDir, "2"), sortDir)
	info, err = os.Stat(importDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())

	// dir already exist, do nothing
	sortDir, err = prepareSortDir(e, "3", tidbCfg)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(importDir, "3"), sortDir)
	info, err = os.Stat(importDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())

	// sortdir already exist, remove it
	require.NoError(t, os.Mkdir(sortDir, 0755))
	sortDir, err = prepareSortDir(e, "3", tidbCfg)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(importDir, "3"), sortDir)
	info, err = os.Stat(importDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	info, err = os.Stat(sortDir)
	require.True(t, os.IsNotExist(err))
	require.Nil(t, info)
}

func TestCalculateSubtaskCnt(t *testing.T) {
	tests := []struct {
		totalSize       int64
		maxEngineSize   config.ByteSize
		executeNodeCnt  int
		cloudStorageURL string
		want            int
	}{
		{1, 500, 0, "", 1},
		{499, 500, 1, "", 1},
		{500, 500, 2, "", 1},
		{749, 500, 3, "", 1},
		{750, 500, 4, "", 2},
		{1249, 500, 5, "", 2},
		{1250, 500, 6, "", 3},
		{100, 30, 7, "", 3},

		{1, 500, 0, "url", 1},
		{499, 500, 1, "url", 1},
		{500, 500, 2, "url", 2},
		{749, 500, 3, "url", 3},
		{750, 500, 4, "url", 4},
		{1249, 500, 5, "url", 5},
		{1250, 500, 6, "url", 6},
		{100, 30, 2, "url", 4},
		{400, 99, 3, "url", 6},
		{500, 100, 5, "url", 5},
		{500, 200, 5, "url", 5},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d/%d", tt.totalSize, tt.maxEngineSize), func(t *testing.T) {
			e := &LoadDataController{
				Plan: &Plan{
					MaxEngineSize:   tt.maxEngineSize,
					TotalFileSize:   tt.totalSize,
					CloudStorageURI: tt.cloudStorageURL,
				},
			}
			e.SetExecuteNodeCnt(tt.executeNodeCnt)
			if got := e.calculateSubtaskCnt(); got != tt.want {
				t.Errorf("calculateSubtaskCnt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoadDataControllerGetAdjustedMaxEngineSize(t *testing.T) {
	tests := []struct {
		totalSize       int64
		maxEngineSize   config.ByteSize
		executeNodeCnt  int
		cloudStorageURL string
		want            int64
	}{
		{1, 500, 0, "", 1},
		{499, 500, 1, "", 499},
		{500, 500, 2, "", 500},
		{749, 500, 3, "", 749},
		{750, 500, 4, "", 375},
		{1249, 500, 5, "", 625},
		{1250, 500, 6, "", 417},
		// ceil(100/3)
		{100, 30, 7, "", 34},

		{1, 500, 0, "url", 1},
		{499, 500, 1, "url", 499},
		{500, 500, 2, "url", 250},
		{749, 500, 3, "url", 250},
		{750, 500, 4, "url", 188},
		{1249, 500, 5, "url", 250},
		{1250, 500, 6, "url", 209},
		{100, 30, 2, "url", 25},
		{400, 99, 3, "url", 67},
		{500, 100, 5, "url", 100},
		{500, 200, 5, "url", 100},
		{500, 100, 1, "url", 100},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d/%d", tt.totalSize, tt.maxEngineSize), func(t *testing.T) {
			e := &LoadDataController{
				Plan: &Plan{
					MaxEngineSize:   tt.maxEngineSize,
					TotalFileSize:   tt.totalSize,
					CloudStorageURI: tt.cloudStorageURL,
				},
			}
			e.SetExecuteNodeCnt(tt.executeNodeCnt)
			if got := e.getAdjustedMaxEngineSize(); got != tt.want {
				t.Errorf("getAdjustedMaxEngineSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

type mockPDClient struct {
	pd.Client
}

// GetAllStores return fake stores.
func (c *mockPDClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return nil, nil
}

func (c *mockPDClient) Close() {
}

func TestGetRegionSplitSizeKeys(t *testing.T) {
	bak := NewClientWithContext
	t.Cleanup(func() {
		NewClientWithContext = bak
	})
	NewClientWithContext = func(_ context.Context, _ []string, _ pd.SecurityOption, _ ...pd.ClientOption) (pd.Client, error) {
		return nil, errors.New("mock error")
	}
	_, _, err := GetRegionSplitSizeKeys(context.Background())
	require.ErrorContains(t, err, "mock error")

	NewClientWithContext = func(_ context.Context, _ []string, _ pd.SecurityOption, _ ...pd.ClientOption) (pd.Client, error) {
		return &mockPDClient{}, nil
	}
	_, _, err = GetRegionSplitSizeKeys(context.Background())
	require.ErrorContains(t, err, "get region split size and keys failed")
	// no positive case, more complex to mock it
}
