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
	"os"
	"path/filepath"
	"testing"

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

	// dir not exist
	sortDir, err := prepareSortDir(e, 1, tidbCfg)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(importDir, "1"), sortDir)
	info, err := os.Stat(importDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	info, err = os.Stat(sortDir)
	require.True(t, os.IsNotExist(err))
	require.Nil(t, info)

	// dir is a file
	require.NoError(t, os.Remove(importDir))
	_, err = os.Create(importDir)
	require.NoError(t, err)
	sortDir, err = prepareSortDir(e, 2, tidbCfg)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(importDir, "2"), sortDir)
	info, err = os.Stat(importDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())

	// dir already exist
	sortDir, err = prepareSortDir(e, 3, tidbCfg)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(importDir, "3"), sortDir)
	info, err = os.Stat(importDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}
