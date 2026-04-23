// Copyright 2020 PingCAP, Inc.
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

package disk_test

import (
	"os"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/stretchr/testify/require"
)

func RunRemoveDir(t *testing.T) {
	path := t.TempDir()
	defer config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = path
	})
	err := os.RemoveAll(path) // clean the uncleared temp file during the last run.
	require.NoError(t, err)
	err = os.MkdirAll(path, 0755) //nolint:gosec // test code, permissions are acceptable
	require.NoError(t, err)

	err = disk.CheckAndInitTempDir()
	require.NoError(t, err)
	require.Equal(t, disk.ExportedCheckTempDirExist(), true)
	require.NoError(t, os.RemoveAll(config.GetGlobalConfig().TempStoragePath))
	require.Equal(t, disk.ExportedCheckTempDirExist(), false)
	wg := sync.WaitGroup{}
	for range 10 {
		wg.Add(1)
		go func(t *testing.T) {
			err := disk.CheckAndInitTempDir()
			require.NoError(t, err)
			wg.Done()
		}(t)
	}
	wg.Wait()
	err = disk.CheckAndInitTempDir()
	require.NoError(t, err)
	require.Equal(t, disk.ExportedCheckTempDirExist(), true)
}
