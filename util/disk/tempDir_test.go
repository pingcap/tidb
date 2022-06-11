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

package disk

import (
	"os"
	"sync"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/stretchr/testify/require"
)

func TestRemoveDir(t *testing.T) {
	path := t.TempDir()
	defer config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = path
	})
	err := os.RemoveAll(path) // clean the uncleared temp file during the last run.
	require.NoError(t, err)
	err = os.MkdirAll(path, 0755)
	require.NoError(t, err)

	err = CheckAndInitTempDir()
	require.NoError(t, err)
	require.Equal(t, checkTempDirExist(), true)
	require.NoError(t, os.RemoveAll(config.GetGlobalConfig().TempStoragePath))
	require.Equal(t, checkTempDirExist(), false)
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(t *testing.T) {
			err := CheckAndInitTempDir()
			require.NoError(t, err)
			wg.Done()
		}(t)
	}
	wg.Wait()
	err = CheckAndInitTempDir()
	require.NoError(t, err)
	require.Equal(t, checkTempDirExist(), true)
}
