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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ingest_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/stretchr/testify/require"
)

func TestGenLightningDataDir(t *testing.T) {
	tmpDir := t.TempDir()
	port, iPort := "5678", uint(5678)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempDir = tmpDir
		conf.Port = iPort
	})
	sPath, err := ingest.GenIngestTempDataDir()
	require.NoError(t, err)
	require.Equal(t, tmpDir+"/tmp_ddl-"+port, sPath)
}

func TestLitBackendCtxMgr(t *testing.T) {
	sortPath := t.TempDir()
	staleJobDir := filepath.Join(sortPath, "100")
	staleJobDir2 := filepath.Join(sortPath, "101")
	err := os.MkdirAll(staleJobDir, 0o700)
	require.NoError(t, err)
	err = os.MkdirAll(staleJobDir2, 0o700)
	require.NoError(t, err)

	runningJobs := map[int64]struct{}{
		100: {},
		101: {},
	}

	ingest.CleanUpTempDir(sortPath, runningJobs)
	require.DirExists(t, staleJobDir)
	require.DirExists(t, staleJobDir2)

	delete(runningJobs, 101)
	ingest.CleanUpTempDir(sortPath, runningJobs)
	require.DirExists(t, staleJobDir)
	require.NoDirExists(t, staleJobDir2)

	ingest.CleanUpTempDir(sortPath, nil)
	require.NoDirExists(t, staleJobDir)
	require.NoDirExists(t, staleJobDir2)
}
