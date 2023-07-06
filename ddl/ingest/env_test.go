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
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/stretchr/testify/require"
)

func TestGenLightningDataDir(t *testing.T) {
	tmpDir := t.TempDir()
	port, iPort := "5678", uint(5678)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempDir = tmpDir
		conf.Port = iPort
	})
	sPath, err := ingest.GenLightningDataDirForTest()
	require.NoError(t, err)
	require.Equal(t, tmpDir+"/tmp_ddl-"+port, sPath)
}
