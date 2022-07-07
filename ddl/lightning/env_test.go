// Copyright 2021 PingCAP, Inc.
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

package lightning

import (
	"testing"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestGenSortPath(t *testing.T) {
	type TestCase struct {
		name       string
		inputPath  string
		outputPath string
	}
	tests := []TestCase{
		{"path1", "/tmp/", "/tmp/tmp_ddl-4000"},
		{"path2", "/datalit/tidb/data", "/tmp/tidb/tmp_ddl-4000"},
		{"path3", "127.0.0.1", "127.0.0.1/tmp_ddl-4000"},
		{"path4", "~/data1/", "~/data1/tmp_ddl-4000"},
		{"path5", "../data1/", "../data1/tmp_ddl-4000"},
		{"path6", "/datalit/tidb/data/", "/tmp/tidb/tmp_ddl-4000"},
		{"path7", "", "/tmp/tidb/tmp_ddl-4000"},
		{"path8", "/lightning", "/tmp/tidb/tmp_ddl-4000"},
	}
	for _, test := range tests {
		result, err := genLightningDataDir(test.inputPath, 4000)
		if err == nil {
			require.Equal(t, test.outputPath, result)
		} else {
			require.Error(t, err)
		}
	}
}

func TestSetDiskQuota(t *testing.T) {
	var minQuota int64 = 1 * _gb
	type TestCase struct {
		name        string
		sortPath    string
		inputQuota  int64
		outputQuota int64
	}
	tests := []TestCase{

		{"quota1", "/tmp/", 10 * _gb, 100 * _gb},
		{"quota2", "/data/tidb/data", 100 * _gb, 100 * _gb},
		{"quota3", "127.0.0.1", 1000 * _gb, 1000 * _gb},
		{"quota4", "~/data1/", 512 * _gb, 512 * _gb},
		{"quota5", "../data1/", 10000 * _gb, 10000 * _gb},
		{"quota6", "/data/tidb/data/", 100000 * _gb, 100000 * _gb},
		{"quota7", "", 10000 * _gb, 10000 * _gb},
		{"quota8", "/lightning", 10000000 * _gb, 10000 * _gb},
	}
	GlobalEnv.diskQuota = minQuota
	for _, test := range tests {
		result, _ := genLightningDataDir(test.sortPath, 4000)
		GlobalEnv.SortPath = result
		diskQuota, err := GlobalEnv.parseDiskQuota(test.inputQuota)
		if err != nil {
			require.Greater(t, minQuota, diskQuota)
		}

		require.GreaterOrEqual(t, test.inputQuota, GlobalEnv.diskQuota)
	}
}

func TestAdjustDiskQuota(t *testing.T) {
	type TestCase struct {
		name        string
		sortPath    string
		minQuota    int64
		maxQuota    int64
		resetVal    int64
		outputQuota int64
	}
	tests := []TestCase{
		{"quota1", "/tmp/", 1 * _gb, 10 * _gb, 30 * _gb, 10 * _gb},
		{"quota2", "/data/tidb/data", 1 * _gb, 100 * _gb, 50 * _gb, 100 * _gb},
		{"quota3", "127.0.0.1", 10 * _gb, 201 * _gb, 100 * _gb, 201 * _gb},
		{"quota4", "~/data1/", 100 * _gb, 1 * _tb, 2 * _tb, 102 * _gb},
		{"quota5", "../data1/", 100 * _gb, 100 * _tb, 1 * _tb, 103 * _gb},
		{"quota6", "/data/tidb/data/", 100 * _gb, 1 * _pb, 1 * _tb, 104 * _gb},
		{"quota7", "", 100 * _gb, 2 * _pb, 100 * _gb, 205 * _gb},
		{"quota8", "/lightning", 1000 * _tb, 1 * _pb, 100 * _gb, 106 * _gb},
	}
	GlobalEnv.SetMinQuota()
	for _, test := range tests {
		result, _ := genLightningDataDir(test.sortPath, 4000)
		GlobalEnv.SortPath = result
		// Set GlobalEnv.diskQuota to 1 GB
		GlobalEnv.diskQuota = test.minQuota
		quota, err := GlobalEnv.parseDiskQuota(test.maxQuota)
		// If err means disk available less than test minQuota
		if err != nil {
			require.Greater(t, test.minQuota, quota)
		}

		require.GreaterOrEqual(t, test.maxQuota, GlobalEnv.diskQuota)

		oldDiskQuota := GlobalEnv.diskQuota
		variable.DiskQuota.Store(test.resetVal)
		GlobalEnv.checkAndResetQuota()
		if test.resetVal <= GlobalEnv.diskQuota {
			require.Equal(t, test.resetVal, GlobalEnv.diskQuota)
		} else {
			require.Equal(t, oldDiskQuota, GlobalEnv.diskQuota)
		}

	}
}
