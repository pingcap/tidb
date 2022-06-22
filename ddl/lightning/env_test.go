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
		name        string
		inputPath   string
		outputPath  string
	}
	tests := []TestCase{
		{"path1", "/tmp/", "/tmp/lightning"},
		{"path2", "/data/tidb/data", "/data/tidb/data/lightning"},
		{"path3", "127.0.0.1", "/tmp/lightning"},
		{"path4", "~/data1/", "/tmp/lightning"},
		{"path5", "../data1/", "/tmp/lightning"},
		{"path6", "/data/tidb/data/", "/data/tidb/data/lightning"},
		{"path7", "", "/data/tidb/data/lightning"},
		{"path8", "/lightning", "/lightning"},
	}
	for _, test := range tests {
		result, err := genLightningDataDir(test.inputPath)
		if err == nil {
			require.Equal(t, test.outputPath, result)
		} else {
			require.Error(t, err)
		}
	}		
}

func TestSetDiskQuota(t *testing.T) {
	type TestCase struct {
		name        string
		sortPath    string
		inputQuota  int
		outputQuota int64
	}
	tests := []TestCase{
		{"quota1", "/tmp/", 10, 100 * _gb},
		{"quota2", "/data/tidb/data", 100, 100 * _gb},
		{"quota3", "127.0.0.1", 1000, 1000  * _gb},
		{"quota4", "~/data1/", 512, 512 * _gb},
		{"quota5", "../data1/", 10000, 10000 * _gb},
		{"quota6", "/data/tidb/data/", 100000, 100000 * _gb},
		{"quota7", "", 10000, 10000 * _gb},
		{"quota8", "/lightning", 10000, 10000 * _gb},
	}
	for _, test := range tests {
		result, _ := genLightningDataDir(test.sortPath)
		GlobalLightningEnv.SortPath = result
		GlobalLightningEnv.parseDiskQuota(test.inputQuota)
		if GlobalLightningEnv.diskQuota > int64(test.inputQuota * _gb) &&
		    GlobalLightningEnv.diskQuota <= minStorageQuota {
			require.Equal(t, test.outputQuota, GlobalLightningEnv.diskQuota)
		}
	}		
}

func TestAdjustDiskQuota(t *testing.T) {
	type TestCase struct {
		name        string
		sortPath    string
		inputQuota  int
		resetQuota  int32
		outputQuota int64
	}
	tests := []TestCase{
		{"quota1", "/tmp/", 10, 100, 100 * _gb},
		{"quota2", "/data/tidb/data", 100, 100, 100 * _gb},
		{"quota3", "127.0.0.1", 1000, 101, 101 * _gb},
		{"quota4", "~/data1/", 512, 102, 102 * _gb},
		{"quota5", "../data1/", 10000, 103, 103 * _gb},
		{"quota6", "/data/tidb/data/", 100000, 104, 104 * _gb},
		{"quota7", "", 10000, 105, 105 * _gb},
		{"quota8", "/lightning", 10000, 106, 106 * _gb},
	}
	for _, test := range tests {
		result, _ := genLightningDataDir(test.sortPath)
		GlobalLightningEnv.SortPath = result
		GlobalLightningEnv.parseDiskQuota(test.inputQuota)
		if GlobalLightningEnv.diskQuota > int64(test.inputQuota * _gb) &&
		    GlobalLightningEnv.diskQuota <= minStorageQuota {
			require.Equal(t, test.outputQuota, GlobalLightningEnv.diskQuota)
		}
        variable.DiskQuota.Store(test.resetQuota)
		GlobalLightningEnv.checkAndResetQuota()
		require.Equal(t, test.outputQuota, GlobalLightningEnv.diskQuota)
	}		
}
