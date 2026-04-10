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

package cgmon_test

import (
	"errors"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/pkg/util/cgmon"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/stretchr/testify/require"
)

func RunUploadDefaultValueWithoutCgroup(t *testing.T) {
	cgmon.ExportedSetCgroupCPUPeriodAndQuota(func() (period int64, quota int64, err error) {
		return 0, 0, errors.New("mock error")
	})
	cgmon.ExportedSetCgroupMemoryLimit(func() (uint64, error) {
		return 0, errors.New("mock error")
	})

	require.Error(t, cgmon.ExportedRefreshCgroupCPU())
	require.Error(t, cgmon.ExportedRefreshCgroupMemory())

	require.Equal(t, runtime.NumCPU(), *cgmon.ExportedLastCPU)
	vmem, err := mem.VirtualMemory()
	require.NoError(t, err)
	require.Equal(t, vmem.Total, *cgmon.ExportedLastMemoryLimit)
}
