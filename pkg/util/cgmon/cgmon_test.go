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

package cgmon

import (
	"errors"
	"runtime"
	"testing"

	"github.com/shirou/gopsutil/v3/mem"
	"github.com/stretchr/testify/require"
)

func TestUploadDefaultValueWithoutCgroup(t *testing.T) {
	getCgroupCPUPeriodAndQuota = func() (period int64, quota int64, err error) {
		return 0, 0, errors.New("mock error")
	}
	getCgroupMemoryLimit = func() (uint64, error) {
		return 0, errors.New("mock error")
	}

	require.Error(t, refreshCgroupCPU())
	require.Error(t, refreshCgroupMemory())

	require.Equal(t, runtime.NumCPU(), lastCPU)
	vmem, err := mem.VirtualMemory()
	require.NoError(t, err)
	require.Equal(t, vmem.Total, lastMemoryLimit)
}
