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

//go:build !linux

package cgroup

import (
	"runtime"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
)

// GetCgroupCPU returns the CPU usage and quota for the current cgroup.
func GetCgroupCPU() (CPUUsage, error) {
	var cpuUsage CPUUsage
	failpoint.Inject("GetCgroupCPUErr", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			failpoint.Return(cpuUsage, errors.Errorf("mockAddBatchDDLJobsErr"))
		}
	})
	cpuUsage.NumCPU = runtime.NumCPU()
	return cpuUsage, nil
}

// GetCPUPeriodAndQuota returns CPU period and quota time of cgroup.
// This is Linux-specific and not supported in the current OS.
func GetCPUPeriodAndQuota() (period int64, quota int64, err error) {
	return -1, -1, nil
}

// CPUQuotaToGOMAXPROCS converts the CPU quota applied to the calling process
// to a valid GOMAXPROCS value. This is Linux-specific and not supported in the
// current OS.
func CPUQuotaToGOMAXPROCS(_ int) (int, CPUQuotaStatus, error) {
	return -1, CPUQuotaUndefined, nil
}

// InContainer returns true if the process is running in a container.
func InContainer() bool {
	return false
}
