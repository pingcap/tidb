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

//go:build linux

package cgroup

import (
	"math"
	"os"
	"runtime"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
)

// GetCgroupCPU returns the CPU usage and quota for the current cgroup.
func GetCgroupCPU() (CPUUsage, error) {
	failpoint.Inject("GetCgroupCPUErr", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			var cpuUsage CPUUsage
			failpoint.Return(cpuUsage, errors.Errorf("mockAddBatchDDLJobsErr"))
		}
	})
	cpuusage, err := getCgroupCPU("/")

	cpuusage.NumCPU = runtime.NumCPU()
	return cpuusage, err
}

// CPUQuotaToGOMAXPROCS converts the CPU quota applied to the calling process
// to a valid GOMAXPROCS value.
func CPUQuotaToGOMAXPROCS(minValue int) (int, CPUQuotaStatus, error) {
	quota, err := GetCgroupCPU()
	if err != nil {
		return -1, CPUQuotaUndefined, err
	}
	maxProcs := int(math.Ceil(quota.CPUShares()))
	if minValue > 0 && maxProcs < minValue {
		return minValue, CPUQuotaMinUsed, nil
	}
	return maxProcs, CPUQuotaUsed, nil
}

// GetCPUPeriodAndQuota returns CPU period and quota time of cgroup.
func GetCPUPeriodAndQuota() (period int64, quota int64, err error) {
	return getCgroupCPUPeriodAndQuota("/")
}

// InContainer returns true if the process is running in a container.
func InContainer() bool {
	// for cgroup V1, check /proc/self/cgroup, for V2, check /proc/self/mountinfo
	return inContainer(procPathCGroup) || inContainer(procPathMountInfo)
}

func inContainer(path string) bool {
	v, err := os.ReadFile(path)
	if err != nil {
		return false
	}

	// For cgroup V1, check /proc/self/cgroup
	if path == procPathCGroup {
		if strings.Contains(string(v), "docker") ||
			strings.Contains(string(v), "kubepods") ||
			strings.Contains(string(v), "containerd") {
			return true
		}
	}

	// For cgroup V2, check /proc/self/mountinfo
	if path == procPathMountInfo {
		lines := strings.Split(string(v), "\n")
		for _, line := range lines {
			v := strings.Split(line, " ")
			// check mount point of root dir is on overlay or not.
			// v[4] means `mount point`, v[8] means `filesystem type`.
			// see details from https://man7.org/linux/man-pages/man5/proc.5.html
			// TODO: enhance this check, as overlay is not the only storage driver for container.
			if len(v) > 8 && v[4] == "/" && v[8] == "overlay" {
				return true
			}
		}
	}

	return false
}
