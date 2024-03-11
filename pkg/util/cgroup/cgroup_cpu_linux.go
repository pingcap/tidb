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
	"fmt"
	"math"
	"os"
	"runtime"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
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

	// seems there is no standard way to detect if the process is running in a container
	// possible ways, see https://www.baeldung.com/linux/is-process-running-inside-container:
	// 	- check existence of /.dockerenv (not always there)
	// 	- check whether self pid is same as scheduled pid in /proc/self/sched (not always there)
	// 	- check keywords inside /proc/self/cgroup (for cgroup V1)
	// 	- check keywords inside /proc/self/mountinfo (for cgroup V2)
	//
	// but there might be false positive when check keywords, so for cgroup V2,
	// we also check where there is any mount point which is on overlay.
	// NOTE: this check is not always correct, as container can use other filesystem
	// as storage driver, see https://docs.docker.com/storage/storagedriver/select-storage-driver/
	containsKeywords := strings.Contains(string(v), "docker") ||
		strings.Contains(string(v), "kubepods") ||
		strings.Contains(string(v), "containerd")
	// For cgroup V1, check /proc/self/cgroup
	if path == procPathCGroup {
		return containsKeywords
	}

	// For cgroup V2, check /proc/self/mountinfo
	if path == procPathMountInfo {
		if !containsKeywords {
			return false
		}
		lines := strings.Split(string(v), "\n")
		for _, line := range lines {
			v := strings.Split(line, " ")
			// check mount point is on overlay or not, v[8] means `filesystem type`.
			// see details from https://man7.org/linux/man-pages/man5/proc.5.html
			// TODO: enhance this check, as overlay is not the only storage driver for container.
			if len(v) > 8 && v[8] == "overlay" {
				log.Info(fmt.Sprintf("TiDB runs in a container, mount info: %s", line))
				return true
			}
		}
	}

	return false
}
