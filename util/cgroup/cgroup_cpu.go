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

package cgroup

import (
	"fmt"
	"path/filepath"

	"github.com/pingcap/errors"
)

var errNoCPUControllerDetected = errors.New("no cpu controller detected")

// Helper function for getCgroupCPU. Root is always "/", except in tests.
func getCgroupCPU(root string) (CPUUsage, error) {
	path, err := detectControlPath(filepath.Join(root, procPathCGroup), "cpu,cpuacct")
	if err != nil {
		return CPUUsage{}, err
	}

	// No CPU controller detected
	if path == "" {
		return CPUUsage{}, errNoCPUControllerDetected
	}

	mount, ver, err := getCgroupDetails(filepath.Join(root, procPathMountInfo), path, "cpu,cpuacct")
	if err != nil {
		return CPUUsage{}, err
	}

	var res CPUUsage

	switch ver {
	case 1:
		cgroupRoot := filepath.Join(root, mount)
		res.Period, res.Quota, err = detectCPUQuotaInV1(cgroupRoot)
		if err != nil {
			return res, err
		}
		res.Stime, res.Utime, err = detectCPUUsageInV1(cgroupRoot)
		if err != nil {
			return res, err
		}
	case 2:
		cgroupRoot := filepath.Join(root, mount, path)
		res.Period, res.Quota, err = detectCPUQuotaInV2(cgroupRoot)
		if err != nil {
			return res, err
		}
		res.Stime, res.Utime, err = detectCPUUsageInV2(cgroupRoot)
		if err != nil {
			return res, err
		}
	default:
		return CPUUsage{}, fmt.Errorf("detected unknown cgroup version index: %d", ver)
	}

	return res, nil
}

// CPUShares returns the number of CPUs this cgroup can be expected to
// max out. If there's no limit, NumCPU is returned.
func (c CPUUsage) CPUShares() float64 {
	if c.Period <= 0 || c.Quota <= 0 {
		return float64(c.NumCPU)
	}
	return float64(c.Quota) / float64(c.Period)
}
