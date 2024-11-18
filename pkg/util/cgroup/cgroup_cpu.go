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
		return CPUUsage{}, errors.New("no cpu controller detected")
	}

	mount, ver, err := getCgroupDetails(filepath.Join(root, procPathMountInfo), path, "cpu,cpuacct")
	if err != nil {
		return CPUUsage{}, err
	}

	var res CPUUsage
	if len(mount) == 2 {
		cgroupRootV1 := filepath.Join(root, mount[0])
		cgroupRootV2 := filepath.Join(root, mount[1], path)
		res.Period, res.Quota, err = detectCPUQuotaInV2(cgroupRootV2)
		if err != nil {
			res.Period, res.Quota, err = detectCPUQuotaInV1(cgroupRootV1)
		}
		if err != nil {
			return res, err
		}
		res.Stime, res.Utime, err = detectCPUUsageInV2(cgroupRootV2)
		if err != nil {
			res.Stime, res.Utime, err = detectCPUUsageInV1(cgroupRootV1)
		}
		if err != nil {
			return res, err
		}
	} else {
		switch ver[0] {
		case 1:
			cgroupRoot := filepath.Join(root, mount[0])
			res.Period, res.Quota, err = detectCPUQuotaInV1(cgroupRoot)
			if err != nil {
				return res, err
			}
			res.Stime, res.Utime, err = detectCPUUsageInV1(cgroupRoot)
			if err != nil {
				return res, err
			}
		case 2:
			cgroupRoot := filepath.Join(root, mount[0], path)
			res.Period, res.Quota, err = detectCPUQuotaInV2(cgroupRoot)
			if err != nil {
				return res, err
			}
			res.Stime, res.Utime, err = detectCPUUsageInV2(cgroupRoot)
			if err != nil {
				return res, err
			}
		default:
			return CPUUsage{}, fmt.Errorf("detected unknown cgroup version index: %d", ver[0])
		}
	}

	return res, nil
}

// Helper function for getCgroupCPUPeriodAndQuota. Root is always "/", except in tests.
func getCgroupCPUPeriodAndQuota(root string) (period int64, quota int64, err error) {
	path, err := detectControlPath(filepath.Join(root, procPathCGroup), "cpu")
	if err != nil {
		return
	}

	// No CPU controller detected
	if path == "" {
		err = errors.New("no cpu controller detected")
		return
	}

	mount, ver, err := getCgroupDetails(filepath.Join(root, procPathMountInfo), path, "cpu")
	if err != nil {
		return
	}

	if len(mount) == 2 {
		cgroupRootV1 := filepath.Join(root, mount[0])
		cgroupRootV2 := filepath.Join(root, mount[1], path)
		period, quota, err = detectCPUQuotaInV2(cgroupRootV2)
		if err != nil {
			period, quota, err = detectCPUQuotaInV1(cgroupRootV1)
		}
		if err != nil {
			return
		}
	} else {
		switch ver[0] {
		case 1:
			cgroupRoot := filepath.Join(root, mount[0])
			period, quota, err = detectCPUQuotaInV1(cgroupRoot)
		case 2:
			cgroupRoot := filepath.Join(root, mount[0], path)
			period, quota, err = detectCPUQuotaInV2(cgroupRoot)
		default:
			err = fmt.Errorf("detected unknown cgroup version index: %d", ver[0])
		}
	}
	return
}

// CPUShares returns the number of CPUs this cgroup can be expected to
// max out. If there's no limit, NumCPU is returned.
func (c CPUUsage) CPUShares() float64 {
	if c.Period <= 0 || c.Quota <= 0 {
		return float64(c.NumCPU)
	}
	return float64(c.Quota) / float64(c.Period)
}
