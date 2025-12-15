// Copyright 2025 PingCAP, Inc.
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
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

// GetMemoryLimit returns the memory limit for the current process.
// This is Linux-specific and not supported on the current OS.
func GetMemoryLimit() (limit uint64, err error) {
	return 0, nil
}

// GetCgroupMemLimit returns the cgroup memory limit for the current process.
// This is Linux-specific and not supported on the current OS.
func GetCgroupMemLimit() (uint64, Version, error) {
	return 0, Unknown, nil
}

// GetMemoryUsage returns the memory usage for the current process.
// This is Linux-specific and not supported on the current OS.
func GetMemoryUsage() (usage uint64, err error) {
	return 0, nil
}

// GetMemoryInactiveFileUsage returns the inactive file memory usage for the current process.
// This is Linux-specific and not supported on the current OS.
func GetMemoryInactiveFileUsage() (usage uint64, err error) {
	return 0, nil
}

// getCgroupMemUsage is used by tests but not supported on non-Linux platforms.
func getCgroupMemUsage(root string) (usage uint64, err error) {
	path, err := detectControlPath(filepath.Join(root, procPathCGroup), "memory")
	if err != nil {
		return 0, err
	}

	if path == "" {
		log.Warn("no cgroup memory controller detected")
		return 0, nil
	}

	mount, ver, err := getCgroupDetails(filepath.Join(root, procPathMountInfo), path, "memory")
	if err != nil {
		return 0, err
	}

	if len(ver) == 2 {
		usage, err = detectMemUsageInV1(filepath.Join(root, mount[0]))
		if err != nil {
			usage, err = detectMemUsageInV2(filepath.Join(root, mount[0], path))
		}
	} else {
		switch ver[0] {
		case 1:
			// cgroupv1
			usage, err = detectMemUsageInV1(filepath.Join(root, mount[0]))
		case 2:
			// cgroupv2
			usage, err = detectMemUsageInV2(filepath.Join(root, mount[0], path))
		default:
			usage, err = 0, fmt.Errorf("detected unknown cgroup version index: %d", ver)
		}
	}

	return usage, err
}

// getCgroupMemInactiveFileUsage is used by tests but not supported on non-Linux platforms.
func getCgroupMemInactiveFileUsage(root string) (usage uint64, err error) {
	path, err := detectControlPath(filepath.Join(root, procPathCGroup), "memory")
	if err != nil {
		return 0, err
	}

	if path == "" {
		log.Warn("no cgroup memory controller detected")
		return 0, nil
	}

	mount, ver, err := getCgroupDetails(filepath.Join(root, procPathMountInfo), path, "memory")
	if err != nil {
		return 0, err
	}

	if len(ver) == 2 {
		usage, err = detectMemInactiveFileInV1(filepath.Join(root, mount[0]))
		if err != nil {
			usage, err = detectMemInactiveFileInV2(filepath.Join(root, mount[0], path))
		}
	} else {
		switch ver[0] {
		case 1:
			// cgroupv1
			usage, err = detectMemInactiveFileInV1(filepath.Join(root, mount[0]))
		case 2:
			// cgroupv2
			usage, err = detectMemInactiveFileInV2(filepath.Join(root, mount[0], path))
		default:
			usage, err = 0, fmt.Errorf("detected unknown cgroup version index: %d", ver)
		}
	}

	return usage, err
}

// getCgroupMemLimit is used by tests but not supported on non-Linux platforms.
func getCgroupMemLimit(root string) (limit uint64, version Version, err error) {
	version = Unknown
	path, err := detectControlPath(filepath.Join(root, procPathCGroup), "memory")
	if err != nil {
		return 0, version, err
	}

	if path == "" {
		log.Warn("no cgroup memory controller detected")
		return 0, version, nil
	}

	mount, ver, err := getCgroupDetails(filepath.Join(root, procPathMountInfo), path, "memory")
	if err != nil {
		return 0, version, err
	}

	if len(ver) == 2 {
		limit, version, err = detectMemLimitInV1(filepath.Join(root, mount[0]))
		if err != nil {
			limit, version, err = detectMemLimitInV2(filepath.Join(root, mount[0], path))
		}
	} else {
		switch ver[0] {
		case 1:
			// cgroupv1
			limit, version, err = detectMemLimitInV1(filepath.Join(root, mount[0]))
		case 2:
			// cgroupv2
			limit, version, err = detectMemLimitInV2(filepath.Join(root, mount[0], path))
		default:
			limit, err = 0, fmt.Errorf("detected unknown cgroup version index: %d", ver)
		}
	}

	return limit, version, err
}

// Mock implementations for testing - these functions are only available on Linux
func detectMemUsageInV1(cRoot string) (usage uint64, err error) {
	return readInt64Value(cRoot, cgroupV1MemUsage, 1)
}

func detectMemUsageInV2(cRoot string) (usage uint64, err error) {
	return readInt64Value(cRoot, cgroupV2MemUsage, 2)
}

func detectMemLimitInV1(cRoot string) (limit uint64, version Version, err error) {
	limit, err = detectMemStatValue(cRoot, cgroupV1MemStat, cgroupV1MemLimitStatKey, 1)
	return limit, V1, err
}

func detectMemLimitInV2(cRoot string) (limit uint64, version Version, err error) {
	limit, err = readInt64Value(cRoot, cgroupV2MemLimit, 2)
	return limit, V2, err
}

func detectMemInactiveFileInV1(root string) (uint64, error) {
	return detectMemStatValue(root, cgroupV1MemStat, cgroupV1MemInactiveFileUsageStatKey, 1)
}

func detectMemInactiveFileInV2(root string) (uint64, error) {
	return detectMemStatValue(root, cgroupV2MemStat, cgroupV2MemInactiveFileUsageStatKey, 2)
}

func detectMemStatValue(cRoot, filename, key string, cgVersion int) (value uint64, err error) {
	statFilePath := filepath.Join(cRoot, filename)
	//nolint:gosec
	stat, err := os.Open(statFilePath)
	if err != nil {
		return 0, errors.Wrapf(err, "can't read file %s from cgroup v%d", filename, cgVersion)
	}
	defer func() {
		_ = stat.Close()
	}()

	scanner := bufio.NewScanner(stat)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) != 2 || string(fields[0]) != key {
			continue
		}

		trimmed := string(bytes.TrimSpace(fields[1]))
		value, err = strconv.ParseUint(trimmed, 10, 64)
		if err != nil {
			return 0, errors.Wrapf(err, "can't read %q memory stat from cgroup v%d in %s", key, cgVersion, filename)
		}

		return value, nil
	}

	return 0, fmt.Errorf("failed to find expected memory stat %q for cgroup v%d in %s", key, cgVersion, filename)
}
