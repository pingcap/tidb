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
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

// Version represents the cgroup version.
type Version int

// cgroup versions.
const (
	Unknown Version = 0
	V1      Version = 1
	V2      Version = 2
)

// GetMemoryLimit attempts to retrieve the cgroup memory limit for the current
// process.
func GetMemoryLimit() (limit uint64, err error) {
	limit, _, err = getCgroupMemLimit("/")
	return
}

// GetCgroupMemLimit attempts to retrieve the cgroup memory limit for the current
// process, and return cgroup version too.
func GetCgroupMemLimit() (uint64, Version, error) {
	return getCgroupMemLimit("/")
}

// GetMemoryUsage attempts to retrieve the cgroup memory usage value (in bytes)
// for the current process.
func GetMemoryUsage() (usage uint64, err error) {
	return getCgroupMemUsage("/")
}

// GetMemoryInactiveFileUsage attempts to retrieve the cgroup memory usage value (in bytes)
// for the current process.
func GetMemoryInactiveFileUsage() (usage uint64, err error) {
	return getCgroupMemInactiveFileUsage("/")
}

// root is always "/" in the production. It will be changed for testing.
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
	if len(mount) == 2 {
		usage, err = detectMemInactiveFileUsageInV1(filepath.Join(root, mount[0]))
		if err != nil {
			usage, err = detectMemInactiveFileUsageInV2(filepath.Join(root, mount[1], path))
		}
	} else {
		switch ver[0] {
		case 1:
			usage, err = detectMemInactiveFileUsageInV1(filepath.Join(root, mount[0]))
		case 2:
			usage, err = detectMemInactiveFileUsageInV2(filepath.Join(root, mount[0], path))
		default:
			usage, err = 0, fmt.Errorf("detected unknown cgroup version index: %d", ver)
		}
	}

	return usage, err
}

// getCgroupMemUsage reads the memory cgroup's current memory usage (in bytes).
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

// root is always "/" in the production. It will be changed for testing.
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
		version = V1
		limit, err = detectMemLimitInV1(filepath.Join(root, mount[0]))
		if err != nil {
			version = V2
			limit, err = detectMemLimitInV2(filepath.Join(root, mount[1], path))
		}
	} else {
		switch ver[0] {
		case 1:
			// cgroupv1
			version = V1
			limit, err = detectMemLimitInV1(filepath.Join(root, mount[0]))
		case 2:
			// cgroupv2
			version = V2
			limit, err = detectMemLimitInV2(filepath.Join(root, mount[0], path))
		default:
			limit, err = 0, fmt.Errorf("detected unknown cgroup version index: %d", ver)
		}
	}

	return limit, version, err
}

func detectMemLimitInV1(cRoot string) (limit uint64, err error) {
	return detectMemStatValue(cRoot, cgroupV1MemStat, cgroupV1MemLimitStatKey, 1)
}

// TODO(hawkingrei): this implementation was based on podman+criu environment.
// It may cover not all the cases when v2 becomes more widely used in container
// world.
// In K8S env, the value hold in memory.max is the memory limit defined in pod definition,
// the real memory limit should be the minimum value in the whole cgroup hierarchy
// as in cgroup V1, but cgroup V2 lacks the feature, see this patch for more details:
// https://lore.kernel.org/linux-kernel/ZctiM5RintD_D0Lt@host1.jankratochvil.net/T/
// So, in cgroup V2, the value better be adjusted by some factor, like 0.9, to
// avoid OOM. see taskexecutor/manager.go too.
func detectMemLimitInV2(cRoot string) (limit uint64, err error) {
	return readInt64Value(cRoot, cgroupV2MemLimit, 2)
}

func detectMemInactiveFileUsageInV1(root string) (uint64, error) {
	return detectMemStatValue(root, cgroupV1MemStat, cgroupV1MemInactiveFileUsageStatKey, 1)
}

func detectMemInactiveFileUsageInV2(root string) (uint64, error) {
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

func detectMemUsageInV1(cRoot string) (memUsage uint64, err error) {
	return readInt64Value(cRoot, cgroupV1MemUsage, 1)
}

// TODO(hawkingrei): this implementation was based on podman+criu environment. It
// may cover not all the cases when v2 becomes more widely used in container
// world.
func detectMemUsageInV2(cRoot string) (memUsage uint64, err error) {
	return readInt64Value(cRoot, cgroupV2MemUsage, 2)
}
