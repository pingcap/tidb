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
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// CPUQuotaStatus presents the status of how CPU quota is used
type CPUQuotaStatus int

const (
	// CPUQuotaUndefined is returned when CPU quota is undefined
	CPUQuotaUndefined CPUQuotaStatus = iota
	// CPUQuotaUsed is returned when a valid CPU quota can be used
	CPUQuotaUsed
	// CPUQuotaMinUsed is return when CPU quota is smaller than the min value
	CPUQuotaMinUsed
)

const (
	_maxProcsKey = "GOMAXPROCS"

	// They are cgroup filename for different data
	cgroupV1MemStat      = "memory.stat"
	cgroupV2MemStat      = "memory.stat"
	cgroupV2MemLimit     = "memory.max"
	cgroupV1MemUsage     = "memory.usage_in_bytes"
	cgroupV2MemUsage     = "memory.current"
	cgroupV1CPUQuota     = "cpu.cfs_quota_us"
	cgroupV1CPUPeriod    = "cpu.cfs_period_us"
	cgroupV1CPUSysUsage  = "cpuacct.usage_sys"
	cgroupV1CPUUserUsage = "cpuacct.usage_user"
	cgroupV2CPUMax       = "cpu.max"
	cgroupV2CPUStat      = "cpu.stat"

	// {memory|cpu}.stat file keys
	//
	// key for # of bytes of file-backed memory on inactive LRU list in cgroupv1
	cgroupV1MemInactiveFileUsageStatKey = "total_inactive_file"
	// key for # of bytes of file-backed memory on inactive LRU list in cgroupv2
	cgroupV2MemInactiveFileUsageStatKey = "inactive_file"
	cgroupV1MemLimitStatKey             = "hierarchical_memory_limit"
)
const (
	procPathCGroup    = "/proc/self/cgroup"
	procPathMountInfo = "/proc/self/mountinfo"
)

// CPUUsage returns CPU usage and quotas for an entire cgroup.
type CPUUsage struct {
	// System time and user time taken by this cgroup or process. In nanoseconds.
	Stime, Utime uint64
	// CPU period and quota for this process, in microseconds. This cgroup has
	// access to up to (quota/period) proportion of CPU resources on the system.
	// For instance, if there are 4 CPUs, quota = 150000, period = 100000,
	// this cgroup can use around ~1.5 CPUs, or 37.5% of total scheduler time.
	// If quota is -1, it's unlimited.
	Period, Quota int64
	// NumCPUs is the number of CPUs in the system. Always returned even if
	// not called from a cgroup.
	NumCPU int
}

// SetGOMAXPROCS is to set GOMAXPROCS to the number of CPUs.
func SetGOMAXPROCS() (func(), error) {
	const minGOMAXPROCS int = 1

	undoNoop := func() {
		log.Info("maxprocs: No GOMAXPROCS change to reset")
	}

	if max, exists := os.LookupEnv(_maxProcsKey); exists {
		log.Info(fmt.Sprintf("maxprocs: Honoring GOMAXPROCS=%q as set in environment", max))
		return undoNoop, nil
	}

	maxProcs, status, err := CPUQuotaToGOMAXPROCS(minGOMAXPROCS)
	if err != nil {
		return undoNoop, err
	}

	if status == CPUQuotaUndefined {
		log.Info(fmt.Sprintf("maxprocs: Leaving GOMAXPROCS=%v: CPU quota undefined", runtime.GOMAXPROCS(0)))
		return undoNoop, nil
	}

	prev := runtime.GOMAXPROCS(0)
	undo := func() {
		log.Info(fmt.Sprintf("maxprocs: Resetting GOMAXPROCS to %v", prev))
		runtime.GOMAXPROCS(prev)
	}
	if prev == maxProcs {
		return undoNoop, nil
	}
	switch status {
	case CPUQuotaMinUsed:
		log.Info(fmt.Sprintf("maxprocs: Updating GOMAXPROCS=%v: using minimum allowed GOMAXPROCS", maxProcs))
	case CPUQuotaUsed:
		log.Info(fmt.Sprintf("maxprocs: Updating GOMAXPROCS=%v: determined from CPU quota", maxProcs))
	}
	runtime.GOMAXPROCS(maxProcs)
	return undo, nil
}

func readFile(filepath string) (res []byte, err error) {
	var f *os.File
	//nolint:gosec
	f, err = os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.CombineErrors(err, f.Close())
	}()
	res, err = io.ReadAll(f)
	return res, err
}

// The field in /proc/self/cgroup and /proc/self/mountinfo may appear as "cpuacct,cpu" or "rw,cpuacct,cpu"
// while the input controller is "cpu,cpuacct"
func controllerMatch(field string, controller string) bool {
	if field == controller {
		return true
	}

	fs := strings.Split(field, ",")
	if len(fs) < 2 {
		return false
	}
	cs := strings.Split(controller, ",")
	if len(fs) < len(cs) {
		return false
	}
	fmap := make(map[string]struct{}, len(fs))
	for _, f := range fs {
		fmap[f] = struct{}{}
	}
	for _, c := range cs {
		if _, ok := fmap[c]; !ok {
			return false
		}
	}
	return true
}

// The controller is defined via either type `memory` for cgroup v1 or via empty type for cgroup v2,
// where the type is the second field in /proc/[pid]/cgroup file
func detectControlPath(cgroupFilePath string, controller string) (string, error) {
	//nolint:gosec
	cgroup, err := os.Open(cgroupFilePath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to read %s cgroup from cgroups file: %s", controller, cgroupFilePath)
	}
	defer func() {
		err := cgroup.Close()
		if err != nil {
			log.Error("close cgroupFilePath", zap.Error(err))
		}
	}()

	scanner := bufio.NewScanner(cgroup)
	var unifiedPathIfFound string
	for scanner.Scan() {
		fields := bytes.Split(scanner.Bytes(), []byte{':'})
		if len(fields) < 3 {
			// The lines should always have three fields, there's something fishy here.
			continue
		}

		f0, f1 := string(fields[0]), string(fields[1])
		// First case if v2, second - v1. We give v2 the priority here.
		// There is also a `hybrid` mode when both  versions are enabled,
		// but no known container solutions support it.
		if f0 == "0" && f1 == "" {
			unifiedPathIfFound = string(fields[2])
		} else if controllerMatch(f1, controller) {
			var result []byte
			// In some case, the cgroup path contains `:`. We need to join them back.
			if len(fields) > 3 {
				result = bytes.Join(fields[2:], []byte(":"))
			} else {
				result = fields[2]
			}
			return string(result), nil
		}
	}

	return unifiedPathIfFound, nil
}

// See http://man7.org/linux/man-pages/man5/proc.5.html for `mountinfo` format.
func getCgroupDetails(mountInfoPath string, cRoot string, controller string) (mount []string, version []int, err error) {
	//nolint:gosec
	info, err := os.Open(mountInfoPath)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to read mounts info from file: %s", mountInfoPath)
	}
	defer func() {
		err := info.Close()
		if err != nil {
			log.Error("close mountInfoPath", zap.Error(err))
		}
	}()
	var foundVer1, foundVer2 = false, false
	var mountPointVer1, mountPointVer2 string

	scanner := bufio.NewScanner(info)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) < 10 {
			continue
		}

		ver, ok := detectCgroupVersion(fields, controller)
		if ok {
			mountPoint := string(fields[4])
			if ver == 2 {
				foundVer2 = true
				mountPointVer2 = mountPoint
				continue
			}
			// It is possible that the controller mount and the cgroup path are not the same (both are relative to the NS root).
			// So start with the mount and construct the relative path of the cgroup.
			// To test:
			//  1、start a docker to run unit test or tidb-server
			//   > docker run -it --cpus=8 --memory=8g --name test --rm ubuntu:18.04 bash
			//
			//  2、change the limit when the container is running
			//	docker update --cpus=8 <containers>
			nsRelativePath := string(fields[3])
			if !strings.Contains(nsRelativePath, "..") {
				// We don't expect to see err here ever but in case that it happens
				// the best action is to ignore the line and hope that the rest of the lines
				// will allow us to extract a valid path.
				if relPath, err := filepath.Rel(nsRelativePath, cRoot); err == nil {
					mountPointVer1 = filepath.Join(mountPoint, relPath)
					foundVer1 = true
				}
			}
		}
	}
	if foundVer1 && foundVer2 {
		return []string{mountPointVer1, mountPointVer2}, []int{1, 2}, nil
	}
	if foundVer1 {
		return []string{mountPointVer1}, []int{1}, nil
	}
	if foundVer2 {
		return []string{mountPointVer2}, []int{2}, nil
	}

	return nil, nil, fmt.Errorf("failed to detect cgroup root mount and version")
}

func cgroupFileToUint64(filepath, desc string) (res uint64, err error) {
	contents, err := readFile(filepath)
	if err != nil {
		return 0, errors.Wrapf(err, "error when reading %s from cgroup v1 at %s", desc, filepath)
	}
	res, err = strconv.ParseUint(string(bytes.TrimSpace(contents)), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "error when parsing %s from cgroup v1 at %s", desc, filepath)
	}
	return res, err
}

func cgroupFileToInt64(filepath, desc string) (res int64, err error) {
	contents, err := readFile(filepath)
	if err != nil {
		return 0, errors.Wrapf(err, "error when reading %s from cgroup v1 at %s", desc, filepath)
	}
	res, err = strconv.ParseInt(string(bytes.TrimSpace(contents)), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "error when parsing %s from cgroup v1 at %s", desc, filepath)
	}
	return res, nil
}

// Return version of cgroup mount for memory controller if found
func detectCgroupVersion(fields [][]byte, controller string) (_ int, found bool) {
	if len(fields) < 10 {
		return 0, false
	}

	// Due to strange format there can be optional fields in the middle of the set, starting
	// from the field #7. The end of the fields is marked with "-" field
	var pos = 6
	for pos < len(fields) {
		if bytes.Equal(fields[pos], []byte{'-'}) {
			break
		}

		pos++
	}

	// No optional fields separator found or there is less than 3 fields after it which is wrong
	if (len(fields) - pos - 1) < 3 {
		return 0, false
	}

	pos++

	// Check for controller specifically in cgroup v1 (it is listed in super
	// options field), as the value can't be found if it is not enforced.
	if bytes.Equal(fields[pos], []byte("cgroup")) && controllerMatch(string(fields[pos+2]), controller) {
		return 1, true
	} else if bytes.Equal(fields[pos], []byte("cgroup2")) {
		return 2, true
	}

	return 0, false
}

func detectCPUQuotaInV1(cRoot string) (period, quota int64, err error) {
	quotaFilePath := filepath.Join(cRoot, cgroupV1CPUQuota)
	periodFilePath := filepath.Join(cRoot, cgroupV1CPUPeriod)
	quota, err = cgroupFileToInt64(quotaFilePath, "cpu quota")
	if err != nil {
		return 0, 0, err
	}
	period, err = cgroupFileToInt64(periodFilePath, "cpu period")
	if err != nil {
		return 0, 0, err
	}

	return period, quota, err
}

func detectCPUUsageInV1(cRoot string) (stime, utime uint64, err error) {
	sysFilePath := filepath.Join(cRoot, cgroupV1CPUSysUsage)
	userFilePath := filepath.Join(cRoot, cgroupV1CPUUserUsage)
	stime, err = cgroupFileToUint64(sysFilePath, "cpu system time")
	if err != nil {
		return 0, 0, err
	}
	utime, err = cgroupFileToUint64(userFilePath, "cpu user time")
	if err != nil {
		return 0, 0, err
	}

	return stime, utime, err
}

func detectCPUQuotaInV2(cRoot string) (period, quota int64, err error) {
	maxFilePath := filepath.Join(cRoot, cgroupV2CPUMax)
	contents, err := readFile(maxFilePath)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "error when read cpu quota from cgroup v2 at %s", maxFilePath)
	}
	fields := strings.Fields(string(contents))
	if len(fields) > 2 || len(fields) == 0 {
		return 0, 0, errors.Errorf("unexpected format when reading cpu quota from cgroup v2 at %s: %s", maxFilePath, contents)
	}
	if fields[0] == "max" {
		// Negative quota denotes no limit.
		quota = -1
	} else {
		quota, err = strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0, 0, errors.Wrapf(err, "error when reading cpu quota from cgroup v2 at %s", maxFilePath)
		}
	}
	if len(fields) == 2 {
		period, err = strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, 0, errors.Wrapf(err, "error when reading cpu period from cgroup v2 at %s", maxFilePath)
		}
	}
	return period, quota, nil
}

func detectCPUUsageInV2(cRoot string) (stime, utime uint64, err error) {
	statFilePath := filepath.Join(cRoot, cgroupV2CPUStat)
	var stat *os.File
	//nolint:gosec
	stat, err = os.Open(statFilePath)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "can't read cpu usage from cgroup v2 at %s", statFilePath)
	}
	defer func() {
		err = errors.CombineErrors(err, stat.Close())
	}()

	scanner := bufio.NewScanner(stat)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) != 2 || (string(fields[0]) != "user_usec" && string(fields[0]) != "system_usec") {
			continue
		}
		keyField := string(fields[0])

		trimmed := string(bytes.TrimSpace(fields[1]))
		usageVar := &stime
		if keyField == "user_usec" {
			usageVar = &utime
		}
		*usageVar, err = strconv.ParseUint(trimmed, 10, 64)
		if err != nil {
			return 0, 0, errors.Wrapf(err, "can't read cpu usage %s from cgroup v1 at %s", keyField, statFilePath)
		}
	}

	return stime, utime, err
}

func readInt64Value(root, filename string, cgVersion int) (value uint64, err error) {
	filePath := filepath.Join(root, filename)
	//nolint:gosec
	file, err := os.Open(filePath)
	if err != nil {
		return 0, errors.Wrapf(err, "can't read %s from cgroup v%d", filename, cgVersion)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	present := scanner.Scan()
	if !present {
		return 0, errors.Wrapf(err, "no value found in %s from cgroup v%d", filename, cgVersion)
	}
	data := scanner.Bytes()
	trimmed := string(bytes.TrimSpace(data))
	// cgroupv2 has certain control files that default to "max", so handle here.
	if trimmed == "max" {
		return math.MaxInt64, nil
	}
	value, err = strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to parse value in %s from cgroup v%d", filename, cgVersion)
	}
	return value, nil
}
