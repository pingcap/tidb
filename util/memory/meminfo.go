// Copyright 2018 PingCAP, Inc.
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

package memory

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/parser/terror"
	"github.com/shirou/gopsutil/v3/mem"
)

// MemTotal returns the total amount of RAM on this system
var MemTotal func() (uint64, error)

// MemUsed returns the total used amount of RAM on this system
var MemUsed func() (uint64, error)

// MemTotalNormal returns the total amount of RAM on this system in non-container environment.
func MemTotalNormal() (uint64, error) {
	total, t := memLimit.get()
	if time.Since(t) < 60*time.Second {
		return total, nil
	}
	v, err := mem.VirtualMemory()
	if err != nil {
		return v.Total, err
	}
	memLimit.set(v.Total, time.Now())
	return v.Total, nil
}

// MemUsedNormal returns the total used amount of RAM on this system in non-container environment.
func MemUsedNormal() (uint64, error) {
	used, t := memUsage.get()
	if time.Since(t) < 500*time.Millisecond {
		return used, nil
	}
	v, err := mem.VirtualMemory()
	if err != nil {
		return v.Used, err
	}
	memUsage.set(v.Used, time.Now())
	return v.Used, nil
}

const (
	// A process can access its own /proc/PID directory using the symbolic link /proc/self
	selfCGroupPath = "/proc/self/cgroup"
	memCtrName     = "memory"
	// the maximum limit of the Linux virtual memory system, which is 2^63 - 4096
	maximumBytes uint64 = 0x7FFFFFFFFFFFF000
)

type memInfoCache struct {
	*sync.RWMutex
	mem        uint64
	updateTime time.Time
}

func (c *memInfoCache) get() (mem uint64, t time.Time) {
	c.RLock()
	defer c.RUnlock()
	mem, t = c.mem, c.updateTime
	return
}

func (c *memInfoCache) set(mem uint64, t time.Time) {
	c.Lock()
	defer c.Unlock()
	c.mem, c.updateTime = mem, t
}

// expiration time is 60s
var memLimit *memInfoCache

// expiration time is 500ms
var memUsage *memInfoCache

// expiration time is 500ms
// save the memory usage of the server process
var serverMemUsage *memInfoCache

// whether hierarchical accounting is enabled
var isHierarchical bool

// isCgroupV1 is true when the container is using cgroup v1
// Docker supports cgroup v2 since Docker 20.10
var isCgroupV1 bool

// cgroupEntrypoint is mostly /sys/fs/cgroup
var cgroupEntrypoint string

// hostTotalMem is the container's host machine's total memory
var hostTotalMem uint64

func init() {
	var err error
	if inContainer() {
		MemTotal = MemTotalCGroup
		MemUsed = MemUsedCGroup
		isHierarchical = getHierarchicalStatus()
		isCgroupV1, err = isCgrpV1()
		terror.MustNil(err)
		cgroupEntrypoint, err = getCgroupEntrypoint()
		terror.MustNil(err)
		v, err := mem.VirtualMemory()
		terror.MustNil(err)
		hostTotalMem = v.Total
	} else {
		MemTotal = MemTotalNormal
		MemUsed = MemUsedNormal
	}
	memLimit = &memInfoCache{
		RWMutex: &sync.RWMutex{},
	}
	memUsage = &memInfoCache{
		RWMutex: &sync.RWMutex{},
	}
	serverMemUsage = &memInfoCache{
		RWMutex: &sync.RWMutex{},
	}
	_, err = MemTotal()
	terror.MustNil(err)
	_, err = MemUsed()
	terror.MustNil(err)
}

func inContainer() bool {
	v, err := os.ReadFile(selfCGroupPath)
	if err != nil {
		return false
	}
	if strings.Contains(string(v), "docker") ||
		strings.Contains(string(v), "kubepods") ||
		strings.Contains(string(v), "containerd") {
		return true
	}
	return false
}

// getCgroupEntrypoint returns mount entrypoint of cgroup
func getCgroupEntrypoint() (string, error) {
	if isCgroupV1 {
		return "/sys/fs/cgroup", nil
	}
	selfMounts, err := safeReadFile("/proc/self/mounts")
	if err != nil {
		return "", err
	}
	gf := func(line string) bool {
		return strings.Contains(line, "cgroup2")
	}
	return grepFirstMatch(string(selfMounts), gf, 1, " ")
}

// InstanceMemUsed returns the memory usage of this TiDB server
func InstanceMemUsed() (uint64, error) {
	used, t := serverMemUsage.get()
	if time.Since(t) < 500*time.Millisecond {
		return used, nil
	}
	var memoryUsage uint64
	instanceStats := &runtime.MemStats{}
	runtime.ReadMemStats(instanceStats)
	memoryUsage = instanceStats.HeapAlloc
	serverMemUsage.set(memoryUsage, time.Now())
	return memoryUsage, nil
}

// MemTotalCGroup returns the total amount of RAM that TIDB is limited to use in container environment.
func MemTotalCGroup() (uint64, error) {
	mem, t := memLimit.get()
	if time.Since(t) < 60*time.Second {
		return mem, nil
	}
	mem, err := getMemoryLimit()
	if err != nil {
		return mem, err
	}
	if mem == maximumBytes {
		mem = hostTotalMem
	}
	memLimit.set(mem, time.Now())
	return mem, nil
}

// MemUsedCGroup returns the total used amount of RAM on this system in container environment.
func MemUsedCGroup() (uint64, error) {
	mem, t := memUsage.get()
	if time.Since(t) < 500*time.Millisecond {
		return mem, nil
	}
	mem, err := getMemoryUsed()
	if err != nil {
		return mem, err
	}
	memUsage.set(mem, time.Now())
	return mem, nil
}

// getMemoryLimit reads from memory subsystem and returns TIDB's memory limit
func getMemoryLimit() (uint64, error) {
	if isCgroupV1 {
		limitInBytes, err := getCgroupFileValue(memCtrName, "memory.limit_in_bytes")
		if err != nil {
			return maximumBytes, err
		}
		if limitInBytes == uint64(maximumBytes) {
			if isHierarchical {
				return grepCgroupFile(memCtrName, "memory.stat", "hierarchical_memory_limit")
			}
		}
		return limitInBytes, nil
	}
	// for cgroup v2, we get memory limit directly from /sys/fs/cgroup/memory.max
	v, err := getCgroupFileValue("", "memory.max")
	if err != nil {
		return maximumBytes, nil
	}
	return v, nil
}

// getMemoryUsed calculate memory usage of the container
// On cgroup v1 host, the result is `memory.usage_in_bytes - mem.Stats["total_inactive_file"]` .
// On cgroup v2 host, the result is `memory.usage_in_bytes - mem.Stats["inactive_file"] `.
func getMemoryUsed() (uint64, error) {
	var (
		ctr, memUsedFile, metric string
		mem                      uint64
		err                      error
	)

	if isCgroupV1 {
		ctr, memUsedFile, metric = memCtrName, "memory.usage_in_bytes", "total_inactive_file"
	} else {
		ctr, memUsedFile, metric = "", "memory.current", "inactive_file"
	}
	mem, err = getCgroupFileValue(ctr, memUsedFile)
	if err != nil {
		return mem, err
	}

	v, err := grepCgroupFile(memCtrName, "memory.stat", metric)
	if err != nil {
		return mem, err
	}
	if v < mem {
		return mem - v, nil
	}
	return mem, nil
}

// getHierarchicalStatus returns true when hierarchical accounting is enabled
// find more information on https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
// cgroup v1 only
func getHierarchicalStatus() bool {
	value, _ := readCgroupFile(memCtrName, "memory.use_hierarchy")
	return strings.TrimSpace(string(value)) == "1"
}

// isCgrpV1 tells whether we are using cgroupV1
// see https://docs.docker.com/config/containers/runmetrics/#enumerate-cgroups
func isCgrpV1() (bool, error) {
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

// safeReadFile is a wrapper for os.ReadFile, it's panic free
func safeReadFile(path string) ([]byte, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	} else {
		return os.ReadFile(path)
	}
}

// getCtrRelativePath returns a cgroup controller's relative path to cgroups' mount entrypoint
func getCtrRelativePath(controller string) (string, error) {
	var gf grepMatchFunc

	cgroupData, err := safeReadFile(selfCGroupPath)
	if err != nil {
		return "", err
	}
	if isCgroupV1 {
		gf = func(line string) bool {
			return strings.Contains(line, controller)
		}
	} else { // cgroup v2
		gf = func(line string) bool {
			fields := strings.Split(line, ":")
			return len(fields) == 3 && fields[0] == "0" && fields[1] == ""
		}
	}

	// controllerPath is a path relative to cgroup's mount entrypoint,
	// e.g., /docker/0050c0fc269ced92965d8cda0a818125b10c3f7c7125f14f233ae9d69ba65e3f
	controllerPath, err := grepFirstMatch(string(cgroupData), gf, 2, ":")
	if err != nil {
		return "", fmt.Errorf("cannot find controller: %s's path using self cgroup: %w", controller, err)
	}
	return controllerPath, nil

}

// readCgroupFile reads a file from a cgroup hierarchy
// e.g., read file memory.limit_in_bytes in /sys/fs/cgroup/memory/docker/0050c0fc269ced92965d8cda0a818125b10c3f7c7125f14f233ae9d69ba65e3f
func readCgroupFile(controller, filename string) ([]byte, error) {
	relativePath, err := getCtrRelativePath(controller)
	if err != nil {
		return nil, err
	}
	var template string
	if controller != "" {
		template = "%s/%s%s/%s"
	} else {
		template = "%s%s%s/%s"
	}
	return safeReadFile(fmt.Sprintf(template, cgroupEntrypoint, controller, relativePath, filename))
}

// getCgroupFileValue reads a file from a cgroup hierarchy and returns its value
func getCgroupFileValue(controller, filename string) (uint64, error) {
	v, err := readCgroupFile(controller, filename)
	if err != nil {
		return 0, err
	}
	// for cgroup v2, "max" means no limit, see https://man7.org/linux/man-pages/man7/cgroups.7.html
	if strings.TrimSpace(string(v)) == "max" {
		return maximumBytes, nil
	}
	return parseUint(strings.TrimSpace(string(v)), 10, 64)
}

// grepCgroupFile greps a cgroup file by row names,
// e.g., "inactive_file"(as an itemName) in /sys/fs/cgroup/memory/docker/0050c0fc269ced92965d8cda0a818125b10c3f7c7125f14f233ae9d69ba65e3f/memory.stat
func grepCgroupFile(controller, fileName, itemName string) (uint64, error) {
	memStatF, err := readCgroupFile(controller, fileName)
	if err != nil {
		return maximumBytes, err
	}
	gf := func(line string) bool {
		return strings.Contains(line, itemName)
	}
	memStat, err := grepFirstMatch(string(memStatF), gf, 1, " ")
	if err != nil {
		return maximumBytes, err
	}
	v, err := strconv.ParseUint(memStat, 10, 64)
	if err != nil {
		return maximumBytes, err
	}
	return v, nil
}

// grepMatchFunc is used when greping a file line by line, if grepMatchFunc returns true
// then we find the wanted line
type grepMatchFunc func(line string) bool

// grepFirstMatch searches match line at data via a match function and returns item from it by index with given delimiter.
func grepFirstMatch(data string, match grepMatchFunc, index int, delimiter string) (string, error) {
	lines := strings.Split(string(data), "\n")
	for _, s := range lines {
		if !match(s) {
			continue
		}
		parts := strings.Split(s, delimiter)
		if index < len(parts) {
			return strings.TrimSpace(parts[index]), nil
		}
	}
	return "", fmt.Errorf("cannot find match in data: %s", data)
}

// refer to https://github.com/containerd/cgroups/blob/318312a373405e5e91134d8063d04d59768a1bff/utils.go#L251
func parseUint(s string, base, bitSize int) (uint64, error) {
	v, err := strconv.ParseUint(s, base, bitSize)
	if err != nil {
		intValue, intErr := strconv.ParseInt(s, base, bitSize)
		// 1. Handle negative values greater than MinInt64 (and)
		// 2. Handle negative values lesser than MinInt64
		if intErr == nil && intValue < 0 {
			return 0, nil
		} else if intErr != nil &&
			intErr.(*strconv.NumError).Err == strconv.ErrRange &&
			intValue < 0 {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}
