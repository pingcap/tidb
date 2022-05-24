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
	selfCGroupPath         = "/proc/self/cgroup"
	memorySubsystem        = "memory"
	maximumBytes    uint64 = 0x7FFFFFFFFFFFF000
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

func init() {
	if inContainer() {
		MemTotal = MemTotalCGroup
		MemUsed = MemUsedCGroup
		isHierarchical = getHierarchicalStatus()
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
	_, err := MemTotal()
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
	memLimit.set(mem, time.Now())
	return mem, nil
}

// MemUsedCGroup returns the total used amount of RAM on this system in container environment.
func MemUsedCGroup() (uint64, error) {
	mem, t := memUsage.get()
	if time.Since(t) < 500*time.Millisecond {
		return mem, nil
	}
	mem, err := getSubsystemFileValue(memorySubsystem, "memory.usage_in_bytes")
	if err != nil {
		return mem, err
	}
	memUsage.set(mem, time.Now())
	return mem, nil
}

// getMemoryLimit reads from memory subsystem and returns TIDB's memory limit
func getMemoryLimit() (uint64, error) {
	limitInBytes, err := getSubsystemFileValue(memorySubsystem, "memory.limit_in_bytes")
	if err != nil {
		return maximumBytes, err
	}
	if limitInBytes == uint64(maximumBytes) {
		if isHierarchical {
			memStatF, err := readSubsystemFile(memorySubsystem, "memory.stat")
			if err != nil {
				return maximumBytes, err
			}
			memStat, err := grepFirstMatch(string(memStatF), "hierarchical_memory_limit", 1, " ")
			if err != nil {
				return maximumBytes, err
			}
			v, err := strconv.ParseUint(memStat, 10, 64)
			if err != nil {
				return maximumBytes, err
			}
			return v, nil
		}
	}
	return limitInBytes, nil
}

// getSubsystemPath returns sub-system path of current process's cgroups
func getSubsystemPath(subsystem string) (string, error) {
	cgroupData, err := safeReadFile(selfCGroupPath)
	if err != nil {
		return "", err
	}
	subsystemPath, err := grepFirstMatch(string(cgroupData), subsystem, 2, ":")
	if err != nil {
		return "", fmt.Errorf("cannot find sub-system: %s's path of self cgroup: %w", subsystem, err)
	}
	return subsystemPath, nil
}

// getHierarchicalStatus returns true when hierarchical accounting is enabled
// find more information on https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
func getHierarchicalStatus() bool {
	value, _ := readSubsystemFile(memorySubsystem, "memory.use_hierarchy")
	return string(value) == "1"
}

// readSubsystemFile reads a file of a cgroup sub-system
// e.g., memory.limit_in_bytes in /sys/fs/cgroup/memory/docker/0050c0fc269ced92965d8cda0a818125b10c3f7c7125f14f233ae9d69ba65e3f
func readSubsystemFile(subsystem, filename string) ([]byte, error) {
	path, err := getSubsystemPath(subsystem)
	if err != nil {
		return nil, err
	}
	return safeReadFile(fmt.Sprintf("%s/%s", path, filename))
}

// getSubsystemFileValue reads a file of a cgroup sub-system and returns its value
func getSubsystemFileValue(subsystem, filename string) (uint64, error) {
	path, err := getSubsystemPath(subsystem)
	if err != nil {
		return 0, err
	}
	v, err := safeReadFile(fmt.Sprintf("%s/%s", path, filename))
	if err != nil {
		return 0, err
	}
	return parseUint(strings.TrimSpace(string(v)), 10, 64)
}

// safeReadFile is a wrapper for os.ReadFile, it's panic free
func safeReadFile(path string) ([]byte, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	} else {
		return os.ReadFile(path)
	}
}

// refer to https://github.com/VictoriaMetrics/VictoriaMetrics/blob/5acd70109b98a05f20375e0b4bca67ad4176ac23/lib/cgroup/util.go#L47
func grepFirstMatch(data string, match string, index int, delimiter string) (string, error) {
	lines := strings.Split(string(data), "\n")
	for _, s := range lines {
		if !strings.Contains(s, match) {
			continue
		}
		parts := strings.Split(s, delimiter)
		if index < len(parts) {
			return strings.TrimSpace(parts[index]), nil
		}
	}
	return "", fmt.Errorf("cannot find %q in %q", match, data)
}
