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
	cGroupMemLimitPath = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	cGroupMemUsagePath = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
	selfCGroupPath     = "/proc/self/cgroup"
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

// MemTotalCGroup returns the total amount of RAM on this system in container environment.
func MemTotalCGroup() (uint64, error) {
	mem, t := memLimit.get()
	if time.Since(t) < 60*time.Second {
		return mem, nil
	}
	mem, err := readUint(cGroupMemLimitPath)
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
	mem, err := readUint(cGroupMemUsagePath)
	if err != nil {
		return mem, err
	}
	memUsage.set(mem, time.Now())
	return mem, nil
}

func init() {
	if inContainer() {
		MemTotal = MemTotalCGroup
		MemUsed = MemUsedCGroup
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

// refer to https://github.com/containerd/cgroups/blob/318312a373405e5e91134d8063d04d59768a1bff/utils.go#L243
func readUint(path string) (uint64, error) {
	v, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return parseUint(strings.TrimSpace(string(v)), 10, 64)
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
