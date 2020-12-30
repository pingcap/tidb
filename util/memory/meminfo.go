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
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/mem"
	"sync"
)

// MemTotal returns the total amount of RAM on this system
var MemTotal func() (uint64, error)

// MemUsed returns the total used amount of RAM on this system
var MemUsed func() (uint64, error)

// MemTotalNormal returns the total amount of RAM on this system in non-container environment.
func MemTotalNormal() (uint64, error) {
	v, err := mem.VirtualMemory()
	return v.Total, err
}

// MemUsedNormal returns the total used amount of RAM on this system in non-container environment.
func MemUsedNormal() (uint64, error) {
	v, err := mem.VirtualMemory()
	return v.Used, err
}

const (
	cGroupMemLimitPath = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	cGroupMemUsagePath = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
	selfCGroupPath     = "/proc/self/cgroup"
)

type memInContainer struct {
	sync.RWMutex
	mem        uint64
	updateTime time.Time
}

// expiration time is 60s
var memLimitCache memInContainer

// expiration time is 500ms
var memUsageCache memInContainer

// MemTotalCGroup returns the total amount of RAM on this system in container environment.
func MemTotalCGroup() (uint64, error) {
	memLimitCache.RLock()
	if time.Since(memLimitCache.updateTime) < 60*time.Second {
		memLimitCache.RUnlock()
		return memLimitCache.mem, nil
	}
	memLimitCache.RUnlock()
	mem, err := readUint(cGroupMemLimitPath)
	if err != nil {
		return mem, err
	}
	memLimitCache.Lock()
	memLimitCache.mem, memLimitCache.updateTime = mem, time.Now()
	memLimitCache.Unlock()
	return mem, nil
}

// MemUsedCGroup returns the total used amount of RAM on this system in container environment.
func MemUsedCGroup() (uint64, error) {
	memUsageCache.RLock()
	if time.Since(memUsageCache.updateTime) < 500*time.Millisecond {
		memUsageCache.RUnlock()
		return memUsageCache.mem, nil
	}
	memUsageCache.RUnlock()
	mem, err := readUint(cGroupMemUsagePath)
	if err != nil {
		return mem, err
	}
	memUsageCache.Lock()
	memUsageCache.mem, memUsageCache.updateTime = mem, time.Now()
	memUsageCache.Unlock()
	return mem, nil
}

func init() {
	if inContainer() {
		MemTotal = MemTotalCGroup
		MemUsed = MemUsedCGroup
		limit, err := MemTotalCGroup()
		if err != nil {
		}
		usage, err := MemUsedCGroup()
		if err != nil {
		}
		curTime := time.Now()
		memLimitCache = memInContainer{
			RWMutex:    sync.RWMutex{},
			mem:        limit,
			updateTime: curTime,
		}
		memUsageCache = memInContainer{
			RWMutex:    sync.RWMutex{},
			mem:        usage,
			updateTime: curTime,
		}
	} else {
		MemTotal = MemTotalNormal
		MemUsed = MemUsedNormal
	}
}

func inContainer() bool {
	v, err := ioutil.ReadFile(selfCGroupPath)
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
	v, err := ioutil.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return parseUint(strings.TrimSpace(string(v)), 10, 64)
}
