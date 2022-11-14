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
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/cgroup"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/shirou/gopsutil/v3/mem"
)

// MemTotal returns the total amount of RAM on this system
var MemTotal func() (uint64, error)

// MemUsed returns the total used amount of RAM on this system
var MemUsed func() (uint64, error)

// GetMemTotalIgnoreErr returns the total amount of RAM on this system/container. If error occurs, return 0.
func GetMemTotalIgnoreErr() uint64 {
	if memTotal, err := MemTotal(); err == nil {
		failpoint.Inject("GetMemTotalError", func(val failpoint.Value) {
			if val, ok := val.(bool); val && ok {
				memTotal = 0
			}
		})
		return memTotal
	}
	return 0
}

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

type memInfoCache struct {
	updateTime time.Time
	mu         *sync.RWMutex
	mem        uint64
}

func (c *memInfoCache) get() (memo uint64, t time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	memo, t = c.mem, c.updateTime
	return
}

func (c *memInfoCache) set(memo uint64, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mem, c.updateTime = memo, t
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
	memo, t := memLimit.get()
	if time.Since(t) < 60*time.Second {
		return memo, nil
	}
	memo, err := cgroup.GetMemoryLimit()
	if err != nil {
		return memo, err
	}
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	memo = mathutil.Min(v.Total, memo)
	memLimit.set(memo, time.Now())
	return memo, nil
}

// MemUsedCGroup returns the total used amount of RAM on this system in container environment.
func MemUsedCGroup() (uint64, error) {
	memo, t := memUsage.get()
	if time.Since(t) < 500*time.Millisecond {
		return memo, nil
	}
	memo, err := cgroup.GetMemoryUsage()
	if err != nil {
		return memo, err
	}
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	memo = mathutil.Min(v.Used, memo)
	memUsage.set(memo, time.Now())
	return memo, nil
}

func init() {
	if cgroup.InContainer() {
		MemTotal = MemTotalCGroup
		MemUsed = MemUsedCGroup
	} else {
		MemTotal = MemTotalNormal
		MemUsed = MemUsedNormal
	}
	memLimit = &memInfoCache{
		mu: &sync.RWMutex{},
	}
	memUsage = &memInfoCache{
		mu: &sync.RWMutex{},
	}
	serverMemUsage = &memInfoCache{
		mu: &sync.RWMutex{},
	}
	_, err := MemTotal()
	terror.MustNil(err)
	_, err = MemUsed()
	terror.MustNil(err)
}

// InstanceMemUsed returns the memory usage of this TiDB server
func InstanceMemUsed() (uint64, error) {
	used, t := serverMemUsage.get()
	if time.Since(t) < 500*time.Millisecond {
		return used, nil
	}
	var memoryUsage uint64
	instanceStats := ReadMemStats()
	memoryUsage = instanceStats.HeapAlloc
	serverMemUsage.set(memoryUsage, time.Now())
	return memoryUsage, nil
}
