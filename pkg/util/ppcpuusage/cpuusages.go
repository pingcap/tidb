// Copyright 2024 PingCAP, Inc.
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

package ppcpuusage

import (
	"sync"
	"time"
)

// CPUUsages is used to record tidb/tikv cpu usages
type CPUUsages struct {
	sync.Mutex
	sqlID       uint64
	tidbCPUTime time.Duration
	tikvCPUTime time.Duration
}

// MergeTidbCPUTime merges tidbCPU time into self when sqlID matches
// Checks sqlID here, because tidb cpu time can only be collected by profiler now, and updated in concurrent goroutines
func (c *CPUUsages) MergeTidbCPUTime(sqlID uint64, d time.Duration) {
	c.Lock()
	defer c.Unlock()
	if c.sqlID == sqlID {
		c.tidbCPUTime += d
	}
}

// MergeTikvCPUTime merges tikvCPU time into self.
// Doesn't need to check sqlID here, because tikv cpu time is updated in executors now.
func (c *CPUUsages) MergeTikvCPUTime(d time.Duration) {
	c.Lock()
	defer c.Unlock()
	c.tikvCPUTime += d
}

// GetSQLID returns current SQLID
func (c *CPUUsages) GetSQLID() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.sqlID
}

// GetAllCPUTime returns tidbCPU, tikvCPU time
func (c *CPUUsages) GetAllCPUTime() (time.Duration, time.Duration) {
	c.Lock()
	defer c.Unlock()
	return c.tidbCPUTime, c.tidbCPUTime
}

// GetTidbCPUTime returns tidbCPU time
func (c *CPUUsages) GetTidbCPUTime() time.Duration {
	c.Lock()
	defer c.Unlock()
	return c.tidbCPUTime
}

// GetTikvCPUTime returns tikvCPU time
func (c *CPUUsages) GetTikvCPUTime() time.Duration {
	c.Lock()
	defer c.Unlock()
	return c.tikvCPUTime
}

// AllocNewSQLID alloc new ID, will restart from 0 when exceeds uint64 max limit
func (c *CPUUsages) AllocNewSQLID() uint64 {
	c.Lock()
	defer c.Unlock()
	c.sqlID++
	return c.sqlID
}

func (c *CPUUsages) ResetCPUTimes() {
	c.Lock()
	defer c.Unlock()
	c.tidbCPUTime = 0
	c.tikvCPUTime = 0
}
