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

// CPUUsages records tidb/tikv cpu usages
type CPUUsages struct {
	TidbCPUTime time.Duration
	TikvCPUTime time.Duration
}

// SQLCPUUsages is used to record sqlID and its cpu usages
type SQLCPUUsages struct {
	sync.Mutex
	sqlID     uint64
	cpuUsages CPUUsages
}

// Reset resets all cpu times to 0
func (c *CPUUsages) Reset() {
	c.TikvCPUTime = 0
	c.TidbCPUTime = 0
}

// SetCPUUsages sets cpu usages value
func (c *SQLCPUUsages) SetCPUUsages(usage CPUUsages) {
	c.Lock()
	defer c.Unlock()
	c.cpuUsages = usage
}

// MergeTidbCPUTime merges tidbCPU time into self when sqlID matches
// Checks sqlID here, because tidb cpu time can only be collected by profiler now, and updated in concurrent goroutines
func (c *SQLCPUUsages) MergeTidbCPUTime(sqlID uint64, d time.Duration) {
	c.Lock()
	defer c.Unlock()
	if c.sqlID == sqlID {
		c.cpuUsages.TidbCPUTime += d
	}
}

// MergeTikvCPUTime merges tikvCPU time into self.
// Doesn't need to check sqlID here, because tikv cpu time is updated in executors now.
func (c *SQLCPUUsages) MergeTikvCPUTime(d time.Duration) {
	c.Lock()
	defer c.Unlock()
	c.cpuUsages.TikvCPUTime += d
}

// GetCPUUsages returns tidbCPU, tikvCPU time
func (c *SQLCPUUsages) GetCPUUsages() CPUUsages {
	c.Lock()
	defer c.Unlock()
	return c.cpuUsages
}

// AllocNewSQLID alloc new ID, will restart from 0 when exceeds uint64 max limit
func (c *SQLCPUUsages) AllocNewSQLID() uint64 {
	c.Lock()
	defer c.Unlock()
	c.sqlID++
	return c.sqlID
}

// ResetCPUTimes resets tidb/tikv cpu times to 0
func (c *SQLCPUUsages) ResetCPUTimes() {
	c.Lock()
	defer c.Unlock()
	c.cpuUsages.Reset()
}
