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

package cpu

import (
	"os"
	"sync"
	"time"

	"github.com/cloudfoundry/gosigar"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/cgroup"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var cpuUsage atomic.Float64

// GetCPUUsage returns the cpu usage of the current process.
func GetCPUUsage() float64 {
	return cpuUsage.Load()
}

// Observer is used to observe the cpu usage of the current process.
type Observer struct {
	utime int64
	stime int64
	now   int64
	exit  chan struct{}
	cpu   mathutil.ExponentialMovingAverage
	wg    sync.WaitGroup
}

// NewCPUObserver returns a cpu observer.
func NewCPUObserver() *Observer {
	return &Observer{
		exit: make(chan struct{}),
		now:  time.Now().UnixNano(),
		cpu:  *mathutil.NewExponentialMovingAverage(0.95, 10),
	}
}

// Start starts the cpu observer.
func (c *Observer) Start() {
	c.wg.Add(1)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer func() {
			ticker.Stop()
			c.wg.Done()
		}()
		for {
			select {
			case <-ticker.C:
				curr := c.observe()
				c.cpu.Add(curr)
				cpuUsage.Store(c.cpu.Get())
				metrics.EMACPUUsageGauge.Set(c.cpu.Get())
			case <-c.exit:
				return
			}
		}
	}()
}

// Stop stops the cpu observer.
func (c *Observer) Stop() {
	close(c.exit)
	c.wg.Wait()
}

func (c *Observer) observe() float64 {
	user, sys, err := getCPUTime()
	if err != nil {
		log.Error("getCPUTime", zap.Error(err))
	}
	cgroupCPU, _ := cgroup.GetCgroupCPU()
	cpuShare := cgroupCPU.CPUShares()
	now := time.Now().UnixNano()
	dur := float64(now - c.now)
	utime := user * 1e6
	stime := sys * 1e6
	urate := float64(utime-c.utime) / dur
	srate := float64(stime-c.stime) / dur
	c.now = now
	c.utime = utime
	c.stime = stime
	return (srate + urate) / cpuShare
}

// getCPUTime returns the cumulative user/system time (in ms) since the process start.
func getCPUTime() (userTimeMillis, sysTimeMillis int64, err error) {
	pid := os.Getpid()
	cpuTime := sigar.ProcTime{}
	if err := cpuTime.Get(pid); err != nil {
		return 0, 0, err
	}
	return int64(cpuTime.User), int64(cpuTime.Sys), nil
}
