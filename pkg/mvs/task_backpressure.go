// Copyright 2026 PingCAP, Inc.
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

package mvs

import (
	"time"

	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tidb/pkg/util/memory"
)

const (
	defaultMVTaskBackpressureCPUThreshold = 0.8
	defaultMVTaskBackpressureMemThreshold = 0.8
	defaultTaskBackpressureDelay          = 100 * time.Millisecond
)

// TaskBackpressureController decides whether task execution should be delayed.
type TaskBackpressureController interface {
	// ShouldBackpressure returns whether workers should temporarily stop picking
	// new tasks and how long to wait before retry.
	ShouldBackpressure() (backpressure bool, delay time.Duration)
}

// CPUMemBackpressureController applies backpressure when CPU or memory usage is
// over a configured threshold.
//
// CPUThreshold and MemThreshold are percentages in [0, 1]. If set to <= 0, the
// corresponding check is disabled.
type CPUMemBackpressureController struct {
	CPUThreshold float64
	MemThreshold float64
	Delay        time.Duration

	getCPUUsage func() (float64, bool)
	getMemUsed  func() (uint64, error)
	getMemTotal func() uint64
}

// NewCPUMemBackpressureController creates a CPU/memory based backpressure controller.
func NewCPUMemBackpressureController(cpuThreshold, memThreshold float64, delay time.Duration) *CPUMemBackpressureController {
	return &CPUMemBackpressureController{
		CPUThreshold: cpuThreshold,
		MemThreshold: memThreshold,
		Delay:        delay,
		getCPUUsage:  cpu.GetCPUUsage,
		getMemUsed:   memory.InstanceMemUsed,
		getMemTotal:  memory.GetMemTotalIgnoreErr,
	}
}

// ShouldBackpressure implements TaskBackpressureController.
func (c *CPUMemBackpressureController) ShouldBackpressure() (bool, time.Duration) {
	if c == nil {
		return false, 0
	}
	if c.CPUThreshold > 0 {
		if usage, unsupported := c.getCPUUsage(); !unsupported && usage >= c.CPUThreshold {
			return true, c.backpressureDelay()
		}
	}
	if c.MemThreshold > 0 {
		total := c.getMemTotal()
		if total > 0 {
			used, err := c.getMemUsed()
			if err == nil && float64(used)/float64(total) >= c.MemThreshold {
				return true, c.backpressureDelay()
			}
		}
	}
	return false, 0
}

// backpressureDelay returns configured delay or a package default.
func (c *CPUMemBackpressureController) backpressureDelay() time.Duration {
	if c.Delay > 0 {
		return c.Delay
	}
	return defaultTaskBackpressureDelay
}
