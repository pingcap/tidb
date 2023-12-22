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

package scheduler

import (
	"time"

	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/util/cpu"
)

// CPUScheduler is a cpu scheduler
type CPUScheduler struct{}

// NewCPUScheduler is to create a new cpu scheduler
func NewCPUScheduler() *CPUScheduler {
	return &CPUScheduler{}
}

// Tune is to tune the goroutine pool
func (*CPUScheduler) Tune(_ util.Component, pool util.GoroutinePool) Command {
	if time.Since(pool.LastTunerTs()) < util.MinSchedulerInterval.Load() {
		return Hold
	}
	value, unsupported := cpu.GetCPUUsage()
	if unsupported {
		return Hold
	}
	if value < 0.5 {
		return Overclock
	}
	if value > 0.7 {
		return Downclock
	}
	return Hold
}
