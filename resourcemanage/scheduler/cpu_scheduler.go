// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"time"

	"github.com/pingcap/tidb/resourcemanage"
	"github.com/pingcap/tidb/util/cpu"
)

const minCPUSchedulerInterval = 5 * time.Second

type CPUScheduler struct {
}

func (c *CPUScheduler) Tune(component resourcemanage.Component, p resourcemanage.GorotinuePool) SchedulerCommand {
	// TODO: time.Since(c.next) < minCPUSchedulerInterval
	if component != resourcemanage.DDL || time.Since(p.LastTunerTs()) < minCPUSchedulerInterval {
		return Hold
	}
	usage := cpu.GetCPUUsage()
	if usage > 0.8 {
		return Downclock
	}
	if usage < 0.7 {
		return Overclock
	}
	return Hold
}
