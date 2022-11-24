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

	"github.com/pingcap/tidb/resourcemanage/util"
	"github.com/pingcap/tidb/util/mathutil"
)

const (
	defaultSmoothing      float64 = 0.2
	defaultMinConcurrency int     = 2

	defaultOverloadConcurrencyDelta int = 2 // minimum concurrency = user setting concurrency - delta
)

// Gradient2Scheduler is a scheduler that uses the gradient of the queue length
type Gradient2Scheduler struct {
	smoothing      float64
	estimatedLimit int16
}

// NewGradient2Scheduler is to create a new Gradient2Scheduler
func NewGradient2Scheduler() *Gradient2Scheduler {
	return &Gradient2Scheduler{
		smoothing:      defaultSmoothing,
		estimatedLimit: 1,
	}
}

// Tune is to tune the concurrency of the component
func (b *Gradient2Scheduler) Tune(_ util.Component, p util.GorotinuePool) Command {
	if time.Since(p.LastTunerTs()) < minCPUSchedulerInterval {
		return Hold
	}

	if p.InFlight() < int64(p.Cap())/2 {
		return Hold
	}
	gradient := mathutil.Max(0.5, mathutil.Min(1.0, p.LongRTT()/float64(p.ShortRTT())))
	newLimit := float64(p.Running())*gradient + float64(p.GetQueueSize())
	newLimit = float64(p.Running())*(1-b.smoothing) + newLimit*b.smoothing
	newLimit = mathutil.Max(float64(defaultMinConcurrency), mathutil.Min(float64(defaultOverloadConcurrencyDelta+p.Cap()), newLimit))
	if newLimit > float64(p.Running()) {
		return Overclock
	} else if newLimit < float64(p.Running()) {
		return Downclock
	}
	return Hold
}
