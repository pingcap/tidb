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

	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util/mathutil"
)

const (
	// defaultSmoothing is a factor, it will be used to smooth the old concurrency limit and new one.
	defaultSmoothing float64 = 0.2
	// defaultMinConcurrency is the minimum concurrency limit for a component.
	defaultMinConcurrency int = 1
	// max concurrency = user setting concurrency + delta
	maxOverloadConcurrencyDelta int = 2
)

// Gradient2Scheduler is a scheduler using Gradient2 algorithm.
// Gradient2 attempts to reduce the delay by obeserving the long and short window RTT and queue length to predict
// reasonable concurrency. Using average algorithm to smooth the result for making scheduler more robust.
type Gradient2Scheduler struct {
	smoothing float64
}

// NewGradient2Scheduler is to create a new Gradient2Scheduler
func NewGradient2Scheduler() *Gradient2Scheduler {
	return &Gradient2Scheduler{
		smoothing: defaultSmoothing,
	}
}

// Tune is to tune the concurrency of the component
func (b *Gradient2Scheduler) Tune(c util.Component, p util.GorotinuePool) Command {
	newLimit := b.tune(c, p)
	if newLimit > float64(p.Running()) {
		return Overclock
	} else if newLimit < float64(p.Running()) {
		return Downclock
	}
	return Hold
}

func (b *Gradient2Scheduler) tune(_ util.Component, p util.GorotinuePool) float64 {
	if time.Since(p.LastTunerTs()) < minCPUSchedulerInterval {
		return float64(p.Running())
	}

	if p.InFlight() < int64(p.Cap())/2 {
		return float64(p.Running())
	}

	if (p.LongRTT() / float64(p.ShortRTT())) > 2 {
		p.UpdateLongRTT(func(old float64) float64 {
			return old * 0.9
		})
	}

	gradient := mathutil.Max(0.5, mathutil.Min(1.0, p.LongRTT()/float64(p.ShortRTT())))
	newLimit := float64(p.Running())*gradient + float64(p.GetQueueSize())
	newLimit = float64(p.Running())*(1-b.smoothing) + newLimit*b.smoothing
	newLimit = mathutil.Max(float64(defaultMinConcurrency), mathutil.Min(float64(maxOverloadConcurrencyDelta+p.Cap()), newLimit))
	return newLimit
}
