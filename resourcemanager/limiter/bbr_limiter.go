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

package limiter

import (
	"time"

	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util/cpu"
)

// BBRLimiter is a limiter based on cpu usage.
type BBRLimiter struct {
	cpuThreshold int64
}

// NewBBRLimiter is to create a bbr limiter.
func NewBBRLimiter(cpuThreshold int64) Limiter {
	return &BBRLimiter{
		cpuThreshold: cpuThreshold,
	}
}

// Limit is to limit the goroutine pool.
func (b *BBRLimiter) Limit(_ util.Component, p util.GorotinuePool) bool {
	usage := cpu.GetCPUUsage() * 100
	if usage < float64(b.cpuThreshold) {
		// current cpu payload below the threshold
		prevDropTime := p.LastTunerTs()
		if prevDropTime.IsZero() {
			// haven't start drop,
			// accept current request
			return false
		}
		if time.Since(prevDropTime) <= time.Second {
			// just start drop one second ago,
			// check current inflight count
			inFlight := p.InFlight()
			return inFlight > 1 && inFlight > p.MaxInFlight()
		}
		return false
	}
	// current cpu payload exceeds the threshold
	inFlight := p.InFlight()
	drop := inFlight > 1 && inFlight > p.MaxInFlight()
	if drop {
		prevDropTime := p.LastTunerTs()
		if prevDropTime.IsZero() {
			// already started drop, return directly
			return drop
		}
	}
	return drop
}
