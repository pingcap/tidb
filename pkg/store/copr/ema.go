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

package copr

import (
	"math"
	"sync"
	"time"
)

// defaultRUEMATau is the EMA decay time constant (τ): a sample's weight
// drops to 1/e after τ.
const defaultRUEMATau = time.Second

// ruEMA is a time-aware EMA weighting older samples by exp(-Δt/τ) so long
// gaps decay stale samples. Safe for concurrent Observe/Predict.
//
// A fresh ruEMA has value=0 and a zero lastObsAt; the first Observe then
// computes a dt of many years, alpha saturates to 1, and value is seeded
// to the sample — no special cold-start branch required.
type ruEMA struct {
	mu        sync.Mutex
	tau       time.Duration
	value     float64
	lastObsAt time.Time
}

func newRUEMA() *ruEMA {
	return &ruEMA{tau: defaultRUEMATau}
}

func (e *ruEMA) Observe(bytes uint64, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	dt := now.Sub(e.lastObsAt)
	if dt < 0 {
		dt = 0
	}
	alpha := 1 - math.Exp(-float64(dt)/float64(e.tau))
	e.value += alpha * (float64(bytes) - e.value)
	// Guard against out-of-order concurrent Observes rewinding the clock,
	// which would inflate the next dt and overweight the next sample.
	if now.After(e.lastObsAt) {
		e.lastObsAt = now
	}
}

// Predict returns the current EMA estimate. 0 means either "no sample
// yet" or "EMA has converged to zero"; both are treated as "no useful
// hint" by callers.
func (e *ruEMA) Predict() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.value < 0 {
		return 0
	}
	return uint64(e.value)
}
