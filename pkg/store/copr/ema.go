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
type ruEMA struct {
	mu        sync.Mutex
	tau       time.Duration
	value     float64
	lastObsAt time.Time
	hasSample bool
}

func newRUEMA() *ruEMA {
	return &ruEMA{tau: defaultRUEMATau}
}

func (e *ruEMA) Observe(bytes uint64, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	x := float64(bytes)
	if !e.hasSample {
		e.value = x
		e.hasSample = true
	} else {
		dt := now.Sub(e.lastObsAt)
		if dt < 0 {
			dt = 0
		}
		alpha := 1 - math.Exp(-float64(dt)/float64(e.tau))
		e.value += alpha * (x - e.value)
	}
	e.lastObsAt = now
}

func (e *ruEMA) IsReady() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.hasSample
}

// Predict returns the current EMA estimate, or 0 before any sample has
// been observed. Callers should gate on IsReady to distinguish "no data"
// from a genuine zero-byte estimate.
func (e *ruEMA) Predict() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.hasSample || e.value < 0 {
		return 0
	}
	return uint64(e.value)
}
