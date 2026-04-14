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
	"time"
)

// defaultRUEMATau is the half-life of the per-logical-scan read-bytes EMA.
// An observation's weight decays by half after this duration. Chosen so
// that after a handful of paging RPCs at typical 50-200 ms latency the EMA
// has effectively forgotten the first sample.
const defaultRUEMATau = time.Second

// ruEMAMinSamples is the number of observations required before IsReady
// returns true. Until then Predict returns 0 and callers should leave the
// pre-charge hint unset so PD falls back to PagingSizeBytes (the paging
// byte budget / worst-case cap).
const ruEMAMinSamples = 2

// ruEMA is a time-aware exponential moving average over a scan's observed
// per-RPC MVCC read bytes. It lives on a copTask for the duration of one
// logical scan: initialized on the first paging RPC, updated after each
// paging response, and released together with the task. When a region/lock
// error forces the task to split, the EMA is propagated to the remain
// tasks so the learned estimate survives the retry.
//
// The time-aware decay weights an older observation by exp(-Δt/τ); gaps
// between observations (e.g. a slow TiKV round trip) therefore reduce the
// weight of the older sample automatically.
//
// Not safe for concurrent use. Each copTask's paging loop is serialized
// through one worker, so no synchronization is needed.
type ruEMA struct {
	tau       time.Duration
	value     float64
	lastObsAt time.Time
	samples   int
}

// newRUEMA returns an EMA with the default half-life.
func newRUEMA() *ruEMA {
	return &ruEMA{tau: defaultRUEMATau}
}

// Observe folds a new read-bytes sample into the EMA, using a time-aware
// decay based on the elapsed wall time since the previous observation.
func (e *ruEMA) Observe(bytes uint64, now time.Time) {
	x := float64(bytes)
	if e.samples == 0 {
		e.value = x
	} else {
		dt := now.Sub(e.lastObsAt)
		if dt < 0 {
			dt = 0
		}
		// alpha is the weight of the new sample: 1 - exp(-Δt/τ). As Δt→0
		// the new sample barely moves the EMA; as Δt→∞ the EMA collapses
		// onto the latest sample.
		alpha := 1 - math.Exp(-float64(dt)/float64(e.tau))
		e.value += alpha * (x - e.value)
	}
	e.lastObsAt = now
	e.samples++
}

// IsReady reports whether the EMA has seen enough samples to be trusted
// as a prediction. Callers should leave the hint unset before readiness
// so PD falls back to PagingSizeBytes as the worst-case pre-charge basis.
// Safe to call on a nil receiver (reports false).
func (e *ruEMA) IsReady() bool {
	return e != nil && e.samples >= ruEMAMinSamples
}

// Predict returns the current EMA estimate of per-RPC read bytes, or 0 if
// the EMA has not yet seen enough samples. Safe to call on a nil receiver
// (returns 0).
func (e *ruEMA) Predict() uint64 {
	if !e.IsReady() {
		return 0
	}
	if e.value < 0 {
		return 0
	}
	return uint64(e.value)
}
