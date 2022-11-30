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

package mathutil

import "sync"

func factor(n int) float64 {
	return 2.0 / float64(n+1)
}

// ExponentialAverageMeasurement is an exponential average measurement implementation.
type ExponentialAverageMeasurement struct {
	value        float64
	sum          float64
	window       int
	warmupWindow int
	count        int

	mu sync.RWMutex
}

// NewExponentialAverageMeasurement will create a new ExponentialAverageMeasurement
func NewExponentialAverageMeasurement(
	window int,
	warmupWindow int,
) *ExponentialAverageMeasurement {
	return &ExponentialAverageMeasurement{
		window:       window,
		warmupWindow: warmupWindow,
	}
}

// Add a single sample and update the internal state.
func (m *ExponentialAverageMeasurement) Add(value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.count < m.warmupWindow {
		m.count++
		m.sum += value
		m.value = m.sum / float64(m.count)
	} else {
		f := factor(m.window)
		m.value = m.value*(1-f) + value*f
	}
}

// Get the current value.
func (m *ExponentialAverageMeasurement) Get() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.value
}
