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

// ExponentialMovingAverage is an exponential moving average measurement implementation. It is not thread-safe.
type ExponentialMovingAverage struct {
	value        float64
	sum          float64
	factor       float64
	warmupWindow int
	count        int
}

// NewExponentialMovingAverage will create a new ExponentialMovingAverage.
func NewExponentialMovingAverage(
	factor float64,
	warmupWindow int,
) *ExponentialMovingAverage {
	if factor >= 1 || factor <= 0 {
		panic("factor must be (0, 1)")
	}
	return &ExponentialMovingAverage{
		factor:       factor,
		warmupWindow: warmupWindow,
	}
}

// Add a single sample and update the internal state.
func (m *ExponentialMovingAverage) Add(value float64) {
	if m.count < m.warmupWindow {
		m.count++
		m.sum += value
		m.value = m.sum / float64(m.count)
	} else {
		m.value = m.value*(1-m.factor) + value*m.factor
	}
}

// Get the current value.
func (m *ExponentialMovingAverage) Get() float64 {
	return m.value
}
