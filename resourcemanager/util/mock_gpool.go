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

package util

import "time"

// MockGPool is only for test
type MockGPool struct {
	name        string
	index       int32
	maxInFlight []int64
	inFlight    []int64
	minRT       []uint64
	maxPASS     []uint64
	cap         []int
	longRTT     []float64
	shortRTT    []uint64
	queueSize   []int64
	running     []int
	lastTunerTs []time.Time
}

// NewMockGPool is only for test
func NewMockGPool(name string) *MockGPool {
	return &MockGPool{
		name:        name,
		maxInFlight: make([]int64, 0, 100),
		inFlight:    make([]int64, 0, 100),
		minRT:       make([]uint64, 0, 100),
		maxPASS:     make([]uint64, 0, 100),
		cap:         make([]int, 0, 100),
		longRTT:     make([]float64, 0, 100),
		shortRTT:    make([]uint64, 0, 100),
		queueSize:   make([]int64, 0, 100),
		running:     make([]int, 0, 100),
		lastTunerTs: make([]time.Time, 0, 100),
	}
}

// OnSample is only for test
func (m *MockGPool) OnSample(maxInFlight, inFlight int64, minRT, maxPASS uint64, capa int, longRTT float64, shortRTT uint64, queueSize int64, running int) {
	m.maxInFlight = append(m.maxInFlight, maxInFlight)
	m.inFlight = append(m.inFlight, inFlight)
	m.minRT = append(m.minRT, minRT)
	m.maxPASS = append(m.maxPASS, maxPASS)
	m.cap = append(m.cap, capa)
	m.longRTT = append(m.longRTT, longRTT)
	m.shortRTT = append(m.shortRTT, shortRTT)
	m.queueSize = append(m.queueSize, queueSize)
	m.running = append(m.running, running)
}

// ImportLastTunerTs is only for test
func (m *MockGPool) ImportLastTunerTs(ts ...time.Time) {
	m.lastTunerTs = append(m.lastTunerTs, ts...)
}

// Release is only for test
func (*MockGPool) Release() {
	panic("implement me")
}

// Tune is only for test
func (*MockGPool) Tune(_ int, _ bool) {
	panic("implement me")
}

// LastTunerTs is only for test
func (m *MockGPool) LastTunerTs() time.Time {
	val := m.lastTunerTs[m.index]
	return val
}

// Next is only for test
func (m *MockGPool) Next() {
	m.index++
}

// MaxInFlight is only for test
func (m *MockGPool) MaxInFlight() int64 {
	val := m.maxInFlight[m.index]
	return val
}

// InFlight is only for test
func (m *MockGPool) InFlight() int64 {
	val := m.inFlight[m.index]
	return val
}

// MinRT is only for test
func (m *MockGPool) MinRT() uint64 {
	val := m.minRT[m.index]
	return val
}

// MaxPASS is only for test
func (m *MockGPool) MaxPASS() uint64 {
	val := m.maxPASS[m.index]
	return val
}

// Cap is only for test
func (m *MockGPool) Cap() int {
	val := m.cap[m.index]
	return val
}

// LongRTT is only for test
func (m *MockGPool) LongRTT() float64 {
	val := m.longRTT[m.index]
	return val
}

// UpdateLongRTT is only for test
func (m *MockGPool) UpdateLongRTT(fn func(float64) float64) {
	m.longRTT[m.index] = fn(m.longRTT[m.index])
}

// ShortRTT is only for test
func (m *MockGPool) ShortRTT() uint64 {
	val := m.shortRTT[m.index]
	return val
}

// GetQueueSize is only for test
func (m *MockGPool) GetQueueSize() int64 {
	val := m.queueSize[m.index]
	return val
}

// Running is only for test
func (m *MockGPool) Running() int {
	val := m.running[m.index]
	return val
}

// Name is only for test
func (m *MockGPool) Name() string {
	return m.name
}

// BoostTask is only for test
func (*MockGPool) BoostTask() {}

// DecreaseTask is only for test
func (*MockGPool) DecreaseTask() {}
