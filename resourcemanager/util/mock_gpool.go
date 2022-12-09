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
func NewMockGPool(size int) *MockGPool {
	return &MockGPool{
		maxInFlight: make([]int64, 0, size),
		inFlight:    make([]int64, 0, size),
		minRT:       make([]uint64, 0, size),
		maxPASS:     make([]uint64, 0, size),
		cap:         make([]int, 0, size),
		longRTT:     make([]float64, 0, size),
		shortRTT:    make([]uint64, 0, size),
		queueSize:   make([]int64, 0, size),
		running:     make([]int, 0, size),
		lastTunerTs: make([]time.Time, 0, size),
	}
}

// OnSample is only for test
func (f *MockGPool) OnSample(maxInFlight, inFlight int64, minRT, maxPASS uint64, capa int, longRTT float64, shortRTT uint64, queueSize int64, running int) {
	f.maxInFlight = append(f.maxInFlight, maxInFlight)
	f.inFlight = append(f.inFlight, inFlight)
	f.minRT = append(f.minRT, minRT)
	f.maxPASS = append(f.maxPASS, maxPASS)
	f.cap = append(f.cap, capa)
	f.longRTT = append(f.longRTT, longRTT)
	f.shortRTT = append(f.shortRTT, shortRTT)
	f.queueSize = append(f.queueSize, queueSize)
	f.running = append(f.running, running)
}

// ImportLastTunerTs is only for test
func (f *MockGPool) ImportLastTunerTs(ts ...time.Time) {
	f.lastTunerTs = append(f.lastTunerTs, ts...)
}

// Release is only for test
func (*MockGPool) Release() {}

// Tune is only for test
func (*MockGPool) Tune(_ int) {}

// LastTunerTs is only for test
func (f *MockGPool) LastTunerTs() time.Time {
	val := f.lastTunerTs[f.index]
	return val
}

// Next is only for test
func (f *MockGPool) Next() {
	f.index++
}

// MaxInFlight is only for test
func (f *MockGPool) MaxInFlight() int64 {
	val := f.maxInFlight[f.index]
	return val
}

// InFlight is only for test
func (f *MockGPool) InFlight() int64 {
	val := f.inFlight[f.index]
	return val
}

// MinRT is only for test
func (f *MockGPool) MinRT() uint64 {
	val := f.minRT[f.index]
	return val
}

// MaxPASS is only for test
func (f *MockGPool) MaxPASS() uint64 {
	val := f.maxPASS[f.index]
	return val
}

// Cap is only for test
func (f *MockGPool) Cap() int {
	val := f.cap[f.index]
	return val
}

// LongRTT is only for test
func (f *MockGPool) LongRTT() float64 {
	val := f.longRTT[f.index]
	return val
}

// UpdateLongRTT is only for test
func (f *MockGPool) UpdateLongRTT(fn func(float64) float64) {
	f.longRTT[f.index] = fn(f.longRTT[f.index])
}

// ShortRTT is only for test
func (f *MockGPool) ShortRTT() uint64 {
	val := f.shortRTT[f.index]
	return val
}

// GetQueueSize is only for test
func (f *MockGPool) GetQueueSize() int64 {
	val := f.queueSize[f.index]
	return val
}

// Running is only for test
func (f *MockGPool) Running() int {
	val := f.running[f.index]
	return val
}

// Name is only for test
func (*MockGPool) Name() string {
	return "fake"
}
