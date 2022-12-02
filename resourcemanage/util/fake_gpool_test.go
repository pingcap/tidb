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

type FakeGPool struct {
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
}

func NewFakeGPool() *FakeGPool {
	return &FakeGPool{
		maxInFlight: make([]int64, 0),
		inFlight:    make([]int64, 0),
		minRT:       make([]uint64, 0),
		maxPASS:     make([]uint64, 0),
		cap:         make([]int, 0),
		longRTT:     make([]float64, 0),
		shortRTT:    make([]uint64, 0),
		queueSize:   make([]int64, 0),
		running:     make([]int, 0),
	}
}

func (f *FakeGPool) OnSample(maxInFlight, inFlight int64, minRT, maxPASS uint64, cap int, longRTT float64, shortRTT uint64, queueSize int64, running int) {
	f.maxInFlight = append(f.maxInFlight, maxInFlight)
	f.inFlight = append(f.inFlight, inFlight)
	f.minRT = append(f.minRT, minRT)
	f.maxPASS = append(f.maxPASS, maxPASS)
	f.cap = append(f.cap, cap)
	f.longRTT = append(f.longRTT, longRTT)
	f.shortRTT = append(f.shortRTT, shortRTT)
	f.queueSize = append(f.queueSize, queueSize)
	f.running = append(f.running, running)
}

func (f *FakeGPool) Next() {
	f.index++
}

func (f *FakeGPool) MaxInFlight() int64 {
	val := f.maxInFlight[f.index]
	return val
}

func (f *FakeGPool) InFlight() int64 {
	val := f.inFlight[f.index]
	return val
}

func (f *FakeGPool) MinRT() uint64 {
	val := f.minRT[f.index]
	return val
}

func (f *FakeGPool) MaxPASS() uint64 {
	val := f.maxPASS[f.index]
	return val
}

func (f *FakeGPool) Cap() int {
	val := f.cap[f.index]
	return val
}
func (f *FakeGPool) LongRTT() float64 {
	val := f.longRTT[f.index]
	return val
}
func (f *FakeGPool) ShortRTT() uint64 {
	val := f.shortRTT[f.index]
	return val
}

func (f *FakeGPool) GetQueueSize() int64 {
	val := f.queueSize[f.index]
	return val
}

func (f *FakeGPool) Running() int {
	val := f.running[f.index]
	return val
}

func (f *FakeGPool) Name() string {
	return "fake"
}
