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
	name string
}

// NewMockGPool is only for test
func NewMockGPool(name string) *MockGPool {
	return &MockGPool{name: name}
}

// ReleaseAndWait is only for test
func (*MockGPool) ReleaseAndWait() {
	panic("implement me")
}

// Tune is only for test
func (*MockGPool) Tune(_ int) {
	panic("implement me")
}

// LastTunerTs is only for test
func (*MockGPool) LastTunerTs() time.Time {
	panic("implement me")
}

// MaxInFlight is only for test
func (*MockGPool) MaxInFlight() int64 {
	panic("implement me")
}

// InFlight is only for test
func (*MockGPool) InFlight() int64 {
	panic("implement me")
}

// MinRT is only for test
func (*MockGPool) MinRT() uint64 {
	panic("implement me")
}

// MaxPASS is only for test
func (*MockGPool) MaxPASS() uint64 {
	panic("implement me")
}

// Cap is only for test
func (*MockGPool) Cap() int {
	panic("implement me")
}

// LongRTT is to represent the baseline latency by tracking a measurement of the long term, less volatile RTT.
func (*MockGPool) LongRTT() float64 {
	panic("implement me")
}

// UpdateLongRTT is only for test
func (*MockGPool) UpdateLongRTT(_ func(float64) float64) {
	panic("implement me")
}

// ShortRTT is to represent the current system latency by tracking a measurement of the short time, and more volatile RTT.
func (*MockGPool) ShortRTT() uint64 {
	panic("implement me")
}

// GetQueueSize is only for test
func (*MockGPool) GetQueueSize() int64 {
	panic("implement me")
}

// Running is only for test
func (*MockGPool) Running() int {
	panic("implement me")
}

// Name is only for test
func (m *MockGPool) Name() string {
	return m.name
}
