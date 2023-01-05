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

// GorotinuePool is a pool interface
type GorotinuePool interface {
	Release()
	Tune(size int)
	LastTunerTs() time.Time
	MaxInFlight() int64
	InFlight() int64
	MinRT() uint64
	MaxPASS() uint64
	Cap() int
	// LongRTT is to represent the baseline latency by tracking a measurement of the long term, less volatile RTT.
	LongRTT() float64
	UpdateLongRTT(f func(float64) float64)
	// ShortRTT is to represent the current system latency by tracking a measurement of the short time, and more volatile RTT.
	ShortRTT() uint64
	GetQueueSize() int64
	Running() int
	Name() string
}

// PoolContainer is a pool container
type PoolContainer struct {
	Pool      GorotinuePool
	Component Component
}

// Component is ID for difference component
type Component int

const (
	// UNKNOWN is for unknown component. It is only for test
	UNKNOWN Component = iota
	// DDL is for ddl component
	DDL
)
