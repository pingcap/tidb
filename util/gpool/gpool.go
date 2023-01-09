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

package gpool

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/resourcemanager/pooltask"
	atomicutil "go.uber.org/atomic"
)

const (
	// DefaultCleanIntervalTime is the interval time to clean up goroutines.
	DefaultCleanIntervalTime = 5 * time.Second

	// OPENED represents that the pool is opened.
	OPENED = iota

	// CLOSED represents that the pool is closed.
	CLOSED
)

var (
	// ErrPoolClosed will be returned when submitting task to a closed pool.
	ErrPoolClosed = errors.New("this pool has been closed")

	// ErrPoolOverload will be returned when the pool is full and no workers available.
	ErrPoolOverload = errors.New("too many goroutines blocked on submit or Nonblocking is set")

	// ErrProducerClosed will be returned when the producer is closed.
	ErrProducerClosed = errors.New("this producer has been closed")
)

// BasePool is base class of pool
type BasePool struct {
	lastTuneTs atomicutil.Time
	limiterTTL atomicutil.Time // it is relation with limiter
	statistic  *pooltask.Statistic
	name       string
	limit      atomic.Bool
	generator  atomic.Uint64
}

// NewBasePool is to create a new BasePool.
func NewBasePool() BasePool {
	return BasePool{
		statistic:  pooltask.NewStatistic(),
		lastTuneTs: *atomicutil.NewTime(time.Now()),
		limiterTTL: *atomicutil.NewTime(time.Now()),
	}
}

// SetName is to set name.
func (p *BasePool) SetName(name string) {
	p.name = name
}

// Name is to get name.
func (p *BasePool) Name() string {
	return p.name
}

// SetStatistic is to set Statistic
func (p *BasePool) SetStatistic(statistic *pooltask.Statistic) {
	p.statistic = statistic
}

// GetStatistic is to get Statistic
func (p *BasePool) GetStatistic() *pooltask.Statistic {
	return p.statistic
}

// MaxInFlight is to get max in flight.
func (p *BasePool) MaxInFlight() int64 {
	return p.statistic.MaxInFlight()
}

// GetQueueSize is to get queue size.
func (p *BasePool) GetQueueSize() int64 {
	return p.statistic.GetQueueSize()
}

// MaxPASS is to get max pass.
func (p *BasePool) MaxPASS() uint64 {
	return p.statistic.MaxPASS()
}

// MinRT is to get min rt.
func (p *BasePool) MinRT() uint64 {
	return p.statistic.MinRT()
}

// InFlight is to get in flight.
func (p *BasePool) InFlight() int64 {
	return p.statistic.InFlight()
}

// LongRTT is to get long rtt.
func (p *BasePool) LongRTT() float64 {
	return p.statistic.LongRTT()
}

// ShortRTT is to get short rtt.
func (p *BasePool) ShortRTT() uint64 {
	return p.statistic.ShortRTT()
}

// UpdateLongRTT is to update long rtt.
func (p *BasePool) UpdateLongRTT(fn func(float64) float64) {
	p.statistic.UpdateLongRTT(fn)
}

// NewTaskID is to get new task id.
func (p *BasePool) NewTaskID() uint64 {
	return p.generator.Add(1)
}

// LastTunerTs returns the last time when the pool was tuned.
func (p *BasePool) LastTunerTs() time.Time {
	return p.lastTuneTs.Load()
}

// SetLastTuneTs sets the last time when the pool was tuned.
func (p *BasePool) SetLastTuneTs(t time.Time) {
	p.lastTuneTs.Store(t)
}

// OnLimit is to be in limit mode.
func (p *BasePool) OnLimit() {
	p.limit.Store(true)
}

// offLimit is to be in non-limit mode.
func (p *BasePool) offLimit() {
	p.limit.Store(false)
}

// IsLimit is to check if in limit mode.
func (p *BasePool) IsLimit() bool {
	if !p.limit.Load() {
		if time.Now().Before(p.limiterTTL.Load()) {
			return true
		}
		p.limit.Store(false)
	}
	return false
}

// Start is to start the pool.
func (p *BasePool) Start() {
	p.statistic.Start()
}

// Stop is to stop the pool.
func (p *BasePool) Stop() {
	p.statistic.Stop()
}
