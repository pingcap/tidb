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
	name       string
	generator  atomic.Uint64
}

// NewBasePool is to create a new BasePool.
func NewBasePool() BasePool {
	return BasePool{
		lastTuneTs: *atomicutil.NewTime(time.Now()),
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

// NewTaskID is to get a new task ID.
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
