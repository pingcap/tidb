// Copyright 2024 PingCAP, Inc.
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

package collector

import (
	"sync"
	"time"
)

const (
	defaultTimeout     = 5 * time.Minute
	defaultChannelSize = 10
)

// GlobalCollector provides a utility to collect stats data from each session
type GlobalCollector[T any] interface {
	SpawnSession() SessionCollector[T]
	Close()
	StartWorker()
}

var _ GlobalCollector[int] = &globalCollector[int]{}

// globalCollector is an implementation of `GlobalCollector`
type globalCollector[T any] struct {
	mergeFn            func(T)
	dataCh             chan T
	highPriorityDataCh chan T
	closeCh            chan struct{}
	wg                 sync.WaitGroup
	timeout            time.Duration

	closeOnce sync.Once
}

// NewGlobalCollector creates a new global collector
func NewGlobalCollector[T any](mergeFn func(T)) GlobalCollector[T] {
	g := &globalCollector[T]{
		mergeFn: mergeFn,
		// Now the timeout and channel size is not configurable for the simplicity.
		// If there is a scenario in which tuning timeout and channel size is necessary, feel free to expand this
		// constructor.
		timeout:            defaultTimeout,
		dataCh:             make(chan T, defaultChannelSize),
		highPriorityDataCh: make(chan T, defaultChannelSize),
		closeCh:            make(chan struct{}),
	}
	return g
}

// SpawnSession creates a related session collector from the global collector
func (g *globalCollector[T]) SpawnSession() SessionCollector[T] {
	return &sessionCollector[T]{
		timeout:            g.timeout,
		dataCh:             g.dataCh,
		highPriorityDataCh: g.highPriorityDataCh,
		lastUpdate:         time.Now(),
	}
}

// StartWorker spawns a goroutine to merge the data
func (g *globalCollector[T]) StartWorker() {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()

	loop:
		for {
			// nested selection to make sure `highPriorityDataCh` is selected before the normal `dataCh`
			select {
			case data := <-g.highPriorityDataCh:
				g.mergeFn(data)
			case <-g.closeCh:
				break loop
			default:
				select {
				case data := <-g.dataCh:
					g.mergeFn(data)
				case data := <-g.highPriorityDataCh:
					g.mergeFn(data)
				case <-g.closeCh:
					break loop
				}
			}
		}

		// drain out the data from channel
		g.flush()
	}()
}

// flush reads all data from the channel, until the channel is empty
func (g *globalCollector[T]) flush() {
	for {
		select {
		case data := <-g.highPriorityDataCh:
			g.mergeFn(data)
		case data := <-g.dataCh:
			g.mergeFn(data)
		default:
			return
		}
	}
}

// Close closes the background worker of the global collector
func (g *globalCollector[T]) Close() {
	g.closeOnce.Do(func() {
		close(g.closeCh)
		g.wg.Wait()
	})
}

// SessionCollector is an interface to send stats data to the global collector
type SessionCollector[T any] interface {
	// SendDelta sends the data to the global collector. This function will not block (unless the `timeout` reached). It
	// returns a bool to represent whether the data has been sent successfully.
	SendDelta(data T) bool
	// SendDeltaSync sends the data to the global collector. Unlike `SendDelta`, this function will always block and
	// wait until the data has been received by the global collector.
	SendDeltaSync(data T) bool
}

var _ SessionCollector[int] = &sessionCollector[int]{}

// sessionCollector is the collector attached to each session to send the data to global collector
type sessionCollector[T any] struct {
	lastUpdate         time.Time
	dataCh             chan<- T
	highPriorityDataCh chan<- T
	closeCh            <-chan struct{}
	timeout            time.Duration
}

// SendDelta implements `SessionCollector[T]` interface
func (s *sessionCollector[T]) SendDelta(data T) bool {
	if time.Since(s.lastUpdate) > s.timeout {
		return s.SendDeltaSync(data)
	}

	// don't block on the channel
	select {
	case s.dataCh <- data:
		s.lastUpdate = time.Now()
		return true
	default:
		return false
	}
}

// SendDeltaSync implements `SessionCollector[T]` interface
func (s *sessionCollector[T]) SendDeltaSync(data T) bool {
	select {
	case s.highPriorityDataCh <- data:
		s.lastUpdate = time.Now()
		return true
	case <-s.closeCh:
		return false
	}
}
