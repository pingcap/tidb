// Copyright 2021 PingCAP, Inc.
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

package cpuprofile

import (
	"bytes"
	"errors"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/atomic"
)

var defProfileDuration = time.Second

// globalCPUProfiler is the global CPU profiler.
// If you want to create a new global cpu profiler, you should stop the old global cpu profiler.
var globalCPUProfiler = newParallelCPUProfiler()

// ProfileConsumer is the profile data consumer.
// ProfileConsumer is a channel alias, if the channel is full, then the channel won't receive the latest profile data
// until it is not blocked.
type ProfileConsumer = chan *ProfileData

// ProfileData contains the cpu profile data and some additional information.
type ProfileData struct {
	Data  *bytes.Buffer
	Begin time.Time
	End   time.Time
	Error error
}

// StartCPUProfiler uses to start to run the global ParallelCPUProfiler.
func StartCPUProfiler() error {
	return globalCPUProfiler.start()
}

// CloseCPUProfiler uses to close the global ParallelCPUProfiler.
func CloseCPUProfiler() {
	globalCPUProfiler.close()
}

// Register register a ProfileConsumer into the global CPU profiler.
// Normally, the registered ProfileConsumer will receive the cpu profile data per second.
// If the ProfileConsumer (channel) is full, the latest cpu profile data will not be sent to it.
// This function is thread-safe.
func Register(ch ProfileConsumer) {
	globalCPUProfiler.register(ch)
}

// Unregister unregister a ProfileConsumer from the global CPU profiler.
// The unregistered ProfileConsumer won't receive the cpu profile data any more.
// This function is thread-safe.
func Unregister(ch ProfileConsumer) {
	globalCPUProfiler.unregister(ch)
}

// ConsumersCount returns the count of the global ParallelCPUProfiler's consumer.It is exporting for test.
func ConsumersCount() int {
	return globalCPUProfiler.consumersCount()
}

// ParallelCPUProfiler is a cpu profiler.
// With ParallelCPUProfiler, it is possible to have multiple profile consumer at the same time.
// WARN: Only one running ParallelCPUProfiler is allowed in the process, otherwise some profiler may profiling fail.
type ParallelCPUProfiler struct {
	sync.Mutex
	cs     map[ProfileConsumer]struct{}
	notify chan struct{}

	// some data cache for profiling.
	data         *ProfileData
	nextData     *ProfileData
	lastDataSize int

	started  atomic.Bool
	closed   chan struct{}
	isClosed atomic.Bool
	wg       sync.WaitGroup
}

// newParallelCPUProfiler crate a new ParallelCPUProfiler.
func newParallelCPUProfiler() *ParallelCPUProfiler {
	return &ParallelCPUProfiler{
		cs:     make(map[ProfileConsumer]struct{}),
		notify: make(chan struct{}),
		closed: make(chan struct{}),
	}
}

var (
	errProfilerAlreadyStarted = errors.New("ParallelCPUProfiler already started")
	errProfilerAlreadyClosed  = errors.New("ParallelCPUProfiler already closed")
)

func (p *ParallelCPUProfiler) start() error {
	if !p.started.CAS(false, true) {
		return errProfilerAlreadyStarted
	}
	if p.isClosed.Load() {
		return errProfilerAlreadyClosed
	}

	logutil.BgLogger().Info("parallel cpu profiler started")
	p.wg.Add(1)
	go util.WithRecovery(p.profilingLoop, nil)
	return nil
}

func (p *ParallelCPUProfiler) close() {
	if p.isClosed.CAS(false, true) {
		close(p.closed)
	}
	p.wg.Wait()
}

func (p *ParallelCPUProfiler) register(ch ProfileConsumer) {
	if ch == nil {
		return
	}
	p.Lock()
	p.cs[ch] = struct{}{}
	p.Unlock()
	// notify
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

func (p *ParallelCPUProfiler) unregister(ch ProfileConsumer) {
	if ch == nil {
		return
	}
	p.Lock()
	delete(p.cs, ch)
	p.Unlock()
}

func (p *ParallelCPUProfiler) profilingLoop() {
	checkTicker := time.NewTicker(defProfileDuration)
	defer func() {
		checkTicker.Stop()
		pprof.StopCPUProfile()
		p.wg.Done()
	}()
	for {
		select {
		case <-p.closed:
			return
		case <-p.notify:
			if !p.inProfilingStatus() {
				p.doProfiling()
			}
		case <-checkTicker.C:
			p.doProfiling()
		}
	}
}

func (p *ParallelCPUProfiler) doProfiling() {
	if p.inProfilingStatus() {
		p.stopCPUProfile()
		p.sendToConsumers()
	}

	// Only do cpu profiling when there are consumers.
	if p.needProfile() {
		err := p.startCPUProfile()
		if err != nil {
			p.data.Error = err
			// notify error as soon as possible
			p.sendToConsumers()
		}
	}
}

func (p *ParallelCPUProfiler) needProfile() bool {
	return p.consumersCount() > 0
}

func (p *ParallelCPUProfiler) inProfilingStatus() bool {
	return p.data != nil
}

func (p *ParallelCPUProfiler) startCPUProfile() error {
	if p.nextData != nil {
		p.data, p.nextData = p.nextData, nil
	} else {
		p.data = &ProfileData{Data: bytes.NewBuffer(make([]byte, 0, 1024*8)), Begin: time.Now()}
	}
	metrics.CPUProfileCounter.Inc()
	return pprof.StartCPUProfile(p.data.Data)
}

func (p *ParallelCPUProfiler) stopCPUProfile() {
	now := time.Now()
	p.data.End = now

	// create next profile data before stop profiling, to reduce profiling interval.
	if p.lastDataSize != 0 {
		p.nextData = &ProfileData{Data: bytes.NewBuffer(make([]byte, 0, p.lastDataSize)), Begin: now}
	}

	pprof.StopCPUProfile()
}

func (p *ParallelCPUProfiler) consumersCount() int {
	p.Lock()
	n := len(p.cs)
	p.Unlock()
	return n
}

func (p *ParallelCPUProfiler) sendToConsumers() {
	p.Lock()
	for c := range p.cs {
		select {
		case c <- p.data:
		default:
			// ignore
		}
	}
	p.Unlock()
	p.lastDataSize = p.data.Data.Len()
	p.data = nil
}
