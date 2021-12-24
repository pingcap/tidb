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
	"context"
	"errors"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
)

var defProfileDuration = time.Second

// globalCPUProfiler is the global CPU profiler.
var globalCPUProfiler = newParallelCPUProfiler()

// ProfileConsumer is a channel that will receive profiling data from the CPU profiler periodically.
// If the channel is full, then the channel won't receive the latest profile data until it is not blocked.
type ProfileConsumer = chan *ProfileData

// ProfileData contains the cpu profile data between the start and end time, usually the interval between start and end is about 1 second.
type ProfileData struct {
	Data  *bytes.Buffer
	Begin time.Time
	End   time.Time
	Error error
}

// StartCPUProfiler uses to start to run the global parallelCPUProfiler.
func StartCPUProfiler() error {
	return globalCPUProfiler.start()
}

// StopCPUProfiler uses to stop the global parallelCPUProfiler.
func StopCPUProfiler() {
	globalCPUProfiler.stop()
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

// parallelCPUProfiler is a cpu profiler.
// With parallelCPUProfiler, it is possible to have multiple profile consumer at the same time.
// WARN: Only one running parallelCPUProfiler is allowed in the process, otherwise some profiler may profiling fail.
type parallelCPUProfiler struct {
	sync.Mutex
	cs             map[ProfileConsumer]struct{}
	notifyRegister chan struct{}

	// some data cache for profiling.
	data         *ProfileData
	nextData     *ProfileData
	lastDataSize int

	started bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// newParallelCPUProfiler crate a new parallelCPUProfiler.
func newParallelCPUProfiler() *parallelCPUProfiler {
	return &parallelCPUProfiler{
		cs:             make(map[ProfileConsumer]struct{}),
		notifyRegister: make(chan struct{}),
	}
}

var (
	errProfilerAlreadyStarted = errors.New("parallelCPUProfiler is already started")
)

func (p *parallelCPUProfiler) start() error {
	p.Lock()
	defer p.Unlock()
	if p.started {
		return errProfilerAlreadyStarted
	}

	p.started = true
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.wg.Add(1)
	go util.WithRecovery(p.profilingLoop, nil)

	logutil.BgLogger().Info("parallel cpu profiler started")
	return nil
}

func (p *parallelCPUProfiler) stop() {
	p.Lock()
	defer p.Unlock()
	if !p.started {
		return
	}
	p.started = false
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	logutil.BgLogger().Info("parallel cpu profiler stopped")
}

func (p *parallelCPUProfiler) register(ch ProfileConsumer) {
	if ch == nil {
		return
	}
	p.Lock()
	p.cs[ch] = struct{}{}
	p.Unlock()

	select {
	case p.notifyRegister <- struct{}{}:
	default:
	}
}

func (p *parallelCPUProfiler) unregister(ch ProfileConsumer) {
	if ch == nil {
		return
	}
	p.Lock()
	delete(p.cs, ch)
	p.Unlock()
}

func (p *parallelCPUProfiler) profilingLoop() {
	checkTicker := time.NewTicker(defProfileDuration)
	defer func() {
		checkTicker.Stop()
		pprof.StopCPUProfile()
		p.wg.Done()
	}()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.notifyRegister:
			if !p.inProfilingStatus() {
				p.profileCycle()
			}
		case <-checkTicker.C:
			p.profileCycle()
		}
	}
}

func (p *parallelCPUProfiler) profileCycle() {
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

func (p *parallelCPUProfiler) needProfile() bool {
	return p.consumersCount() > 0
}

func (p *parallelCPUProfiler) inProfilingStatus() bool {
	return p.data != nil
}

func (p *parallelCPUProfiler) startCPUProfile() error {
	if p.nextData != nil {
		p.data, p.nextData = p.nextData, nil
	} else {
		p.data = &ProfileData{Data: bytes.NewBuffer(make([]byte, 0, 1024*8)), Begin: time.Now()}
	}
	metrics.CPUProfileCounter.Inc()
	return pprof.StartCPUProfile(p.data.Data)
}

func (p *parallelCPUProfiler) stopCPUProfile() {
	now := time.Now()
	p.data.End = now

	// create next profile data before stop profiling, to reduce profiling interval.
	if p.lastDataSize != 0 {
		p.nextData = &ProfileData{Data: bytes.NewBuffer(make([]byte, 0, p.lastDataSize)), Begin: now}
	}

	pprof.StopCPUProfile()
}

func (p *parallelCPUProfiler) consumersCount() int {
	p.Lock()
	n := len(p.cs)
	p.Unlock()
	return n
}

func (p *parallelCPUProfiler) sendToConsumers() {
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
