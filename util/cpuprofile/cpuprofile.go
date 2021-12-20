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
	"runtime/pprof"
	"sync"
	"time"

	"github.com/pingcap/tidb/util"
	"go.uber.org/atomic"
)

var GlobalCPUProfiler = NewParallelCPUProfiler()

func Register(ch ProfileConsumer) {
	GlobalCPUProfiler.register(ch)
}

func Unregister(ch ProfileConsumer) {
	GlobalCPUProfiler.unregister(ch)
}

type ProfileConsumer chan *ProfileData

type ParallelCPUProfiler struct {
	sync.Mutex
	cs       map[ProfileConsumer]struct{}
	closed   chan struct{}
	isClosed atomic.Bool
	wg       sync.WaitGroup

	data         *ProfileData
	nextData     *ProfileData
	lastDataSize int
}

type ProfileData struct {
	Data  *bytes.Buffer
	Begin time.Time
	End   time.Time
	Error error
}

func NewParallelCPUProfiler() *ParallelCPUProfiler {
	return &ParallelCPUProfiler{
		cs:     make(map[ProfileConsumer]struct{}),
		closed: make(chan struct{}),
	}
}

func (p *ParallelCPUProfiler) register(ch ProfileConsumer) {
	if ch == nil {
		return
	}
	p.Lock()
	p.cs[ch] = struct{}{}
	p.Unlock()
}

func (p *ParallelCPUProfiler) unregister(ch ProfileConsumer) {
	if ch == nil {
		return
	}
	p.Lock()
	delete(p.cs, ch)
	p.Unlock()
}

func (p *ParallelCPUProfiler) Start() {
	p.wg.Add(1)
	go util.WithRecovery(p.profilingLoop, nil)
}

var defProfileDuration = time.Second

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
		case <-checkTicker.C:
			if p.inProfilingStatus() {
				p.stopCPUProfile()
				p.sendToConsumers()
			}

			if p.consumersCount() > 0 {
				err := p.startCPUProfile()
				if err != nil {
					p.data.Error = err
					// notify error as soon as possible
					p.sendToConsumers()
				}
			}
		}
	}
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

func (p *ParallelCPUProfiler) Stop() {
	if p.isClosed.CAS(false, true) {
		close(p.closed)
	}
	p.wg.Wait()
}
