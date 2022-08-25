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

package spmc

import (
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
)

const (
	RunTimes           = 1000000
	BenchParam         = 10
	BenchAntsSize      = 200000
	DefaultExpiredTime = 10 * time.Second
)

func demoFunc() {
	time.Sleep(time.Duration(BenchParam) * time.Millisecond)
}

func BenchmarkSemaphore(b *testing.B) {
	var wg sync.WaitGroup
	sema := make(chan struct{}, BenchAntsSize)

	for i := 0; i < b.N; i++ {
		wg.Add(RunTimes)
		for j := 0; j < RunTimes; j++ {
			sema <- struct{}{}
			go func() {
				demoFunc()
				<-sema
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func BenchmarkAntsPool(b *testing.B) {
	p := NewSPMCPool[struct{}, struct{}, int](10, WithExpiryDuration(DefaultExpiredTime))
	defer p.ReleaseAndWait()
	p.SetConsumerFunc(func(a struct{}, b int) struct{} {
		return struct{}{}
	})

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sema := make(chan struct{}, 10)
		for j := 0; j < RunTimes; j++ {
			sema <- struct{}{}
		}
		producerFunc := func() (struct{}, error) {
			select {
			case <-sema:
				return struct{}{}, nil
			default:
				return struct{}{}, errors.New("not job")
			}
		}
		_, ctl := p.AddProducer(producerFunc, RunTimes, 6)
		ctl.Wait()
	}
	b.StopTimer()
}
