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
	"testing"
	"time"

	"github.com/pingcap/tidb/resourcemanager/pooltask"
	rmutil "github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/gpool"
)

const (
	RunTimes           = 10000
	DefaultExpiredTime = 10 * time.Second
)

func BenchmarkGPool(b *testing.B) {
	p, err := NewSPMCPool[struct{}, struct{}, int, any, pooltask.NilContext]("test", 10, rmutil.UNKNOWN)
	if err != nil {
		b.Fatal(err)
	}
	defer p.ReleaseAndWait()
	p.SetConsumerFunc(func(a struct{}, b int, c any) struct{} {
		return struct{}{}
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sema := make(chan struct{}, 10)
		var wg util.WaitGroupWrapper
		wg.Run(func() {
			for j := 0; j < RunTimes; j++ {
				sema <- struct{}{}
			}
			close(sema)
		})
		producerFunc := func() (struct{}, error) {
			_, ok := <-sema
			if ok {
				return struct{}{}, nil
			}
			return struct{}{}, gpool.ErrProducerClosed
		}
		resultCh, ctl := p.AddProducer(producerFunc, RunTimes, pooltask.NilContext{}, WithConcurrency(6), WithResultChanLen(10))
		exitCh := make(chan struct{})
		wg.Run(func() {
			for {
				select {
				case <-resultCh:
				case <-exitCh:
					return
				}
			}
		})
		ctl.Wait()
		close(exitCh)
		wg.Wait()
	}
}

func BenchmarkGoCommon(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg util.WaitGroupWrapper
		var wgp util.WaitGroupWrapper
		sema := make(chan struct{}, 10)
		result := make(chan struct{}, 10)
		wg.Run(func() {
			for j := 0; j < RunTimes; j++ {
				sema <- struct{}{}
			}
			close(sema)
		})

		for n := 0; n < 6; n++ {
			wg.Run(func() {
				item, ok := <-sema
				if !ok {
					return
				}
				result <- item
			})
		}
		exitCh := make(chan struct{})
		wgp.Run(func() {
			for {
				select {
				case <-result:
				case <-exitCh:
					return
				}
			}
		})
		wg.Wait()
		close(exitCh)
		wgp.Wait()
	}
}
