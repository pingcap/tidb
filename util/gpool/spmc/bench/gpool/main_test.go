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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/gpool/spmc"
)

const (
	RunTimes = 10000
)

func BenchmarkGPool(b *testing.B) {
	p := spmc.NewSPMCPool[struct{}, struct{}, int, any, spmc.NilContext](10)
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
			return struct{}{}, errors.New("not job")
		}
		resultCh, ctl := p.AddProducer(producerFunc, RunTimes, spmc.NilContext{}, spmc.WithConcurrency(6))
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
