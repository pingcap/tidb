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

package gocommon

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/util"
)

const (
	RunTimes           = 10000
	DefaultExpiredTime = 10 * time.Second
)

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
				for item := range sema {
					result <- item
				}
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
