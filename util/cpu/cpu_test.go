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

package cpu

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
)

func TestCPUValue(t *testing.T) {
	observer := NewCPUObserver()
	exit := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-exit:
					return
				default:
					runtime.Gosched()
				}
			}
		}()
	}
	observer.Start()
	for n := 0; n < 10; n++ {
		time.Sleep(1 * time.Second)
		value, unsupported := GetCPUUsage()
		require.False(t, unsupported)
		require.Greater(t, value, 0.0)
		require.Less(t, value, 1.0)
	}
	observer.Stop()
	close(exit)
	wg.Wait()
}

func TestFailpointCPUValue(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/util/cgroup/GetCgroupCPUErr", "return(true)")
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/util/cgroup/GetCgroupCPUErr")
	}()
	observer := NewCPUObserver()
	exit := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-exit:
					return
				default:
					runtime.Gosched()
				}
			}
		}()
	}
	observer.Start()
	for n := 0; n < 10; n++ {
		time.Sleep(1 * time.Second)
		value, unsupported := GetCPUUsage()
		require.True(t, unsupported)
		require.Equal(t, value, 0.0)
	}
	// we do not stop the observer, because we inject the fail-point and the observer will not start.
	// if this test case happen goleak, it must have a bug.
	close(exit)
	wg.Wait()
}
