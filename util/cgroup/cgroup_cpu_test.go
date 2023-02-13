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

//go:build linux

package cgroup

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetCgroupCPU(t *testing.T) {
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
	require.True(t, InContainer())
	cpu, err := GetCgroupCPU()
	require.NoError(t, err)
	require.NotZero(t, cpu.Period)
	require.Less(t, int64(1), cpu.Period)
	close(exit)
	wg.Wait()
}
