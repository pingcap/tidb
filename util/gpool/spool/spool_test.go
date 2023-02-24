// Copyright 2023 PingCAP, Inc.
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

package spool

import (
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/stretchr/testify/require"
)

func TestSPool(t *testing.T) {
	var wg sync.WaitGroup
	p, err := NewPool("TestAntsPoolWaitToGetWorker", PoolCap, util.UNKNOWN)
	require.NoError(t, err)
	defer p.ReleaseAndWait()

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		_ = p.Run(func() {
			demoPoolFunc(100)
			wg.Done()
		})
	}
	wg.Wait()
}

func TestReleaseWhenRunningPool(t *testing.T) {
	var wg sync.WaitGroup
	p, err := NewPool("TestReleaseWhenRunningPool", 1, util.UNKNOWN)
	require.NoError(t, err)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			j := i
			_ = p.Run(func() {
				time.Sleep(1 * time.Second)
			})
		}
	}()

	go func() {
		t.Log("start bbb")
		defer wg.Done()
		for i := 100; i < 130; i++ {
			j := i
			_ = p.Run(func() {
				t.Log("do task", j)
				time.Sleep(1 * time.Second)
			})
		}
	}()

	time.Sleep(3 * time.Second)
	p.ReleaseAndWait()
	t.Log("wait for all goroutines to exit...")
	wg.Wait()
}

func TestReleaseWhenRunningPool(t *testing.T) {

}
