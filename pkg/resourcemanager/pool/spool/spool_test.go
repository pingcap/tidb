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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/resourcemanager/pool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/stretchr/testify/require"
)

func demoFunc() {
	f(2)
}

func f(n int) {
	if n == 0 {
		return
	}
	var useStack [100]byte
	_ = useStack[3]
	f(n - 1)
}

func TestReleaseWhenRunningPool(t *testing.T) {
	var wg sync.WaitGroup
	p, err := NewPool("TestReleaseWhenRunningPool", 1, util.UNKNOWN)
	require.NoError(t, err)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			_ = p.Run(func() {
				time.Sleep(100 * time.Microsecond)
			})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 100; i < 130; i++ {
			_ = p.Run(func() {
				time.Sleep(100 * time.Microsecond)
			})
		}
	}()
	time.Sleep(100 * time.Microsecond)
	p.ReleaseAndWait()
	wg.Wait()
}

func TestPoolTuneScaleUpAndDown(t *testing.T) {
	c := make(chan struct{})
	p, _ := NewPool("TestPoolTuneScaleUp", 2, util.UNKNOWN, WithBlocking(true))
	for i := 0; i < 2; i++ {
		_ = p.Run(func() {
			<-c
		})
	}
	require.Equal(t, int32(2), p.Running())
	// test pool tune scale up one
	p.Tune(3)
	_ = p.Run(func() {
		<-c
	})
	require.Equal(t, int32(3), p.Running())

	// test pool tune scale up multiple
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.Run(func() {
				<-c
			})
		}()
	}
	p.Tune(8)
	wg.Wait()
	require.Eventually(t, func() bool { return p.Running() == 8 }, 1*time.Second, 200*time.Millisecond)
	// test pool tune scale down
	p.Tune(2)
	for i := 0; i < 6; i++ {
		c <- struct{}{}
	}
	require.Eventually(t, func() bool { return p.Running() == 2 }, 1*time.Second, 200*time.Millisecond)
	for i := 0; i < 2; i++ {
		c <- struct{}{}
	}
	require.Eventually(t, func() bool { return p.Running() == 0 }, 1*time.Second, 200*time.Millisecond)

	// test with RunWithConcurrency
	var cnt atomic.Int32
	workerFn := func() {
		cnt.Add(1)
	}
	fnChan := make(chan func(), 10)
	wg.Wait()
	err := p.RunWithConcurrency(fnChan, 2)
	require.NoError(t, err)
	require.Equal(t, int32(2), p.Running())
	for i := 0; i < 10; i++ {
		fnChan <- workerFn
	}
	require.Eventually(t, func() bool { return cnt.Load() == 10 }, 1*time.Second, 200*time.Millisecond)
	require.Equal(t, int32(2), p.Running())
	close(fnChan)
	require.Eventually(t, func() bool { return p.Running() == 0 }, 1*time.Second, 200*time.Millisecond)
	p.ReleaseAndWait()
}

func TestRunOverload(t *testing.T) {
	var stop atomic.Bool
	longRunningFunc := func() {
		for {
			if stop.Load() {
				break
			}
			runtime.Gosched()
		}
	}
	poolSize := 10
	p, err := NewPool("TestMaxBlockingSubmit", int32(poolSize), util.UNKNOWN, WithBlocking(false))
	require.NoErrorf(t, err, "create TimingPool failed: %v", err)
	defer p.ReleaseAndWait()
	defer stop.Store(true)
	for i := 0; i < poolSize-1; i++ {
		require.NoError(t, p.Run(longRunningFunc), "submit when pool is not full shouldn't return error")
	}
	// p is full now.
	require.NoError(t, p.Run(longRunningFunc), "submit when pool is not full shouldn't return error")
	require.EqualError(t, p.Run(demoFunc), pool.ErrPoolOverload.Error(),
		"blocking submit when pool reach max blocking submit should return ErrPoolOverload")
}

func TestRunWithNotEnough(t *testing.T) {
	var stop atomic.Bool
	fnChan := make(chan func(), 10)
	poolSize := 10
	p, err := NewPool("TestRunWithNotEnough", int32(poolSize), util.UNKNOWN, WithBlocking(false))
	require.NoErrorf(t, err, "create TimingPool failed: %v", err)
	defer p.ReleaseAndWait()
	defer stop.Store(true)
	require.NoError(t, p.RunWithConcurrency(fnChan, uint32(poolSize+100)), "submit when pool is not full shouldn't return error")
	require.Equal(t, int32(10), p.Running())
	require.Error(t, p.RunWithConcurrency(fnChan, 1))
	require.Error(t, p.Run(func() {}))
	close(fnChan)
	time.Sleep(1 * time.Second)
	require.Equal(t, int32(0), p.Running())
}

func TestRunWithNotEnough2(t *testing.T) {
	fnChan := make(chan func(), 10)
	var cnt atomic.Int32
	fn := func() {
		cnt.Add(1)
	}
	p, err := NewPool("TestRunWithNotEnough2", int32(1), util.UNKNOWN, WithBlocking(false))
	require.NoErrorf(t, err, "create TimingPool failed: %v", err)
	defer p.ReleaseAndWait()
	require.NoError(t, p.RunWithConcurrency(fnChan, 2), "submit when pool is not full shouldn't return error")
	require.Equal(t, int32(1), p.Running())
	require.Error(t, p.RunWithConcurrency(fnChan, 1))
	require.Error(t, p.Run(func() {}))
	for i := 0; i < 100; i++ {
		fnChan <- fn
	}
	close(fnChan)
	time.Sleep(100 * time.Microsecond)
	require.Equal(t, int32(0), p.Running())
	require.Equal(t, int32(100), cnt.Load())
}

func TestWithTaskManager(t *testing.T) {
	p, err := NewPool("TestWithTaskManager", int32(1), util.UNKNOWN, WithBlocking(false))
	require.NoError(t, err)
	defer p.ReleaseAndWait()
	fnChan := make(chan func(), 10)
	require.NoError(t, p.RunWithConcurrency(fnChan, 2), "submit when pool is not full shouldn't return error")
	time.Sleep(100 * time.Microsecond)
	require.Equal(t, int32(1), p.Running())

	// increase the concurrency
	p.Tune(2)
	time.Sleep(100 * time.Microsecond)
	require.Equal(t, int32(2), p.Running())
	p.Tune(3)
	require.Eventually(t, func() bool { return p.Running() == 3 }, 1*time.Second, 200*time.Millisecond)

	// decrease the concurrency
	p.Tune(2)
	require.Eventually(t, func() bool { return p.Running() == 2 }, 1*time.Second, 200*time.Millisecond)
	p.Tune(1)
	require.Eventually(t, func() bool { return p.Running() == 1 }, 1*time.Second, 200*time.Millisecond)
	close(fnChan)
}
